// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::sync::{Arc, Mutex};

use bytes::Bytes;

use crate::{
    engine::{bitmap::STREAM_PAGE_ID_SPAN, tables::scan_get_all},
    error::{MonadChainDataError, Result},
    store::{blob::BlobStore, CachedKvTable, CachedScannableKvTable, MetaStore, WriteSession},
};

/// Buckets are aligned to the bitmap page span so both indexes share one seal
/// frontier: ingest tracks a single `sealed_id` per family and the open
/// (unsealed) region is identical for pages and buckets.
pub const DIRECTORY_BUCKET_SIZE: u64 = STREAM_PAGE_ID_SPAN as u64;

/// One id-producing block within a compacted bucket. Blocks that mint no ids
/// are omitted, so a bucket's encoded size is bounded by its id-producing
/// blocks, not its full block span (sparse buckets stay under backend write
/// limits).
#[derive(Debug, Clone, Copy, PartialEq, Eq, alloy_rlp::RlpEncodable, alloy_rlp::RlpDecodable)]
pub struct PrimaryDirEntry {
    pub block_number: u64,
    pub first_primary_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, alloy_rlp::RlpEncodable, alloy_rlp::RlpDecodable)]
pub struct PrimaryDirBucket {
    /// Id-producing blocks in the bucket, ordered by `first_primary_id` (which is
    /// therefore strictly increasing, since each entry mints at least one id).
    pub entries: Vec<PrimaryDirEntry>,
    /// Exclusive id frontier after the bucket's last block — the upper bound that
    /// closes the final entry's id range.
    pub end_primary_id_exclusive: u64,
}

impl PrimaryDirBucket {
    /// Single constructor (RLP decode and compaction both funnel here)
    /// enforcing the invariants: entries strictly increasing in both
    /// `first_primary_id` and `block_number`; sentinel strictly above the last
    /// entry's first id.
    pub fn new(
        entries: Vec<PrimaryDirEntry>,
        end_primary_id_exclusive: u64,
    ) -> Result<Self> {
        if entries.windows(2).any(|window| {
            window[0].first_primary_id >= window[1].first_primary_id
                || window[0].block_number >= window[1].block_number
        }) {
            return Err(MonadChainDataError::Decode(
                "primary directory bucket entries must be strictly increasing",
            ));
        }
        if let Some(last) = entries.last() {
            if last.first_primary_id >= end_primary_id_exclusive {
                return Err(MonadChainDataError::Decode(
                    "primary directory bucket sentinel must exceed its last id",
                ));
            }
        }
        Ok(Self {
            entries,
            end_primary_id_exclusive,
        })
    }

    pub fn encode(&self) -> Vec<u8> {
        alloy_rlp::encode(self)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let raw: Self = alloy_rlp::decode_exact(bytes)
            .map_err(|_| MonadChainDataError::Decode("invalid primary directory bucket rlp"))?;
        Self::new(raw.entries, raw.end_primary_id_exclusive)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, alloy_rlp::RlpEncodable, alloy_rlp::RlpDecodable)]
pub struct PrimaryDirFragment {
    pub block_number: u64,
    pub first_primary_id: u64,
    pub end_primary_id_exclusive: u64,
}

impl PrimaryDirFragment {
    pub fn encode(&self) -> Vec<u8> {
        alloy_rlp::encode(self)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        alloy_rlp::decode_exact(bytes)
            .map_err(|_| MonadChainDataError::Decode("invalid primary directory fragment rlp"))
    }
}

/// Cache decoder for a sealed-bucket summary.
pub(crate) fn decode_bucket(bytes: Bytes) -> Result<PrimaryDirBucket> {
    PrimaryDirBucket::decode(&bytes)
}

/// Cache decoder for a per-bucket directory fragment.
pub(crate) fn decode_fragment(bytes: Bytes) -> Result<PrimaryDirFragment> {
    PrimaryDirFragment::decode(&bytes)
}

#[derive(Clone)]
pub struct PrimaryDirTables<M: MetaStore> {
    fragments: CachedScannableKvTable<M, PrimaryDirFragment>,
    buckets: CachedKvTable<M, PrimaryDirBucket>,
    /// Incremental fold of the one open bucket's fragments:
    /// `(bucket_start, folded_through_head, block-ordered fragments)`.
    /// Fragments key by block and heads publish only at flush boundaries, so
    /// every fragment at or below a published head is durably visible: a
    /// head-current fold is complete and serves with zero store reads, and a
    /// stale one extends with only the blocks flushed since. `Arc`-shared so
    /// table clones fold once per store, not once per handle.
    open_bucket_fold: Arc<Mutex<Option<(u64, u64, Arc<Vec<PrimaryDirFragment>>)>>>,
}

impl<M: MetaStore> PrimaryDirTables<M> {
    pub fn new(
        fragments: CachedScannableKvTable<M, PrimaryDirFragment>,
        buckets: CachedKvTable<M, PrimaryDirBucket>,
    ) -> Self {
        Self {
            fragments,
            buckets,
            open_bucket_fold: Arc::new(Mutex::new(None)),
        }
    }

    pub(crate) fn fragments_cache(&self) -> &CachedScannableKvTable<M, PrimaryDirFragment> {
        &self.fragments
    }

    pub(crate) fn buckets_cache(&self) -> &CachedKvTable<M, PrimaryDirBucket> {
        &self.buckets
    }

    /// Loads the compacted summary for one sealed bucket.
    pub async fn load_bucket(&self, bucket_start: u64) -> Result<Option<PrimaryDirBucket>> {
        let key = u64_key(bucket_start);
        self.buckets.get(&key).await
    }

    /// Loads all retained fragments for one bucket, block-ordered.
    pub async fn load_bucket_fragments(
        &self,
        bucket_start: u64,
    ) -> Result<Vec<PrimaryDirFragment>> {
        let partition = u64_key(bucket_start);
        scan_get_all(
            &self.fragments,
            &partition,
            "missing primary directory fragment",
        )
        .await
    }

    /// The open bucket's fragments folded through `published_head`, served
    /// from the shared incremental fold (see `open_bucket_fold`): a
    /// head-current fold costs zero store reads; a stale one extends with
    /// only the blocks flushed since (one keys-only scan plus cached point
    /// gets). Fragments flushed beyond the published head are left for a
    /// later fold. A different `bucket_start` (the previous bucket sealed)
    /// replaces the slot outright.
    pub async fn load_open_bucket_fold(
        &self,
        bucket_start: u64,
        published_head: u64,
    ) -> Result<Arc<Vec<PrimaryDirFragment>>> {
        let cached = self
            .open_bucket_fold
            .lock()
            .expect("open fold mutex poisoned")
            .clone();
        let cached = cached.filter(|(bucket, _, _)| *bucket == bucket_start);
        if let Some((_, folded_head, fragments)) = &cached {
            if *folded_head >= published_head {
                return Ok(Arc::clone(fragments));
            }
        }

        let folded_below = cached.as_ref().map(|(_, head, _)| *head).unwrap_or(0);
        let partition = u64_key(bucket_start);
        let new_keys: Vec<Vec<u8>> = self
            .fragments
            .scan_keys(&partition)
            .await?
            .into_iter()
            .filter(|key| {
                fragment_block(key)
                    .is_some_and(|block| block > folded_below && block <= published_head)
            })
            .collect();

        let mut fragments = cached
            .as_ref()
            .map(|(_, _, fragments)| fragments.as_ref().clone())
            .unwrap_or_default();
        let new_fragments: Vec<PrimaryDirFragment> =
            futures::future::try_join_all(new_keys.iter().map(|clustering| async {
                self.fragments.get(&partition, clustering).await?.ok_or(
                    MonadChainDataError::MissingData("missing primary directory fragment"),
                )
            }))
            .await?;
        // Clustering keys scan in block order and ids assign in block order,
        // so the extended vec stays sorted by `first_primary_id`.
        fragments.extend(new_fragments);
        let fragments = Arc::new(fragments);

        let mut slot = self
            .open_bucket_fold
            .lock()
            .expect("open fold mutex poisoned");
        // A racing fold may have advanced further; never regress the slot.
        match &*slot {
            Some((bucket, head, _)) if *bucket == bucket_start && *head >= published_head => {}
            _ => *slot = Some((bucket_start, published_head, Arc::clone(&fragments))),
        }
        Ok(fragments)
    }

    pub fn stage_bucket<B: BlobStore>(
        &self,
        w: &mut WriteSession<'_, M, B>,
        bucket_start: u64,
        bucket: &PrimaryDirBucket,
    ) {
        let key = u64_key(bucket_start);
        w.put(&self.buckets, &key, Bytes::from(bucket.encode()));
    }

    /// Stages every per-bucket fragment write the block contributes.
    ///
    /// Ingest never stages a `count == 0` fragment: empty blocks mint no ids
    /// (`if count > 0` in `ingest/index.rs`), and compaction closes the id
    /// range across them via the next id-producing block (or the sentinel).
    pub fn stage_block_fragment<B: BlobStore>(
        &self,
        w: &mut WriteSession<'_, M, B>,
        block_number: u64,
        first_primary_id: u64,
        count: u32,
    ) {
        let fragment = PrimaryDirFragment {
            block_number,
            first_primary_id,
            end_primary_id_exclusive: first_primary_id.saturating_add(u64::from(count)),
        };

        let encoded = Bytes::from(fragment.encode());
        for bucket_start in fragment_bucket_starts(first_primary_id, count) {
            let partition = u64_key(bucket_start);
            let clustering = u64_key(block_number);
            w.scan_put(&self.fragments, &partition, &clustering, encoded.clone());
        }
    }
}

pub fn bucket_start(primary_id: u64) -> u64 {
    primary_id - (primary_id % DIRECTORY_BUCKET_SIZE)
}

pub(crate) fn fragment_bucket_starts(first_primary_id: u64, count: u32) -> Vec<u64> {
    // Ingest never stages a zero-count fragment (empty blocks mint no ids).
    debug_assert!(count > 0, "fragment must cover at least one id");

    let first = bucket_start(first_primary_id);
    let last = bucket_start(
        first_primary_id
            .saturating_add(u64::from(count))
            .saturating_sub(1),
    );
    (first..=last)
        .step_by(DIRECTORY_BUCKET_SIZE as usize)
        .collect()
}

fn u64_key(value: u64) -> [u8; 8] {
    value.to_be_bytes()
}

/// Decodes a fragment clustering key back to its block number (see
/// [`PrimaryDirTables::stage_block_fragment`]); `None` for malformed keys.
fn fragment_block(key: &[u8]) -> Option<u64> {
    key.try_into().ok().map(u64::from_be_bytes)
}
