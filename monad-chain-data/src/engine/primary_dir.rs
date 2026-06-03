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

use std::collections::BTreeMap;

use bytes::Bytes;

use crate::{
    error::{MonadChainDataError, Result},
    store::{
        blob::BlobStore, CachedKvTable, CachedScannableTable, MetaStore, MetaWriteOp, WriteSession,
    },
};

pub const DIRECTORY_BUCKET_SIZE: u64 = 10_000;

/// One stored block within a compacted bucket: the block number and the first
/// primary id it minted. Only blocks that actually mint ids (`count > 0`) get an
/// entry — empty blocks consume no ids and are omitted, so a bucket's encoded
/// size is bounded by the number of *id-producing* blocks it spans, not its full
/// block range. (Storing one entry per block, empty or not, is what let a sparse
/// 10k-id bucket spanning millions of blocks balloon past the backend's 16 MiB
/// write limit.)
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
    /// Constructs a bucket after enforcing its invariants. All producers — RLP
    /// decode and ingest-side compaction — funnel through here so the type's
    /// invariants live in one place: entries are strictly increasing in both
    /// `first_primary_id` and `block_number`, and the sentinel sits strictly
    /// above the last entry's first id.
    pub(crate) fn new(entries: Vec<PrimaryDirEntry>, end_primary_id_exclusive: u64) -> Result<Self> {
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
        alloy_rlp::decode_exact(bytes).map_err(|_| {
            crate::error::MonadChainDataError::Decode("invalid primary directory fragment rlp")
        })
    }
}

pub struct PrimaryDirTables<M: MetaStore> {
    fragments: CachedScannableTable<M>,
    buckets: CachedKvTable<M>,
}

impl<M: MetaStore> PrimaryDirTables<M> {
    pub fn new(fragments: CachedScannableTable<M>, buckets: CachedKvTable<M>) -> Self {
        Self { fragments, buckets }
    }

    pub(crate) fn fragments_cache(&self) -> &CachedScannableTable<M> {
        &self.fragments
    }

    pub(crate) fn buckets_cache(&self) -> &CachedKvTable<M> {
        &self.buckets
    }

    /// Loads the compacted summary for one sealed 10k bucket.
    pub async fn load_bucket(&self, bucket_start: u64) -> Result<Option<PrimaryDirBucket>> {
        let key = u64_key(bucket_start);
        let Some(bytes) = self.buckets.get(&key).await? else {
            return Ok(None);
        };

        Ok(Some(PrimaryDirBucket::decode(&bytes)?))
    }

    /// Loads all retained fragments for one 10k bucket.
    pub async fn load_bucket_fragments(
        &self,
        bucket_start: u64,
    ) -> Result<Vec<PrimaryDirFragment>> {
        let partition = u64_key(bucket_start);
        let page = self
            .fragments
            .list_prefix(&partition, &[], None, usize::MAX)
            .await?;
        let mut fragments = Vec::with_capacity(page.keys.len());

        for clustering in page.keys {
            let bytes = self.fragments.get(&partition, &clustering).await?.ok_or(
                crate::error::MonadChainDataError::MissingData(
                    "missing primary directory fragment",
                ),
            )?;
            fragments.push(PrimaryDirFragment::decode(&bytes)?);
        }

        Ok(fragments)
    }

    pub(crate) async fn list_bucket_fragments_for_rebuild(
        &self,
        bucket_start: u64,
        published_head: u64,
    ) -> Result<BTreeMap<u64, Bytes>> {
        let partition = u64_key(bucket_start);
        let page = self
            .fragments
            .list_prefix(&partition, &[], None, usize::MAX)
            .await?;
        let mut out = BTreeMap::new();
        for key in page.keys {
            let block = u64_from_key(&key)?;
            if block <= published_head {
                let bytes = self.fragments.get(&partition, &key).await?.ok_or(
                    crate::error::MonadChainDataError::MissingData(
                        "missing primary directory fragment",
                    ),
                )?;
                out.insert(block, bytes);
            }
        }
        Ok(out)
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

    /// Stages every per-bucket fragment write the block contributes. Mirrors
    /// [`Self::persist_block_fragment`] but pushes into a meta batch instead
    /// of issuing one write per bucket.
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

    pub fn stage_block_fragment_filtered<B, F>(
        &self,
        w: &mut WriteSession<'_, M, B>,
        block_number: u64,
        first_primary_id: u64,
        count: u32,
        mut should_write_bucket: F,
    ) where
        B: BlobStore,
        F: FnMut(u64) -> bool,
    {
        // Empty blocks (`count == 0`) mint no ids; a fragment for them resolves
        // nothing, so we omit it. Bucket compaction closes the id range across
        // omitted empty blocks via the next id-producing block (or the sentinel).
        if count == 0 {
            return;
        }

        let fragment = PrimaryDirFragment {
            block_number,
            first_primary_id,
            end_primary_id_exclusive: first_primary_id.saturating_add(u64::from(count)),
        };

        let encoded = Bytes::from(fragment.encode());
        let clustering = u64_key(block_number);
        let mut ops = Vec::new();
        for bucket_start in fragment_bucket_starts(first_primary_id, count) {
            if !should_write_bucket(bucket_start) {
                continue;
            }
            let partition = u64_key(bucket_start);
            ops.push(MetaWriteOp::ScanPut {
                table: self.fragments.table_id(),
                partition: partition.to_vec(),
                clustering: clustering.to_vec(),
                value: encoded.clone(),
            });
        }
        w.extend_meta_uncached(ops);
    }
}

pub fn bucket_start(primary_id: u64) -> u64 {
    aligned_u64_start(primary_id, DIRECTORY_BUCKET_SIZE)
}

pub(crate) fn fragment_bucket_starts(first_primary_id: u64, count: u32) -> Vec<u64> {
    if count == 0 {
        return vec![bucket_start(first_primary_id)];
    }

    let mut out = Vec::new();
    let mut current = bucket_start(first_primary_id);
    let last = bucket_start(
        first_primary_id
            .saturating_add(u64::from(count))
            .saturating_sub(1),
    );
    loop {
        out.push(current);
        if current == last {
            break;
        }
        current = current.saturating_add(DIRECTORY_BUCKET_SIZE);
    }
    out
}

fn aligned_u64_start(value: u64, alignment: u64) -> u64 {
    value - (value % alignment)
}

fn u64_key(value: u64) -> [u8; 8] {
    value.to_be_bytes()
}

fn u64_from_key(key: &[u8]) -> Result<u64> {
    let bytes: [u8; 8] = key
        .try_into()
        .map_err(|_| MonadChainDataError::Decode("invalid u64 clustering key"))?;
    Ok(u64::from_be_bytes(bytes))
}
