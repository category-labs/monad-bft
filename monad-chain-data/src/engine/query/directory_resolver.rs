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

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::{
    engine::{
        primary_dir::{bucket_start, PrimaryDirBucket, PrimaryDirEntry, PrimaryDirFragment},
        tables::FamilyTables,
    },
    error::{MonadChainDataError, Result},
    primitives::state::PrimaryId,
    store::MetaStore,
};

/// Resolves a primary id to its block number and position within that block.
///
/// A bucket's representation is a pure function of the family id frontier:
/// every bucket whose entire 10k id range lies below the published-head
/// frontier is sealed and carries a compacted summary; at most one bucket —
/// the one holding the frontier — is open and resolves from fragments. The
/// resolver routes each lookup analytically on `sealed_below` rather than
/// probing the summary and scanning fragments on a miss. Sealed buckets take
/// a single summary `get`; only the open bucket scans.
///
/// Caches the chosen directory source for each 10k bucket so repeated id
/// lookups do not re-read summaries or fragments.
///
/// Safe to share across the concurrent page futures of the indexed pipeline:
/// `resolve` takes `&self` and guards the memo with a plain `Mutex` that is
/// held ONLY for the in-memory check/insert and NEVER across the
/// `load_bucket`/`load_bucket_fragments` `await` (mirroring the cache layer's
/// no-await-under-lock discipline). Two tasks racing on the same bucket each
/// fetch and the second insert wins harmlessly; concurrent `load_bucket` gets
/// for the same sealed bucket are already coalesced by the cache-layer
/// single-flight, so only the single open bucket's fragment scan can duplicate
/// — an idempotent read.
pub(crate) struct PrimaryIdResolver<'a, M: MetaStore> {
    family: &'a FamilyTables<M>,
    /// First bucket start that is *not* sealed — i.e. `bucket_start` of the
    /// family's frontier id at the publication head. Buckets strictly below
    /// this are sealed; the bucket at or above it is the single open bucket.
    sealed_below: u64,
    bucket_cache: Mutex<HashMap<u64, Arc<CachedBucket>>>,
}

impl<'a, M: MetaStore> PrimaryIdResolver<'a, M> {
    pub(crate) fn new(family: &'a FamilyTables<M>, sealed_below: u64) -> Self {
        Self {
            family,
            sealed_below,
            bucket_cache: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) async fn resolve(&self, id: PrimaryId) -> Result<Option<ResolvedPrimaryIdLocation>> {
        let bucket = bucket_start(id.as_u64());

        // Fast path: the bucket is already memoized. Lock, clone the resolved
        // source out, drop the lock before doing any work. Never await here.
        if let Some(cached) = self.lookup(bucket) {
            return cached.resolve(id);
        }

        // Miss: fetch the bucket's source WITHOUT holding the lock. A concurrent
        // task may fetch the same bucket; that is acceptable (sealed gets are
        // coalesced by the cache single-flight, the open bucket's fragment scan
        // is idempotent), and the insert below is last-write-wins.
        let cached = if bucket < self.sealed_below {
            // Guaranteed sealed: its summary was flushed in the same
            // `WriteSession` as the batch, before the publication CAS, so a
            // missing summary breaks the commit contract — surface it loudly
            // instead of falling back to a fragment scan.
            let summary = self.family.load_bucket(bucket).await?.ok_or(
                MonadChainDataError::SealedDirectoryBucketMissingSummary {
                    bucket_start: bucket,
                },
            )?;
            CachedBucket::Summary(summary)
        } else {
            // The single open/frontier bucket has no summary yet; resolve it
            // from the retained fragments.
            CachedBucket::Fragments(self.family.load_bucket_fragments(bucket).await?)
        };

        // Re-lock only to insert the memo entry and resolve against it.
        let mut guard = self.bucket_cache.lock().expect("resolver mutex poisoned");
        guard
            .entry(bucket)
            .or_insert_with(|| Arc::new(cached))
            .resolve(id)
    }

    /// Clones the memoized source handle for `bucket`, if present. The value is
    /// an `Arc`, so this bumps a refcount rather than deep-copying the bucket
    /// summary or fragment list; the lock is held only for the in-memory `get`
    /// and released before any `resolve` work.
    fn lookup(&self, bucket: u64) -> Option<Arc<CachedBucket>> {
        self.bucket_cache
            .lock()
            .expect("resolver mutex poisoned")
            .get(&bucket)
            .cloned()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ResolvedPrimaryIdLocation {
    pub(crate) block_number: u64,
    pub(crate) idx_in_block: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum CachedBucket {
    Summary(PrimaryDirBucket),
    Fragments(Vec<PrimaryDirFragment>),
}

impl CachedBucket {
    fn resolve(&self, id: PrimaryId) -> Result<Option<ResolvedPrimaryIdLocation>> {
        match self {
            Self::Summary(bucket) => resolved_location_from_bucket(bucket, id)?
                .ok_or(MonadChainDataError::Decode(
                    "compacted primary directory bucket missing queried id",
                ))
                .map(Some),
            Self::Fragments(fragments) => resolved_location_from_fragments(fragments, id)?
                .ok_or(MonadChainDataError::Decode(
                    "primary directory fragments missing queried id",
                ))
                .map(Some),
        }
    }
}

fn resolved_location_from_bucket(
    bucket: &PrimaryDirBucket,
    id: PrimaryId,
) -> Result<Option<ResolvedPrimaryIdLocation>> {
    let Some(entry) = containing_bucket_entry(bucket, id.as_u64()) else {
        return Ok(None);
    };

    Ok(Some(ResolvedPrimaryIdLocation {
        block_number: entry.block_number,
        idx_in_block: id.idx_in_block(PrimaryId::new(entry.first_primary_id))?,
    }))
}

fn containing_bucket_entry(bucket: &PrimaryDirBucket, id: u64) -> Option<&PrimaryDirEntry> {
    // Entries are strictly increasing in `first_primary_id`; the entry holding
    // `id` is the last one whose first id is `<= id`, and `id` must fall below
    // the next entry's first id (or the bucket sentinel for the last entry).
    let upper = bucket
        .entries
        .partition_point(|entry| entry.first_primary_id <= id);
    if upper == 0 {
        return None;
    }

    let entry = &bucket.entries[upper - 1];
    let end = bucket
        .entries
        .get(upper)
        .map(|next| next.first_primary_id)
        .unwrap_or(bucket.end_primary_id_exclusive);
    (id < end).then_some(entry)
}

fn resolved_location_from_fragments(
    fragments: &[PrimaryDirFragment],
    id: PrimaryId,
) -> Result<Option<ResolvedPrimaryIdLocation>> {
    let Some(fragment) = fragments.iter().find(|fragment| {
        id.as_u64() >= fragment.first_primary_id && id.as_u64() < fragment.end_primary_id_exclusive
    }) else {
        return Ok(None);
    };

    Ok(Some(ResolvedPrimaryIdLocation {
        block_number: fragment.block_number,
        idx_in_block: id.idx_in_block(PrimaryId::new(fragment.first_primary_id))?,
    }))
}

#[cfg(test)]
mod tests {
    use super::{
        containing_bucket_entry, resolved_location_from_bucket, CachedBucket, PrimaryDirFragment,
        PrimaryIdResolver,
    };
    use crate::{
        engine::{
            family::Family,
            primary_dir::{PrimaryDirBucket, PrimaryDirEntry, DIRECTORY_BUCKET_SIZE},
            tables::Tables,
        },
        primitives::state::PrimaryId,
        store::{InMemoryBlobStore, InMemoryMetaStore},
    };

    fn tables() -> Tables<InMemoryMetaStore, InMemoryBlobStore> {
        Tables::new(InMemoryMetaStore::default(), InMemoryBlobStore::default())
    }

    #[test]
    fn bucket_lookup_routes_past_omitted_empty_blocks() {
        // Block 50 minted ids [1000, 1003), block 51 was empty (omitted), block 52
        // minted [1003, 1008). An id in block 52's range must resolve to block 52,
        // not to the omitted empty block between them.
        let bucket = PrimaryDirBucket {
            entries: vec![
                PrimaryDirEntry {
                    block_number: 50,
                    first_primary_id: 1000,
                },
                PrimaryDirEntry {
                    block_number: 52,
                    first_primary_id: 1003,
                },
            ],
            end_primary_id_exclusive: 1008,
        };

        assert_eq!(
            containing_bucket_entry(&bucket, 1006),
            Some(&PrimaryDirEntry {
                block_number: 52,
                first_primary_id: 1003,
            })
        );

        let location =
            resolved_location_from_bucket(&bucket, PrimaryId::new(1006)).expect("resolve bucket");
        let location = location.expect("contained");
        assert_eq!(location.block_number, 52);
        assert_eq!(location.idx_in_block, 3);
    }

    #[test]
    fn bucket_lookup_rejects_ids_past_the_sentinel() {
        let bucket = PrimaryDirBucket {
            entries: vec![
                PrimaryDirEntry {
                    block_number: 50,
                    first_primary_id: 1000,
                },
                PrimaryDirEntry {
                    block_number: 52,
                    first_primary_id: 1003,
                },
            ],
            end_primary_id_exclusive: 1008,
        };

        assert_eq!(containing_bucket_entry(&bucket, 1008), None);
    }

    #[test]
    fn fragment_lookup_errors_when_candidate_id_is_missing() {
        let error = CachedBucket::Fragments(vec![PrimaryDirFragment {
            block_number: 7,
            first_primary_id: 100,
            end_primary_id_exclusive: 103,
        }])
        .resolve(PrimaryId::new(104))
        .expect_err("missing fragment candidate should fail loud");

        assert_eq!(
            error.to_string(),
            "decode error: primary directory fragments missing queried id"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn sealed_bucket_resolves_from_summary_without_a_fragment_scan() {
        let tables = tables();
        let family = tables.family(Family::Log);

        // Bucket 0 is sealed: its summary is the only directory artifact. No
        // fragments are written, so a routing bug that scanned would resolve to
        // `None`/error instead of finding the id.
        tables
            .seed_dir_bucket(
                Family::Log,
                0,
                &PrimaryDirBucket {
                    entries: vec![
                        PrimaryDirEntry {
                            block_number: 7,
                            first_primary_id: 0,
                        },
                        PrimaryDirEntry {
                            block_number: 8,
                            first_primary_id: 3,
                        },
                    ],
                    end_primary_id_exclusive: 8,
                },
            )
            .await;

        // sealed_below = DIRECTORY_BUCKET_SIZE => bucket 0 is sealed.
        let resolver = PrimaryIdResolver::new(family, DIRECTORY_BUCKET_SIZE);
        let location = resolver
            .resolve(PrimaryId::new(5))
            .await
            .expect("resolve sealed id")
            .expect("id contained in summary");

        // Second entry: ids [3, 8) at start_block + 1.
        assert_eq!(location.block_number, 8);
        assert_eq!(location.idx_in_block, 2);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn sealed_bucket_missing_summary_is_a_hard_error_not_a_scan() {
        let tables = tables();
        let family = tables.family(Family::Log);

        // Fragments exist for bucket 0 but the summary does not. A correct
        // routing implementation must NOT fall back to these fragments for a
        // bucket it classified as sealed; it must surface the broken commit
        // contract loudly. (If it scanned, id 5 would resolve cleanly.)
        tables.seed_dir_fragment(Family::Log, 7, 0, 8).await;

        let resolver = PrimaryIdResolver::new(family, DIRECTORY_BUCKET_SIZE);
        let error = resolver
            .resolve(PrimaryId::new(5))
            .await
            .expect_err("sealed bucket without summary must fail loud");

        assert_eq!(
            error.to_string(),
            "sealed primary directory bucket 0 missing its compacted summary; \
             the ingestion/compaction commit contract is broken"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn open_bucket_resolves_from_fragments() {
        let tables = tables();
        let family = tables.family(Family::Log);

        // The frontier sits inside bucket 0, so bucket 0 is the single open
        // bucket and resolves from fragments — no summary is written.
        tables.seed_dir_fragment(Family::Log, 7, 0, 8).await;

        // sealed_below = 0 => no bucket is sealed; everything routes to scan.
        let resolver = PrimaryIdResolver::new(family, 0);
        let location = resolver
            .resolve(PrimaryId::new(5))
            .await
            .expect("resolve open id")
            .expect("id contained in fragments");

        assert_eq!(location.block_number, 7);
        assert_eq!(location.idx_in_block, 5);
    }
}
