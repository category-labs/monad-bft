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

use monad_query_errors::{QueryError, Result};
use monad_query_primitives::records::PrimaryId;
use monad_query_store::MetaStore;

use crate::{
    primary_dir::{bucket_start, PrimaryDirBucket, PrimaryDirEntry, PrimaryDirFragment},
    tables::FamilyTables,
};

/// Resolves a primary id to its block number and position within that block.
///
/// Routes each lookup analytically on `sealed_below`: buckets below the
/// frontier are sealed and resolve from a compacted summary (single `get`);
/// only the one open bucket scans fragments. Memoizes the chosen source per
/// bucket.
///
/// Safe to share across concurrent page futures: the memo `Mutex` is held only
/// for the in-memory check/insert, never across an `await`. Races duplicate at
/// most an idempotent fetch (last insert wins).
pub(crate) struct PrimaryIdResolver<'a, M: MetaStore> {
    family: &'a FamilyTables<M>,
    /// First bucket start that is *not* sealed — `bucket_start` of the family's
    /// frontier id at the publication head.
    sealed_below: u64,
    /// The published head the query executes against; the open bucket's
    /// fragment fold is served (and tagged) through it. `u64::MAX` bypasses
    /// the shared fold and re-scans fragments fresh (head-agnostic tests).
    published_head: u64,
    bucket_cache: Mutex<HashMap<u64, Arc<CachedBucket>>>,
}

impl<'a, M: MetaStore> PrimaryIdResolver<'a, M> {
    pub(crate) fn new(family: &'a FamilyTables<M>, sealed_below: u64, published_head: u64) -> Self {
        Self {
            family,
            sealed_below,
            published_head,
            bucket_cache: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) async fn resolve(&self, id: PrimaryId) -> Result<ResolvedPrimaryIdLocation> {
        let bucket = bucket_start(id.as_u64());

        if let Some(cached) = self.lookup(bucket) {
            return cached.resolve(id);
        }

        // Miss: fetch WITHOUT holding the lock; a racing fetch of the same
        // bucket is harmless and the insert below is last-write-wins.
        let cached = if bucket < self.sealed_below {
            // A sealed bucket's summary was flushed before the head was
            // published, so a missing summary breaks the commit contract — fail loud
            // instead of falling back to a fragment scan.
            let summary = self.family.load_bucket(bucket).await?.ok_or(
                QueryError::SealedDirectoryBucketMissingSummary {
                    bucket_start: bucket,
                },
            )?;
            CachedBucket::Summary(summary)
        } else if self.published_head == u64::MAX {
            // The open/frontier bucket has no summary yet; resolve from fragments.
            CachedBucket::Fragments(Arc::new(self.family.load_bucket_fragments(bucket).await?))
        } else {
            // Open bucket on the production path: the cross-request
            // incremental fold (zero store reads while the head is unchanged).
            CachedBucket::Fragments(
                self.family
                    .load_open_bucket_fold(bucket, self.published_head)
                    .await?,
            )
        };

        let mut guard = self.bucket_cache.lock().expect("resolver mutex poisoned");
        guard
            .entry(bucket)
            .or_insert_with(|| Arc::new(cached))
            .resolve(id)
    }

    /// Clones the memoized `Arc` handle for `bucket`; the lock is released
    /// before any `resolve` work.
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
    Fragments(Arc<Vec<PrimaryDirFragment>>),
}

impl CachedBucket {
    fn resolve(&self, id: PrimaryId) -> Result<ResolvedPrimaryIdLocation> {
        // Both sources convert a missing id into a hard error: the id came
        // from a bitmap candidate, so the directory must contain it.
        let (located, backing) = match self {
            Self::Summary(bucket) => (
                resolved_location_from_bucket(bucket, id)?,
                "compacted bucket",
            ),
            Self::Fragments(fragments) => (
                resolved_location_from_fragments(fragments, id)?,
                "fragments",
            ),
        };
        located.ok_or(QueryError::PrimaryDirectoryMissingId {
            id: id.as_u64(),
            backing,
        })
    }
}

/// Builds a location from the directory entry holding `id` (the entry's block
/// and `id`'s offset within that block's id range).
fn location_in_block(
    block_number: u64,
    first_primary_id: u64,
    id: PrimaryId,
) -> Result<ResolvedPrimaryIdLocation> {
    Ok(ResolvedPrimaryIdLocation {
        block_number,
        idx_in_block: id.idx_in_block(PrimaryId::new(first_primary_id))?,
    })
}

fn resolved_location_from_bucket(
    bucket: &PrimaryDirBucket,
    id: PrimaryId,
) -> Result<Option<ResolvedPrimaryIdLocation>> {
    let Some(entry) = containing_bucket_entry(bucket, id.as_u64()) else {
        return Ok(None);
    };
    location_in_block(entry.block_number, entry.first_primary_id, id).map(Some)
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
    // Fragments are disjoint id ranges ascending in `first_primary_id`
    // (block-clustered rows, ids assigned in block order), so the candidate
    // is the last fragment starting at or below `id`.
    let upper = fragments.partition_point(|fragment| fragment.first_primary_id <= id.as_u64());
    let Some(fragment) = upper
        .checked_sub(1)
        .map(|idx| &fragments[idx])
        .filter(|fragment| id.as_u64() < fragment.end_primary_id_exclusive)
    else {
        return Ok(None);
    };
    location_in_block(fragment.block_number, fragment.first_primary_id, id).map(Some)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use monad_query_primitives::records::PrimaryId;
    use monad_query_store::{InMemoryBlobStore, InMemoryMetaStore};

    use super::{
        containing_bucket_entry, resolved_location_from_bucket, CachedBucket, PrimaryDirFragment,
        PrimaryIdResolver,
    };
    use crate::{
        family::Family,
        primary_dir::{PrimaryDirBucket, PrimaryDirEntry, DIRECTORY_BUCKET_SIZE},
        tables::Tables,
    };

    fn tables() -> Tables<InMemoryMetaStore, InMemoryBlobStore> {
        Tables::new(InMemoryMetaStore::default(), InMemoryBlobStore::default())
    }

    #[test]
    fn bucket_lookup_routes_past_omitted_empty_blocks() {
        // Block 50 minted [1000, 1003), block 51 empty (omitted), block 52 minted [1003, 1008).
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
        let error = CachedBucket::Fragments(Arc::new(vec![PrimaryDirFragment {
            block_number: 7,
            first_primary_id: 100,
            end_primary_id_exclusive: 103,
        }]))
        .resolve(PrimaryId::new(104))
        .expect_err("missing fragment candidate should fail loud");

        assert_eq!(
            error.to_string(),
            "primary directory fragments missing queried id 104; \
             the ingestion/compaction commit contract is broken"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn sealed_bucket_resolves_from_summary_without_a_fragment_scan() {
        let tables = tables();
        let family = tables.family(Family::Log);

        // Only the sealed-bucket summary exists; a routing bug that scanned fragments would miss.
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
        let resolver = PrimaryIdResolver::new(family, DIRECTORY_BUCKET_SIZE, u64::MAX);
        let location = resolver
            .resolve(PrimaryId::new(5))
            .await
            .expect("resolve sealed id");

        assert_eq!(location.block_number, 8);
        assert_eq!(location.idx_in_block, 2);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn sealed_bucket_missing_summary_is_a_hard_error_not_a_scan() {
        let tables = tables();
        let family = tables.family(Family::Log);

        // Fragments exist but the summary does not: a sealed bucket must fail loud
        // on the broken commit contract, never silently fall back to fragments.
        tables.seed_dir_fragment(Family::Log, 7, 0, 8).await;

        let resolver = PrimaryIdResolver::new(family, DIRECTORY_BUCKET_SIZE, u64::MAX);
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

        tables.seed_dir_fragment(Family::Log, 7, 0, 8).await;

        // sealed_below = 0 => no bucket is sealed; everything routes to scan.
        let resolver = PrimaryIdResolver::new(family, 0, u64::MAX);
        let location = resolver
            .resolve(PrimaryId::new(5))
            .await
            .expect("resolve open id");

        assert_eq!(location.block_number, 7);
        assert_eq!(location.idx_in_block, 5);
    }
}
