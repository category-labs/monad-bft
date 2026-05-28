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
    collections::{BTreeSet, HashMap},
    time::Instant,
};

use futures::{stream, StreamExt, TryStreamExt};

use crate::{
    engine::{
        primary_dir::{bucket_start, PrimaryDirBucket, PrimaryDirFragment},
        query::family_runner::elapsed_us,
        tables::FamilyTables,
    },
    error::{MonadChainDataError, Result},
    primitives::state::PrimaryId,
    store::{BlobStore, MetaStore},
};

const DIRECTORY_BUCKET_LOAD_CONCURRENCY: usize = 32;

/// Resolves a primary id to its block number and position within that block.
///
/// Caches the chosen directory source for each 10k bucket so repeated id
/// lookups do not re-read summaries or fragments.
pub(crate) struct PrimaryIdResolver<'a, M: MetaStore, B: BlobStore> {
    family: &'a FamilyTables<M, B>,
    bucket_cache: HashMap<u64, CachedBucket>,
}

impl<'a, M: MetaStore, B: BlobStore> PrimaryIdResolver<'a, M, B> {
    pub(crate) fn new(family: &'a FamilyTables<M, B>) -> Self {
        Self {
            family,
            bucket_cache: HashMap::new(),
        }
    }

    pub(crate) async fn resolve_many_ordered(
        &mut self,
        ids: &[PrimaryId],
    ) -> Result<ResolvedPrimaryIdBatch> {
        let missing_buckets: BTreeSet<_> = ids
            .iter()
            .map(|id| bucket_start(id.as_u64()))
            .filter(|bucket| !self.bucket_cache.contains_key(bucket))
            .collect();

        let family = self.family;
        let bucket_load_start = Instant::now();
        let loaded: Vec<_> = stream::iter(missing_buckets)
            .map(|bucket| async move {
                load_cached_bucket(family, bucket)
                    .await
                    .map(|cached| (bucket, cached))
            })
            .buffered(DIRECTORY_BUCKET_LOAD_CONCURRENCY)
            .try_collect()
            .await?;
        let bucket_load_us = elapsed_us(bucket_load_start);

        for (bucket, cached) in loaded {
            self.bucket_cache.insert(bucket, cached);
        }

        let resolve_start = Instant::now();
        let locations = ids
            .iter()
            .map(|id| self.resolve_from_cache(*id))
            .collect::<Result<Vec<_>>>()?;
        Ok(ResolvedPrimaryIdBatch {
            locations,
            bucket_load_us,
            resolve_us: elapsed_us(resolve_start),
        })
    }

    fn resolve_from_cache(&self, id: PrimaryId) -> Result<Option<ResolvedPrimaryIdLocation>> {
        let bucket = bucket_start(id.as_u64());
        let cached = self
            .bucket_cache
            .get(&bucket)
            .ok_or(MonadChainDataError::Decode(
                "missing cached primary directory bucket",
            ))?;
        cached.resolve(id)
    }
}

pub(crate) struct ResolvedPrimaryIdBatch {
    pub(crate) locations: Vec<Option<ResolvedPrimaryIdLocation>>,
    pub(crate) bucket_load_us: u64,
    pub(crate) resolve_us: u64,
}

async fn load_cached_bucket<M: MetaStore, B: BlobStore>(
    family: &FamilyTables<M, B>,
    bucket: u64,
) -> Result<CachedBucket> {
    if let Some(summary) = family.load_bucket(bucket).await? {
        Ok(CachedBucket::Summary(summary))
    } else {
        Ok(CachedBucket::Fragments(
            family.load_bucket_fragments(bucket).await?,
        ))
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
    let Some(entry_index) = containing_bucket_entry(bucket, id.as_u64()) else {
        return Ok(None);
    };

    Ok(Some(ResolvedPrimaryIdLocation {
        block_number: bucket.start_block.saturating_add(entry_index as u64),
        idx_in_block: id.idx_in_block(PrimaryId::new(bucket.first_primary_ids[entry_index]))?,
    }))
}

fn containing_bucket_entry(bucket: &PrimaryDirBucket, id: u64) -> Option<usize> {
    if bucket.first_primary_ids.len() < 2 {
        return None;
    }

    let upper = bucket
        .first_primary_ids
        .partition_point(|first_primary_id| *first_primary_id <= id);
    if upper == 0 || upper >= bucket.first_primary_ids.len() {
        return None;
    }

    let entry_index = upper - 1;
    let end = bucket.first_primary_ids[upper];
    (id < end).then_some(entry_index)
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
    };
    use crate::{engine::primary_dir::PrimaryDirBucket, primitives::state::PrimaryId};

    #[test]
    fn bucket_lookup_uses_last_duplicate_boundary() {
        let bucket = PrimaryDirBucket {
            start_block: 50,
            first_primary_ids: vec![1000, 1003, 1003, 1008],
        };

        assert_eq!(containing_bucket_entry(&bucket, 1006), Some(2));

        let location =
            resolved_location_from_bucket(&bucket, PrimaryId::new(1006)).expect("resolve bucket");
        let location = location.expect("contained");
        assert_eq!(location.block_number, 52);
        assert_eq!(location.idx_in_block, 3);
    }

    #[test]
    fn bucket_lookup_rejects_ids_past_the_sentinel() {
        let bucket = PrimaryDirBucket {
            start_block: 50,
            first_primary_ids: vec![1000, 1003, 1003, 1008],
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
}
