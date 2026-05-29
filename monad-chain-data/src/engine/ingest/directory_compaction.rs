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

use itertools::Itertools;

use crate::{
    engine::{
        family::Family,
        ingest::ReadPlanningTimings,
        open_index::{OpenIndexes, OpenIndexesEviction},
        primary_dir::{bucket_start, PrimaryDirBucket, PrimaryDirFragment, DIRECTORY_BUCKET_SIZE},
        tables::FamilyTables,
    },
    error::{MonadChainDataError, Result},
    store::{BlobStore, MetaStore, WriteSession},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectoryCompactionPlan {
    pub buckets: Vec<(u64, PrimaryDirBucket)>,
    pub(crate) timings: ReadPlanningTimings,
}

impl DirectoryCompactionPlan {
    pub(crate) fn eviction(&self, family: Family) -> OpenIndexesEviction {
        OpenIndexesEviction {
            directory_buckets: self
                .buckets
                .iter()
                .map(|(bucket_start, _)| (family, *bucket_start))
                .collect(),
            ..OpenIndexesEviction::default()
        }
    }
}

impl<M: MetaStore, B: BlobStore> FamilyTables<M, B> {
    /// Reads every sealed bucket's fragments and folds them into compacted
    /// bucket summaries. Pure I/O — no writes — so callers can fan out across
    /// families with `try_join_all` before opening the Phase B meta batch.
    pub(crate) async fn plan_directory_compactions(
        &self,
        open_indexes: &OpenIndexes,
        ranges: &[(u64, u64)],
    ) -> Result<DirectoryCompactionPlan> {
        let mut buckets = Vec::new();
        let mut timings = ReadPlanningTimings::default();
        for &(from_next_primary_id, next_primary_id) in ranges {
            for sealed_bucket_start in sealed_ranges(from_next_primary_id, next_primary_id) {
                let index_start = std::time::Instant::now();
                let encoded_fragments =
                    open_indexes.directory_fragments(self.family(), sealed_bucket_start);
                timings.dir_index_ms = timings
                    .dir_index_ms
                    .saturating_add(index_start.elapsed().as_millis() as u64);
                timings.dir_index_us = timings
                    .dir_index_us
                    .saturating_add(index_start.elapsed().as_micros() as u64);
                timings.dir_fragment_count = timings
                    .dir_fragment_count
                    .saturating_add(encoded_fragments.len() as u64);
                let decode_start = std::time::Instant::now();
                let fragments = encoded_fragments
                    .iter()
                    .map(|bytes| PrimaryDirFragment::decode(bytes))
                    .collect::<Result<Vec<_>>>()?;
                timings.dir_decode_ms = timings
                    .dir_decode_ms
                    .saturating_add(decode_start.elapsed().as_millis() as u64);
                timings.dir_decode_us = timings
                    .dir_decode_us
                    .saturating_add(decode_start.elapsed().as_micros() as u64);
                let bucket = compact_bucket_from_fragments(&fragments)?;
                buckets.push((sealed_bucket_start, bucket));
            }
        }
        Ok(DirectoryCompactionPlan { buckets, timings })
    }

    pub fn stage_directory_compactions(
        &self,
        w: &mut WriteSession<'_, M, B>,
        plan: &DirectoryCompactionPlan,
    ) {
        for (bucket_start, bucket) in &plan.buckets {
            // Sealed fragments are intentionally retained after the compacted bucket
            // is written. Reads prefer the bucket via `load_bucket`, so the fragments
            // are unreferenced but kept available for replay/recovery. Eager deletion
            // is deferred to a backend that exposes safe delete semantics.
            self.dir().stage_bucket(w, *bucket_start, bucket);
        }
    }
}

fn compact_bucket_from_fragments(fragments: &[PrimaryDirFragment]) -> Result<PrimaryDirBucket> {
    let Some(first_fragment) = fragments.first() else {
        return Err(MonadChainDataError::MissingData(
            "missing sealed primary directory bucket fragments",
        ));
    };
    let last_fragment = fragments.last().expect("fragments.first() returned Some");
    validate_fragment(first_fragment)?;

    let start_block = first_fragment.block_number;
    let mut first_primary_ids = Vec::with_capacity(fragments.len() + 1);
    first_primary_ids.push(first_fragment.first_primary_id);

    for (previous, fragment) in fragments.iter().tuple_windows() {
        validate_fragment(fragment)?;

        if fragment.block_number != previous.block_number.saturating_add(1) {
            return Err(MonadChainDataError::Decode(
                "inconsistent primary directory bucket block sequence",
            ));
        }
        if previous.end_primary_id_exclusive != fragment.first_primary_id {
            return Err(MonadChainDataError::Decode(
                "inconsistent primary directory bucket primary-id sequence",
            ));
        }

        first_primary_ids.push(fragment.first_primary_id);
    }

    first_primary_ids.push(last_fragment.end_primary_id_exclusive);

    PrimaryDirBucket::new(start_block, first_primary_ids)
}

fn sealed_ranges(from_next_primary_id: u64, next_primary_id: u64) -> Vec<u64> {
    if next_primary_id <= from_next_primary_id {
        return Vec::new();
    }

    let mut current_bucket_start = bucket_start(from_next_primary_id);
    let mut out = Vec::new();
    loop {
        let bucket_end = current_bucket_start.saturating_add(DIRECTORY_BUCKET_SIZE);
        if bucket_end > next_primary_id {
            break;
        }
        if bucket_end > from_next_primary_id {
            out.push(current_bucket_start);
        }
        current_bucket_start = current_bucket_start.saturating_add(DIRECTORY_BUCKET_SIZE);
    }

    out
}

fn validate_fragment(fragment: &PrimaryDirFragment) -> Result<()> {
    if fragment.first_primary_id > fragment.end_primary_id_exclusive {
        return Err(MonadChainDataError::Decode(
            "primary directory fragment range inverted",
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        compact_bucket_from_fragments, sealed_ranges, PrimaryDirBucket, PrimaryDirFragment,
        DIRECTORY_BUCKET_SIZE,
    };

    #[test]
    fn compact_bucket_from_fragments_builds_summary_with_sentinel() {
        let bucket = compact_bucket_from_fragments(&[
            PrimaryDirFragment {
                block_number: 7,
                first_primary_id: 100,
                end_primary_id_exclusive: 103,
            },
            PrimaryDirFragment {
                block_number: 8,
                first_primary_id: 103,
                end_primary_id_exclusive: 103,
            },
            PrimaryDirFragment {
                block_number: 9,
                first_primary_id: 103,
                end_primary_id_exclusive: 108,
            },
        ])
        .expect("compact bucket");

        assert_eq!(
            bucket,
            PrimaryDirBucket {
                start_block: 7,
                first_primary_ids: vec![100, 103, 103, 108],
            }
        );
    }

    #[test]
    fn compact_bucket_from_fragments_rejects_noncontiguous_blocks() {
        let error = compact_bucket_from_fragments(&[
            PrimaryDirFragment {
                block_number: 7,
                first_primary_id: 100,
                end_primary_id_exclusive: 103,
            },
            PrimaryDirFragment {
                block_number: 9,
                first_primary_id: 103,
                end_primary_id_exclusive: 108,
            },
        ])
        .expect_err("detect gap");

        assert_eq!(
            error.to_string(),
            "decode error: inconsistent primary directory bucket block sequence"
        );
    }

    #[test]
    fn sealed_ranges_reports_each_crossed_bucket_boundary() {
        assert_eq!(
            sealed_ranges(DIRECTORY_BUCKET_SIZE - 2, DIRECTORY_BUCKET_SIZE * 2 + 3),
            vec![0, DIRECTORY_BUCKET_SIZE]
        );
    }
}
