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

use crate::{
    engine::{
        primary_dir::{
            bucket_start, PrimaryDirBucket, PrimaryDirFragment, PrimaryDirTables,
            DIRECTORY_BUCKET_SIZE,
        },
        tables::FamilyTables,
    },
    error::{MonadChainDataError, Result},
    store::{BlobStore, MetaStore},
};

impl<M: MetaStore, B: BlobStore> FamilyTables<M, B> {
    /// Compacts every directory bucket sealed by the given ingest transition.
    pub async fn compact_newly_sealed_directory_buckets(
        &self,
        from_next_primary_id: u64,
        next_primary_id: u64,
    ) -> Result<()> {
        compact_newly_sealed_buckets(self.dir(), from_next_primary_id, next_primary_id).await
    }
}

async fn compact_newly_sealed_buckets<M: MetaStore>(
    dir: &PrimaryDirTables<M>,
    from_next_primary_id: u64,
    next_primary_id: u64,
) -> Result<()> {
    for sealed_bucket_start in sealed_ranges(from_next_primary_id, next_primary_id) {
        let bucket =
            compact_bucket_from_fragments(&dir.load_bucket_fragments(sealed_bucket_start).await?)?;
        // Sealed fragments are intentionally retained after the compacted bucket
        // is written. Reads prefer the bucket via `load_bucket`, so the fragments
        // are unreferenced but kept available for replay/recovery. Eager deletion
        // is deferred to a backend that exposes safe delete semantics.
        dir.put_bucket(sealed_bucket_start, &bucket).await?;
    }

    Ok(())
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

    for window in fragments.windows(2) {
        let [previous, fragment] = window else {
            unreachable!("windows(2) yields slices of length 2");
        };
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
