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
        family::Family,
        open_index::{OpenIndexes, OpenIndexesEviction},
        primary_dir::{
            bucket_start, PrimaryDirBucket, PrimaryDirEntry, PrimaryDirFragment,
            DIRECTORY_BUCKET_SIZE,
        },
        tables::FamilyTables,
    },
    error::{MonadChainDataError, Result},
    store::{BlobStore, MetaStore, WriteSession},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectoryCompactionPlan {
    pub buckets: Vec<(u64, PrimaryDirBucket)>,
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

impl<M: MetaStore> FamilyTables<M> {
    /// Reads every sealed bucket's fragments and folds them into compacted
    /// bucket summaries. Pure I/O — no writes — so callers can fan out across
    /// families with `try_join_all` before opening the Phase B meta batch.
    pub(crate) async fn plan_directory_compactions(
        &self,
        open_indexes: &OpenIndexes,
        ranges: &[(u64, u64)],
    ) -> Result<DirectoryCompactionPlan> {
        let mut buckets = Vec::new();
        for &(from_next_primary_id, next_primary_id) in ranges {
            for sealed_bucket_start in sealed_ranges(from_next_primary_id, next_primary_id) {
                let encoded_fragments =
                    open_indexes.directory_fragments(self.family(), sealed_bucket_start);
                let fragments = encoded_fragments
                    .iter()
                    .map(|bytes| PrimaryDirFragment::decode(bytes))
                    .collect::<Result<Vec<_>>>()?;
                let bucket = compact_bucket_from_fragments(&fragments)?;
                buckets.push((sealed_bucket_start, bucket));
            }
        }
        Ok(DirectoryCompactionPlan { buckets })
    }

    pub fn stage_directory_compactions<B: BlobStore>(
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

    // The fragment list is the bucket's full, contiguous per-block record (one
    // fragment per block, empty or not), so we still validate block- and
    // id-continuity across *all* of it. But we only emit an entry for blocks
    // that mint ids; empty blocks (`first == end`) would each cost a `u64` while
    // resolving to nothing, which is exactly what let sparse buckets overflow
    // the backend write limit. The sentinel comes from the last fragment's end,
    // closing the final retained entry's range across any trailing empty blocks.
    let mut entries = Vec::new();
    push_entry_if_id_producing(&mut entries, first_fragment);

    for pair in fragments.windows(2) {
        let (previous, fragment) = (&pair[0], &pair[1]);
        validate_fragment(fragment)?;

        // Block numbers must strictly increase, but need not be contiguous:
        // empty blocks mint no ids and so write no fragment, leaving legitimate
        // gaps. Id continuity is the real integrity check — empty blocks don't
        // advance the frontier, so consecutive id-producing blocks still chain
        // exactly (`previous.end == current.first`).
        if fragment.block_number <= previous.block_number {
            return Err(MonadChainDataError::Decode(
                "inconsistent primary directory bucket block sequence",
            ));
        }
        if previous.end_primary_id_exclusive != fragment.first_primary_id {
            return Err(MonadChainDataError::Decode(
                "inconsistent primary directory bucket primary-id sequence",
            ));
        }

        push_entry_if_id_producing(&mut entries, fragment);
    }

    PrimaryDirBucket::new(entries, last_fragment.end_primary_id_exclusive)
}

fn push_entry_if_id_producing(entries: &mut Vec<PrimaryDirEntry>, fragment: &PrimaryDirFragment) {
    if fragment.end_primary_id_exclusive > fragment.first_primary_id {
        entries.push(PrimaryDirEntry {
            block_number: fragment.block_number,
            first_primary_id: fragment.first_primary_id,
        });
    }
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
        compact_bucket_from_fragments, sealed_ranges, PrimaryDirBucket, PrimaryDirEntry,
        PrimaryDirFragment, DIRECTORY_BUCKET_SIZE,
    };

    #[test]
    fn compact_bucket_from_fragments_omits_empty_blocks_and_keeps_sentinel() {
        // Block 8 is empty (103 == 103): it must not cost an entry, but the run
        // of ids it spans is still closed by the trailing sentinel.
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
                entries: vec![
                    PrimaryDirEntry {
                        block_number: 7,
                        first_primary_id: 100,
                    },
                    PrimaryDirEntry {
                        block_number: 9,
                        first_primary_id: 103,
                    },
                ],
                end_primary_id_exclusive: 108,
            }
        );
    }

    #[test]
    fn compact_bucket_from_fragments_collapses_long_empty_run() {
        // One id-producing block followed by a million empty blocks compacts to a
        // single entry plus the sentinel — the whole point of the sparse format.
        let mut fragments = vec![PrimaryDirFragment {
            block_number: 0,
            first_primary_id: 0,
            end_primary_id_exclusive: 5,
        }];
        for block_number in 1..=1_000_000 {
            fragments.push(PrimaryDirFragment {
                block_number,
                first_primary_id: 5,
                end_primary_id_exclusive: 5,
            });
        }

        let bucket = compact_bucket_from_fragments(&fragments).expect("compact bucket");

        assert_eq!(
            bucket,
            PrimaryDirBucket {
                entries: vec![PrimaryDirEntry {
                    block_number: 0,
                    first_primary_id: 0,
                }],
                end_primary_id_exclusive: 5,
            }
        );
    }

    #[test]
    fn compact_bucket_from_fragments_allows_block_gaps_from_omitted_empty_blocks() {
        // Block 8 was empty and wrote no fragment, so the surviving fragments
        // skip from block 7 to block 9. The frontier is unchanged across the gap
        // (103 == 103), so this is valid and compacts cleanly.
        let bucket = compact_bucket_from_fragments(&[
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
        .expect("compact bucket across block gap");

        assert_eq!(
            bucket,
            PrimaryDirBucket {
                entries: vec![
                    PrimaryDirEntry {
                        block_number: 7,
                        first_primary_id: 100,
                    },
                    PrimaryDirEntry {
                        block_number: 9,
                        first_primary_id: 103,
                    },
                ],
                end_primary_id_exclusive: 108,
            }
        );
    }

    #[test]
    fn compact_bucket_from_fragments_rejects_out_of_order_blocks() {
        let error = compact_bucket_from_fragments(&[
            PrimaryDirFragment {
                block_number: 9,
                first_primary_id: 100,
                end_primary_id_exclusive: 103,
            },
            PrimaryDirFragment {
                block_number: 7,
                first_primary_id: 103,
                end_primary_id_exclusive: 108,
            },
        ])
        .expect_err("detect out-of-order blocks");

        assert_eq!(
            error.to_string(),
            "decode error: inconsistent primary directory bucket block sequence"
        );
    }

    #[test]
    fn compact_bucket_from_fragments_rejects_id_gaps() {
        // A gap in the id chain (103 -> 105) means a fragment was lost, not that
        // an empty block was skipped — empty blocks leave the frontier unchanged.
        let error = compact_bucket_from_fragments(&[
            PrimaryDirFragment {
                block_number: 7,
                first_primary_id: 100,
                end_primary_id_exclusive: 103,
            },
            PrimaryDirFragment {
                block_number: 8,
                first_primary_id: 105,
                end_primary_id_exclusive: 108,
            },
        ])
        .expect_err("detect id gap");

        assert_eq!(
            error.to_string(),
            "decode error: inconsistent primary directory bucket primary-id sequence"
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
