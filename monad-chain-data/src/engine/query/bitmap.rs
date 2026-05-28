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

use std::time::Instant;

use futures::{stream, StreamExt, TryStreamExt};
use roaring::RoaringBitmap;

use crate::{
    engine::{
        bitmap::{decode_bitmap_blob, page_start_local, LOCAL_ID_BITS, STREAM_PAGE_LOCAL_ID_SPAN},
        clause::IndexedClause,
        query::family_runner::{elapsed_us, QueryExecutionStats},
        tables::FamilyTables,
    },
    error::Result,
    store::{BlobStore, MetaStore},
};

const BITMAP_PAGE_GROUP_CONCURRENCY: usize = 32;
const BITMAP_CLAUSE_STREAM_CONCURRENCY: usize = 16;

pub(crate) struct BitmapLoadOutcome {
    pub bitmap: RoaringBitmap,
    pub stats: QueryExecutionStats,
}

impl<M: MetaStore, B: BlobStore> FamilyTables<M, B> {
    /// Loads the AND-intersection of all clauses for one shard, clipped to
    /// the local-id range. Returns `None` if any clause yields an empty
    /// bitmap, meaning the shard cannot contribute candidates.
    pub(crate) async fn load_intersection_bitmap(
        &self,
        clauses: &[IndexedClause],
        shard: u64,
        local_from: u32,
        local_to: u32,
        open_fragment_page_start: Option<u32>,
    ) -> Result<Option<BitmapLoadOutcome>> {
        let mut stats = QueryExecutionStats::default();
        let pages = page_starts_in_range(local_from, local_to);
        let clause_order = planned_clause_order(clauses);
        let baseline_jobs = pages.len().saturating_mul(
            clauses
                .iter()
                .map(|clause| clause.values.len())
                .sum::<usize>(),
        );
        stats.bitmap_page_groups_planned = pages.len() as u64;
        stats.bitmap_page_jobs_baseline = baseline_jobs as u64;

        let page_outcomes: Vec<BitmapLoadOutcome> = stream::iter(pages)
            .map(|page_start| {
                let clause_order = &clause_order;
                async move {
                    self.load_page_intersection_bitmap(
                        clauses,
                        clause_order,
                        shard,
                        page_start,
                        open_fragment_page_start,
                        local_from,
                        local_to,
                    )
                    .await
                }
            })
            .buffered(BITMAP_PAGE_GROUP_CONCURRENCY)
            .try_collect()
            .await?;

        let merge_start = Instant::now();
        let mut accumulator = RoaringBitmap::new();
        for outcome in page_outcomes {
            stats.merge(&outcome.stats);
            accumulator |= outcome.bitmap;
        }
        stats.bitmap_cpu_us = stats.bitmap_cpu_us.saturating_add(elapsed_us(merge_start));
        stats.bitmap_page_jobs_pruned = stats
            .bitmap_page_jobs_baseline
            .saturating_sub(stats.bitmap_page_probes);

        Ok((!accumulator.is_empty()).then_some(BitmapLoadOutcome {
            bitmap: accumulator,
            stats,
        }))
    }

    async fn load_page_intersection_bitmap(
        &self,
        clauses: &[IndexedClause],
        clause_order: &[usize],
        shard: u64,
        page_start: u32,
        open_fragment_page_start: Option<u32>,
        local_from: u32,
        local_to: u32,
    ) -> Result<BitmapLoadOutcome> {
        let mut stats = QueryExecutionStats::default();
        let mut accumulator: Option<RoaringBitmap> = None;
        let intersect_start = Instant::now();

        for clause_idx in clause_order {
            let outcome = self
                .load_clause_page_bitmap(
                    &clauses[*clause_idx],
                    shard,
                    page_start,
                    open_fragment_page_start,
                    local_from,
                    local_to,
                )
                .await?;
            stats.merge(&outcome.stats);
            if outcome.bitmap.is_empty() {
                stats.bitmap_page_groups_short_circuited =
                    stats.bitmap_page_groups_short_circuited.saturating_add(1);
                stats.bitmap_cpu_us = stats
                    .bitmap_cpu_us
                    .saturating_add(elapsed_us(intersect_start));
                return Ok(BitmapLoadOutcome {
                    bitmap: RoaringBitmap::new(),
                    stats,
                });
            }

            match accumulator.as_mut() {
                Some(current) => {
                    *current &= &outcome.bitmap;
                    if current.is_empty() {
                        stats.bitmap_page_groups_short_circuited =
                            stats.bitmap_page_groups_short_circuited.saturating_add(1);
                        stats.bitmap_cpu_us = stats
                            .bitmap_cpu_us
                            .saturating_add(elapsed_us(intersect_start));
                        return Ok(BitmapLoadOutcome {
                            bitmap: RoaringBitmap::new(),
                            stats,
                        });
                    }
                }
                None => accumulator = Some(outcome.bitmap),
            }
        }

        stats.bitmap_cpu_us = stats
            .bitmap_cpu_us
            .saturating_add(elapsed_us(intersect_start));
        Ok(BitmapLoadOutcome {
            bitmap: accumulator.unwrap_or_default(),
            stats,
        })
    }

    async fn load_clause_page_bitmap(
        &self,
        clause: &IndexedClause,
        shard: u64,
        page_start: u32,
        open_fragment_page_start: Option<u32>,
        local_from: u32,
        local_to: u32,
    ) -> Result<BitmapLoadOutcome> {
        let stream_ids = clause.stream_ids_for_shard(shard);
        if stream_ids.is_empty() {
            return Ok(BitmapLoadOutcome {
                bitmap: RoaringBitmap::new(),
                stats: QueryExecutionStats::default(),
            });
        }
        let mut stats = QueryExecutionStats::default();
        stats.clause_streams = stats.clause_streams.saturating_add(stream_ids.len() as u64);

        let page_outcomes: Vec<BitmapLoadOutcome> = stream::iter(stream_ids)
            .map(|stream_id| async move {
                self.load_bitmap_page(
                    &stream_id,
                    page_start,
                    open_fragment_page_start,
                    local_from,
                    local_to,
                )
                .await
            })
            .buffer_unordered(BITMAP_CLAUSE_STREAM_CONCURRENCY)
            .try_collect()
            .await?;

        let merge_start = Instant::now();
        let mut clause_bitmap = RoaringBitmap::new();
        for outcome in page_outcomes {
            stats.merge(&outcome.stats);
            clause_bitmap |= outcome.bitmap;
        }

        clip_bitmap_to_local_range(&mut clause_bitmap, local_from, local_to);
        stats.bitmap_cpu_us = stats.bitmap_cpu_us.saturating_add(elapsed_us(merge_start));
        Ok(BitmapLoadOutcome {
            bitmap: clause_bitmap,
            stats,
        })
    }

    async fn load_bitmap_page(
        &self,
        stream_id: &str,
        page_start_local: u32,
        open_fragment_page_start: Option<u32>,
        local_from: u32,
        local_to: u32,
    ) -> Result<BitmapLoadOutcome> {
        let mut stats = QueryExecutionStats::default();
        stats.bitmap_page_probes = stats.bitmap_page_probes.saturating_add(1);
        let page_artifact_start = Instant::now();
        let page_artifact = self
            .load_bitmap_page_artifact(stream_id, page_start_local)
            .await?;
        stats.bitmap_io_us = stats
            .bitmap_io_us
            .saturating_add(elapsed_us(page_artifact_start));
        if let Some(page) = page_artifact {
            stats.compacted_bitmap_pages_read = stats.compacted_bitmap_pages_read.saturating_add(1);
            if !overlaps(
                page.meta.min_local,
                page.meta.max_local,
                local_from,
                local_to,
            ) {
                return Ok(BitmapLoadOutcome {
                    bitmap: RoaringBitmap::new(),
                    stats,
                });
            }

            // Page loads may include out-of-range bits from a partially overlapping
            // page; the caller clips the final merged bitmap once per clause.
            let decode_start = Instant::now();
            let bitmap = decode_bitmap_blob(page.bitmap_blob.as_ref())?.bitmap;
            stats.bitmap_cpu_us = stats.bitmap_cpu_us.saturating_add(elapsed_us(decode_start));
            return Ok(BitmapLoadOutcome { bitmap, stats });
        }

        if open_fragment_page_start != Some(page_start_local) {
            stats.bitmap_fragment_scans_skipped =
                stats.bitmap_fragment_scans_skipped.saturating_add(1);
            return Ok(BitmapLoadOutcome {
                bitmap: RoaringBitmap::new(),
                stats,
            });
        }

        let mut page_bitmap = RoaringBitmap::new();
        let fragments_start = Instant::now();
        let fragments = self
            .load_bitmap_fragments(stream_id, page_start_local)
            .await?;
        stats.bitmap_io_us = stats
            .bitmap_io_us
            .saturating_add(elapsed_us(fragments_start));
        stats.open_bitmap_pages_read = stats.open_bitmap_pages_read.saturating_add(1);
        stats.bitmap_fragments_read = stats
            .bitmap_fragments_read
            .saturating_add(fragments.len() as u64);
        let decode_merge_start = Instant::now();
        for fragment in fragments {
            let fragment = decode_bitmap_blob(fragment.as_ref())?;
            if overlaps(fragment.min_local, fragment.max_local, local_from, local_to) {
                page_bitmap |= fragment.bitmap;
            }
        }
        stats.bitmap_cpu_us = stats
            .bitmap_cpu_us
            .saturating_add(elapsed_us(decode_merge_start));

        Ok(BitmapLoadOutcome {
            bitmap: page_bitmap,
            stats,
        })
    }
}

pub(crate) const fn max_local_id() -> u32 {
    (1u32 << LOCAL_ID_BITS) - 1
}

fn overlaps(start: u32, end: u32, query_start: u32, query_end: u32) -> bool {
    start <= query_end && end >= query_start
}

fn clip_bitmap_to_local_range(bitmap: &mut RoaringBitmap, local_from: u32, local_to: u32) {
    if local_from > 0 {
        bitmap.remove_range(0..local_from);
    }
    if local_to < u32::MAX {
        bitmap.remove_range(local_to.saturating_add(1)..u32::MAX);
    }
}

fn page_starts_in_range(local_from: u32, local_to: u32) -> Vec<u32> {
    let mut pages = Vec::new();
    let mut page_start = page_start_local(local_from);
    let last_page_start = page_start_local(local_to);
    loop {
        pages.push(page_start);
        if page_start == last_page_start {
            break;
        }
        page_start = page_start.saturating_add(STREAM_PAGE_LOCAL_ID_SPAN);
    }
    pages
}

fn planned_clause_order(clauses: &[IndexedClause]) -> Vec<usize> {
    let mut order: Vec<_> = (0..clauses.len()).collect();
    order.sort_by_key(|idx| {
        (
            clause_rank(&clauses[*idx]),
            clauses[*idx].values.len(),
            *idx,
        )
    });
    order
}

fn clause_rank(clause: &IndexedClause) -> u8 {
    match clause.kind {
        "addr" | "from" | "to" => 0,
        "topic1" | "topic2" | "topic3" => 1,
        "selector" => 2,
        "top_level" => 3,
        "topic0" | "has_transfer" => 4,
        _ => 2,
    }
}
