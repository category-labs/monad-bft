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

use futures::{stream, StreamExt, TryStreamExt};
use roaring::RoaringBitmap;

use crate::{
    engine::{
        bitmap::{decode_bitmap_blob, page_start_local, LOCAL_ID_BITS, STREAM_PAGE_LOCAL_ID_SPAN},
        clause::IndexedClause,
        query::family_runner::QueryExecutionStats,
        tables::FamilyTables,
    },
    error::Result,
    store::{BlobStore, MetaStore},
};

const BITMAP_CLAUSE_CONCURRENCY: usize = 4;
const BITMAP_PAGE_CONCURRENCY: usize = 64;

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
    ) -> Result<Option<BitmapLoadOutcome>> {
        let mut accumulator: Option<RoaringBitmap> = None;
        let mut stats = QueryExecutionStats::default();

        let clause_outcomes: Vec<BitmapLoadOutcome> = stream::iter(clauses)
            .map(|clause| async move {
                self.load_clause_bitmap(clause, shard, local_from, local_to)
                    .await
            })
            .buffered(BITMAP_CLAUSE_CONCURRENCY)
            .try_collect()
            .await?;

        for outcome in clause_outcomes {
            stats.merge(&outcome.stats);
            let clause_bitmap = outcome.bitmap;
            if clause_bitmap.is_empty() {
                return Ok(None);
            }

            match accumulator.as_mut() {
                Some(current) => {
                    *current &= &clause_bitmap;
                    if current.is_empty() {
                        return Ok(None);
                    }
                }
                None => accumulator = Some(clause_bitmap),
            }
        }

        Ok(accumulator
            .filter(|bitmap| !bitmap.is_empty())
            .map(|bitmap| BitmapLoadOutcome { bitmap, stats }))
    }

    async fn load_clause_bitmap(
        &self,
        clause: &IndexedClause,
        shard: u64,
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

        let first_page_start = page_start_local(local_from);
        let last_page_start = page_start_local(local_to);
        let mut jobs = Vec::new();
        for stream_id in &stream_ids {
            let mut page_start = first_page_start;
            loop {
                jobs.push((stream_id.clone(), page_start));

                if page_start == last_page_start {
                    break;
                }
                page_start = page_start.saturating_add(STREAM_PAGE_LOCAL_ID_SPAN);
            }
        }

        let page_outcomes: Vec<BitmapLoadOutcome> = stream::iter(jobs)
            .map(|(stream_id, page_start)| async move {
                self.load_bitmap_page(&stream_id, page_start, local_from, local_to)
                    .await
            })
            .buffer_unordered(BITMAP_PAGE_CONCURRENCY)
            .try_collect()
            .await?;

        let mut clause_bitmap = RoaringBitmap::new();
        for outcome in page_outcomes {
            stats.merge(&outcome.stats);
            clause_bitmap |= outcome.bitmap;
        }

        clip_bitmap_to_local_range(&mut clause_bitmap, local_from, local_to);
        Ok(BitmapLoadOutcome {
            bitmap: clause_bitmap,
            stats,
        })
    }

    async fn load_bitmap_page(
        &self,
        stream_id: &str,
        page_start_local: u32,
        local_from: u32,
        local_to: u32,
    ) -> Result<BitmapLoadOutcome> {
        let mut stats = QueryExecutionStats::default();
        stats.bitmap_page_probes = stats.bitmap_page_probes.saturating_add(1);
        if let Some(page) = self
            .load_bitmap_page_artifact(stream_id, page_start_local)
            .await?
        {
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
            return Ok(BitmapLoadOutcome {
                bitmap: decode_bitmap_blob(page.bitmap_blob.as_ref())?.bitmap,
                stats,
            });
        }

        let mut page_bitmap = RoaringBitmap::new();
        let fragments = self
            .load_bitmap_fragments(stream_id, page_start_local)
            .await?;
        stats.open_bitmap_pages_read = stats.open_bitmap_pages_read.saturating_add(1);
        stats.bitmap_fragments_read = stats
            .bitmap_fragments_read
            .saturating_add(fragments.len() as u64);
        for fragment in fragments {
            let fragment = decode_bitmap_blob(fragment.as_ref())?;
            if overlaps(fragment.min_local, fragment.max_local, local_from, local_to) {
                page_bitmap |= fragment.bitmap;
            }
        }

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
