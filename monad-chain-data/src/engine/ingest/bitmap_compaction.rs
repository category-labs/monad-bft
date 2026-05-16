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
    collections::{BTreeMap, BTreeSet},
    ops::Bound::Excluded,
    time::Instant,
};

use bytes::Bytes;

use crate::{
    engine::{
        bitmap::{compact_bitmap_page, global_page_start, local_page_start, BitmapPageMeta},
        family::Family,
        ingest::ReadPlanningTimings,
        open_index::{OpenIndexes, OpenIndexesEviction},
        tables::FamilyTables,
    },
    error::Result,
    store::{BlobStore, MetaStore, WriteSession},
};

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
struct BitmapCompactionPlan {
    sealed_pages: Vec<(u64, BTreeSet<String>)>,
    final_open_page_start: u64,
    final_open_streams: BTreeSet<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactedPageWrite {
    pub page_global_start: u64,
    pub stream_id: String,
    pub page_start_local: u32,
    pub meta: BitmapPageMeta,
    pub blob: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BitmapBatchCompactionPlan {
    pub compacted_pages: Vec<CompactedPageWrite>,
    /// Per-frontier-page open-stream sets to record. The vec preserves
    /// insertion order (one entry per frontier-page advance within the
    /// batch); each entry's `BTreeSet` is the cumulative set for that page
    /// at the point the frontier moved past it (for non-final entries) or
    /// at the end of the batch (for the final entry).
    pub open_stream_writes: Vec<(u64, BTreeSet<String>)>,
    pub has_writes: bool,
    pub(crate) timings: ReadPlanningTimings,
}

impl BitmapBatchCompactionPlan {
    pub(crate) fn eviction(&self, family: Family) -> OpenIndexesEviction {
        OpenIndexesEviction {
            bitmap_pages: self
                .compacted_pages
                .iter()
                .map(|page| (family, page.stream_id.clone(), page.page_start_local))
                .collect(),
            bitmap_open_pages: self
                .compacted_pages
                .iter()
                .map(|page| (family, page.page_global_start))
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect(),
            ..OpenIndexesEviction::default()
        }
    }
}

impl<M: MetaStore, B: BlobStore> FamilyTables<M, B> {
    /// Reads the prior open-stream inventory and the sealed-page fragments
    /// across a batch's primary-id ranges, returning the compacted page
    /// artifacts the Phase B meta batch will stage. Pure I/O — no writes.
    pub(crate) async fn plan_bitmap_compactions(
        &self,
        open_indexes: &OpenIndexes,
        touched_streams_by_page_per_block: &[&BTreeMap<u64, BTreeSet<String>>],
        ranges: &[(u64, u64)],
    ) -> Result<BitmapBatchCompactionPlan> {
        debug_assert_eq!(touched_streams_by_page_per_block.len(), ranges.len());

        if ranges.is_empty() {
            return Ok(BitmapBatchCompactionPlan {
                compacted_pages: Vec::new(),
                open_stream_writes: Vec::new(),
                has_writes: false,
                timings: ReadPlanningTimings::default(),
            });
        }

        let mut compacted_pages = Vec::new();
        let mut open_stream_writes: Vec<(u64, BTreeSet<String>)> = Vec::new();
        let mut timings = ReadPlanningTimings::default();

        let Some((from_next_primary_id, next_primary_id, touched_by_page)) =
            batch_bitmap_shape(touched_streams_by_page_per_block, ranges, &mut timings)
        else {
            return Ok(BitmapBatchCompactionPlan {
                compacted_pages: Vec::new(),
                open_stream_writes: Vec::new(),
                has_writes: false,
                timings,
            });
        };

        let shape_start = Instant::now();
        let start_open_page = global_page_start(from_next_primary_id);
        let final_open_page_start = global_page_start(next_primary_id);
        let open_streams_start = Instant::now();
        let previous_open_streams =
            open_indexes.bitmap_open_streams(self.family(), start_open_page);
        timings.bitmap_open_streams_ms = timings
            .bitmap_open_streams_ms
            .saturating_add(open_streams_start.elapsed().as_millis() as u64);
        timings.bitmap_open_streams_us = timings
            .bitmap_open_streams_us
            .saturating_add(open_streams_start.elapsed().as_micros() as u64);
        timings.bitmap_open_streams_count = timings.bitmap_open_streams_count.saturating_add(1);
        timings.bitmap_frontier_stream_count = timings
            .bitmap_frontier_stream_count
            .saturating_add(previous_open_streams.len() as u64);

        let frontier_start = Instant::now();
        let mut final_open_streams = touched_by_page
            .get(&final_open_page_start)
            .cloned()
            .unwrap_or_default();
        let mut start_page_streams = previous_open_streams;
        if let Some(touched) = touched_by_page.get(&start_open_page) {
            start_page_streams.extend(touched.iter().cloned());
        }
        let same_frontier_page = start_open_page == final_open_page_start;
        if same_frontier_page {
            final_open_streams.append(&mut start_page_streams);
        }
        timings.bitmap_frontier_us = timings
            .bitmap_frontier_us
            .saturating_add(frontier_start.elapsed().as_micros() as u64);
        timings.bitmap_shape_ms = timings
            .bitmap_shape_ms
            .saturating_add(shape_start.elapsed().as_millis() as u64);
        timings.bitmap_shape_us = timings
            .bitmap_shape_us
            .saturating_add(shape_start.elapsed().as_micros() as u64);

        if !same_frontier_page && start_open_page < final_open_page_start {
            if !start_page_streams.is_empty() {
                self.plan_bitmap_page_from_streams(
                    open_indexes,
                    start_open_page,
                    start_page_streams.iter(),
                    &mut timings,
                    &mut compacted_pages,
                )?;
                record_open_stream_write(
                    &mut open_stream_writes,
                    start_open_page,
                    start_page_streams,
                    &mut timings,
                );
            }
            for (page_global_start, streams) in
                touched_by_page.range((Excluded(start_open_page), Excluded(final_open_page_start)))
            {
                self.plan_bitmap_page_from_streams(
                    open_indexes,
                    *page_global_start,
                    streams.iter(),
                    &mut timings,
                    &mut compacted_pages,
                )?;
            }
        }
        record_open_stream_write(
            &mut open_stream_writes,
            final_open_page_start,
            final_open_streams,
            &mut timings,
        );

        let has_writes = !compacted_pages.is_empty() || !open_stream_writes.is_empty();
        Ok(BitmapBatchCompactionPlan {
            compacted_pages,
            open_stream_writes,
            has_writes,
            timings,
        })
    }

    pub fn stage_bitmap_compactions(
        &self,
        w: &WriteSession<'_, M, B>,
        plan: &BitmapBatchCompactionPlan,
    ) {
        for page in &plan.compacted_pages {
            self.bitmap().stage_page_blob(
                w,
                &page.stream_id,
                page.page_start_local,
                page.blob.clone(),
            );
            self.bitmap()
                .stage_page_meta(w, &page.stream_id, page.page_start_local, &page.meta);
        }
        for (page_start, streams) in &plan.open_stream_writes {
            self.bitmap().stage_open_streams(w, *page_start, streams);
        }
    }

    fn plan_bitmap_page_from_streams<'a, I>(
        &self,
        open_indexes: &OpenIndexes,
        page_global_start: u64,
        streams: I,
        timings: &mut ReadPlanningTimings,
        compacted_pages: &mut Vec<CompactedPageWrite>,
    ) -> Result<()>
    where
        I: IntoIterator<Item = &'a String>,
    {
        let page_start_local = local_page_start(page_global_start);
        for stream_id in streams {
            let index_start = Instant::now();
            let fragments =
                open_indexes.bitmap_fragments(self.family(), stream_id, page_start_local);
            timings.bitmap_index_ms = timings
                .bitmap_index_ms
                .saturating_add(index_start.elapsed().as_millis() as u64);
            timings.bitmap_index_us = timings
                .bitmap_index_us
                .saturating_add(index_start.elapsed().as_micros() as u64);
            timings.bitmap_fragment_count = timings
                .bitmap_fragment_count
                .saturating_add(fragments.len() as u64);
            timings.bitmap_fragment_bytes = timings.bitmap_fragment_bytes.saturating_add(
                fragments
                    .iter()
                    .map(|fragment| fragment.len() as u64)
                    .sum::<u64>(),
            );
            let compact_start = Instant::now();
            let (meta, bitmap_blob) = compact_bitmap_page(page_start_local, &fragments)?;
            timings.bitmap_compact_ms = timings
                .bitmap_compact_ms
                .saturating_add(compact_start.elapsed().as_millis() as u64);
            timings.bitmap_compact_us = timings
                .bitmap_compact_us
                .saturating_add(compact_start.elapsed().as_micros() as u64);
            timings.bitmap_compact_count = timings.bitmap_compact_count.saturating_add(1);
            compacted_pages.push(CompactedPageWrite {
                page_global_start,
                stream_id: stream_id.clone(),
                page_start_local,
                meta,
                blob: bitmap_blob,
            });
        }
        Ok(())
    }
}

fn batch_bitmap_shape(
    touched_streams_by_page_per_block: &[&BTreeMap<u64, BTreeSet<String>>],
    ranges: &[(u64, u64)],
    timings: &mut ReadPlanningTimings,
) -> Option<(u64, u64, BTreeMap<u64, BTreeSet<String>>)> {
    let union_start = Instant::now();
    let mut from_next_primary_id = None;
    let mut next_primary_id = None;
    let mut touched_by_page = BTreeMap::<u64, BTreeSet<String>>::new();

    for (block_idx, &(from, next)) in ranges.iter().enumerate() {
        if next <= from {
            continue;
        }
        from_next_primary_id.get_or_insert(from);
        next_primary_id = Some(next);

        let block_touched = touched_streams_by_page_per_block[block_idx];
        timings.bitmap_touched_page_count = timings
            .bitmap_touched_page_count
            .saturating_add(block_touched.len() as u64);
        timings.bitmap_touched_stream_count = timings.bitmap_touched_stream_count.saturating_add(
            block_touched
                .values()
                .map(|streams| streams.len() as u64)
                .sum::<u64>(),
        );
        for (page, streams) in block_touched {
            touched_by_page
                .entry(*page)
                .or_default()
                .extend(streams.iter().cloned());
        }
    }

    timings.bitmap_union_us = timings
        .bitmap_union_us
        .saturating_add(union_start.elapsed().as_micros() as u64);
    timings.bitmap_union_page_count = timings
        .bitmap_union_page_count
        .saturating_add(touched_by_page.len() as u64);
    timings.bitmap_union_stream_count = timings.bitmap_union_stream_count.saturating_add(
        touched_by_page
            .values()
            .map(|streams| streams.len() as u64)
            .sum(),
    );

    Some((from_next_primary_id?, next_primary_id?, touched_by_page))
}

fn record_open_stream_write(
    open_stream_writes: &mut Vec<(u64, BTreeSet<String>)>,
    page_global_start: u64,
    streams: BTreeSet<String>,
    timings: &mut ReadPlanningTimings,
) {
    if streams.is_empty() {
        return;
    }

    let open_write_start = Instant::now();
    timings.bitmap_final_open_stream_count = timings
        .bitmap_final_open_stream_count
        .saturating_add(streams.len() as u64);
    open_stream_writes.push((page_global_start, streams));
    timings.bitmap_open_write_us = timings
        .bitmap_open_write_us
        .saturating_add(open_write_start.elapsed().as_micros() as u64);
}

#[cfg(test)]
fn build_compaction_plan(
    previous_open_streams: BTreeSet<String>,
    touched_by_page: &BTreeMap<u64, BTreeSet<String>>,
    from_next_primary_id: u64,
    next_primary_id: u64,
) -> BitmapCompactionPlan {
    let start_open_page = global_page_start(from_next_primary_id);
    let final_open_page_start = global_page_start(next_primary_id);
    let mut sealed_pages = Vec::new();

    if next_primary_id <= from_next_primary_id {
        return BitmapCompactionPlan {
            sealed_pages,
            final_open_page_start: start_open_page,
            final_open_streams: previous_open_streams,
        };
    }

    let mut final_open_streams = touched_by_page
        .get(&final_open_page_start)
        .cloned()
        .unwrap_or_default();

    let mut start_page_streams = previous_open_streams;
    if let Some(touched) = touched_by_page.get(&start_open_page) {
        start_page_streams.extend(touched.iter().cloned());
    }
    if start_open_page == final_open_page_start {
        final_open_streams.extend(start_page_streams);
        return BitmapCompactionPlan {
            sealed_pages,
            final_open_page_start,
            final_open_streams,
        };
    }

    if start_open_page < final_open_page_start && !start_page_streams.is_empty() {
        sealed_pages.push((start_open_page, start_page_streams));
    }

    for (page_start, streams) in touched_by_page {
        if *page_start <= start_open_page || *page_start >= final_open_page_start {
            continue;
        }
        sealed_pages.push((*page_start, streams.clone()));
    }

    BitmapCompactionPlan {
        sealed_pages,
        final_open_page_start,
        final_open_streams,
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, BTreeSet},
        iter,
    };

    use bytes::Bytes;

    use super::{batch_bitmap_shape, build_compaction_plan, BitmapCompactionPlan};
    use crate::engine::{
        bitmap::{touched_streams_by_page, BitmapFragmentWrite, STREAM_PAGE_LOCAL_ID_SPAN},
        ingest::ReadPlanningTimings,
    };

    #[test]
    fn compaction_plan_carries_previous_open_streams_into_the_sealed_start_page() {
        let plan = build_compaction_plan(
            BTreeSet::from(["addr/a/0000000000".to_string()]),
            &BTreeMap::from([(0, BTreeSet::from(["addr/b/0000000000".to_string()]))]),
            STREAM_PAGE_LOCAL_ID_SPAN as u64 - 2,
            STREAM_PAGE_LOCAL_ID_SPAN as u64 + 3,
        );

        assert_eq!(
            plan,
            BitmapCompactionPlan {
                sealed_pages: vec![(
                    0,
                    BTreeSet::from([
                        "addr/a/0000000000".to_string(),
                        "addr/b/0000000000".to_string(),
                    ]),
                )],
                final_open_page_start: STREAM_PAGE_LOCAL_ID_SPAN as u64,
                final_open_streams: BTreeSet::new(),
            }
        );
    }

    #[test]
    fn compaction_plan_only_compacts_touched_sparse_sealed_pages() {
        let shard_span = 1u64 << 24;
        let page_span = STREAM_PAGE_LOCAL_ID_SPAN as u64;
        let from_next = shard_span - 2;
        let next = shard_span * 3 + 3;
        let touched = BTreeMap::from_iter([
            (
                shard_span - page_span,
                BTreeSet::from(["addr/a/0000000000".to_string()]),
            ),
            (
                shard_span,
                BTreeSet::from(["addr/b/0000000001".to_string()]),
            ),
            (
                shard_span * 2,
                BTreeSet::from(["addr/c/0000000002".to_string()]),
            ),
            (
                shard_span * 3,
                BTreeSet::from(["addr/d/0000000003".to_string()]),
            ),
        ]);

        let plan = build_compaction_plan(BTreeSet::new(), &touched, from_next, next);

        assert_eq!(
            plan.sealed_pages,
            vec![
                (
                    shard_span - page_span,
                    BTreeSet::from(["addr/a/0000000000".to_string()]),
                ),
                (
                    shard_span,
                    BTreeSet::from(["addr/b/0000000001".to_string()]),
                ),
                (
                    shard_span * 2,
                    BTreeSet::from(["addr/c/0000000002".to_string()]),
                ),
            ]
        );
        assert_eq!(plan.final_open_page_start, shard_span * 3);
        assert_eq!(
            plan.final_open_streams,
            BTreeSet::from(["addr/d/0000000003".to_string()])
        );
    }

    #[test]
    fn compaction_plan_handles_non_advancing_inputs_without_looping() {
        let page_span = STREAM_PAGE_LOCAL_ID_SPAN as u64;
        let previous = BTreeSet::from(["addr/a/0000000000".to_string()]);
        let touched =
            BTreeMap::from([(page_span, BTreeSet::from(["addr/b/0000000000".to_string()]))]);

        let plan = build_compaction_plan(previous.clone(), &touched, page_span, page_span - 1);

        assert_eq!(
            plan,
            BitmapCompactionPlan {
                sealed_pages: Vec::new(),
                final_open_page_start: page_span,
                final_open_streams: previous,
            }
        );
    }

    #[test]
    fn touched_streams_by_page_groups_duplicate_fragments_once() {
        let shard_span = 1u64 << 24;
        let fragments = vec![
            BitmapFragmentWrite {
                stream_id: "addr/aa/0000000000".to_string(),
                page_start_local: 0,
                bitmap_blob: Bytes::new(),
            },
            BitmapFragmentWrite {
                stream_id: "addr/aa/0000000000".to_string(),
                page_start_local: 0,
                bitmap_blob: Bytes::new(),
            },
            BitmapFragmentWrite {
                stream_id: "addr/bb/0000000001".to_string(),
                page_start_local: 0,
                bitmap_blob: Bytes::new(),
            },
        ];

        let grouped = touched_streams_by_page(&fragments).expect("group touched streams");

        assert_eq!(
            grouped,
            BTreeMap::from_iter([
                (0, BTreeSet::from(["addr/aa/0000000000".to_string()]),),
                (
                    shard_span,
                    BTreeSet::from(["addr/bb/0000000001".to_string()]),
                ),
            ])
        );
    }

    #[test]
    fn batch_bitmap_shape_unions_touched_pages_once() {
        let page_span = STREAM_PAGE_LOCAL_ID_SPAN as u64;
        let block_a = BTreeMap::from_iter([(
            0,
            BTreeSet::from([
                "addr/a/0000000000".to_string(),
                "addr/b/0000000000".to_string(),
            ]),
        )]);
        let block_b = BTreeMap::from_iter([
            (
                0,
                BTreeSet::from([
                    "addr/b/0000000000".to_string(),
                    "addr/c/0000000000".to_string(),
                ]),
            ),
            (page_span, BTreeSet::from(["addr/d/0000000001".to_string()])),
        ]);
        let inputs = vec![&block_a, &block_b];
        let ranges = vec![
            (page_span - 2, page_span - 1),
            (page_span - 1, page_span + 1),
        ];
        let mut timings = ReadPlanningTimings::default();

        let (from, next, grouped) =
            batch_bitmap_shape(&inputs, &ranges, &mut timings).expect("advancing shape");

        assert_eq!(from, page_span - 2);
        assert_eq!(next, page_span + 1);
        assert_eq!(
            grouped,
            BTreeMap::from_iter([
                (
                    0,
                    BTreeSet::from([
                        "addr/a/0000000000".to_string(),
                        "addr/b/0000000000".to_string(),
                        "addr/c/0000000000".to_string(),
                    ]),
                ),
                (page_span, BTreeSet::from(["addr/d/0000000001".to_string()]),),
            ])
        );
        assert_eq!(timings.bitmap_touched_page_count, 3);
        assert_eq!(timings.bitmap_touched_stream_count, 5);
        assert_eq!(timings.bitmap_union_page_count, 2);
        assert_eq!(timings.bitmap_union_stream_count, 4);
    }

    #[test]
    fn compaction_plan_keeps_final_frontier_page_open_even_when_empty() {
        let plan = build_compaction_plan(
            BTreeSet::from(["addr/a/0000000000".to_string()]),
            &BTreeMap::from_iter(iter::once((
                0,
                BTreeSet::from(["addr/a/0000000000".to_string()]),
            ))),
            STREAM_PAGE_LOCAL_ID_SPAN as u64 - 1,
            STREAM_PAGE_LOCAL_ID_SPAN as u64,
        );

        assert_eq!(
            plan,
            BitmapCompactionPlan {
                sealed_pages: vec![(0, BTreeSet::from(["addr/a/0000000000".to_string()]),)],
                final_open_page_start: STREAM_PAGE_LOCAL_ID_SPAN as u64,
                final_open_streams: BTreeSet::new(),
            }
        );
    }
}
