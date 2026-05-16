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
    time::Instant,
};

use bytes::Bytes;

use crate::{
    engine::{
        bitmap::{
            compact_bitmap_page, global_page_start, local_page_start, stream_page_global_start,
            BitmapFragmentWrite, BitmapPageMeta,
        },
        ingest::ReadPlanningTimings,
        open_index::{OpenIndexes, OpenIndexesEviction},
        tables::FamilyTables,
    },
    error::Result,
    store::{BlobStore, MetaStore, WriteSession},
};

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

impl<M: MetaStore, B: BlobStore> FamilyTables<M, B> {
    /// Reads the prior open-stream inventory and the sealed-page fragments
    /// across a batch's primary-id ranges, returning the compacted page
    /// artifacts the Phase B meta batch will stage. Pure I/O — no writes.
    pub(crate) async fn plan_bitmap_compactions(
        &self,
        open_indexes: &OpenIndexes,
        written_fragments_per_block: &[&[BitmapFragmentWrite]],
        ranges: &[(u64, u64)],
    ) -> Result<BitmapBatchCompactionPlan> {
        debug_assert_eq!(written_fragments_per_block.len(), ranges.len());

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
        let mut frontier: Option<(u64, BTreeSet<String>)> = None;
        let mut timings = ReadPlanningTimings::default();

        for (block_idx, &(from_next_primary_id, next_primary_id)) in ranges.iter().enumerate() {
            if next_primary_id <= from_next_primary_id {
                continue;
            }

            let start_open_page = global_page_start(from_next_primary_id);
            let previous_open_streams = match frontier.take() {
                Some((page, streams)) if page == start_open_page => streams,
                _ => open_indexes.bitmap_open_streams(self.family(), start_open_page),
            };

            let touched_by_page = touched_streams_by_page(written_fragments_per_block[block_idx])?;
            let shape = build_compaction_plan(
                previous_open_streams,
                touched_by_page,
                from_next_primary_id,
                next_primary_id,
            );

            for (page_global_start, streams) in &shape.sealed_pages {
                let page_start_local = local_page_start(*page_global_start);
                for stream_id in streams {
                    let blocks =
                        open_indexes.bitmap_blocks(self.family(), stream_id, page_start_local);
                    let fragments = self
                        .load_bitmap_fragments_by_blocks(
                            stream_id,
                            page_start_local,
                            &blocks,
                            &mut timings,
                        )
                        .await?;
                    let compact_start = Instant::now();
                    let (meta, bitmap_blob) = compact_bitmap_page(page_start_local, &fragments)?;
                    timings.bitmap_compact_ms = timings
                        .bitmap_compact_ms
                        .saturating_add(compact_start.elapsed().as_millis() as u64);
                    timings.bitmap_compact_count = timings.bitmap_compact_count.saturating_add(1);
                    compacted_pages.push(CompactedPageWrite {
                        page_global_start: *page_global_start,
                        stream_id: stream_id.clone(),
                        page_start_local,
                        meta,
                        blob: bitmap_blob,
                    });
                }
            }

            // Per-block, the original `record_open_bitmap_streams` was called
            // with the per-block `final_open_streams` at `final_open_page_start`.
            // Within a batch we update the latest entry for that page so the
            // batched commit still produces the on-disk union.
            if !shape.final_open_streams.is_empty() {
                if let Some(last) = open_stream_writes.last_mut() {
                    if last.0 == shape.final_open_page_start {
                        last.1.extend(shape.final_open_streams.iter().cloned());
                    } else {
                        open_stream_writes.push((
                            shape.final_open_page_start,
                            shape.final_open_streams.clone(),
                        ));
                    }
                } else {
                    open_stream_writes.push((
                        shape.final_open_page_start,
                        shape.final_open_streams.clone(),
                    ));
                }
            }

            frontier = Some((shape.final_open_page_start, shape.final_open_streams));
        }

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

    pub(crate) fn bitmap_eviction(&self, plan: &BitmapBatchCompactionPlan) -> OpenIndexesEviction {
        OpenIndexesEviction {
            bitmap_pages: plan
                .compacted_pages
                .iter()
                .map(|page| (self.family(), page.stream_id.clone(), page.page_start_local))
                .collect(),
            bitmap_open_pages: plan
                .compacted_pages
                .iter()
                .map(|page| (self.family(), page.page_global_start))
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect(),
            ..OpenIndexesEviction::default()
        }
    }
}

fn touched_streams_by_page(
    written_fragments: &[BitmapFragmentWrite],
) -> Result<BTreeMap<u64, BTreeSet<String>>> {
    let mut out = BTreeMap::<u64, BTreeSet<String>>::new();

    for fragment in written_fragments {
        let page_start = stream_page_global_start(&fragment.stream_id, fragment.page_start_local)?;
        out.entry(page_start)
            .or_default()
            .insert(fragment.stream_id.clone());
    }

    Ok(out)
}

fn build_compaction_plan(
    previous_open_streams: BTreeSet<String>,
    touched_by_page: BTreeMap<u64, BTreeSet<String>>,
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
        if page_start <= start_open_page || page_start >= final_open_page_start {
            continue;
        }
        sealed_pages.push((page_start, streams));
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

    use super::{build_compaction_plan, touched_streams_by_page, BitmapCompactionPlan};
    use crate::engine::bitmap::{BitmapFragmentWrite, STREAM_PAGE_LOCAL_ID_SPAN};

    #[test]
    fn compaction_plan_carries_previous_open_streams_into_the_sealed_start_page() {
        let plan = build_compaction_plan(
            BTreeSet::from(["addr/a/0000000000".to_string()]),
            BTreeMap::from([(0, BTreeSet::from(["addr/b/0000000000".to_string()]))]),
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

        let plan = build_compaction_plan(BTreeSet::new(), touched, from_next, next);

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

        let plan = build_compaction_plan(previous.clone(), touched, page_span, page_span - 1);

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
    fn compaction_plan_keeps_final_frontier_page_open_even_when_empty() {
        let plan = build_compaction_plan(
            BTreeSet::from(["addr/a/0000000000".to_string()]),
            BTreeMap::from_iter(iter::once((
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
