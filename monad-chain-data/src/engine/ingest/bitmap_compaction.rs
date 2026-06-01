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
};

use bytes::Bytes;
use rayon::prelude::*;

use crate::{
    engine::{
        bitmap::{
            compact_bitmap_page, global_page_start, local_page_start, BitmapPageArtifact,
            BitmapPageCounts, BitmapPageMeta, STREAM_PAGES_PER_SHARD, STREAM_PAGE_LOCAL_ID_SPAN,
        },
        family::Family,
        open_index::{OpenIndexes, OpenIndexesEviction},
        tables::FamilyTables,
    },
    error::{MonadChainDataError, Result},
    primitives::state::PrimaryId,
    store::{BlobStore, MetaStore, WriteSession},
};

/// Local-id span of one shard (`2^LOCAL_ID_BITS`). A shard fully seals once
/// the ingestion frontier crosses this boundary, at which point every one of
/// its pages is compacted and its per-stream page-count manifests are final.
const SHARD_LOCAL_ID_SPAN: u64 = 1u64 << PrimaryId::LOCAL_ID_BITS;

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
struct BitmapCompactionJob {
    page_global_start: u64,
    stream_id: String,
    page_start_local: u32,
    fragments: Vec<Bytes>,
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
    /// Per-stream page-count manifests for shards that fully sealed in this
    /// batch (the frontier crossed their last page). One entry per stream that
    /// has any non-empty page in a newly-sealed shard; immutable once written.
    pub page_count_manifests: Vec<(String, BitmapPageCounts)>,
    pub has_writes: bool,
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
                page_count_manifests: Vec::new(),
                has_writes: false,
            });
        }

        let mut compaction_jobs = Vec::new();
        let mut open_stream_writes: Vec<(u64, BTreeSet<String>)> = Vec::new();

        let Some((from_next_primary_id, next_primary_id, touched_by_page)) =
            batch_bitmap_shape(touched_streams_by_page_per_block, ranges)
        else {
            return Ok(BitmapBatchCompactionPlan {
                compacted_pages: Vec::new(),
                open_stream_writes: Vec::new(),
                page_count_manifests: Vec::new(),
                has_writes: false,
            });
        };

        let start_open_page = global_page_start(from_next_primary_id);
        let final_open_page_start = global_page_start(next_primary_id);
        let previous_open_streams =
            open_indexes.bitmap_open_streams(self.family(), start_open_page);

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

        if !same_frontier_page && start_open_page < final_open_page_start {
            if !start_page_streams.is_empty() {
                self.plan_bitmap_page_from_streams(
                    open_indexes,
                    start_open_page,
                    start_page_streams.iter(),
                    &mut compaction_jobs,
                )?;
                record_open_stream_write(
                    &mut open_stream_writes,
                    start_open_page,
                    start_page_streams,
                );
            }
            for (page_global_start, streams) in
                touched_by_page.range((Excluded(start_open_page), Excluded(final_open_page_start)))
            {
                self.plan_bitmap_page_from_streams(
                    open_indexes,
                    *page_global_start,
                    streams.iter(),
                    &mut compaction_jobs,
                )?;
            }
        }
        record_open_stream_write(
            &mut open_stream_writes,
            final_open_page_start,
            final_open_streams,
        );

        let compacted_pages = compact_bitmap_jobs(&compaction_jobs)?;

        // A shard fully seals once the frontier crosses its last page; at that
        // point every page in the shard is compacted, so its per-stream
        // page-count manifests are final and never change again. Roll them up
        // from this batch's compacted page metas plus the already-durable
        // per-page metas of pages this shard sealed in earlier batches.
        let page_count_manifests = self
            .plan_page_count_manifests(
                open_indexes,
                from_next_primary_id,
                next_primary_id,
                &compacted_pages,
            )
            .await?;

        let has_writes = !compacted_pages.is_empty()
            || !open_stream_writes.is_empty()
            || !page_count_manifests.is_empty();
        Ok(BitmapBatchCompactionPlan {
            compacted_pages,
            open_stream_writes,
            page_count_manifests,
            has_writes,
        })
    }

    /// Builds the per-stream page-count manifests for every shard the frontier
    /// fully sealed in this batch. For each newly-sealed shard the streams come
    /// from the durable, append-only open-stream inventory (committed by prior
    /// batches) unioned with the streams this batch is sealing; the per-page
    /// counts come from this batch's compacted metas where available, else the
    /// already-durable per-page metas. The 256 inventory probes per sealed
    /// shard are amortized over a full shard's worth of ingested ids.
    async fn plan_page_count_manifests(
        &self,
        open_indexes: &OpenIndexes,
        from_next_primary_id: u64,
        next_primary_id: u64,
        compacted_pages: &[CompactedPageWrite],
    ) -> Result<Vec<(String, BitmapPageCounts)>> {
        let mut manifests = Vec::new();
        // This batch's compacted page counts, indexed by (stream_id, page) so
        // we never re-read a page whose count we just produced.
        let mut fresh_counts: BTreeMap<(&str, u32), u32> = BTreeMap::new();
        for page in compacted_pages {
            fresh_counts.insert(
                (page.stream_id.as_str(), page.page_start_local),
                page.meta.count,
            );
        }

        for sealed_shard in newly_sealed_shards(from_next_primary_id, next_primary_id) {
            // Map each stream that ever touched this shard to the local page
            // starts it touched, so the per-page meta reads below probe only
            // the cells that actually carry data rather than all 256 pages.
            let mut pages_by_stream: BTreeMap<String, BTreeSet<u32>> = BTreeMap::new();
            for page_idx in 0..STREAM_PAGES_PER_SHARD {
                let page_start_local = page_idx * STREAM_PAGE_LOCAL_ID_SPAN;
                let page_global_start = sealed_shard
                    .saturating_mul(SHARD_LOCAL_ID_SPAN)
                    .saturating_add(u64::from(page_idx) * u64::from(STREAM_PAGE_LOCAL_ID_SPAN));
                // Durable inventory for pages sealed in prior batches.
                for stream in self.bitmap().load_open_streams(page_global_start).await? {
                    pages_by_stream
                        .entry(stream)
                        .or_default()
                        .insert(page_start_local);
                }
                // In-memory projected inventory for pages this batch is sealing
                // (staged to the durable table in the same session, not yet
                // committed when planning runs).
                for stream in open_indexes.bitmap_open_streams(self.family(), page_global_start) {
                    pages_by_stream
                        .entry(stream)
                        .or_default()
                        .insert(page_start_local);
                }
            }

            for (stream_id, pages) in pages_by_stream {
                let mut pairs = Vec::new();
                for page_start_local in pages {
                    let count = if let Some(count) =
                        fresh_counts.get(&(stream_id.as_str(), page_start_local))
                    {
                        *count
                    } else if let Some(artifact) = self
                        .bitmap()
                        .load_page_artifact(&stream_id, page_start_local)
                        .await?
                    {
                        // Pages sealed in earlier batches keep their count in
                        // the durable compacted artifact (its meta header), so
                        // no fragment re-read is needed for the roll-up.
                        artifact.meta.count
                    } else {
                        // Inventory recorded a touch but no compacted page
                        // artifact exists for it. On a sealing shard every
                        // referenced page must already be compacted (fresh in
                        // this batch's `fresh_counts` or durable from a prior
                        // batch), so a missing one means the
                        // ingestion/compaction commit contract is broken. We
                        // cannot silently drop the cell: a page absent from a
                        // PRESENT manifest reads as count 0 on the query side
                        // (`query::bitmap::clause_page_counts` -> `unwrap_or(0)`),
                        // so on this now-sealed shard the page would be skipped
                        // with zero fetches, silently dropping real matches.
                        // Surface it loudly instead.
                        return Err(MonadChainDataError::SealedShardPageMissingArtifact {
                            stream_id: stream_id.clone(),
                            page_start_local,
                        });
                    };
                    pairs.push((page_start_local, count));
                }
                let counts = BitmapPageCounts::from_pairs(pairs);
                if !counts.pages.is_empty() {
                    manifests.push((stream_id, counts));
                }
            }
        }

        Ok(manifests)
    }

    pub fn stage_bitmap_compactions(
        &self,
        w: &mut WriteSession<'_, M, B>,
        plan: &BitmapBatchCompactionPlan,
    ) {
        for page in &plan.compacted_pages {
            self.bitmap().stage_page_artifact(
                w,
                &page.stream_id,
                page.page_start_local,
                &BitmapPageArtifact {
                    meta: page.meta,
                    bitmap_blob: page.blob.clone(),
                },
            );
        }
        for (page_start, streams) in &plan.open_stream_writes {
            self.bitmap().stage_open_streams(w, *page_start, streams);
        }
        for (stream_id, counts) in &plan.page_count_manifests {
            self.bitmap().stage_page_counts(w, stream_id, counts);
        }
    }

    fn plan_bitmap_page_from_streams<'a, I>(
        &self,
        open_indexes: &OpenIndexes,
        page_global_start: u64,
        streams: I,
        compaction_jobs: &mut Vec<BitmapCompactionJob>,
    ) -> Result<()>
    where
        I: IntoIterator<Item = &'a String>,
    {
        let page_start_local = local_page_start(page_global_start);
        for stream_id in streams {
            let fragments =
                open_indexes.bitmap_fragments(self.family(), stream_id, page_start_local);
            compaction_jobs.push(BitmapCompactionJob {
                page_global_start,
                stream_id: stream_id.clone(),
                page_start_local,
                fragments,
            });
        }
        Ok(())
    }
}

/// Shard indices the frontier *fully sealed* moving from `from_next_primary_id`
/// (this batch's first id) to `next_primary_id` (its exclusive end). A shard
/// `S` seals when the frontier reaches `(S + 1) * SHARD_LOCAL_ID_SPAN`; a shard
/// already fully below `from_next_primary_id` sealed in an earlier batch and is
/// excluded. Mirrors `directory_compaction::sealed_ranges`.
fn newly_sealed_shards(from_next_primary_id: u64, next_primary_id: u64) -> Vec<u64> {
    if next_primary_id <= from_next_primary_id {
        return Vec::new();
    }

    let mut shard = from_next_primary_id / SHARD_LOCAL_ID_SPAN;
    let mut out = Vec::new();
    loop {
        let shard_end = shard.saturating_add(1).saturating_mul(SHARD_LOCAL_ID_SPAN);
        if shard_end > next_primary_id {
            break;
        }
        if shard_end > from_next_primary_id {
            out.push(shard);
        }
        shard = shard.saturating_add(1);
    }

    out
}

fn compact_bitmap_jobs(jobs: &[BitmapCompactionJob]) -> Result<Vec<CompactedPageWrite>> {
    jobs.par_iter()
        .map(|job| {
            let (meta, bitmap_blob) = compact_bitmap_page(job.page_start_local, &job.fragments)?;
            Ok(CompactedPageWrite {
                page_global_start: job.page_global_start,
                stream_id: job.stream_id.clone(),
                page_start_local: job.page_start_local,
                meta,
                blob: bitmap_blob,
            })
        })
        .collect()
}

fn batch_bitmap_shape(
    touched_streams_by_page_per_block: &[&BTreeMap<u64, BTreeSet<String>>],
    ranges: &[(u64, u64)],
) -> Option<(u64, u64, BTreeMap<u64, BTreeSet<String>>)> {
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
        for (page, streams) in block_touched {
            touched_by_page
                .entry(*page)
                .or_default()
                .extend(streams.iter().cloned());
        }
    }

    Some((from_next_primary_id?, next_primary_id?, touched_by_page))
}

fn record_open_stream_write(
    open_stream_writes: &mut Vec<(u64, BTreeSet<String>)>,
    page_global_start: u64,
    streams: BTreeSet<String>,
) {
    if streams.is_empty() {
        return;
    }

    open_stream_writes.push((page_global_start, streams));
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

    use super::{
        batch_bitmap_shape, build_compaction_plan, newly_sealed_shards, BitmapCompactionPlan,
        SHARD_LOCAL_ID_SPAN,
    };
    use crate::engine::bitmap::{
        touched_streams_by_page, BitmapFragmentWrite, STREAM_PAGE_LOCAL_ID_SPAN,
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
        let (from, next, grouped) = batch_bitmap_shape(&inputs, &ranges).expect("advancing shape");

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
    }

    #[test]
    fn newly_sealed_shards_reports_each_fully_crossed_shard() {
        // Frontier moves from inside shard 0 to inside shard 2: shards 0 and 1
        // fully sealed; shard 2 is the new open frontier shard.
        assert_eq!(
            newly_sealed_shards(SHARD_LOCAL_ID_SPAN - 2, SHARD_LOCAL_ID_SPAN * 2 + 3),
            vec![0, 1]
        );
        // No advance: nothing seals.
        assert_eq!(
            newly_sealed_shards(SHARD_LOCAL_ID_SPAN + 5, SHARD_LOCAL_ID_SPAN + 5),
            Vec::<u64>::new()
        );
        // Advance within a single shard never crosses its last page.
        assert_eq!(
            newly_sealed_shards(1, SHARD_LOCAL_ID_SPAN - 1),
            Vec::<u64>::new()
        );
        // Landing exactly on the shard boundary seals the lower shard.
        assert_eq!(newly_sealed_shards(0, SHARD_LOCAL_ID_SPAN), vec![0]);
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

    #[tokio::test(flavor = "current_thread")]
    async fn plan_page_count_manifests_rolls_up_sealed_shard_counts() {
        use roaring::RoaringBitmap;

        use crate::{
            engine::{
                bitmap::{
                    encode_bitmap_blob, sharded_stream_id, BitmapBlob, BitmapPageArtifact,
                    BitmapPageMeta,
                },
                family::Family,
                open_index::OpenIndexes,
                tables::Tables,
            },
            store::{InMemoryBlobStore, InMemoryMetaStore},
        };

        let tables = Tables::new(InMemoryMetaStore::default(), InMemoryBlobStore::default());
        let family = tables.family(Family::Log);
        let page_span = STREAM_PAGE_LOCAL_ID_SPAN;

        // Build a sealed shard 0 with two streams across distinct pages, seeding
        // both the durable compacted artifact (the count source for prior-batch
        // pages) and the durable open-stream inventory (the stream-set source).
        let artifact = |count: u32| {
            let bitmap = RoaringBitmap::from_iter(0..count);
            let blob = BitmapBlob {
                min_local: 0,
                max_local: count.saturating_sub(1),
                count,
                bitmap,
            };
            BitmapPageArtifact {
                meta: BitmapPageMeta {
                    min_local: 0,
                    max_local: count.saturating_sub(1),
                    count,
                },
                bitmap_blob: encode_bitmap_blob(&blob).unwrap(),
            }
        };

        let s_a = sharded_stream_id("addr", &[0xAA], 0);
        let s_b = sharded_stream_id("addr", &[0xBB], 0);

        // s_a: page 0 (count 3) and page 2 (count 5). s_b: page 1 (count 7).
        tables
            .seed_bitmap_page_artifact(Family::Log, &s_a, 0, &artifact(3))
            .await;
        tables
            .seed_bitmap_page_artifact(Family::Log, &s_a, 2 * page_span, &artifact(5))
            .await;
        tables
            .seed_bitmap_page_artifact(Family::Log, &s_b, page_span, &artifact(7))
            .await;
        family
            .record_open_bitmap_streams(0, &BTreeSet::from([s_a.clone()]))
            .await
            .unwrap();
        family
            .record_open_bitmap_streams(u64::from(page_span), &BTreeSet::from([s_b.clone()]))
            .await
            .unwrap();
        family
            .record_open_bitmap_streams(2 * u64::from(page_span), &BTreeSet::from([s_a.clone()]))
            .await
            .unwrap();

        // Frontier crosses shard 0's last page => shard 0 fully seals. No
        // fresh compacted pages in this batch (all sealed earlier).
        let manifests = family
            .plan_page_count_manifests(&OpenIndexes::default(), 0, SHARD_LOCAL_ID_SPAN, &[])
            .await
            .expect("plan manifests");

        let by_stream: BTreeMap<_, _> = manifests.into_iter().collect();
        assert_eq!(
            by_stream.get(&s_a).map(|m| m.pages.clone()),
            Some(vec![(0, 3), (2 * page_span, 5)])
        );
        assert_eq!(
            by_stream.get(&s_b).map(|m| m.pages.clone()),
            Some(vec![(page_span, 7)])
        );
        assert_eq!(by_stream.len(), 2);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn plan_page_count_manifests_hard_errors_on_missing_artifact() {
        use crate::{
            engine::{
                bitmap::sharded_stream_id, family::Family, open_index::OpenIndexes, tables::Tables,
            },
            error::MonadChainDataError,
            store::{InMemoryBlobStore, InMemoryMetaStore},
        };

        let tables = Tables::new(InMemoryMetaStore::default(), InMemoryBlobStore::default());
        let family = tables.family(Family::Log);

        let s_a = sharded_stream_id("addr", &[0xAA], 0);

        // Seed the durable open-stream inventory with a (stream, page) touch in
        // sealing shard 0, but write NO compacted artifact for it. On a sealing
        // shard this is a broken commit contract, not a skippable cell.
        family
            .record_open_bitmap_streams(0, &BTreeSet::from([s_a.clone()]))
            .await
            .unwrap();

        // Frontier crosses shard 0's last page => shard 0 fully seals. No fresh
        // compacted pages in this batch, so the missing artifact must surface.
        let err = family
            .plan_page_count_manifests(&OpenIndexes::default(), 0, SHARD_LOCAL_ID_SPAN, &[])
            .await
            .expect_err("missing artifact on a sealing shard must hard-error");

        assert!(matches!(
            err,
            MonadChainDataError::SealedShardPageMissingArtifact {
                stream_id,
                page_start_local: 0,
            } if stream_id == s_a
        ));
    }
}
