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
use rayon::prelude::*;

use crate::{
    engine::{
        bitmap::{
            compact_bitmap_page, global_page_start, local_page_start, BitmapPageArtifact,
            BitmapPageCounts, BitmapPageMeta,
        },
        family::Family,
        open_index::{
            insert_bitmap_page_count, OpenIndexes, OpenIndexesDelta, OpenIndexesEviction,
        },
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
    pub(crate) page_count_delta: OpenIndexesDelta,
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
    pub sealed_page_count_shards: Vec<u64>,
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
            bitmap_page_count_shards: self
                .sealed_page_count_shards
                .iter()
                .map(|shard| (family, *shard))
                .collect(),
            ..OpenIndexesEviction::default()
        }
    }
}

impl<M: MetaStore> FamilyTables<M> {
    /// Reads the prior open-stream inventory and the sealed-page fragments
    /// across a batch's primary-id ranges, returning the compacted page
    /// artifacts the Phase B meta batch will stage. Pure I/O — no writes.
    pub(crate) async fn plan_bitmap_compactions(
        &self,
        open_indexes: &OpenIndexes,
        committed_open_indexes: &OpenIndexes,
        touched_streams_by_page_per_block: &[&BTreeMap<u64, BTreeSet<String>>],
        ranges: &[(u64, u64)],
    ) -> Result<BitmapBatchCompactionPlan> {
        debug_assert_eq!(touched_streams_by_page_per_block.len(), ranges.len());

        if ranges.is_empty() {
            return Ok(BitmapBatchCompactionPlan {
                compacted_pages: Vec::new(),
                page_count_delta: OpenIndexesDelta::default(),
                open_stream_writes: Vec::new(),
                page_count_manifests: Vec::new(),
                sealed_page_count_shards: Vec::new(),
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
                page_count_delta: OpenIndexesDelta::default(),
                open_stream_writes: Vec::new(),
                page_count_manifests: Vec::new(),
                sealed_page_count_shards: Vec::new(),
                has_writes: false,
            });
        };

        let final_open_page_start = global_page_start(next_primary_id);
        let start_open_page = global_page_start(from_next_primary_id);
        let family = self.family();
        tracing::info!(
            ?family,
            range_count = ranges.len(),
            from_next_primary_id,
            next_primary_id,
            start_open_page,
            final_open_page_start,
            touched_pages = touched_by_page.len(),
            "bitmap compaction planning shape resolved"
        );
        // Pre-batch durable inventory for the start page — the baseline for
        // deciding which open-stream rows actually need writing.
        let durable_start_streams =
            committed_open_indexes.bitmap_open_streams(family, start_open_page);

        let same_frontier_page = start_open_page == final_open_page_start;

        // The open-stream inventory is an append-only set keyed by stream_id, so
        // only streams NOT already durable — i.e. newly touched this batch — need
        // a write. Re-writing the full cumulative set every batch (the projected
        // inventory, which folds in prior batches) was the dominant Phase B
        // write-op source (the 0-byte `*_open_bitmap_stream` re-puts). Diffing
        // against the *committed* baseline is safe because both readers union the
        // durable rows with the in-memory projected inventory: recovery seeds the
        // open index from the durable table, and the page-count manifest roll-up
        // reads `bitmap_open_streams` (kept complete by the per-fragment
        // `delta.bitmap_open_streams`).
        if same_frontier_page {
            // Frontier stayed on one page; nothing seals. Write only new streams.
            if let Some(touched) = touched_by_page.get(&final_open_page_start) {
                let new_streams: BTreeSet<String> = touched
                    .difference(&durable_start_streams)
                    .cloned()
                    .collect();
                record_open_stream_write(
                    &mut open_stream_writes,
                    final_open_page_start,
                    new_streams,
                );
            }
        } else if start_open_page < final_open_page_start {
            let discover_start = Instant::now();
            let mut discovered_pages = 0usize;
            let mut discovered_stream_refs = 0usize;
            // The projected open index is authoritative for live page sealing.
            // Once the frontier moves to `final_open_page_start`, every open
            // bitmap page below it must become a compacted artifact in this
            // batch, even if that page was not touched by the current batch and
            // even if sparse family windows made it older than `start_open_page`.
            // Discovery stays in memory: no durable open-stream inventory scans
            // are needed on the steady path.
            for (page_global_start, streams) in
                open_indexes.bitmap_open_pages_before(family, final_open_page_start)
            {
                discovered_pages += 1;
                discovered_stream_refs += streams.len();
                self.plan_bitmap_page_from_streams(
                    open_indexes,
                    page_global_start,
                    streams.iter(),
                    &mut compaction_jobs,
                )?;
            }
            tracing::info!(
                ?family,
                final_open_page_start,
                discovered_pages,
                discovered_stream_refs,
                compaction_jobs = compaction_jobs.len(),
                elapsed_ms = discover_start.elapsed().as_millis() as u64,
                "bitmap compaction projected open pages discovered"
            );

            // Durably write any new open-stream rows for the starting page. The
            // compacted artifact above uses the full projected inventory; this
            // only records the delta needed for recovery and future roll-ups.
            if let Some(touched) = touched_by_page.get(&start_open_page) {
                let new_streams: BTreeSet<String> = touched
                    .difference(&durable_start_streams)
                    .cloned()
                    .collect();
                record_open_stream_write(&mut open_stream_writes, start_open_page, new_streams);
            }
            // The new frontier page is fresh (the monotonic frontier was never on
            // it before), so every stream it touched this batch is new.
            if let Some(touched) = touched_by_page.get(&final_open_page_start) {
                record_open_stream_write(
                    &mut open_stream_writes,
                    final_open_page_start,
                    touched.clone(),
                );
            }
        }

        let compact_start = Instant::now();
        let compacted_pages = compact_bitmap_jobs(&compaction_jobs)?;
        let mut page_count_delta = OpenIndexesDelta::default();
        for page in &compacted_pages {
            let shard = page.page_global_start / SHARD_LOCAL_ID_SPAN;
            insert_bitmap_page_count(
                &mut page_count_delta,
                family,
                shard,
                page.stream_id.clone(),
                page.page_start_local,
                page.meta.count,
            );
        }
        tracing::info!(
            ?family,
            compaction_jobs = compaction_jobs.len(),
            compacted_pages = compacted_pages.len(),
            elapsed_ms = compact_start.elapsed().as_millis() as u64,
            "bitmap compaction jobs compacted"
        );

        // A shard fully seals once the frontier crosses its last page; at that
        // point every page in the shard is compacted, so its per-stream
        // page-count manifests are final and never change again. Roll them up
        // from this batch's compacted page metas plus the already-durable
        // per-page metas of pages this shard sealed in earlier batches.
        let page_count_manifests = self.plan_page_count_manifests(
            open_indexes,
            from_next_primary_id,
            next_primary_id,
            &compacted_pages,
        )?;
        let sealed_page_count_shards = newly_sealed_shards(from_next_primary_id, next_primary_id);
        tracing::info!(
            ?family,
            compacted_pages = compacted_pages.len(),
            open_stream_writes = open_stream_writes.len(),
            page_count_manifests = page_count_manifests.len(),
            "bitmap compaction planning completed"
        );

        let has_writes = !compacted_pages.is_empty()
            || !open_stream_writes.is_empty()
            || !page_count_manifests.is_empty();
        Ok(BitmapBatchCompactionPlan {
            compacted_pages,
            page_count_delta,
            open_stream_writes,
            page_count_manifests,
            sealed_page_count_shards,
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
    fn plan_page_count_manifests(
        &self,
        open_indexes: &OpenIndexes,
        from_next_primary_id: u64,
        next_primary_id: u64,
        compacted_pages: &[CompactedPageWrite],
    ) -> Result<Vec<(String, BitmapPageCounts)>> {
        let mut manifests = Vec::new();
        let family = self.family();
        let sealed_shards = newly_sealed_shards(from_next_primary_id, next_primary_id);
        tracing::info!(
            ?family,
            from_next_primary_id,
            next_primary_id,
            sealed_shards = sealed_shards.len(),
            fresh_compacted_pages = compacted_pages.len(),
            "bitmap page count manifest planning starting"
        );
        for sealed_shard in sealed_shards {
            let shard_start = Instant::now();
            tracing::info!(
                ?family,
                sealed_shard,
                "bitmap page count manifest shard accumulator read starting"
            );
            let mut pages_by_stream =
                open_indexes.bitmap_page_counts_for_shard(family, sealed_shard);
            for page in compacted_pages {
                if page.page_global_start / SHARD_LOCAL_ID_SPAN != sealed_shard {
                    continue;
                }
                pages_by_stream
                    .entry(page.stream_id.clone())
                    .or_default()
                    .insert(page.page_start_local, page.meta.count);
            }
            let shard_global_start = sealed_shard.saturating_mul(SHARD_LOCAL_ID_SPAN);
            let shard_global_end = sealed_shard
                .saturating_add(1)
                .saturating_mul(SHARD_LOCAL_ID_SPAN);
            for (page_global_start, streams) in
                open_indexes.bitmap_open_pages_before(family, shard_global_end)
            {
                if page_global_start < shard_global_start {
                    continue;
                }
                let page_start_local = local_page_start(page_global_start);
                for stream_id in streams {
                    if pages_by_stream
                        .get(&stream_id)
                        .and_then(|pages| pages.get(&page_start_local))
                        .is_none()
                    {
                        return Err(MonadChainDataError::SealedShardPageMissingArtifact {
                            stream_id,
                            page_start_local,
                        });
                    }
                }
            }
            let page_refs: usize = pages_by_stream.values().map(|pages| pages.len()).sum();
            tracing::info!(
                ?family,
                sealed_shard,
                streams = pages_by_stream.len(),
                page_refs,
                elapsed_ms = shard_start.elapsed().as_millis() as u64,
                "bitmap page count manifest shard accumulator read completed"
            );

            let manifest_start = Instant::now();
            let before_count = manifests.len();
            for (stream_id, pages) in pages_by_stream {
                let counts = BitmapPageCounts::from_pairs(pages.into_iter());
                if !counts.pages.is_empty() {
                    manifests.push((stream_id, counts));
                }
            }
            let shard_manifest_count = manifests.len() - before_count;
            tracing::info!(
                ?family,
                sealed_shard,
                shard_manifests = shard_manifest_count,
                total_manifests = manifests.len(),
                page_refs,
                elapsed_ms = manifest_start.elapsed().as_millis() as u64,
                "bitmap page count manifest shard built from accumulator"
            );
        }

        tracing::info!(
            ?family,
            manifests = manifests.len(),
            "bitmap page count manifest planning completed"
        );
        Ok(manifests)
    }

    pub fn stage_bitmap_compactions<B: BlobStore>(
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

pub(crate) fn sealed_page_count_shards_for_batch(
    touched_streams_by_page_per_block: &[&BTreeMap<u64, BTreeSet<String>>],
    ranges: &[(u64, u64)],
) -> Vec<u64> {
    let Some((from_next_primary_id, next_primary_id, _)) =
        batch_bitmap_shape(touched_streams_by_page_per_block, ranges)
    else {
        return Vec::new();
    };
    newly_sealed_shards(from_next_primary_id, next_primary_id)
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

    #[test]
    fn plan_page_count_manifests_rolls_up_sealed_shard_counts() {
        use crate::{
            engine::{
                bitmap::sharded_stream_id,
                family::Family,
                open_index::{OpenIndexes, OpenIndexesDelta},
                tables::Tables,
            },
            store::{InMemoryBlobStore, InMemoryMetaStore},
        };

        let tables = Tables::new(InMemoryMetaStore::default(), InMemoryBlobStore::default());
        let family = tables.family(Family::Log);
        let page_span = STREAM_PAGE_LOCAL_ID_SPAN;

        let s_a = sharded_stream_id("addr", &[0xAA], 0);
        let s_b = sharded_stream_id("addr", &[0xBB], 0);

        let open_indexes = OpenIndexes::default();
        open_indexes
            .try_apply_delta(OpenIndexesDelta {
                bitmap_open_streams: vec![
                    (Family::Log, 0, s_a.clone()),
                    (Family::Log, u64::from(page_span), s_b.clone()),
                    (Family::Log, 2 * u64::from(page_span), s_a.clone()),
                ],
                bitmap_page_counts: vec![
                    (Family::Log, 0, s_a.clone(), 0, 3),
                    (Family::Log, 0, s_a.clone(), 2 * page_span, 5),
                    (Family::Log, 0, s_b.clone(), page_span, 7),
                ],
                ..OpenIndexesDelta::default()
            })
            .unwrap();

        // Frontier crosses shard 0's last page => shard 0 fully seals. No
        // fresh compacted pages in this batch (all sealed earlier).
        let manifests = family
            .plan_page_count_manifests(&open_indexes, 0, SHARD_LOCAL_ID_SPAN, &[])
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

    #[test]
    fn plan_page_count_manifests_hard_errors_on_missing_accumulator_entry() {
        use crate::{
            engine::{
                bitmap::sharded_stream_id,
                family::Family,
                open_index::{OpenIndexes, OpenIndexesDelta},
                tables::Tables,
            },
            error::MonadChainDataError,
            store::{InMemoryBlobStore, InMemoryMetaStore},
        };

        let tables = Tables::new(InMemoryMetaStore::default(), InMemoryBlobStore::default());
        let family = tables.family(Family::Log);

        let s_a = sharded_stream_id("addr", &[0xAA], 0);

        let open_indexes = OpenIndexes::default();
        open_indexes
            .try_apply_delta(OpenIndexesDelta {
                bitmap_open_streams: vec![(Family::Log, 0, s_a.clone())],
                ..OpenIndexesDelta::default()
            })
            .unwrap();

        // Frontier crosses shard 0's last page => shard 0 fully seals. No fresh
        // compacted pages in this batch, so the missing accumulator count must surface.
        let err = family
            .plan_page_count_manifests(&open_indexes, 0, SHARD_LOCAL_ID_SPAN, &[])
            .expect_err("missing artifact on a sealing shard must hard-error");

        assert!(matches!(
            err,
            MonadChainDataError::SealedShardPageMissingArtifact {
                stream_id,
                page_start_local: 0,
            } if stream_id == s_a
        ));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn plan_bitmap_compactions_seals_every_projected_open_page_below_frontier() {
        use roaring::RoaringBitmap;

        use crate::{
            engine::{
                bitmap::{encode_bitmap_blob, sharded_stream_id, BitmapBlob, BitmapFragmentWrite},
                family::Family,
                open_index::{OpenIndexes, OpenIndexesDelta},
                tables::Tables,
            },
            store::{InMemoryBlobStore, InMemoryMetaStore},
        };

        let tables = Tables::new(InMemoryMetaStore::default(), InMemoryBlobStore::default());
        let family = tables.family(Family::Log);
        let s_a = sharded_stream_id("addr", &[0xAA], 0);

        let mut bitmap = RoaringBitmap::new();
        bitmap.insert(7);
        bitmap.insert(9);
        let fragment = BitmapFragmentWrite {
            stream_id: s_a.clone(),
            page_start_local: 0,
            bitmap_blob: encode_bitmap_blob(&BitmapBlob {
                min_local: 7,
                max_local: 9,
                count: 2,
                bitmap,
            })
            .unwrap(),
        };

        let open_indexes = OpenIndexes::default();
        open_indexes
            .try_apply_delta(OpenIndexesDelta {
                bitmap_fragments: vec![(
                    Family::Log,
                    s_a.clone(),
                    fragment.page_start_local,
                    37_000_000,
                    fragment.bitmap_blob,
                )],
                bitmap_open_streams: vec![(Family::Log, 0, s_a.clone())],
                ..OpenIndexesDelta::default()
            })
            .unwrap();

        // Simulate the sparse-window bug shape: this batch seals shard 0, but
        // the older open page 0 is neither the start page nor touched in the
        // current batch. The live planner must still compact every projected
        // open page below the new frontier before manifest roll-up.
        let touched = BTreeMap::new();
        let plan = family
            .plan_bitmap_compactions(
                &open_indexes,
                &OpenIndexes::default(),
                &[&touched],
                &[(SHARD_LOCAL_ID_SPAN - 1, SHARD_LOCAL_ID_SPAN)],
            )
            .await
            .expect("seal projected open page below frontier");

        assert_eq!(plan.compacted_pages.len(), 1);
        assert_eq!(plan.compacted_pages[0].stream_id, s_a);
        assert_eq!(plan.compacted_pages[0].page_start_local, 0);
        assert_eq!(plan.compacted_pages[0].meta.count, 2);
        assert_eq!(plan.page_count_manifests.len(), 1);
        assert_eq!(plan.page_count_manifests[0].1.pages, vec![(0, 2)]);
    }
}
