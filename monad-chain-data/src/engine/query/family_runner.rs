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
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Instant,
};

use futures::{stream, StreamExt, TryStreamExt};
use roaring::RoaringBitmap;
use tracing::debug;

use crate::{
    engine::{
        clause::IndexedFilter,
        family::Family,
        primary_dir::bucket_start,
        query::{
            bitmap::ShardPagePlan,
            directory_resolver::{PrimaryIdResolver, ResolvedPrimaryIdLocation},
            window::resolve_primary_id_window,
        },
        tables::{BlockTables, Tables},
    },
    error::{MonadChainDataError, Result},
    primitives::{
        page::QueryOrder,
        range::ResolvedBlockWindow,
        refs::{BlockRef, BlockSpan},
        state::PrimaryId,
    },
    store::{BlobStore, MetaStore},
};

/// Pages whose per-page clause intersection runs concurrently in stage 1 of the
/// indexed pipeline. Each in-flight page issues its own bitmap fetches, so this
/// bounds the bitmap read fan-out. Static for now; independent of the
/// materialize fan-out (`QueryRuntimeConfig::materialize_concurrency`) — they
/// govern different stages and backends (bitmap pages vs block blobs). A
/// low-tens value overlaps enough page fetches to hide per-fetch latency without
/// flooding the backend.
const PAGE_CONCURRENCY: usize = 32;

// Stage-2 fan-out and the byte-weighted decode budget are operator-tunable via
// `QueryRuntimeConfig` (on `Tables`), not constants: a fast local backend wants
// a modest fan-out (wide fan-out is pure scheduling overhead there) while a
// high-latency backend wants it high so a sparse query reaches ≈ one round-trip.
// See `Tables::materialize_concurrency` / `materialize_budget` /
// `materialize_permit_bytes` and `block_materialize_permits` below.

const MATERIALIZE_SPAN_MAX_BYTES: usize = 512 * 1024;
const MATERIALIZE_SPAN_MAX_GAP_BYTES: usize = 16 * 1024;
pub(crate) const MATERIALIZE_WHOLE_REGION_SPAN_THRESHOLD: usize = 8;
pub(crate) const MATERIALIZE_WHOLE_REGION_MAX_BYTES: usize = 8 * 1024 * 1024;

/// Blocks whose filtered-record loads run concurrently in the block-scan
/// runner. Each in-flight block reads that block's header + blob, so this
/// bounds the block-blob read fan-out. Block blobs are large and uncached by
/// default, so keep this modest (lower than [`PAGE_CONCURRENCY`]) — enough to
/// overlap a handful of block reads without flooding the backend with large
/// payloads.
const BLOCK_SCAN_CONCURRENCY: usize = 16;

/// Outcome returned by the shared family query runners. Families wrap this
/// into their own response types.
pub(crate) struct IndexedQueryOutcome<T> {
    pub records: Vec<T>,
    pub span: BlockSpan,
}

/// Per-family interface consumed by the shared indexed and block-scan
/// runners. Implemented by each family's materializer.
pub(crate) trait IndexedFamilyQuery {
    type Filter: IndexedFilter<Record = Self::Record>;
    type Record;

    fn family() -> Family;

    async fn load_record_at(
        &self,
        block_number: u64,
        idx_in_block: usize,
        stats: &IndexedQueryStats,
    ) -> Result<Self::Record>;

    async fn load_records_in_block(
        &self,
        block_number: u64,
        indices: &[usize],
        stats: &IndexedQueryStats,
    ) -> Result<Vec<Self::Record>> {
        let mut records = Vec::with_capacity(indices.len());
        for idx in indices {
            records.push(self.load_record_at(block_number, *idx, stats).await?);
        }
        Ok(records)
    }

    async fn load_block_ref(&self, block_number: u64) -> Result<BlockRef>;

    async fn load_filtered_block_records(
        &self,
        block_number: u64,
        order: QueryOrder,
        filter: &Self::Filter,
    ) -> Result<(BlockRef, Vec<Self::Record>)>;
}

#[derive(Default)]
pub(crate) struct IndexedQueryStats {
    plan_us: AtomicU64,
    page_intersect_us: AtomicU64,
    page_resolve_us: AtomicU64,
    materialize_us: AtomicU64,
    materialize_block_record_us: AtomicU64,
    materialize_header_us: AtomicU64,
    materialize_blob_frame_us: AtomicU64,
    materialize_decode_row_us: AtomicU64,
    materialize_entry_decode_us: AtomicU64,
    materialize_blocks: AtomicU64,
    materialize_spans: AtomicU64,
    materialize_selected_frame_bytes: AtomicU64,
    materialize_coalesced_read_bytes: AtomicU64,
    materialize_coalesced_overread_bytes: AtomicU64,
    load_cursor_block_ref_us: AtomicU64,
    pages: AtomicU64,
    bitmap_candidates: AtomicU64,
    resolved_locations: AtomicU64,
    materialized_records: AtomicU64,
    emitted_records: AtomicU64,
    post_filter_dropped: AtomicU64,
    work_items: AtomicU64,
}

impl IndexedQueryStats {
    fn add_us(target: &AtomicU64, started: Instant) {
        target.fetch_add(started.elapsed().as_micros() as u64, Ordering::Relaxed);
    }

    fn add_count(target: &AtomicU64, value: u64) {
        target.fetch_add(value, Ordering::Relaxed);
    }

    fn load(target: &AtomicU64) -> u64 {
        target.load(Ordering::Relaxed)
    }

    pub(crate) fn record_materialize_block_record(&self, started: Instant) {
        Self::add_us(&self.materialize_block_record_us, started);
    }

    pub(crate) fn record_materialize_header(&self, started: Instant) {
        Self::add_us(&self.materialize_header_us, started);
    }

    pub(crate) fn record_materialize_blob_frame(&self, started: Instant) {
        Self::add_us(&self.materialize_blob_frame_us, started);
    }

    pub(crate) fn record_materialize_decode_row(&self, started: Instant) {
        Self::add_us(&self.materialize_decode_row_us, started);
    }

    pub(crate) fn record_materialize_entry_decode(&self, started: Instant) {
        Self::add_us(&self.materialize_entry_decode_us, started);
    }

    pub(crate) fn record_materialize_block(&self) {
        Self::add_count(&self.materialize_blocks, 1);
    }

    pub(crate) fn record_materialize_span(&self, read_bytes: usize, selected_bytes: usize) {
        Self::add_count(&self.materialize_spans, 1);
        Self::add_count(&self.materialize_coalesced_read_bytes, read_bytes as u64);
        Self::add_count(
            &self.materialize_selected_frame_bytes,
            selected_bytes as u64,
        );
        Self::add_count(
            &self.materialize_coalesced_overread_bytes,
            read_bytes.saturating_sub(selected_bytes) as u64,
        );
    }
}

#[derive(Clone, Debug)]
pub(crate) struct CoalescedFrame {
    pub output_pos: usize,
    pub idx_in_block: usize,
    pub abs_start: usize,
    pub abs_end: usize,
}

#[derive(Clone, Debug)]
pub(crate) struct CoalescedFrameSpan {
    pub abs_start: usize,
    pub abs_end: usize,
    pub frames: Vec<CoalescedFrame>,
}

pub(crate) fn coalesce_frame_ranges<I, F>(
    indices: I,
    mut range_for_index: F,
) -> Result<Vec<CoalescedFrameSpan>>
where
    I: IntoIterator<Item = usize>,
    F: FnMut(usize) -> Result<(usize, usize)>,
{
    let mut frames = Vec::new();
    for (output_pos, idx_in_block) in indices.into_iter().enumerate() {
        let (abs_start, abs_end) = range_for_index(idx_in_block)?;
        if abs_start > abs_end {
            return Err(MonadChainDataError::Decode("invalid frame range"));
        }
        frames.push(CoalescedFrame {
            output_pos,
            idx_in_block,
            abs_start,
            abs_end,
        });
    }
    frames.sort_by_key(|frame| (frame.abs_start, frame.abs_end, frame.output_pos));

    let mut spans: Vec<CoalescedFrameSpan> = Vec::new();
    for frame in frames {
        let Some(last) = spans.last_mut() else {
            spans.push(CoalescedFrameSpan {
                abs_start: frame.abs_start,
                abs_end: frame.abs_end,
                frames: vec![frame],
            });
            continue;
        };

        let merged_end = last.abs_end.max(frame.abs_end);
        let gap = frame.abs_start.saturating_sub(last.abs_end);
        let merged_len = merged_end.saturating_sub(last.abs_start);
        if gap <= MATERIALIZE_SPAN_MAX_GAP_BYTES && merged_len <= MATERIALIZE_SPAN_MAX_BYTES {
            last.abs_end = merged_end;
            last.frames.push(frame);
        } else {
            spans.push(CoalescedFrameSpan {
                abs_start: frame.abs_start,
                abs_end: frame.abs_end,
                frames: vec![frame],
            });
        }
    }
    Ok(spans)
}

/// Shared indexed query runner. Walks the primary-id window for the
/// runner's family, intersects bitmaps per shard, resolves candidates
/// through the shared directory, and materializes each match through the
/// runner. Completes the current block when `limit` is reached.
pub(crate) async fn execute_indexed_family_query<M, B, R>(
    tables: &Tables<M, B>,
    runner: &R,
    filter: &R::Filter,
    block_window: ResolvedBlockWindow,
    published_head: u64,
    order: QueryOrder,
    limit: usize,
) -> Result<IndexedQueryOutcome<R::Record>>
where
    M: MetaStore,
    B: BlobStore,
    R: IndexedFamilyQuery,
{
    let query_started = Instant::now();
    let stats = Arc::new(IndexedQueryStats::default());
    let plan_started = Instant::now();
    let (from_block, to_block) = block_window.request_endpoints(order);
    let family = R::family();

    let Some(window) = resolve_primary_id_window(tables.blocks(), family, &block_window).await?
    else {
        return Ok(IndexedQueryOutcome {
            records: Vec::new(),
            span: BlockSpan {
                from_block,
                to_block,
                cursor_block: to_block,
            },
        });
    };

    let clauses = filter.indexed_clauses();
    if clauses.is_empty() {
        return Err(MonadChainDataError::InvalidRequest(
            "indexed query requires at least one indexed clause",
        ));
    }

    // A bucket is sealed iff its whole 10k id range sits below the *global*
    // family frontier at the publication head — the family-window end of the
    // published-head block, not the query's high block. Deriving this from the
    // query range would misclassify a globally-sealed bucket whose ids sit
    // above the range as the open bucket. Compute it once and route every
    // candidate analytically.
    let frontier_id = family_frontier_id(tables.blocks(), family, published_head).await?;
    let sealed_below = bucket_start(frontier_id.as_u64());
    // The single open shard at the publication head. Any shard below it is
    // fully sealed and carries an immutable page-count manifest the
    // intersection may use to skip pages; the frontier shard's manifest is a
    // hint only (see `load_intersection_bitmap`).
    let frontier_shard = frontier_id.shard();

    let resolver = PrimaryIdResolver::new(tables.family(family), sealed_below);

    // Build a flat, query-ordered work-list of `(shard, page)` items. Per
    // shard we precompute the intersection plan (clause streams + manifest)
    // once and ask it for the candidate pages in this shard's local range; the
    // manifest's `Some(0)` skip drops guaranteed-empty pages on sealed shards
    // here, so the work-list is dense — we never enqueue a page the serial
    // loop would have skipped with zero fetches. Page order == block order, so
    // emitting pages in query order keeps the whole pipeline globally ordered.
    let mut work_items: Vec<PageWorkItem> = Vec::new();
    for shard in window.shard_iter(order) {
        let (local_from, local_to) = window.local_range_for_shard(shard);
        let Some(plan) = tables
            .family(family)
            .build_shard_page_plan(&clauses, shard, frontier_shard)
            .await?
        else {
            continue;
        };
        let plan = Arc::new(plan);
        let mut pages = plan.candidate_pages(local_from, local_to);
        if order == QueryOrder::Descending {
            pages.reverse();
        }
        for page_start in pages {
            work_items.push(PageWorkItem {
                shard,
                page_start,
                local_from,
                local_to,
                plan: Arc::clone(&plan),
            });
        }
    }
    stats
        .work_items
        .store(work_items.len() as u64, Ordering::Relaxed);
    IndexedQueryStats::add_us(&stats.plan_us, plan_started);

    let family_tables = tables.family(family);

    // Stage 1 — page filtering + resolve. Each page future runs the per-page
    // clause intersection and resolves every survivor (serially, in in-page
    // query order) to a block location. `buffered(N1)` runs N1 page futures
    // concurrently while preserving input order, so the flattened location
    // stream stays globally query-ordered. The shared resolver is `&self`-safe
    // (memo behind a mutex, never held across a fetch await).
    let location_stream = stream::iter(work_items)
        .map(|item| {
            let resolver = &resolver;
            let stats = Arc::clone(&stats);
            async move {
                let mut page_log = PageFutureLog::new(
                    family,
                    item.shard,
                    item.page_start,
                    item.local_from,
                    item.local_to,
                );
                let intersect_started = Instant::now();
                let Some(page_bitmap) = family_tables
                    .intersect_shard_page(
                        &item.plan,
                        item.page_start,
                        item.local_from,
                        item.local_to,
                    )
                    .await?
                else {
                    page_log.intersect_us = intersect_started.elapsed().as_micros() as u64;
                    return Ok::<_, MonadChainDataError>(Vec::new());
                };
                let intersect_us = intersect_started.elapsed().as_micros() as u64;
                page_log.intersect_us = intersect_us;
                page_log.had_bitmap = true;
                stats
                    .page_intersect_us
                    .fetch_add(intersect_us, Ordering::Relaxed);
                IndexedQueryStats::add_count(&stats.pages, 1);

                let mut locations = Vec::new();
                let mut candidates = 0u64;
                let resolve_started = Instant::now();
                for local in locals_in_query_order(page_bitmap, order) {
                    candidates += 1;
                    page_log.candidates = candidates;
                    let id = PrimaryId::from_parts(item.shard, local)?;
                    if let Some(location) = resolver.resolve(id).await? {
                        locations.push(location);
                        page_log.resolved_locations = locations.len() as u64;
                    }
                }
                page_log.resolve_us = resolve_started.elapsed().as_micros() as u64;
                page_log.completed = true;
                stats
                    .page_resolve_us
                    .fetch_add(page_log.resolve_us, Ordering::Relaxed);
                IndexedQueryStats::add_count(&stats.bitmap_candidates, candidates);
                IndexedQueryStats::add_count(&stats.resolved_locations, locations.len() as u64);
                Ok(locations)
            }
        })
        .buffered(PAGE_CONCURRENCY)
        // Flatten each page's ordered location vec into one ordered stream.
        .map_ok(|locations| stream::iter(locations.into_iter().map(Ok)))
        .try_flatten();

    // Stage 2 — count-gated, byte-budgeted block materialization.
    //
    // We pull the globally-ordered location stream into per-block
    // `(block_number, indices)` groups (`next_block_group`, resolving meta only),
    // accumulating until the cumulative candidate count covers the records still
    // needed, then materialize that batch concurrently (bounded by the decode
    // byte budget below). For families where every candidate is emitted
    // (logs/txs/transfers) one batch satisfies the limit and no block past the
    // limit block is ever resolved or decoded — over-fetch is prevented at the
    // resolve layer. The trace `is_top_level: Some(false)` case can drop
    // candidates, so it loops; that is the only path that materializes beyond the
    // optimistic block set, and only within one batch.
    let mut location_stream = Box::pin(location_stream);
    // The location straddling the last group's block boundary (first of the next
    // block). Stashed by `next_block_group` so `carry.is_some()` answers
    // "is there a later block?" for the cursor with a single already-paid resolve.
    let mut carry: Option<ResolvedPrimaryIdLocation> = None;

    // Process-global, byte-weighted decode budget (CPU/bandwidth): a block
    // acquires permits proportional to the bytes it will decode, so a few huge
    // regions run ~exclusively while many small blocks fan out — bounded across
    // all concurrent queries, not just this one.
    let budget = tables.materialize_budget().clone();

    let mut records: Vec<R::Record> = Vec::new();
    let mut stop_block: Option<u64> = None;
    let mut last_materialized_block: Option<u64> = None;
    let mut groups_exhausted = false;

    while stop_block.is_none() && !groups_exhausted {
        // Collect a batch of groups whose cumulative candidate count covers the
        // records still needed. Exact families need exactly this many and never
        // loop; inexact families may come up short after post-filtering.
        let need = limit - records.len();
        let mut batch: Vec<(u64, Vec<usize>)> = Vec::new();
        let mut batch_candidates = 0usize;
        while batch_candidates < need {
            match next_block_group(&mut location_stream, &mut carry).await? {
                Some((block_number, indices)) => {
                    batch_candidates += indices.len();
                    batch.push((block_number, indices));
                }
                None => {
                    groups_exhausted = true;
                    break;
                }
            }
        }
        if batch.is_empty() {
            break;
        }

        let mut materialized = Box::pin(
            stream::iter(batch)
                .map(|(block_number, indices)| {
                    let stats = Arc::clone(&stats);
                    let budget = Arc::clone(&budget);
                    async move {
                        // Acquire the size-weighted decode budget BEFORE the heavy
                        // read+decode so a few huge regions run ~exclusively while
                        // small blocks fan out.
                        let permits =
                            block_materialize_permits(tables, family, block_number, &indices)
                                .await?;
                        let _permit = budget
                            .acquire_many(permits)
                            .await
                            .expect("materialization budget semaphore is never closed");
                        let block_records =
                            materialize_indexed_block_batch(runner, block_number, &indices, &stats)
                                .await?;
                        Ok::<_, MonadChainDataError>((block_number, block_records))
                    }
                })
                .buffered(tables.materialize_concurrency()),
        );

        while let Some((block_number, block_records)) = materialized.try_next().await? {
            // Once the limit block is complete, the first record of any later
            // block ends the page (cursor = limit block). Stop before counting it
            // so we never emit blocks past the limit — that would duplicate rows
            // on the next page. `last_materialized_block` records that a later
            // block exists (proves a non-terminal page below).
            if stop_block.is_some_and(|sb| block_number != sb) {
                last_materialized_block = Some(block_number);
                break;
            }
            last_materialized_block = Some(block_number);
            for record in block_records {
                // Post-filter is authoritative: the trace family can surface
                // candidates that must be dropped here (`is_top_level: Some(false)`
                // with another clause), which is why the batch loop refills when a
                // batch comes up short.
                if !filter.matches(&record) {
                    IndexedQueryStats::add_count(&stats.post_filter_dropped, 1);
                    continue;
                }
                records.push(record);
                IndexedQueryStats::add_count(&stats.emitted_records, 1);
                if stop_block.is_none() && records.len() >= limit {
                    stop_block = Some(block_number);
                }
            }
        }
    }

    // Cursor: the stop block is the page cursor only if a later block exists
    // (non-terminal page). A block materialized after the stop block proves it;
    // otherwise the straddling `carry` location (pulled while closing the last
    // group) does — both already paid, no extra read. If nothing follows, the
    // page is terminal (cursor stays `to_block`).
    let mut cursor_block = to_block;
    if let Some(sb) = stop_block {
        let more = last_materialized_block.is_some_and(|lb| lb > sb) || carry.is_some();
        if more {
            let cursor_started = Instant::now();
            cursor_block = runner.load_block_ref(sb).await?;
            IndexedQueryStats::add_us(&stats.load_cursor_block_ref_us, cursor_started);
        }
    }

    log_indexed_query_stats(
        family,
        query_started,
        &stats,
        records.len(),
        limit,
        tables.materialize_concurrency(),
        stop_block.is_some(),
    );
    Ok(IndexedQueryOutcome {
        records,
        span: BlockSpan {
            from_block,
            to_block,
            cursor_block,
        },
    })
}

/// Pulls the next per-block group `(block_number, indices)` off the ordered
/// location stream, coalescing consecutive same-block locations. The location
/// straddling the block boundary (the first of the next block) is stashed in
/// `carry` for the next call — so after a group is returned, `carry.is_some()`
/// answers "does a later block exist?" with the single boundary resolve already
/// paid, rather than resolving a whole extra group just to check for a next page.
async fn next_block_group<S>(
    locations: &mut S,
    carry: &mut Option<ResolvedPrimaryIdLocation>,
) -> Result<Option<(u64, Vec<usize>)>>
where
    S: futures::Stream<Item = Result<ResolvedPrimaryIdLocation>> + Unpin,
{
    let head = match carry.take() {
        Some(loc) => loc,
        None => match locations.try_next().await? {
            Some(loc) => loc,
            None => return Ok(None),
        },
    };
    let block_number = head.block_number;
    let mut indices = vec![head.idx_in_block];
    loop {
        match locations.try_next().await? {
            Some(loc) if loc.block_number == block_number => indices.push(loc.idx_in_block),
            // Block boundary: stash the straddling location for the next call.
            Some(loc) => {
                *carry = Some(loc);
                break;
            }
            None => break,
        }
    }
    Ok(Some((block_number, indices)))
}

async fn materialize_indexed_block_batch<R>(
    runner: &R,
    block_number: u64,
    indices: &[usize],
    stats: &IndexedQueryStats,
) -> Result<Vec<R::Record>>
where
    R: IndexedFamilyQuery,
{
    if indices.is_empty() {
        return Ok(Vec::new());
    }
    let materialize_started = Instant::now();
    let records = runner
        .load_records_in_block(block_number, indices, stats)
        .await?;
    IndexedQueryStats::add_us(&stats.materialize_us, materialize_started);
    IndexedQueryStats::add_count(&stats.materialized_records, records.len() as u64);
    Ok(records)
}

/// Permits a block should hold against the stage-2 byte budget, sized from the
/// bytes its materialization will actually decode — not the whole block. Logs
/// and txs read only the coalesced *selected* frames (often a few hundred bytes
/// even in a fat block), so weighting them by region size would needlessly
/// serialize sparse queries over large blocks; the trace family reads the whole
/// region, so it is weighted by region size. Clamped to
/// `[1, materialize_budget_permits]` so a single oversized block runs exclusively
/// rather than deadlocking on a permit request above the budget. A missing
/// header yields one permit (materialize surfaces the real error); the header
/// read is a cached meta lookup.
async fn block_materialize_permits<M, B>(
    tables: &Tables<M, B>,
    family: Family,
    block_number: u64,
    indices: &[usize],
) -> Result<u32>
where
    M: MetaStore,
    B: BlobStore,
{
    let Some(header) = tables
        .family(family)
        .load_block_header(block_number)
        .await?
    else {
        return Ok(1);
    };
    let offsets = &header.offsets;
    let region_bytes = *offsets.last().unwrap_or(&0) as usize;
    // The trace family reads the whole region per block; logs/txs read only the
    // selected frames (with coalescing that approaches the region only when most
    // frames are selected — exactly when heavy weighting is warranted).
    let cost_bytes = if family == Family::Trace {
        region_bytes
    } else {
        indices
            .iter()
            .filter(|&&i| i + 1 < offsets.len())
            .map(|&i| (offsets[i + 1] - offsets[i]) as usize)
            .sum()
    };
    let permits = (cost_bytes / tables.materialize_permit_bytes())
        .clamp(1, tables.materialize_budget_permits());
    Ok(permits as u32)
}

fn log_indexed_query_stats(
    family: Family,
    query_started: Instant,
    stats: &IndexedQueryStats,
    rows: usize,
    limit: usize,
    materialize_concurrency: usize,
    stopped_after_limit_block: bool,
) {
    if !tracing::enabled!(target: module_path!(), tracing::Level::DEBUG) {
        return;
    }
    debug!(
        ?family,
        rows,
        limit,
        materialize_concurrency,
        stopped_after_limit_block,
        total_us = query_started.elapsed().as_micros() as u64,
        plan_us = IndexedQueryStats::load(&stats.plan_us),
        page_intersect_us = IndexedQueryStats::load(&stats.page_intersect_us),
        page_resolve_us = IndexedQueryStats::load(&stats.page_resolve_us),
        materialize_us = IndexedQueryStats::load(&stats.materialize_us),
        materialize_block_record_us = IndexedQueryStats::load(&stats.materialize_block_record_us),
        materialize_header_us = IndexedQueryStats::load(&stats.materialize_header_us),
        materialize_blob_frame_us = IndexedQueryStats::load(&stats.materialize_blob_frame_us),
        materialize_decode_row_us = IndexedQueryStats::load(&stats.materialize_decode_row_us),
        materialize_entry_decode_us = IndexedQueryStats::load(&stats.materialize_entry_decode_us),
        materialize_blocks = IndexedQueryStats::load(&stats.materialize_blocks),
        materialize_spans = IndexedQueryStats::load(&stats.materialize_spans),
        materialize_selected_frame_bytes =
            IndexedQueryStats::load(&stats.materialize_selected_frame_bytes),
        materialize_coalesced_read_bytes =
            IndexedQueryStats::load(&stats.materialize_coalesced_read_bytes),
        materialize_coalesced_overread_bytes =
            IndexedQueryStats::load(&stats.materialize_coalesced_overread_bytes),
        load_cursor_block_ref_us = IndexedQueryStats::load(&stats.load_cursor_block_ref_us),
        work_items = IndexedQueryStats::load(&stats.work_items),
        pages = IndexedQueryStats::load(&stats.pages),
        bitmap_candidates = IndexedQueryStats::load(&stats.bitmap_candidates),
        resolved_locations = IndexedQueryStats::load(&stats.resolved_locations),
        materialized_records = IndexedQueryStats::load(&stats.materialized_records),
        emitted_records = IndexedQueryStats::load(&stats.emitted_records),
        post_filter_dropped = IndexedQueryStats::load(&stats.post_filter_dropped),
        "chain-data indexed query stats"
    );
}

/// One `(shard, page)` unit of stage-1 work. Carries the per-shard plan (shared
/// via `Arc` across the shard's pages) and the shard's clipped local range.
struct PageWorkItem {
    shard: u64,
    page_start: u32,
    local_from: u32,
    local_to: u32,
    plan: Arc<ShardPagePlan>,
}

struct PageFutureLog {
    family: Family,
    shard: u64,
    page_start: u32,
    local_from: u32,
    local_to: u32,
    started: Instant,
    intersect_us: u64,
    resolve_us: u64,
    candidates: u64,
    resolved_locations: u64,
    had_bitmap: bool,
    completed: bool,
}

impl PageFutureLog {
    fn new(family: Family, shard: u64, page_start: u32, local_from: u32, local_to: u32) -> Self {
        Self {
            family,
            shard,
            page_start,
            local_from,
            local_to,
            started: Instant::now(),
            intersect_us: 0,
            resolve_us: 0,
            candidates: 0,
            resolved_locations: 0,
            had_bitmap: false,
            completed: false,
        }
    }
}

impl Drop for PageFutureLog {
    fn drop(&mut self) {
        if !tracing::enabled!(target: module_path!(), tracing::Level::DEBUG) {
            return;
        }
        debug!(
            family = ?self.family,
            shard = self.shard,
            page_start = self.page_start,
            local_from = self.local_from,
            local_to = self.local_to,
            had_bitmap = self.had_bitmap,
            completed = self.completed,
            canceled = !self.completed,
            candidates = self.candidates,
            resolved_locations = self.resolved_locations,
            page_total_us = self.started.elapsed().as_micros() as u64,
            intersect_us = self.intersect_us,
            resolve_us = self.resolve_us,
            "chain-data indexed page future stats"
        );
    }
}

/// Shared block-scan runner used when the query filter carries no indexed
/// clause. Walks blocks in query order, lets the family filter each
/// block's records, and stops once `limit` is reached.
pub(crate) async fn execute_block_scan_family_query<R>(
    runner: &R,
    filter: &R::Filter,
    block_window: ResolvedBlockWindow,
    order: QueryOrder,
    limit: usize,
) -> Result<IndexedQueryOutcome<R::Record>>
where
    R: IndexedFamilyQuery,
{
    let (from_block, to_block) = block_window.request_endpoints(order);
    let mut records = Vec::new();
    let mut cursor_block = from_block;

    // Each block is the unit of concurrency: `load_filtered_block_records`
    // reads that block's header + blob, so overlapping them hides per-block I/O
    // latency. `block_window.iter(order)` yields blocks in query order and
    // `buffered` preserves input order, so the consumed stream stays ordered —
    // identical to the serial walk. Dropping the stream on the `break` below
    // cancels any in-flight read-ahead.
    let mut block_stream = stream::iter(block_window.iter(order))
        .map(|block_number| runner.load_filtered_block_records(block_number, order, filter))
        .buffered(BLOCK_SCAN_CONCURRENCY);

    while let Some((block_ref, block_records)) = block_stream.try_next().await? {
        records.extend(block_records);
        cursor_block = block_ref;
        // Block-aligned stop: this path's unit is a whole block's filtered
        // records, so the limit check stays after extending a full block,
        // exactly as the serial loop did.
        if records.len() >= limit {
            break;
        }
    }

    Ok(IndexedQueryOutcome {
        records,
        span: BlockSpan {
            from_block,
            to_block,
            cursor_block,
        },
    })
}

/// Returns the family's global id frontier — the first id not yet assigned —
/// at the publication head. This is the family-window end of the published-head
/// block (`next_primary_id_exclusive`); even a zero-count window carries it,
/// since `first_primary_id` is the running id cursor. If the published head has
/// no block record (no data), there are no sealed buckets, so the frontier is
/// `PrimaryId::ZERO` and every bucket routes to the open-bucket scan path.
async fn family_frontier_id<M: MetaStore>(
    blocks: &BlockTables<M>,
    family: Family,
    published_head: u64,
) -> Result<PrimaryId> {
    let Some(record) = blocks.load_record(published_head).await? else {
        return Ok(PrimaryId::ZERO);
    };
    match family.window_in(&record) {
        Some(window) => window.next_primary_id_exclusive(),
        None => Ok(PrimaryId::ZERO),
    }
}

fn locals_in_query_order(bitmap: RoaringBitmap, order: QueryOrder) -> Vec<u32> {
    let mut locals: Vec<u32> = bitmap.into_iter().collect();
    if order == QueryOrder::Descending {
        locals.reverse();
    }
    locals
}
