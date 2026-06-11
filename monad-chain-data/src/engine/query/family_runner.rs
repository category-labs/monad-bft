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

use futures::{stream, stream::FuturesOrdered, StreamExt, TryStreamExt};
use tracing::debug;

use crate::{
    engine::{
        bitmap::page_group_start,
        clause::IndexedFilter,
        family::Family,
        primary_dir::bucket_start,
        query::{
            bitmap::PageGroupPlan,
            directory_resolver::{PrimaryIdResolver, ResolvedPrimaryIdLocation},
            row_cache::RowCache,
            window::resolve_primary_id_window,
        },
        row_codec::RowDecompressor,
        tables::{BlockTables, QueryRuntimeConfig, Tables},
    },
    error::{MonadChainDataError, Result},
    primitives::{
        order::QueryOrder,
        range::ResolvedBlockWindow,
        records::{BlockRecord, PrimaryId},
        refs::{BlockRef, BlockSpan},
    },
    store::{BlobStore, MetaStore},
};

// Stage-1/stage-2 fan-outs, the byte-weighted decode budget, and the
// span-coalescing shape are operator-tunable via `QueryRuntimeConfig`
// (`Tables::query_config`), since the right values depend on backend latency.

/// Stage-0 group-plan look-ahead: how many per-group plans (manifest loads)
/// build concurrently ahead of page intersection. Small — most queries span
/// few page groups, and each plan fans out internally across its clause
/// streams.
const GROUP_PLAN_LOOKAHEAD: usize = 4;

/// Block-scan fan-out; bounds concurrent block-blob reads. Blobs are large
/// and uncached, so this stays lower than the default page-intersection
/// fan-out.
const BLOCK_SCAN_CONCURRENCY: usize = 16;

/// Initial block-scan fan-out, doubled per consumed block up to
/// [`BLOCK_SCAN_CONCURRENCY`]. In-flight blocks are wasted whole-region reads
/// if the limit fills first (e.g. `limit=1`), so read-ahead starts small.
const BLOCK_SCAN_INITIAL_CONCURRENCY: usize = 2;

/// Outcome returned by the shared family query runners. Families wrap this
/// into their own response types.
pub(crate) struct IndexedQueryOutcome<T> {
    pub records: Vec<T>,
    pub span: BlockSpan,
}

// Family-agnostic error wording for the shared materialization helpers.
const ERR_INDEX_OUT_OF_RANGE: &str = "row index out of range";
const ERR_INVALID_RANGE: &str = "invalid row frame range";
const ERR_MISSING_HEADER: &str = "missing family block header";
const ERR_MISSING_BLOB: &str = "missing family block blob";
const ERR_MISSING_RECORD: &str = "missing materialized row";

/// Per-family interface consumed by the shared indexed and block-scan runners.
/// A family supplies only the small hooks (decode/stamp/cache selection); the
/// point, batched, and block-scan read paths are provided here once.
pub(crate) trait IndexedFamilyQuery {
    type Meta: MetaStore;
    type Blob: BlobStore;
    type Filter: IndexedFilter<Record = Self::Record>;
    type Record;
    /// Query-invariant decoded form of one row frame (the stored type minus
    /// per-query stamping); the unit the decoded-row cache holds.
    type StoredRow: Clone + Send + Sync + 'static;

    fn family() -> Family;

    fn tables(&self) -> &Tables<Self::Meta, Self::Blob>;

    /// This family's decoded-row cache instance (transfers share the trace
    /// instance — same frames, same stored rows).
    fn row_cache(&self) -> &RowCache<Self::StoredRow>;

    /// Decodes one already-decompressed row frame into its query-invariant
    /// stored form.
    fn decode_stored(bytes: &[u8]) -> Result<Self::StoredRow>;

    /// Stamps per-query context (block number/hash, idx) onto a stored row,
    /// producing the public record. Borrows so a cached row can be stamped
    /// without giving up the cached copy; the default clones into
    /// [`Self::into_record_owned`].
    fn into_record(
        stored: &Self::StoredRow,
        block_record: &BlockRecord,
        idx_in_block: usize,
    ) -> Result<Self::Record> {
        Self::into_record_owned(stored.clone(), block_record, idx_in_block)
    }

    /// Owned variant of [`Self::into_record`]: stamps a row that will not be
    /// cached, moving it rather than cloning-then-dropping.
    fn into_record_owned(
        stored: Self::StoredRow,
        block_record: &BlockRecord,
        idx_in_block: usize,
    ) -> Result<Self::Record>;

    /// One decompressed frame straight to a record: decode then stamp.
    fn decode_record(
        bytes: &[u8],
        block_record: &BlockRecord,
        idx_in_block: usize,
    ) -> Result<Self::Record> {
        Self::into_record_owned(Self::decode_stored(bytes)?, block_record, idx_in_block)
    }

    /// Decodes one decompressed row frame for the block-scan path. `Ok(None)`
    /// skips the frame: projected families (transfers) store frames that yield
    /// no record. The default suits frames that map one-to-one onto records.
    fn decode_scan_record(
        bytes: &[u8],
        block_record: &BlockRecord,
        idx_in_block: usize,
    ) -> Result<Option<Self::Record>> {
        Self::decode_record(bytes, block_record, idx_in_block).map(Some)
    }

    async fn load_block_ref(&self, block_number: u64) -> Result<BlockRef> {
        Ok(BlockRef::from(
            &load_block_record(self.tables().blocks(), block_number).await?,
        ))
    }

    /// Point read: one row through the batched path, so the row cache, the
    /// single-frame range read (one frame coalesces to one span), and the
    /// decode budget all apply uniformly.
    async fn load_record_at(
        &self,
        block_number: u64,
        idx_in_block: usize,
        stats: &IndexedQueryStats,
    ) -> Result<Self::Record> {
        self.load_records_in_block(block_number, &[idx_in_block], stats)
            .await?
            .pop()
            .ok_or(MonadChainDataError::MissingData(ERR_MISSING_RECORD))
    }

    async fn load_records_in_block(
        &self,
        block_number: u64,
        indices: &[usize],
        stats: &IndexedQueryStats,
    ) -> Result<Vec<Self::Record>> {
        let family = Self::family();

        IndexedQueryStats::add_count(&stats.materialize_blocks, 1);
        let started = Instant::now();
        let block_record = load_block_record(self.tables().blocks(), block_number).await?;
        IndexedQueryStats::add_us(&stats.materialize_block_record_us, started);

        // Probe the row cache for every index first: if all rows are resident,
        // the header and blob are never read. Misses remember their output
        // position so fetched rows slot back in place.
        let cache = self.row_cache();
        let mut rows: Vec<Option<Arc<Self::StoredRow>>> = indices
            .iter()
            .map(|&idx| cache.probe(block_number, idx))
            .collect();
        let misses: Vec<(usize, usize)> = rows
            .iter()
            .enumerate()
            .filter(|(_, row)| row.is_none())
            .map(|(pos, _)| (pos, indices[pos]))
            .collect();

        if !misses.is_empty() {
            let started = Instant::now();
            let header = self
                .tables()
                .family(family)
                .load_blob_header(block_number)
                .await?
                .ok_or(MonadChainDataError::MissingData(ERR_MISSING_HEADER))?;
            IndexedQueryStats::add_us(&stats.materialize_header_us, started);

            // Coalesce only the missed rows; a frame's `output_pos` is the
            // caller's position, so fetched rows slot straight into `rows`.
            let mut spans = coalesce_frame_ranges(
                misses.iter().copied(),
                self.tables().query_config().materialize_span_max_gap_bytes,
                self.tables().query_config().materialize_span_max_bytes,
                |idx_in_block| {
                    if idx_in_block + 1 >= header.offsets.len() {
                        return Err(MonadChainDataError::Decode(ERR_INDEX_OUT_OF_RANGE));
                    }
                    Ok(header.abs_range(idx_in_block))
                },
            )?;
            let selected_bytes: usize = spans
                .iter()
                .flat_map(|span| &span.frames)
                .map(|frame| frame.abs_end.saturating_sub(frame.abs_start))
                .sum();
            let decoder = self
                .tables()
                .block_decoder(family, header.dict_version)
                .await?;
            let mut decompressor = RowDecompressor::new(decoder.as_ref())?;

            // A dense selection collapses to one merged span covering the
            // whole family region: same fetch+decode loop, one range read.
            let budget_bytes = merge_dense_selection_spans(
                &mut spans,
                selected_bytes,
                header.region_range(),
                self.tables().query_config(),
            );

            // Byte-weighted decode budget: acquired only on a cache miss
            // (fully-cached blocks never touch the semaphore) and held across
            // the reads+decodes below.
            let permits = materialize_permits_for_bytes(self.tables(), budget_bytes);
            let _permit = self
                .tables()
                .materialize_budget()
                .acquire_many(permits)
                .await
                .expect("materialization budget semaphore is never closed");

            for span in spans {
                let span_selected_bytes: usize = span
                    .frames
                    .iter()
                    .map(|frame| frame.abs_end.saturating_sub(frame.abs_start))
                    .sum();
                let span_read_bytes = span.abs_end.saturating_sub(span.abs_start);
                IndexedQueryStats::add_count(&stats.materialize_spans, 1);
                IndexedQueryStats::add_count(
                    &stats.materialize_coalesced_read_bytes,
                    span_read_bytes as u64,
                );
                IndexedQueryStats::add_count(
                    &stats.materialize_selected_frame_bytes,
                    span_selected_bytes as u64,
                );
                IndexedQueryStats::add_count(
                    &stats.materialize_coalesced_overread_bytes,
                    span_read_bytes.saturating_sub(span_selected_bytes) as u64,
                );
                let started = Instant::now();
                let span_bytes = self
                    .tables()
                    .read_block_blob_header_range(
                        block_number,
                        &header,
                        span.abs_start,
                        span.abs_end,
                    )
                    .await?
                    .ok_or(MonadChainDataError::MissingData(ERR_MISSING_BLOB))?;
                IndexedQueryStats::add_us(&stats.materialize_blob_frame_us, started);

                for frame in span.frames {
                    let start = frame.abs_start.saturating_sub(span.abs_start);
                    let end = frame.abs_end.saturating_sub(span.abs_start);
                    if start > end || end > span_bytes.len() {
                        return Err(MonadChainDataError::Decode(ERR_INVALID_RANGE));
                    }
                    let started = Instant::now();
                    let bytes = decompressor.decompress(&span_bytes[start..end])?;
                    IndexedQueryStats::add_us(&stats.materialize_decode_row_us, started);
                    let started = Instant::now();
                    let stored = Arc::new(Self::decode_stored(&bytes)?);
                    cache.insert(
                        block_number,
                        frame.idx_in_block,
                        Arc::clone(&stored),
                        bytes.len(),
                    );
                    rows[frame.output_pos] = Some(stored);
                    IndexedQueryStats::add_us(&stats.materialize_entry_decode_us, started);
                }
            }
        }

        rows.into_iter()
            .zip(indices)
            .map(|(row, &idx)| {
                let stored = row.ok_or(MonadChainDataError::MissingData(ERR_MISSING_RECORD))?;
                Self::into_record(&stored, &block_record, idx)
            })
            .collect()
    }

    /// Block-scan materialization: reads the block's whole family region and
    /// decodes every row in query order, keeping the rows the filter accepts.
    /// Used when the query carries no viable indexed clause.
    async fn load_filtered_block_records(
        &self,
        block_number: u64,
        order: QueryOrder,
        filter: &Self::Filter,
    ) -> Result<(BlockRef, Vec<Self::Record>)> {
        let family = Self::family();

        let block_record = load_block_record(self.tables().blocks(), block_number).await?;
        let block_ref = BlockRef::from(&block_record);
        if family.window_in(&block_record).count == 0 {
            return Ok((block_ref, Vec::new()));
        }

        let header = self
            .tables()
            .family(family)
            .load_blob_header(block_number)
            .await?
            .ok_or(MonadChainDataError::MissingData(ERR_MISSING_HEADER))?;

        // Whole-region read+decode: weight the shared byte budget by the
        // region length and hold the permits across the read and the loop.
        let (region_start, region_end) = header.region_range();
        let permits =
            materialize_permits_for_bytes(self.tables(), region_end.saturating_sub(region_start));
        let _permit = self
            .tables()
            .materialize_budget()
            .acquire_many(permits)
            .await
            .expect("materialization budget semaphore is never closed");

        let blob = self
            .tables()
            .read_block_blob_region(block_number, &header)
            .await?
            .ok_or(MonadChainDataError::MissingData(ERR_MISSING_BLOB))?;

        let decoder = self
            .tables()
            .block_decoder(family, header.dict_version)
            .await?;
        let mut decompressor = RowDecompressor::new(decoder.as_ref())?;
        let mut records = Vec::new();
        for idx in order.iterate(0..header.row_count()) {
            if idx + 1 >= header.offsets.len() {
                return Err(MonadChainDataError::Decode(ERR_INDEX_OUT_OF_RANGE));
            }
            let start = header.offsets[idx] as usize;
            let end = header.offsets[idx + 1] as usize;
            if start > end || end > blob.len() {
                return Err(MonadChainDataError::Decode(ERR_INVALID_RANGE));
            }
            let bytes = decompressor.decompress(&blob[start..end])?;
            let Some(record) = Self::decode_scan_record(&bytes, &block_record, idx)? else {
                continue;
            };
            if filter.matches(&record) {
                records.push(record);
            }
        }

        Ok((block_ref, records))
    }
}

async fn load_block_record<M: MetaStore>(
    blocks: &BlockTables<M>,
    block_number: u64,
) -> Result<BlockRecord> {
    blocks
        .load_record(block_number)
        .await?
        .ok_or(MonadChainDataError::MissingData("missing block record"))
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
}

#[derive(Clone, Debug)]
struct CoalescedFrame {
    output_pos: usize,
    idx_in_block: usize,
    abs_start: usize,
    abs_end: usize,
}

#[derive(Clone, Debug)]
struct ReadSpan {
    abs_start: usize,
    abs_end: usize,
    frames: Vec<CoalescedFrame>,
}

/// Sorts the requested `(output_pos, idx_in_block)` frames by byte offset and
/// merges neighbors into read spans: a frame joins the previous span when the
/// gap to it is at most `span_max_gap_bytes` (wasted gap bytes are cheaper
/// than another range read) and the merged span stays within `span_max_bytes`.
fn coalesce_frame_ranges<I, F>(
    indices: I,
    span_max_gap_bytes: usize,
    span_max_bytes: usize,
    mut range_for_index: F,
) -> Result<Vec<ReadSpan>>
where
    I: IntoIterator<Item = (usize, usize)>,
    F: FnMut(usize) -> Result<(usize, usize)>,
{
    let mut frames = Vec::new();
    for (output_pos, idx_in_block) in indices {
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

    let mut spans: Vec<ReadSpan> = Vec::new();
    for frame in frames {
        let Some(last) = spans.last_mut() else {
            spans.push(ReadSpan {
                abs_start: frame.abs_start,
                abs_end: frame.abs_end,
                frames: vec![frame],
            });
            continue;
        };

        let merged_end = last.abs_end.max(frame.abs_end);
        let gap = frame.abs_start.saturating_sub(last.abs_end);
        let merged_len = merged_end.saturating_sub(last.abs_start);
        if gap <= span_max_gap_bytes && merged_len <= span_max_bytes {
            last.abs_end = merged_end;
            last.frames.push(frame);
        } else {
            spans.push(ReadSpan {
                abs_start: frame.abs_start,
                abs_end: frame.abs_end,
                frames: vec![frame],
            });
        }
    }
    Ok(spans)
}

/// When the selection coalesced into more spans than the threshold and the
/// family region is small enough, replaces `spans` with ONE span covering the
/// whole region (one range read instead of per-span ranges). Returns the
/// bytes to charge against the byte-weighted decode budget: the whole region
/// length when merged — that is what the read buffers — otherwise the
/// untouched selection's `selected_bytes`.
fn merge_dense_selection_spans(
    spans: &mut Vec<ReadSpan>,
    selected_bytes: usize,
    (region_start, region_end): (usize, usize),
    query_config: &QueryRuntimeConfig,
) -> usize {
    let region_len = region_end.saturating_sub(region_start);
    if spans.len() <= query_config.materialize_whole_region_span_threshold
        || region_len > query_config.materialize_whole_region_max_bytes
    {
        return selected_bytes;
    }
    let frames = std::mem::take(spans)
        .into_iter()
        .flat_map(|span| span.frames)
        .collect();
    *spans = vec![ReadSpan {
        abs_start: region_start,
        abs_end: region_end,
        frames,
    }];
    region_len
}

/// Dispatches a family query: the indexed runner when the filter emits at
/// least one indexed clause, the block-scan runner otherwise. Families wrap
/// the outcome into their own response types.
pub(crate) async fn run_family_query<R>(
    runner: &R,
    filter: &R::Filter,
    block_window: ResolvedBlockWindow,
    published_head: u64,
    order: QueryOrder,
    limit: usize,
) -> Result<IndexedQueryOutcome<R::Record>>
where
    R: IndexedFamilyQuery,
{
    if filter.has_indexed_clause() {
        execute_indexed_family_query(runner, filter, block_window, published_head, order, limit)
            .await
    } else {
        execute_block_scan_family_query(runner, filter, block_window, order, limit).await
    }
}

/// Shared indexed query runner. Walks the primary-id window for the
/// runner's family, intersects bitmaps page by page, resolves candidates
/// through the shared directory, and materializes each match through the
/// runner. Completes the current block when `limit` is reached.
async fn execute_indexed_family_query<R>(
    runner: &R,
    filter: &R::Filter,
    block_window: ResolvedBlockWindow,
    published_head: u64,
    order: QueryOrder,
    limit: usize,
) -> Result<IndexedQueryOutcome<R::Record>>
where
    R: IndexedFamilyQuery,
{
    let tables = runner.tables();
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

    // Sealing is a function of the *global* family frontier at the publication
    // head, not the query's high block — deriving it from the query range would
    // misclassify a sealed bucket above the range as open.
    let frontier_id = family_frontier_id(tables.blocks(), family, published_head).await?;
    let sealed_below = bucket_start(frontier_id.as_u64());
    // Page groups below the frontier's group are sealed and carry an
    // authoritative page-count manifest; the frontier group has no manifest at
    // all (rows are written only when a group completes; absent ⇒ unknown,
    // never a skip).
    let frontier_group = page_group_start(frontier_id.as_u64());

    let family_tables = tables.family(family);
    let resolver = PrimaryIdResolver::new(family_tables, sealed_below);
    IndexedQueryStats::add_us(&stats.plan_us, plan_started);

    // Stage 0 — per-group planning, streamed: plans build concurrently (a
    // small look-ahead) and flatten lazily into the query-ordered page
    // work-list, so page intersection starts as soon as the first group's
    // plan lands instead of after every group's manifest loads. `buffered`
    // preserves group order, plans flatten to pages in order, and page order
    // == block order, so the pipeline stays globally ordered. The per-group
    // plan drops manifest-proven-empty pages, keeping the list dense.
    let clauses = &clauses;
    let window = &window;
    let work_item_stream = stream::iter(window.group_iter(order))
        .map(|group_start| {
            let stats = Arc::clone(&stats);
            async move {
                let plan_started = Instant::now();
                let (first_page, last_page) = window.page_bounds_in_group(group_start);
                let Some(plan) = family_tables
                    .build_page_group_plan(clauses, group_start, frontier_group)
                    .await?
                else {
                    return Ok::<_, MonadChainDataError>(Vec::new());
                };
                let plan = Arc::new(plan);
                let items: Vec<PageWorkItem> = order
                    .iterate(plan.candidate_pages(first_page, last_page))
                    .map(|page_start| {
                        let (from_offset, to_offset) = window.offsets_in_page(page_start);
                        PageWorkItem {
                            page_start,
                            from_offset,
                            to_offset,
                            plan: Arc::clone(&plan),
                        }
                    })
                    .collect();
                IndexedQueryStats::add_count(&stats.work_items, items.len() as u64);
                IndexedQueryStats::add_us(&stats.plan_us, plan_started);
                Ok(items)
            }
        })
        .buffered(GROUP_PLAN_LOOKAHEAD)
        .map_ok(|items| stream::iter(items.into_iter().map(Ok)))
        .try_flatten();

    // Stage 1 — page intersection + resolve. `buffered` runs page futures
    // concurrently while preserving input order, so the flattened location
    // stream stays globally query-ordered.
    let location_stream = work_item_stream
        .map(|item| {
            let resolver = &resolver;
            let stats = Arc::clone(&stats);
            async move {
                let item = item?;
                let intersect_started = Instant::now();
                let Some(page_bitmap) = family_tables
                    .intersect_group_page(
                        &item.plan,
                        item.page_start,
                        item.from_offset,
                        item.to_offset,
                    )
                    .await?
                else {
                    return Ok::<_, MonadChainDataError>(Vec::new());
                };
                let intersect_us = intersect_started.elapsed().as_micros() as u64;
                IndexedQueryStats::add_count(&stats.page_intersect_us, intersect_us);
                IndexedQueryStats::add_count(&stats.pages, 1);

                let mut locations = Vec::new();
                let mut candidates = 0u64;
                let resolve_started = Instant::now();
                // Roaring's iterator is double-ended, so descending order
                // reverses in place with no intermediate collect.
                for offset in order.iterate(page_bitmap.as_bitmap().iter()) {
                    candidates += 1;
                    let id = PrimaryId::new(item.page_start + u64::from(offset));
                    locations.push(resolver.resolve(id).await?);
                }
                let resolve_us = resolve_started.elapsed().as_micros() as u64;
                IndexedQueryStats::add_count(&stats.page_resolve_us, resolve_us);
                IndexedQueryStats::add_count(&stats.bitmap_candidates, candidates);
                IndexedQueryStats::add_count(&stats.resolved_locations, locations.len() as u64);
                debug!(
                    family = ?family,
                    page_start = item.page_start,
                    from_offset = item.from_offset,
                    to_offset = item.to_offset,
                    candidates,
                    resolved_locations = locations.len() as u64,
                    intersect_us,
                    resolve_us,
                    "chain-data indexed page future stats"
                );
                Ok(locations)
            }
        })
        .buffered(tables.query_config().page_intersect_concurrency)
        // Flatten each page's ordered location vec into one ordered stream.
        .map_ok(|locations| stream::iter(locations.into_iter().map(Ok)))
        .try_flatten();

    // Stage 2 — count-gated, byte-budgeted block materialization: pull the
    // ordered location stream into per-block groups until the candidate count
    // covers the records still needed, then materialize that batch
    // concurrently. Families that emit every candidate (logs/txs/transfers)
    // never decode past the limit block; only the trace post-filter
    // (`is_top_level: Some(false)`) can drop candidates and force another loop.
    let mut location_stream = Box::pin(location_stream);
    // Location straddling the last group's block boundary, stashed by
    // `next_block_group`; `carry.is_some()` answers "is there a later block?"
    // for the cursor without an extra resolve.
    let mut carry: Option<ResolvedPrimaryIdLocation> = None;

    let mut records: Vec<R::Record> = Vec::new();
    let mut stop_block: Option<u64> = None;
    let mut groups_exhausted = false;

    while stop_block.is_none() && !groups_exhausted {
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

        // The process-global, byte-weighted decode budget is acquired inside
        // `load_records_in_block`, after the row-cache probe: fully-cached
        // blocks skip the semaphore entirely.
        let mut materialized = Box::pin(
            stream::iter(batch)
                .map(|(block_number, indices)| {
                    let stats = Arc::clone(&stats);
                    async move {
                        let block_records =
                            materialize_indexed_block_batch(runner, block_number, &indices, &stats)
                                .await?;
                        Ok::<_, MonadChainDataError>((block_number, block_records))
                    }
                })
                .buffered(tables.query_config().materialize_concurrency),
        );

        while let Some((block_number, block_records)) = materialized.try_next().await? {
            // Batching invariant: the batch loop stops at the FIRST group
            // crossing the candidate threshold, so every group before the
            // batch's last carries fewer candidates than records still
            // needed — and a group emits at most its candidate count. The
            // limit can therefore only fill on the batch's final block,
            // after which this stream is exhausted.
            debug_assert!(
                stop_block.is_none(),
                "limit filled before the batch's final block"
            );
            for record in block_records {
                // Post-filter is authoritative: trace candidates can be dropped
                // here, which is why the batch loop refills when short.
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

    // Cursor: the stop block is the page cursor only if a later block exists,
    // proved by the straddling `carry` location (already paid — the limit can
    // only fill on the last materialized group, so `carry` is the sole source
    // of a later block). Otherwise the page is terminal and the cursor stays
    // `to_block`.
    let cursor_block = match stop_block {
        Some(sb) if carry.is_some() => {
            let cursor_started = Instant::now();
            let block_ref = runner.load_block_ref(sb).await?;
            IndexedQueryStats::add_us(&stats.load_cursor_block_ref_us, cursor_started);
            block_ref
        }
        _ => to_block,
    };

    log_indexed_query_stats(
        family,
        query_started,
        &stats,
        records.len(),
        limit,
        tables.query_config().materialize_concurrency,
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

/// Pulls the next per-block `(block_number, indices)` group off the ordered
/// location stream. The location straddling the block boundary is stashed in
/// `carry`, so afterwards `carry.is_some()` answers "does a later block
/// exist?" without resolving an extra group.
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
    // Every group off `next_block_group` is seeded with at least one index.
    debug_assert!(!indices.is_empty());
    let materialize_started = Instant::now();
    let records = runner
        .load_records_in_block(block_number, indices, stats)
        .await?;
    IndexedQueryStats::add_us(&stats.materialize_us, materialize_started);
    IndexedQueryStats::add_count(&stats.materialized_records, records.len() as u64);
    Ok(records)
}

/// Permits a materialization should hold against the process-global,
/// byte-weighted decode budget, sized from the bytes it will read+decode.
/// Clamped to `[1, materialize_budget_permits]` so an oversized block runs
/// exclusively rather than deadlocking.
fn materialize_permits_for_bytes<M, B>(tables: &Tables<M, B>, cost_bytes: usize) -> u32
where
    M: MetaStore,
    B: BlobStore,
{
    let query_config = tables.query_config();
    (cost_bytes / query_config.materialize_permit_bytes)
        .clamp(1, query_config.materialize_budget_permits) as u32
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

/// One page of stage-1 work. Carries the global page start, the window's
/// clipped page-relative offset range, and the per-group plan (shared via
/// `Arc` across the group's pages).
struct PageWorkItem {
    page_start: u64,
    from_offset: u32,
    to_offset: u32,
    plan: Arc<PageGroupPlan>,
}

/// Shared block-scan runner used when the query filter carries no indexed
/// clause. Walks blocks in query order, lets the family filter each
/// block's records, and stops once `limit` is reached.
async fn execute_block_scan_family_query<R>(
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

    // `FuturesOrdered` preserves input order, so the consumed stream matches
    // the serial walk; dropping the set on `break` cancels in-flight
    // read-ahead. The fan-out ramps up by doubling per consumed block so a
    // scan that fills its limit early wastes few whole-region reads.
    let mut blocks = block_window.iter(order);
    let mut in_flight = FuturesOrdered::new();
    let mut target = BLOCK_SCAN_INITIAL_CONCURRENCY;
    loop {
        while in_flight.len() < target {
            let Some(block_number) = blocks.next() else {
                break;
            };
            in_flight.push_back(runner.load_filtered_block_records(block_number, order, filter));
        }
        let Some((block_ref, block_records)) = in_flight.try_next().await? else {
            break;
        };
        records.extend(block_records);
        cursor_block = block_ref;
        // Block-aligned stop: the limit check runs only after extending a
        // whole block's records.
        if records.len() >= limit {
            break;
        }
        target = (target * 2).min(BLOCK_SCAN_CONCURRENCY);
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

/// The family's global id frontier (first id not yet assigned) at the
/// publication head: the published-head block's family-window end — carried
/// even by a zero-count window. No block record means no data, so the
/// frontier is `PrimaryId::ZERO` and every bucket routes to the scan path.
async fn family_frontier_id<M: MetaStore>(
    blocks: &BlockTables<M>,
    family: Family,
    published_head: u64,
) -> Result<PrimaryId> {
    let Some(record) = blocks.load_record(published_head).await? else {
        return Ok(PrimaryId::ZERO);
    };
    family.window_in(&record).next_primary_id_exclusive()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One single-frame span of `[abs_start, abs_end)`.
    fn span(abs_start: usize, abs_end: usize) -> ReadSpan {
        ReadSpan {
            abs_start,
            abs_end,
            frames: vec![CoalescedFrame {
                output_pos: 0,
                idx_in_block: 0,
                abs_start,
                abs_end,
            }],
        }
    }

    /// `count` sparse ~200-byte spans spread across an 8 MiB region.
    fn sparse_spans(count: usize) -> (Vec<ReadSpan>, usize) {
        let spans: Vec<ReadSpan> = (0..count).map(|i| span(i << 19, (i << 19) + 200)).collect();
        let selected_bytes = count * 200;
        (spans, selected_bytes)
    }

    #[test]
    fn dense_merge_charges_decode_budget_for_the_whole_region() {
        let config = QueryRuntimeConfig::default();
        let region = (0, config.materialize_whole_region_max_bytes);
        let (mut spans, selected_bytes) =
            sparse_spans(config.materialize_whole_region_span_threshold + 1);

        let budget_bytes = merge_dense_selection_spans(&mut spans, selected_bytes, region, &config);

        // The merged read buffers the whole region, so the budget must be
        // charged for the region length, not the pre-merge selected bytes.
        assert_eq!(budget_bytes, region.1 - region.0);
        assert_eq!(spans.len(), 1);
        assert_eq!((spans[0].abs_start, spans[0].abs_end), region);
        assert_eq!(
            spans[0].frames.len(),
            config.materialize_whole_region_span_threshold + 1
        );
    }

    #[test]
    fn sparse_selection_keeps_spans_and_selected_byte_cost() {
        let config = QueryRuntimeConfig::default();
        let region = (0, config.materialize_whole_region_max_bytes);

        // At (not above) the span threshold: untouched, selected-byte cost.
        let (mut spans, selected_bytes) =
            sparse_spans(config.materialize_whole_region_span_threshold);
        let budget_bytes = merge_dense_selection_spans(&mut spans, selected_bytes, region, &config);
        assert_eq!(budget_bytes, selected_bytes);
        assert_eq!(spans.len(), config.materialize_whole_region_span_threshold);

        // Above the threshold but the region exceeds the whole-region cap.
        let (mut spans, selected_bytes) =
            sparse_spans(config.materialize_whole_region_span_threshold + 1);
        let too_large = (0, config.materialize_whole_region_max_bytes + 1);
        let budget_bytes =
            merge_dense_selection_spans(&mut spans, selected_bytes, too_large, &config);
        assert_eq!(budget_bytes, selected_bytes);
        assert_eq!(
            spans.len(),
            config.materialize_whole_region_span_threshold + 1
        );
    }
}
