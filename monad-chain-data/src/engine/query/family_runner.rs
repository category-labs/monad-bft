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

use std::sync::Arc;

use futures::{stream, StreamExt, TryStreamExt};
use roaring::RoaringBitmap;

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
/// bounds the bitmap read fan-out. Static for now; tunable independently of
/// [`MATERIALIZE_CONCURRENCY_CEILING`] — they govern different stages and
/// different backends (bitmap pages vs block blobs). A low-tens value overlaps
/// enough page fetches to hide per-fetch latency without flooding the backend.
const PAGE_CONCURRENCY: usize = 32;

/// Ceiling on concurrent record materializations in stage 2. The effective
/// window is `min(limit, this)`, so small-limit queries shrink their
/// read-ahead (and waste fewer cancelled materializations when the block-
/// aligned stop fires) while large queries still overlap a healthy batch.
/// Static for now; tunable independently of [`PAGE_CONCURRENCY`].
const MATERIALIZE_CONCURRENCY_CEILING: usize = 32;

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

    async fn load_record_at(&self, block_number: u64, idx_in_block: usize) -> Result<Self::Record>;

    async fn load_block_ref(&self, block_number: u64) -> Result<BlockRef>;

    async fn load_filtered_block_records(
        &self,
        block_number: u64,
        order: QueryOrder,
        filter: &Self::Filter,
    ) -> Result<(BlockRef, Vec<Self::Record>)>;
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
            async move {
                let Some(page_bitmap) = family_tables
                    .intersect_shard_page(
                        &item.plan,
                        item.page_start,
                        item.local_from,
                        item.local_to,
                    )
                    .await?
                else {
                    return Ok::<_, MonadChainDataError>(Vec::new());
                };

                let mut locations = Vec::new();
                for local in locals_in_query_order(page_bitmap, order) {
                    let id = PrimaryId::from_parts(item.shard, local)?;
                    if let Some(location) = resolver.resolve(id).await? {
                        locations.push(location);
                    }
                }
                Ok(locations)
            }
        })
        .buffered(PAGE_CONCURRENCY)
        // Flatten each page's ordered location vec into one ordered stream.
        .map_ok(|locations| stream::iter(locations.into_iter().map(Ok)))
        .try_flatten();

    // Stage 2 — materialization. `min(limit, ceiling)` gives the cleaner
    // small-limit story: a tiny query only reads a tiny window ahead, so the
    // block-aligned stop below cancels few in-flight materializations.
    let materialize_concurrency = limit.clamp(1, MATERIALIZE_CONCURRENCY_CEILING);
    let mut record_stream = location_stream
        .map_ok(|location: ResolvedPrimaryIdLocation| async move {
            let record = runner
                .load_record_at(location.block_number, location.idx_in_block)
                .await?;
            Ok::<_, MonadChainDataError>((location.block_number, record))
        })
        .try_buffered(materialize_concurrency);

    // Consumer — reproduce the serial loop's limit / block-alignment / cursor
    // contract on the in-order materialized stream. Accumulate matches; once
    // `limit` is reached record the stop block, finish that block, then on the
    // first record from another block return with `cursor_block = stop block`.
    // If the stream exhausts first, `cursor_block = to_block`. `matches` can
    // drop a materialized candidate (e.g. the trace `is_top_level: Some(false)`
    // case), so it gates the `limit` count exactly as before.
    let mut records = Vec::new();
    let mut stop_after_block: Option<u64> = None;

    // Because stage 2 reads ahead (`try_buffered` materializes up to
    // `materialize_concurrency` records before the consumer demands them), it
    // may materialize — and so surface a `load_record_at` error from — a record
    // in the post-limit block that the old serial loop would have stopped before
    // ever reaching. This is acceptable: every buffered candidate sits inside
    // the resolved published window, so under correct operation its
    // materialization succeeds; an error there indicates data-layer corruption
    // and SHOULD surface, consistent with the crate's loud-failure stance. We
    // deliberately do NOT suppress or swallow look-ahead errors (the `?` below
    // propagates them).
    while let Some((block_number, record)) = record_stream.try_next().await? {
        if let Some(stop_block) = stop_after_block {
            if block_number != stop_block {
                let cursor_block = runner.load_block_ref(stop_block).await?;
                // Dropping the stream here cancels any in-flight read-ahead.
                return Ok(IndexedQueryOutcome {
                    records,
                    span: BlockSpan {
                        from_block,
                        to_block,
                        cursor_block,
                    },
                });
            }
        }

        // Most families guarantee `matches` for any candidate the bitmap
        // intersection produces. The trace family violates that guarantee in
        // one case — `is_top_level: Some(false)` combined with other indexed
        // clauses can pick up top-level frames that the runner must drop here.
        // Treat the post-filter as authoritative rather than asserting.
        if !filter.matches(&record) {
            continue;
        }

        records.push(record);

        if stop_after_block.is_none() && records.len() >= limit {
            stop_after_block = Some(block_number);
        }
    }

    Ok(IndexedQueryOutcome {
        records,
        span: BlockSpan {
            from_block,
            to_block,
            cursor_block: to_block,
        },
    })
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
