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

use roaring::RoaringBitmap;

use crate::{
    engine::{
        clause::IndexedFilter,
        family::Family,
        primary_dir::bucket_start,
        query::{directory_resolver::PrimaryIdResolver, window::resolve_primary_id_window},
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

/// Outcome returned by the shared family query runners. Families wrap this
/// into their own response types.
pub(crate) struct IndexedQueryOutcome<T> {
    pub records: Vec<T>,
    pub span: BlockSpan,
}

/// Per-family interface consumed by the shared indexed and block-scan
/// runners. Implemented by each family's materializer.
pub(crate) trait IndexedFamilyQuery<M: MetaStore, B: BlobStore> {
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
    R: IndexedFamilyQuery<M, B>,
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

    let mut resolver = PrimaryIdResolver::new(tables.family(family), sealed_below);
    let mut records = Vec::new();
    let mut stop_after_block = None;

    for shard in window.shard_iter(order) {
        let (local_from, local_to) = window.local_range_for_shard(shard);

        let Some(candidate_bitmap) = tables
            .family(family)
            .load_intersection_bitmap(&clauses, shard, frontier_shard, local_from, local_to)
            .await?
        else {
            continue;
        };

        let locals = locals_in_query_order(candidate_bitmap, order);
        for local in locals {
            let id = PrimaryId::from_parts(shard, local)?;
            let Some(location) = resolver.resolve(id).await? else {
                continue;
            };

            if let Some(stop_block) = stop_after_block {
                if location.block_number != stop_block {
                    let cursor_block = runner.load_block_ref(stop_block).await?;
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

            let record = runner
                .load_record_at(location.block_number, location.idx_in_block)
                .await?;
            // Most families guarantee `matches` for any candidate the bitmap
            // intersection produces. The trace family violates that
            // guarantee in one case — `is_top_level: Some(false)` combined
            // with other indexed clauses can pick up top-level frames that
            // the runner must drop here. Treat the post-filter as
            // authoritative rather than asserting.
            if !filter.matches(&record) {
                continue;
            }

            records.push(record);

            if stop_after_block.is_none() && records.len() >= limit {
                stop_after_block = Some(location.block_number);
            }
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

/// Shared block-scan runner used when the query filter carries no indexed
/// clause. Walks blocks in query order, lets the family filter each
/// block's records, and stops once `limit` is reached.
pub(crate) async fn execute_block_scan_family_query<M, B, R>(
    runner: &R,
    filter: &R::Filter,
    block_window: ResolvedBlockWindow,
    order: QueryOrder,
    limit: usize,
) -> Result<IndexedQueryOutcome<R::Record>>
where
    M: MetaStore,
    B: BlobStore,
    R: IndexedFamilyQuery<M, B>,
{
    let (from_block, to_block) = block_window.request_endpoints(order);
    let mut records = Vec::new();
    let mut cursor_block = from_block;

    for block_number in block_window.iter(order) {
        let (block_ref, block_records) = runner
            .load_filtered_block_records(block_number, order, filter)
            .await?;
        records.extend(block_records);
        cursor_block = block_ref;
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
