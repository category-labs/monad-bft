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

use super::{
    bitmap::load_clause_bitmap_for_shard, directory_resolver::LogIdResolver,
    window::resolve_log_window,
};
use crate::{
    error::{MonadChainDataError, Result},
    kernel::tables::Tables,
    logs::{LogMaterializer, QueryLogsRequest, QueryLogsResponse},
    primitives::{page::QueryOrder, range::ResolvedBlockWindow, state::LogId},
    store::{BlobStore, MetaStore},
};

pub(crate) async fn execute_indexed_log_query<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    request: &QueryLogsRequest,
    block_window: ResolvedBlockWindow,
) -> Result<QueryLogsResponse> {
    let (request_from, request_to) = block_window.request_endpoints(request.order);

    let Some(log_window) = resolve_log_window(tables, &block_window).await? else {
        return Ok(QueryLogsResponse {
            logs: Vec::new(),
            from_block: request_from,
            to_block: request_to,
            cursor_block: request_to,
        });
    };

    let clauses = request.filter.indexed_clauses();
    if clauses.is_empty() {
        return Err(MonadChainDataError::InvalidRequest(
            "indexed query requires at least one indexed clause",
        ));
    }

    let materializer = LogMaterializer::new(tables);
    let mut resolver = LogIdResolver::new(tables);
    let mut logs = Vec::new();
    let mut stop_after_block = None;

    for shard in log_window.shard_iter(request.order) {
        let (local_from, local_to) = log_window.local_range_for_shard(shard);

        let Some(candidate_bitmap) =
            load_candidate_bitmap_for_shard(tables, &clauses, shard, local_from, local_to).await?
        else {
            continue;
        };

        let locals = locals_in_query_order(candidate_bitmap, request.order);
        for local in locals {
            let id = LogId::from_parts(shard, local);
            let Some(location) = resolver.resolve(id).await? else {
                continue;
            };

            if let Some(stop_block) = stop_after_block {
                if location.block_number != stop_block {
                    let cursor_block = materializer.load_block_ref(stop_block).await?;
                    return Ok(QueryLogsResponse {
                        logs,
                        from_block: request_from,
                        to_block: request_to,
                        cursor_block,
                    });
                }
            }

            let log = materializer
                .load_log_at(location.block_number, location.log_block_idx)
                .await?;
            debug_assert!(
                request.filter.matches(&log),
                "indexed candidate at block {} idx {} should match filter",
                location.block_number,
                location.log_block_idx,
            );

            logs.push(log);

            if stop_after_block.is_none() && logs.len() >= request.limit {
                stop_after_block = Some(location.block_number);
            }
        }
    }

    Ok(QueryLogsResponse {
        logs,
        from_block: request_from,
        to_block: request_to,
        cursor_block: request_to,
    })
}

async fn load_candidate_bitmap_for_shard<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    clauses: &[crate::logs::IndexedLogClause],
    shard: u64,
    local_from: u32,
    local_to: u32,
) -> Result<Option<RoaringBitmap>> {
    let mut accumulator: Option<RoaringBitmap> = None;

    for clause in clauses {
        let clause_bitmap =
            load_clause_bitmap_for_shard(tables.logs(), clause, shard, local_from, local_to)
                .await?;
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

    Ok(accumulator.filter(|bitmap| !bitmap.is_empty()))
}

fn locals_in_query_order(bitmap: RoaringBitmap, order: QueryOrder) -> Vec<u32> {
    let mut locals: Vec<u32> = bitmap.into_iter().collect();
    if order == QueryOrder::Descending {
        locals.reverse();
    }
    locals
}
