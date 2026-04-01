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

use crate::{
    error::Result,
    kernel::tables::Tables,
    logs::{LogMaterializer, QueryLogsRequest, QueryLogsResponse},
    primitives::range::ResolvedBlockWindow,
    store::{BlobStore, MetaStore},
    QueryOrder,
};

pub async fn execute_unfiltered_block_query<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    request: &QueryLogsRequest,
    window: ResolvedBlockWindow,
) -> Result<QueryLogsResponse> {
    let materializer = LogMaterializer::new(tables);
    let target_log_count = request.limit;
    let mut logs = Vec::new();
    let mut cursor_block = window.from_block;

    for block_number in block_range(window, request.order) {
        let (block_ref, block_logs) = materializer
            .load_filtered_block_logs_for_block(block_number, request)
            .await?;
        logs.extend(block_logs);
        cursor_block = block_ref;
        if logs.len() >= target_log_count {
            break;
        }
    }

    Ok(QueryLogsResponse {
        logs,
        from_block: window.from_block,
        to_block: window.to_block,
        cursor_block,
    })
}

fn block_range(window: ResolvedBlockWindow, order: QueryOrder) -> impl Iterator<Item = u64> {
    let range = window.from_block.number..=window.to_block.number;
    let (fwd, rev) = match order {
        QueryOrder::Ascending => (Some(range), None),
        QueryOrder::Descending => (None, Some(range.rev())),
    };
    fwd.into_iter().flatten().chain(rev.into_iter().flatten())
}
