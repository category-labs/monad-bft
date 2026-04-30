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
    engine::tables::Tables,
    error::Result,
    logs::{LogMaterializer, QueryLogsRequest, QueryLogsResponse},
    primitives::{range::ResolvedBlockWindow, refs::BlockSpan},
    store::{BlobStore, MetaStore},
};

/// Walks blocks in query order, applying `LogFilter` to each block's logs.
pub async fn execute_block_scan_query<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    request: &QueryLogsRequest,
    window: ResolvedBlockWindow,
) -> Result<QueryLogsResponse> {
    let materializer = LogMaterializer::new(tables);
    let target_log_count = request.envelope.limit;
    let (request_from, request_to) = window.request_endpoints(request.envelope.order);
    let mut logs = Vec::new();
    let mut cursor_block = request_from;

    for block_number in window.iter(request.envelope.order) {
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
        blocks: None,
        span: BlockSpan {
            from_block: request_from,
            to_block: request_to,
            cursor_block,
        },
    })
}
