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
    engine::{query::family_runner::execute_block_scan_family_query, tables::Tables},
    error::Result,
    primitives::range::ResolvedBlockWindow,
    store::{BlobStore, MetaStore},
    traces::{QueryTracesRequest, QueryTracesResponse, TraceMaterializer},
};

/// Walks blocks in query order, applying `TraceFilter` to each block's
/// traces. Used when the filter has no indexed clause (e.g.
/// `is_top_level: Some(false)` alone, or no filter at all).
pub(crate) async fn execute_block_scan_trace_query<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    request: &QueryTracesRequest,
    window: ResolvedBlockWindow,
) -> Result<QueryTracesResponse> {
    let materializer = TraceMaterializer::new(tables);
    let outcome = execute_block_scan_family_query(
        &materializer,
        &request.filter,
        window,
        request.envelope.order,
        request.envelope.limit,
    )
    .await?;
    let _stats = outcome.stats;
    Ok(QueryTracesResponse {
        traces: outcome.records,
        blocks: None,
        transactions: None,
        span: outcome.span,
    })
}
