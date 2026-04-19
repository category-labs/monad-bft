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
    engine::{query::family_runner::execute_indexed_family_query, tables::Tables},
    error::Result,
    primitives::range::ResolvedBlockWindow,
    store::{BlobStore, MetaStore},
    txs::{QueryTransactionsRequest, QueryTransactionsResponse, TxMaterializer},
};

pub(crate) async fn execute_indexed_tx_query<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    request: &QueryTransactionsRequest,
    block_window: ResolvedBlockWindow,
) -> Result<QueryTransactionsResponse> {
    let materializer = TxMaterializer::new(tables);
    let outcome = execute_indexed_family_query(
        tables,
        &materializer,
        &request.filter,
        block_window,
        request.envelope.order,
        request.envelope.limit,
    )
    .await?;
    Ok(QueryTransactionsResponse {
        txs: outcome.records,
        blocks: None,
        span: outcome.span,
    })
}
