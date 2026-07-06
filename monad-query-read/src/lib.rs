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

//! queryX read path: the `MonadChainDataService` query service, block/mem-scan
//! helpers, per-family materialization into RPC-facing entry types, and the
//! external-archive decode mirrors.

pub mod api;
pub mod blocks;
pub mod external;
pub mod logs;
pub mod mem_scan;
pub mod traces;
pub mod transfers;
pub mod txs;

pub use api::MonadChainDataService;
pub use blocks::{Block, QueryBlocksRequest, QueryBlocksResponse};
pub use logs::{LogEntry, LogFilter, LogsRelations, QueryLogsRequest, QueryLogsResponse};
pub use mem_scan::{scan_block_logs, scan_block_txs, MemLogsBlock, MemTx};
pub use traces::{
    compute_trace_addresses, QueryTracesRequest, QueryTracesResponse, TraceEntry, TraceFilter,
    TracesRelations,
};
pub use transfers::{
    QueryTransfersRequest, QueryTransfersResponse, TransferEntry, TransferFilter,
    TransfersRelations,
};
pub use txs::{
    QueryTransactionsRequest, QueryTransactionsResponse, StoredTxEnvelope, TxEntry, TxFilter,
    TxsRelations,
};
