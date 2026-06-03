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

pub mod api;
pub mod blocks;
pub mod engine;
pub mod error;
pub mod family;
pub mod logs;
pub mod mem_scan;
pub mod primitives;
pub mod store;
pub mod traces;
pub mod transfers;
pub mod txs;

pub use alloy_primitives::{Address, Bytes, Log, LogData, B256};
pub use api::{
    IngestBatchTimings, IngestOutcome, IngestPlan, IoRetryPolicy, MonadChainDataService,
    ObserveUpstream, PublicationAdvance, VerifyOutcome,
};
pub use blocks::{Block, QueryBlocksRequest, QueryBlocksResponse};
pub use engine::{
    authority::{
        AuthorityState, LeaseAuthority, ReadOnlyAuthority, WriteAuthority, WriteContinuity,
        WriteSession as AuthorityWriteSession,
    },
    digest::{ArtifactChecksum, EMPTY_CHECKSUM},
    family::Family,
    tables::{Tables, WriteOpCounts},
};
pub use error::MonadChainDataError;
pub use family::{CallKind, FinalizedBlock, Hash32, IngestTrace, IngestTx};
pub use logs::{LogEntry, LogFilter, LogsRelations, QueryLogsRequest, QueryLogsResponse};
pub use mem_scan::{scan_block_logs, scan_block_txs, MemLogsBlock, MemTx};
pub use primitives::{
    limits::{LimitExceededKind, QueryEnvelope, QueryLimits},
    page::{QueryOrder, DEFAULT_QUERY_LIMIT},
    refs::{BlockRef, BlockSpan},
    state::{BlockBlobHeader, BlockRecord, FamilyWindowRecord, LogId, PrimaryId, TraceId, TxId},
    EvmBlockHeader,
};
pub use store::{InMemoryBlobStore, InMemoryMetaStore};
pub use traces::{
    compute_trace_addresses, QueryTracesRequest, QueryTracesResponse, TraceEntry, TraceFilter,
    TracesRelations,
};
pub use transfers::{
    QueryTransfersRequest, QueryTransfersResponse, TransferEntry, TransferFilter,
    TransfersRelations,
};
pub use txs::{
    QueryTransactionsRequest, QueryTransactionsResponse, TxEntry, TxFilter, TxsRelations,
};

pub type Topic = B256;
