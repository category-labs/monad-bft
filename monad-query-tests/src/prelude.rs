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

//! The former `monad_chain_data` public surface, re-exported from the origin
//! crates so integration tests import the layered crates directly. Glob-import
//! as `use monad_query_tests::prelude::*;`.

pub use alloy_primitives::{Address, Bytes, Log, LogData, B256};
pub use monad_query_engine::{
    bitmap::*, clause::*, family::*, primary_dir::*, tables::*, SessionFuture, WriteSession,
};
pub use monad_query_errors::{LimitExceededKind, QueryError, Result};
pub use monad_query_primitives::{
    limits::{QueryEnvelope, QueryLimits},
    order::QueryOrder,
    records::*,
    refs::{BlockRef, BlockSpan},
    CallKind, EvmBlockHeader, ExternalBlobReader, Hash32, InMemoryExternalBlobReader,
};
pub use monad_query_read::{
    api::MonadChainDataService,
    blocks::{Block, QueryBlocksRequest, QueryBlocksResponse},
    logs::{LogEntry, LogFilter, LogsRelations, QueryLogsRequest, QueryLogsResponse},
    mem_scan::{scan_block_logs, scan_block_txs, MemLogsBlock, MemTx},
    traces::{
        compute_trace_addresses, QueryTracesRequest, QueryTracesResponse, TraceEntry, TraceFilter,
        TracesRelations,
    },
    transfers::{
        QueryTransfersRequest, QueryTransfersResponse, TransferEntry, TransferFilter,
        TransfersRelations,
    },
    txs::{
        QueryTransactionsRequest, QueryTransactionsResponse, StoredTxEnvelope, TxEntry, TxFilter,
        TxsRelations,
    },
};
pub use monad_query_store::{
    BlobStore, CacheConfig, InMemoryBlobStore, InMemoryMetaStore, MetaStore, NullBlobStore, TableId,
};
pub use monad_query_testkit::*;
pub use monad_query_types::{
    ingest_types::{FinalizedBlock, IngestTrace, IngestTx},
    ExternalFamilyRegion, ExternalPayloadSpec,
};
pub use monad_query_write::{source::ChainDataIngestSource, PayloadMode};

pub use crate::common::{observed_store::*, *};
