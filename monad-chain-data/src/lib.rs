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

//! queryX ingest + RPC-facing layer: configuration, the ingest worker, query
//! materialization into RPC shapes, and external-archive decode. Built on the
//! split-out query crates (errors/primitives/types/store/engine).

// The lower query crates are re-exported under their historical module paths so
// this crate's own modules — and downstream consumers' `monad_chain_data::{...}`
// imports — keep resolving after the split. Transition shims.
pub use monad_query_errors as error;

pub mod store {
    pub use monad_query_engine::{SessionFuture, WriteSession};
    pub use monad_query_store::*;
}

pub mod primitives {
    pub use monad_query_engine::primitives::*;
}

pub mod ingest_types {
    pub use monad_query_primitives::{CallKind, Hash32};
    pub use monad_query_types::ingest_types::*;
}

pub mod engine {
    pub use monad_query_engine::{
        bitmap, clause, digest, family, primary_dir, query, row_codec, seal, tables,
    };
}

pub mod config;
pub mod testkit;

pub use monad_query_read::{api, blocks, external, logs, mem_scan, traces, transfers, txs};
pub use monad_query_write::ingest;

// Root re-exports: the surface intended for external consumers (the transport
// layer and binaries) — service/reader constructors and their config types,
// every query request/response/filter type, ingest wiring, the mem-scan tip
// helpers, and the span/ref primitives those responses carry. Internal types
// stay reachable through their modules but are deliberately not re-exported.
pub use alloy_primitives::{Address, Bytes, Log, LogData, B256};
pub use api::MonadChainDataService;
pub use blocks::{Block, QueryBlocksRequest, QueryBlocksResponse};
pub use config::{
    open_configured_chain_data_reader, run_configured_chain_data_engine_ingest,
    ChainDataArchiveBackendConfig, ChainDataBlobBackendConfig, ChainDataEngineConfig,
    ChainDataMetaBackendConfig, ChainDataPayloadConfig, ChainDataStoreConfig,
    ConfiguredChainDataReader,
};
#[cfg(feature = "dynamo")]
pub use config::{
    ChainDataArchiveDynamoConfig, ChainDataDynamoBlobConfig, ChainDataDynamoMetaConfig,
    ChainDataDynamoTableLayoutConfig,
};
#[cfg(feature = "mongo")]
pub use config::{ChainDataArchiveMongoConfig, ChainDataMongoMetaConfig};
#[cfg(feature = "s3")]
pub use config::{ChainDataArchiveS3Config, ChainDataS3BlobConfig};
pub use engine::{
    family::Family,
    tables::{DictConfig, PublicationTables, QueryRuntimeConfig, Tables},
};
pub use error::{LimitExceededKind, MonadChainDataError};
pub use external::{
    ExternalBlobReader, ExternalFamilyRegion, ExternalPayloadSpec, InMemoryExternalBlobReader,
};
pub use ingest::{source::ChainDataIngestSource, PayloadMode};
pub use ingest_types::{CallKind, FinalizedBlock, Hash32, IngestTrace, IngestTx};
pub use logs::{LogEntry, LogFilter, LogsRelations, QueryLogsRequest, QueryLogsResponse};
pub use mem_scan::{scan_block_logs, scan_block_txs, MemLogsBlock, MemTx};
pub use primitives::{
    limits::{QueryEnvelope, QueryLimits},
    order::QueryOrder,
    records::{BlockBlobHeader, BlockRecord, FamilyWindowRecord, PrimaryId},
    refs::{BlockRef, BlockSpan},
    EvmBlockHeader,
};
pub use store::{CacheConfig, InMemoryBlobStore, InMemoryMetaStore, NullBlobStore};
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

pub type Topic = B256;
