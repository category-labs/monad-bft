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
pub mod config;
pub mod engine;
pub mod error;
pub mod external;
pub mod ingest;
pub mod ingest_types;
pub mod logs;
pub mod mem_scan;
pub mod primitives;
pub mod store;
pub mod testkit;
pub mod traces;
pub mod transfers;
pub mod txs;

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
#[cfg(feature = "mongo")]
pub use config::{ChainDataArchiveMongoConfig, ChainDataMongoMetaConfig};
#[cfg(feature = "s3")]
pub use config::{ChainDataArchiveS3Config, ChainDataS3BlobConfig};
#[cfg(feature = "dynamo")]
pub use config::{
    ChainDataDynamoBlobConfig, ChainDataDynamoMetaConfig, ChainDataDynamoTableLayoutConfig,
};
pub use engine::{
    family::Family,
    tables::{DictConfig, PublicationTables, QueryRuntimeConfig, Tables},
};
pub use error::MonadChainDataError;
pub use external::{
    ExternalBlobReader, ExternalFamilyRegion, ExternalPayloadSpec, InMemoryExternalBlobReader,
};
pub use ingest::{source::ChainDataIngestSource, PayloadMode};
pub use ingest_types::{CallKind, FinalizedBlock, Hash32, IngestTrace, IngestTx};
pub use logs::{LogEntry, LogFilter, LogsRelations, QueryLogsRequest, QueryLogsResponse};
pub use mem_scan::{scan_block_logs, scan_block_txs, MemLogsBlock, MemTx};
pub use primitives::{
    limits::{LimitExceededKind, QueryEnvelope, QueryLimits},
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
