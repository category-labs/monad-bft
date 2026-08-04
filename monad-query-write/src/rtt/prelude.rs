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

//! Import surface for the round-trip submodules, mirroring the layered
//! crates' public API plus the local fixture DSL.

#![allow(unused_imports)]

pub use alloy_primitives::{Address, Bytes, Log, LogData, B256};
pub use monad_query_engine::{
    bitmap::*,
    clause::*,
    family::*,
    primary_dir::*,
    tables::*,
    test_util::{
        block_record, seed_bitmap_page_artifact, seed_bitmap_page_counts,
        seed_bitmap_page_fragment, stage_block, stage_block_header, test_cache_config,
    },
    txs::TxHashIndexTable,
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
    test_util::*, BlobStore, CacheConfig, InMemoryBlobStore, InMemoryMetaStore, MetaStore,
    NullBlobStore, TableId,
};
pub use monad_query_types::{
    ingest_types::{FinalizedBlock, IngestTrace, IngestTx},
    ExternalFamilyRegion, ExternalPayloadSpec,
};

pub use super::fixtures::*;
pub use crate::testing::{self as populate, *};
