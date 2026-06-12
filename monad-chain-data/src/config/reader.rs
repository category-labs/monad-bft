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

//! The configured read-only query service: backend dispatch over the
//! meta × blob backend product, monomorphized behind one enum.

use std::sync::Arc;

use eyre::Result;

#[cfg(all(feature = "s3", feature = "dynamo"))]
use super::build_s3_blob_store;
#[cfg(feature = "dynamo")]
use super::{build_dynamo_blob_store, build_dynamo_meta_store};
#[cfg(feature = "dynamo")]
use super::{
    resolve_cache_config, ChainDataBlobBackendConfig, ChainDataCacheMode, SharedDynamoConnection,
};
use super::{ChainDataMetaBackendConfig, ChainDataStoreConfig};
use crate::{
    blocks::{QueryBlocksRequest, QueryBlocksResponse},
    logs::{QueryLogsRequest, QueryLogsResponse},
    primitives::{records::BlockRecord, EvmBlockHeader},
    traces::{QueryTracesRequest, QueryTracesResponse},
    transfers::{QueryTransfersRequest, QueryTransfersResponse},
    txs::{QueryTransactionsRequest, QueryTransactionsResponse, TxEntry},
    Hash32, QueryLimits,
};

#[derive(Clone)]
pub enum ConfiguredChainDataReader {
    /// In-process in-memory stores: testkit fixtures and embedded harnesses.
    InMemory(
        Arc<
            crate::MonadChainDataService<
                crate::store::InMemoryMetaStore,
                crate::store::InMemoryBlobStore,
            >,
        >,
    ),
    #[cfg(all(feature = "dynamo", feature = "s3"))]
    DynamoS3(
        Arc<crate::MonadChainDataService<crate::store::DynamoMetaStore, crate::store::S3BlobStore>>,
    ),
    #[cfg(feature = "dynamo")]
    DynamoDynamo(
        Arc<
            crate::MonadChainDataService<
                crate::store::DynamoMetaStore,
                crate::store::DynamoBlobStore,
            >,
        >,
    ),
    /// Dynamo meta with NO blob store (`[store.blob]` omitted): a reader of an
    /// external-archive store, whose row payloads are range-read from
    /// `[store.archive]` and never touch the blob backend. Reading a NATIVE
    /// block record through this variant (a store that was actually ingested
    /// with pack blobs) surfaces [`crate::store::NullBlobStore`]'s loud
    /// `Backend` error rather than silently returning nothing.
    #[cfg(feature = "dynamo")]
    DynamoNull(
        Arc<
            crate::MonadChainDataService<
                crate::store::DynamoMetaStore,
                crate::store::NullBlobStore,
            >,
        >,
    ),
}

macro_rules! with_reader {
    ($self:expr, $service:ident => $body:expr) => {
        match $self {
            Self::InMemory($service) => $body,
            #[cfg(all(feature = "dynamo", feature = "s3"))]
            Self::DynamoS3($service) => $body,
            #[cfg(feature = "dynamo")]
            Self::DynamoDynamo($service) => $body,
            #[cfg(feature = "dynamo")]
            Self::DynamoNull($service) => $body,
        }
    };
}

impl ConfiguredChainDataReader {
    /// Wraps an in-memory service (e.g. one over
    /// [`crate::testkit::populate_via_engine`] stores) in the configured-reader
    /// dispatch, so transport layers can be exercised without a backend.
    pub fn in_memory(
        service: crate::MonadChainDataService<
            crate::store::InMemoryMetaStore,
            crate::store::InMemoryBlobStore,
        >,
    ) -> Self {
        Self::InMemory(Arc::new(service))
    }

    pub fn limits(&self) -> &QueryLimits {
        with_reader!(self, service => service.limits())
    }

    pub async fn load_published_head(&self) -> crate::error::Result<Option<u64>> {
        with_reader!(self, service => service.publication().load_published_head().await)
    }

    pub async fn block_number_by_hash(
        &self,
        block_hash: &Hash32,
    ) -> crate::error::Result<Option<u64>> {
        with_reader!(self, service => service.tables().blocks().block_number_by_hash(block_hash).await)
    }

    pub async fn load_block_record(
        &self,
        block_number: u64,
    ) -> crate::error::Result<Option<BlockRecord>> {
        with_reader!(self, service => service.tables().blocks().load_record(block_number).await)
    }

    pub async fn query_blocks(
        &self,
        request: QueryBlocksRequest,
    ) -> crate::error::Result<QueryBlocksResponse> {
        with_reader!(self, service => service.query_blocks(request).await)
    }

    pub async fn query_logs(
        &self,
        request: QueryLogsRequest,
    ) -> crate::error::Result<QueryLogsResponse> {
        with_reader!(self, service => service.query_logs(request).await)
    }

    pub async fn query_transactions(
        &self,
        request: QueryTransactionsRequest,
    ) -> crate::error::Result<QueryTransactionsResponse> {
        with_reader!(self, service => service.query_transactions(request).await)
    }

    pub async fn query_traces(
        &self,
        request: QueryTracesRequest,
    ) -> crate::error::Result<QueryTracesResponse> {
        with_reader!(self, service => service.query_traces(request).await)
    }

    pub async fn query_transfers(
        &self,
        request: QueryTransfersRequest,
    ) -> crate::error::Result<QueryTransfersResponse> {
        with_reader!(self, service => service.query_transfers(request).await)
    }

    pub async fn get_transaction(
        &self,
        tx_hash: Hash32,
    ) -> crate::error::Result<Option<(TxEntry, EvmBlockHeader)>> {
        with_reader!(self, service => service.get_transaction(tx_hash).await)
    }

    pub fn take_cache_window_stats(&self) -> Vec<(&'static str, u64, u64)> {
        with_reader!(self, service => service.tables().take_cache_window_stats())
    }

    #[cfg(feature = "dynamo")]
    pub fn take_dynamo_meta_read_stats(&self) -> Option<crate::store::DynamoMetaReadStatsSnapshot> {
        match self {
            Self::InMemory(_) => None,
            #[cfg(feature = "s3")]
            Self::DynamoS3(service) => Some(service.tables().meta_store().take_read_stats()),
            Self::DynamoDynamo(service) => Some(service.tables().meta_store().take_read_stats()),
            Self::DynamoNull(service) => Some(service.tables().meta_store().take_read_stats()),
        }
    }

    #[cfg(feature = "s3")]
    pub fn take_s3_read_stats(&self) -> Option<crate::store::S3ReadStatsSnapshot> {
        match self {
            Self::InMemory(_) => None,
            #[cfg(feature = "dynamo")]
            Self::DynamoS3(service) => Some(service.tables().blob_store().take_read_stats()),
            #[cfg(feature = "dynamo")]
            Self::DynamoDynamo(_) => None,
            #[cfg(feature = "dynamo")]
            Self::DynamoNull(_) => None,
        }
    }
}

pub async fn open_configured_chain_data_reader(
    store_config: ChainDataStoreConfig,
    limits: QueryLimits,
) -> Result<ConfiguredChainDataReader> {
    store_config.validate_reader()?;
    #[cfg(not(feature = "dynamo"))]
    let _ = limits;
    match &store_config.meta {
        #[cfg(feature = "dynamo")]
        ChainDataMetaBackendConfig::Dynamo(meta_config) => {
            let meta_store = build_dynamo_meta_store(meta_config).await?;
            let shared = Some(meta_store.shared_connection());
            dispatch_dynamo_reader_blob(&store_config, limits, meta_store, shared).await
        }
        #[cfg(not(feature = "dynamo"))]
        ChainDataMetaBackendConfig::Unavailable => {
            eyre::bail!("chain-data configured reader requires the dynamo feature")
        }
    }
}

#[cfg(feature = "dynamo")]
async fn dispatch_dynamo_reader_blob(
    store_config: &ChainDataStoreConfig,
    limits: QueryLimits,
    meta_store: crate::store::DynamoMetaStore,
    dynamo_connection: Option<SharedDynamoConnection>,
) -> Result<ConfiguredChainDataReader> {
    use crate::{
        engine::tables::{DictConfig, QueryRuntimeConfig},
        store::{BlobStore, CacheConfig},
    };

    let cache_config = resolve_cache_config(store_config, ChainDataCacheMode::Reader);
    let query_config = store_config.query.to_runtime();
    let external_reader = super::build_external_payload_reader(&store_config.archive).await?;

    fn service<B: BlobStore>(
        meta_store: crate::store::DynamoMetaStore,
        blob_store: B,
        limits: QueryLimits,
        cache_config: CacheConfig,
        query_config: QueryRuntimeConfig,
        external_reader: Option<Arc<dyn crate::external::ExternalBlobReader>>,
    ) -> Arc<crate::MonadChainDataService<crate::store::DynamoMetaStore, B>> {
        let mut service = crate::MonadChainDataService::with_all_configs(
            meta_store,
            blob_store,
            limits,
            cache_config,
            DictConfig::default(),
            query_config,
        );
        if let Some(reader) = external_reader {
            service = service.with_external_payload_reader(reader);
        }
        Arc::new(service)
    }

    match &store_config.blob {
        #[cfg(feature = "s3")]
        Some(ChainDataBlobBackendConfig::S3(blob_config)) => {
            let blob_store = build_s3_blob_store(blob_config).await?;
            Ok(ConfiguredChainDataReader::DynamoS3(service(
                meta_store,
                blob_store,
                limits,
                cache_config,
                query_config,
                external_reader,
            )))
        }
        Some(ChainDataBlobBackendConfig::Dynamo(blob_config)) => {
            let blob_store = build_dynamo_blob_store(blob_config, dynamo_connection).await?;
            Ok(ConfiguredChainDataReader::DynamoDynamo(service(
                meta_store,
                blob_store,
                limits,
                cache_config,
                query_config,
                external_reader,
            )))
        }
        // No blob store: valid for readers of external-archive stores (see the
        // `DynamoNull` variant docs for what a native read does here).
        None => Ok(ConfiguredChainDataReader::DynamoNull(service(
            meta_store,
            crate::store::NullBlobStore,
            limits,
            cache_config,
            query_config,
            external_reader,
        ))),
    }
}
