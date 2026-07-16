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
use monad_query_primitives::{limits::QueryLimits, records::BlockRecord, EvmBlockHeader, Hash32};
use monad_query_read::{
    blocks::{QueryBlocksRequest, QueryBlocksResponse},
    logs::{QueryLogsRequest, QueryLogsResponse},
    traces::{QueryTracesRequest, QueryTracesResponse},
    transfers::{QueryTransfersRequest, QueryTransfersResponse},
    txs::{QueryTransactionsRequest, QueryTransactionsResponse, TxEntry},
};

#[cfg(all(feature = "s3", feature = "dynamo"))]
use super::build_s3_blob_store;
#[cfg(feature = "dynamo")]
use super::{build_dynamo_blob_store, build_dynamo_meta_store};
#[cfg(any(feature = "dynamo", feature = "mongo"))]
use super::{resolve_cache_config, ChainDataCacheMode};
#[cfg(feature = "dynamo")]
use super::{ChainDataBlobBackendConfig, SharedDynamoConnection};
use super::{ChainDataMetaBackendConfig, ChainDataStoreConfig};

#[derive(Clone)]
pub enum ConfiguredChainDataReader {
    InMemory(
        Arc<
            monad_query_read::api::MonadChainDataService<
                monad_query_store::InMemoryMetaStore,
                monad_query_store::InMemoryBlobStore,
            >,
        >,
    ),
    #[cfg(all(feature = "dynamo", feature = "s3"))]
    DynamoS3(
        Arc<
            monad_query_read::api::MonadChainDataService<
                monad_query_store::DynamoMetaStore,
                monad_query_store::S3BlobStore,
            >,
        >,
    ),
    #[cfg(feature = "dynamo")]
    DynamoDynamo(
        Arc<
            monad_query_read::api::MonadChainDataService<
                monad_query_store::DynamoMetaStore,
                monad_query_store::DynamoBlobStore,
            >,
        >,
    ),
    #[cfg(feature = "dynamo")]
    DynamoNull(
        Arc<
            monad_query_read::api::MonadChainDataService<
                monad_query_store::DynamoMetaStore,
                monad_query_store::NullBlobStore,
            >,
        >,
    ),
    #[cfg(feature = "mongo")]
    MongoNull(
        Arc<
            monad_query_read::api::MonadChainDataService<
                monad_query_store::MongoMetaStore,
                monad_query_store::NullBlobStore,
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
            #[cfg(feature = "mongo")]
            Self::MongoNull($service) => $body,
        }
    };
}

impl ConfiguredChainDataReader {
    pub fn in_memory(
        service: monad_query_read::api::MonadChainDataService<
            monad_query_store::InMemoryMetaStore,
            monad_query_store::InMemoryBlobStore,
        >,
    ) -> Self {
        Self::InMemory(Arc::new(service))
    }

    pub fn limits(&self) -> &QueryLimits {
        with_reader!(self, service => service.limits())
    }

    pub async fn load_published_head(&self) -> monad_query_errors::Result<Option<u64>> {
        with_reader!(self, service => service.publication().load_published_head().await)
    }

    pub async fn block_number_by_hash(
        &self,
        block_hash: &Hash32,
    ) -> monad_query_errors::Result<Option<u64>> {
        with_reader!(self, service => service.tables().blocks().block_number_by_hash(block_hash).await)
    }

    pub async fn load_block_record(
        &self,
        block_number: u64,
    ) -> monad_query_errors::Result<Option<BlockRecord>> {
        with_reader!(self, service => service.tables().blocks().load_record(block_number).await)
    }

    pub async fn query_blocks(
        &self,
        request: QueryBlocksRequest,
    ) -> monad_query_errors::Result<QueryBlocksResponse> {
        with_reader!(self, service => service.query_blocks(request).await)
    }

    pub async fn query_logs(
        &self,
        request: QueryLogsRequest,
    ) -> monad_query_errors::Result<QueryLogsResponse> {
        with_reader!(self, service => service.query_logs(request).await)
    }

    pub async fn query_transactions(
        &self,
        request: QueryTransactionsRequest,
    ) -> monad_query_errors::Result<QueryTransactionsResponse> {
        with_reader!(self, service => service.query_transactions(request).await)
    }

    pub async fn query_traces(
        &self,
        request: QueryTracesRequest,
    ) -> monad_query_errors::Result<QueryTracesResponse> {
        with_reader!(self, service => service.query_traces(request).await)
    }

    pub async fn query_transfers(
        &self,
        request: QueryTransfersRequest,
    ) -> monad_query_errors::Result<QueryTransfersResponse> {
        with_reader!(self, service => service.query_transfers(request).await)
    }

    pub async fn get_transaction(
        &self,
        tx_hash: Hash32,
    ) -> monad_query_errors::Result<Option<(TxEntry, EvmBlockHeader)>> {
        with_reader!(self, service => service.get_transaction(tx_hash).await)
    }

    pub fn take_cache_window_stats(&self) -> Vec<(&'static str, u64, u64)> {
        with_reader!(self, service => service.tables().take_cache_window_stats())
    }
}

/// `external`: a pre-built external payload reader (monad-query-indexer's
/// archive-format readers); required for mongo/dynamo `[store.archive]`
/// backends, `None` lets the S3 backend build from config.
pub async fn open_configured_chain_data_reader(
    store_config: ChainDataStoreConfig,
    limits: QueryLimits,
    external: Option<Arc<dyn monad_query_primitives::ExternalBlobReader>>,
) -> Result<ConfiguredChainDataReader> {
    store_config.validate_reader()?;
    #[cfg(not(any(feature = "dynamo", feature = "mongo")))]
    let _ = (limits, external);
    match &store_config.meta {
        #[cfg(feature = "dynamo")]
        ChainDataMetaBackendConfig::Dynamo(meta_config) => {
            let meta_store = build_dynamo_meta_store(meta_config).await?;
            let shared = Some(meta_store.shared_connection());
            dispatch_dynamo_reader_blob(&store_config, limits, meta_store, shared, external).await
        }
        #[cfg(feature = "mongo")]
        ChainDataMetaBackendConfig::Mongo(meta_config) => {
            use monad_query_engine::tables::DictConfig;

            let meta_store = super::build_mongo_meta_store(meta_config).await?;
            let cache_config = resolve_cache_config(&store_config, ChainDataCacheMode::Reader);
            let mut service = monad_query_read::api::MonadChainDataService::with_all_configs(
                meta_store,
                monad_query_store::NullBlobStore,
                limits,
                cache_config,
                DictConfig::default(),
                store_config.query.to_runtime(),
            );
            if let Some(reader) =
                super::build_external_payload_reader(&store_config.archive, external).await?
            {
                service = service.with_external_payload_reader(reader);
            }
            Ok(ConfiguredChainDataReader::MongoNull(Arc::new(service)))
        }
        #[cfg(not(any(feature = "dynamo", feature = "mongo")))]
        ChainDataMetaBackendConfig::Unavailable => {
            eyre::bail!("chain-data configured reader requires the dynamo or mongo feature")
        }
    }
}

#[cfg(feature = "dynamo")]
async fn dispatch_dynamo_reader_blob(
    store_config: &ChainDataStoreConfig,
    limits: QueryLimits,
    meta_store: monad_query_store::DynamoMetaStore,
    dynamo_connection: Option<SharedDynamoConnection>,
    external: Option<Arc<dyn monad_query_primitives::ExternalBlobReader>>,
) -> Result<ConfiguredChainDataReader> {
    use monad_query_engine::tables::{DictConfig, QueryRuntimeConfig};
    use monad_query_store::{BlobStore, CacheConfig};

    let cache_config = resolve_cache_config(store_config, ChainDataCacheMode::Reader);
    let query_config = store_config.query.to_runtime();
    let external_reader =
        super::build_external_payload_reader(&store_config.archive, external).await?;

    fn service<B: BlobStore>(
        meta_store: monad_query_store::DynamoMetaStore,
        blob_store: B,
        limits: QueryLimits,
        cache_config: CacheConfig,
        query_config: QueryRuntimeConfig,
        external_reader: Option<Arc<dyn monad_query_primitives::ExternalBlobReader>>,
    ) -> Arc<monad_query_read::api::MonadChainDataService<monad_query_store::DynamoMetaStore, B>>
    {
        let mut service = monad_query_read::api::MonadChainDataService::with_all_configs(
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
        None => Ok(ConfiguredChainDataReader::DynamoNull(service(
            meta_store,
            monad_query_store::NullBlobStore,
            limits,
            cache_config,
            query_config,
            external_reader,
        ))),
    }
}
