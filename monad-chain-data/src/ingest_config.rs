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

use std::{
    path::{Path, PathBuf},
    sync::{atomic::AtomicU64, Arc},
};

use eyre::{bail, Context, Result};
use tracing::{info, warn};

use crate::{
    blocks::{QueryBlocksRequest, QueryBlocksResponse},
    ingest_runner::{
        build_reader_writer_service, run_chain_data_ingest, ChainDataIngestConfig,
        ChainDataIngestSource,
    },
    logs::{QueryLogsRequest, QueryLogsResponse},
    primitives::state::BlockRecord,
    store::{BlobStore, CacheConfig, FjallStore, FjallTuning, MetaStoreCas},
    traces::{QueryTracesRequest, QueryTracesResponse},
    transfers::{QueryTransfersRequest, QueryTransfersResponse},
    txs::{QueryTransactionsRequest, QueryTransactionsResponse},
    Hash32, MonadChainDataService, QueryLimits,
};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct ChainDataStoreConfig {
    pub meta: ChainDataMetaBackendConfig,
    pub blob: ChainDataBlobBackendConfig,
    pub cache_mib: Option<usize>,
    pub reader_only: bool,
    pub single_writer: bool,
    pub owner_id: u64,
    pub lease_blocks: u64,
    pub renew_threshold_blocks: u64,
}

impl Default for ChainDataStoreConfig {
    fn default() -> Self {
        Self {
            meta: ChainDataMetaBackendConfig::default(),
            blob: ChainDataBlobBackendConfig::default(),
            cache_mib: None,
            reader_only: false,
            single_writer: false,
            owner_id: 1,
            lease_blocks: 64,
            renew_threshold_blocks: 16,
        }
    }
}

impl ChainDataStoreConfig {
    pub fn validate(&self) -> Result<()> {
        if self.reader_only {
            bail!("chain-data embedded ingest cannot run with reader_only=true");
        }
        self.validate_backends()?;
        if self.lease_blocks == 0 {
            bail!("chain-data lease_blocks must be >= 1");
        }
        if self.renew_threshold_blocks >= self.lease_blocks {
            bail!("chain-data renew_threshold_blocks must be less than lease_blocks");
        }
        Ok(())
    }

    pub fn validate_reader(&self) -> Result<()> {
        self.validate_backends()
    }

    fn validate_backends(&self) -> Result<()> {
        self.meta.validate()?;
        self.blob.validate()?;
        Ok(())
    }
}

#[derive(Clone)]
pub enum ConfiguredChainDataReader {
    #[cfg(feature = "fjall")]
    FjallFjall(Arc<MonadChainDataService<FjallStore, FjallStore>>),
    #[cfg(all(feature = "fjall", feature = "s3"))]
    FjallS3(Arc<MonadChainDataService<FjallStore, crate::store::S3BlobStore>>),
    #[cfg(all(feature = "fjall", feature = "dynamo"))]
    FjallDynamo(Arc<MonadChainDataService<FjallStore, crate::store::DynamoBlobStore>>),
    #[cfg(all(feature = "dynamo", feature = "fjall"))]
    DynamoFjall(Arc<MonadChainDataService<crate::store::DynamoMetaStore, FjallStore>>),
    #[cfg(all(feature = "dynamo", feature = "s3"))]
    DynamoS3(Arc<MonadChainDataService<crate::store::DynamoMetaStore, crate::store::S3BlobStore>>),
    #[cfg(feature = "dynamo")]
    DynamoDynamo(
        Arc<MonadChainDataService<crate::store::DynamoMetaStore, crate::store::DynamoBlobStore>>,
    ),
}

macro_rules! with_reader {
    ($self:expr, $service:ident => $body:expr) => {
        match $self {
            #[cfg(feature = "fjall")]
            Self::FjallFjall($service) => $body,
            #[cfg(all(feature = "fjall", feature = "s3"))]
            Self::FjallS3($service) => $body,
            #[cfg(all(feature = "fjall", feature = "dynamo"))]
            Self::FjallDynamo($service) => $body,
            #[cfg(all(feature = "dynamo", feature = "fjall"))]
            Self::DynamoFjall($service) => $body,
            #[cfg(all(feature = "dynamo", feature = "s3"))]
            Self::DynamoS3($service) => $body,
            #[cfg(feature = "dynamo")]
            Self::DynamoDynamo($service) => $body,
        }
    };
}

impl ConfiguredChainDataReader {
    pub fn fjall(service: MonadChainDataService<FjallStore, FjallStore>) -> Self {
        Self::FjallFjall(Arc::new(service))
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
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum ChainDataMetaBackendConfig {
    Fjall(ChainDataFjallMetaConfig),
    #[cfg(feature = "dynamo")]
    Dynamo(ChainDataDynamoMetaConfig),
}

impl Default for ChainDataMetaBackendConfig {
    fn default() -> Self {
        Self::Fjall(ChainDataFjallMetaConfig::default())
    }
}

impl ChainDataMetaBackendConfig {
    fn validate(&self) -> Result<()> {
        match self {
            Self::Fjall(config) => config.validate(),
            #[cfg(feature = "dynamo")]
            Self::Dynamo(config) => config.validate(),
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum ChainDataBlobBackendConfig {
    Fjall(ChainDataFjallBlobConfig),
    #[cfg(feature = "s3")]
    S3(ChainDataS3BlobConfig),
    #[cfg(feature = "dynamo")]
    Dynamo(ChainDataDynamoBlobConfig),
}

impl Default for ChainDataBlobBackendConfig {
    fn default() -> Self {
        Self::Fjall(ChainDataFjallBlobConfig::default())
    }
}

impl ChainDataBlobBackendConfig {
    fn validate(&self) -> Result<()> {
        match self {
            Self::Fjall(config) => config.validate(),
            #[cfg(feature = "s3")]
            Self::S3(config) => config.validate(),
            #[cfg(feature = "dynamo")]
            Self::Dynamo(config) => config.validate(),
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct ChainDataFjallTuningConfig {
    pub journal_mib: u64,
    pub memtable_mib: u64,
    pub workers: Option<usize>,
}

impl Default for ChainDataFjallTuningConfig {
    fn default() -> Self {
        Self {
            journal_mib: 512,
            memtable_mib: 64,
            workers: None,
        }
    }
}

impl ChainDataFjallTuningConfig {
    fn to_tuning(&self) -> Result<FjallTuning> {
        if self.journal_mib < 64 {
            bail!("chain-data fjall journal_mib must be >= 64");
        }
        Ok(FjallTuning {
            max_journaling_size_bytes: self.journal_mib * 1024 * 1024,
            max_memtable_size_bytes: self.memtable_mib * 1024 * 1024,
            worker_threads: self.workers,
        })
    }
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct ChainDataFjallMetaConfig {
    pub data_dir: Option<PathBuf>,
    pub tuning: ChainDataFjallTuningConfig,
}

impl ChainDataFjallMetaConfig {
    fn validate(&self) -> Result<()> {
        if self.data_dir.is_none() {
            bail!("chain-data fjall meta requires data_dir");
        }
        self.tuning.to_tuning()?;
        Ok(())
    }
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct ChainDataFjallBlobConfig {
    pub data_dir: Option<PathBuf>,
    pub tuning: ChainDataFjallTuningConfig,
}

impl ChainDataFjallBlobConfig {
    fn validate(&self) -> Result<()> {
        if self.data_dir.is_none() {
            bail!("chain-data fjall blob requires data_dir");
        }
        self.tuning.to_tuning()?;
        Ok(())
    }
}

#[cfg(feature = "s3")]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct ChainDataS3BlobConfig {
    pub bucket: Option<String>,
    pub region: Option<String>,
    pub endpoint_urls: Vec<String>,
    pub prefix: String,
    pub force_path_style: bool,
    pub max_concurrency: usize,
    pub create_bucket: bool,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
}

#[cfg(feature = "s3")]
impl Default for ChainDataS3BlobConfig {
    fn default() -> Self {
        Self {
            bucket: None,
            region: None,
            endpoint_urls: Vec::new(),
            prefix: String::new(),
            force_path_style: false,
            max_concurrency: 64,
            create_bucket: false,
            access_key_id: None,
            secret_access_key: None,
        }
    }
}

#[cfg(feature = "s3")]
impl ChainDataS3BlobConfig {
    fn validate(&self) -> Result<()> {
        if self.bucket.is_none() {
            bail!("chain-data s3 blob requires bucket");
        }
        if self.max_concurrency == 0 {
            bail!("chain-data s3 max_concurrency must be >= 1");
        }
        validate_pair(
            &self.access_key_id,
            &self.secret_access_key,
            "s3 access_key_id",
            "s3 secret_access_key",
        )
    }
}

#[cfg(feature = "dynamo")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ChainDataDynamoTableLayoutConfig {
    Single,
    PerLogicalTable,
}

#[cfg(feature = "dynamo")]
impl Default for ChainDataDynamoTableLayoutConfig {
    fn default() -> Self {
        Self::Single
    }
}

#[cfg(feature = "dynamo")]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct ChainDataDynamoMetaConfig {
    pub table: Option<String>,
    pub table_prefix: Option<String>,
    pub table_layout: ChainDataDynamoTableLayoutConfig,
    pub endpoint_url: Option<String>,
    pub region: Option<String>,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub session_token: Option<String>,
    pub create_table: bool,
    pub max_concurrency: usize,
    pub table_max_concurrency: usize,
    pub scylla_profile: bool,
    pub scylla_concurrency: usize,
}

#[cfg(feature = "dynamo")]
impl Default for ChainDataDynamoMetaConfig {
    fn default() -> Self {
        Self {
            table: None,
            table_prefix: None,
            table_layout: ChainDataDynamoTableLayoutConfig::Single,
            endpoint_url: None,
            region: None,
            access_key_id: None,
            secret_access_key: None,
            session_token: None,
            create_table: false,
            max_concurrency: 256,
            table_max_concurrency: 256,
            scylla_profile: false,
            scylla_concurrency: 256,
        }
    }
}

#[cfg(feature = "dynamo")]
impl ChainDataDynamoMetaConfig {
    fn validate(&self) -> Result<()> {
        if self.effective_max_concurrency() == 0 || self.effective_table_max_concurrency() == 0 {
            bail!("chain-data dynamo concurrency must be >= 1");
        }
        validate_pair(
            &self.access_key_id,
            &self.secret_access_key,
            "dynamo access_key_id",
            "dynamo secret_access_key",
        )?;
        match self.effective_table_layout() {
            ChainDataDynamoTableLayoutConfig::Single => {
                if self.table_prefix.is_some() {
                    bail!("chain-data dynamo table_prefix requires per-logical-table layout and cannot be combined with scylla_profile");
                }
                if self.table.is_none() {
                    bail!("chain-data dynamo single table layout requires table");
                }
            }
            ChainDataDynamoTableLayoutConfig::PerLogicalTable => {
                if self.table.is_some() {
                    bail!("chain-data dynamo per-logical-table layout cannot use table");
                }
                if self.table_prefix.is_none() {
                    bail!("chain-data dynamo per-logical-table layout requires table_prefix");
                }
            }
        }
        Ok(())
    }

    fn effective_table_layout(&self) -> ChainDataDynamoTableLayoutConfig {
        if self.scylla_profile {
            ChainDataDynamoTableLayoutConfig::Single
        } else {
            self.table_layout
        }
    }

    fn effective_max_concurrency(&self) -> usize {
        if self.scylla_profile {
            self.scylla_concurrency
        } else {
            self.max_concurrency
        }
    }

    fn effective_table_max_concurrency(&self) -> usize {
        if self.scylla_profile {
            self.scylla_concurrency
        } else {
            self.table_max_concurrency
        }
    }
}

#[cfg(feature = "dynamo")]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct ChainDataDynamoBlobConfig {
    pub table: Option<String>,
    pub endpoint_url: Option<String>,
    pub region: Option<String>,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub session_token: Option<String>,
    pub create_table: bool,
    pub max_concurrency: usize,
    pub chunk_size: Option<usize>,
}

#[cfg(feature = "dynamo")]
impl Default for ChainDataDynamoBlobConfig {
    fn default() -> Self {
        Self {
            table: None,
            endpoint_url: None,
            region: None,
            access_key_id: None,
            secret_access_key: None,
            session_token: None,
            create_table: false,
            max_concurrency: 256,
            chunk_size: None,
        }
    }
}

#[cfg(feature = "dynamo")]
impl ChainDataDynamoBlobConfig {
    fn validate(&self) -> Result<()> {
        if self.table.is_none() {
            bail!("chain-data dynamo blob requires table");
        }
        if self.max_concurrency == 0 {
            bail!("chain-data dynamo blob max_concurrency must be >= 1");
        }
        validate_pair(
            &self.access_key_id,
            &self.secret_access_key,
            "dynamo access_key_id",
            "dynamo secret_access_key",
        )
    }
}

pub async fn run_configured_chain_data_ingest<S>(
    store_config: ChainDataStoreConfig,
    ingest_config: ChainDataIngestConfig,
    source: S,
) -> Result<()>
where
    S: ChainDataIngestSource,
{
    store_config.validate()?;
    ingest_config.validate()?;
    let observed_upstream = Arc::new(AtomicU64::new(u64::MAX));

    match store_config.meta.clone() {
        ChainDataMetaBackendConfig::Fjall(meta_config) => {
            let meta_store = open_fjall(
                meta_config
                    .data_dir
                    .as_ref()
                    .expect("validated fjall meta data_dir"),
                meta_config.tuning.to_tuning()?,
                "meta",
            )?;
            dispatch_blob(
                &store_config,
                ingest_config,
                source,
                observed_upstream,
                meta_store,
                None,
            )
            .await
        }
        #[cfg(feature = "dynamo")]
        ChainDataMetaBackendConfig::Dynamo(meta_config) => {
            let meta_store = build_dynamo_meta_store(&meta_config).await?;
            let client = Some(meta_store.client());
            dispatch_blob(
                &store_config,
                ingest_config,
                source,
                observed_upstream,
                meta_store,
                client,
            )
            .await
        }
    }
}

pub async fn open_configured_chain_data_reader(
    store_config: ChainDataStoreConfig,
    limits: QueryLimits,
) -> Result<ConfiguredChainDataReader> {
    store_config.validate_reader()?;
    match store_config.meta.clone() {
        ChainDataMetaBackendConfig::Fjall(meta_config) => {
            let meta_store = open_fjall(
                meta_config
                    .data_dir
                    .as_ref()
                    .expect("validated fjall meta data_dir"),
                meta_config.tuning.to_tuning()?,
                "meta",
            )?;
            dispatch_fjall_reader_blob(&store_config, limits, meta_store, None).await
        }
        #[cfg(feature = "dynamo")]
        ChainDataMetaBackendConfig::Dynamo(meta_config) => {
            let meta_store = build_dynamo_meta_store(&meta_config).await?;
            let client = Some(meta_store.client());
            dispatch_dynamo_reader_blob(&store_config, limits, meta_store, client).await
        }
    }
}

async fn dispatch_fjall_reader_blob(
    store_config: &ChainDataStoreConfig,
    limits: QueryLimits,
    meta_store: FjallStore,
    #[allow(unused_variables)] dynamo_client: Option<SharedDynamoClient>,
) -> Result<ConfiguredChainDataReader> {
    let cache_config = resolve_cache_config(store_config.cache_mib);
    match store_config.blob.clone() {
        ChainDataBlobBackendConfig::Fjall(blob_config) => {
            let blob_store = open_fjall(
                blob_config
                    .data_dir
                    .as_ref()
                    .expect("validated fjall blob data_dir"),
                blob_config.tuning.to_tuning()?,
                "blob",
            )?;
            Ok(ConfiguredChainDataReader::FjallFjall(Arc::new(
                MonadChainDataService::new_reader_only(
                    meta_store,
                    blob_store,
                    limits,
                    cache_config,
                ),
            )))
        }
        #[cfg(feature = "s3")]
        ChainDataBlobBackendConfig::S3(blob_config) => {
            let blob_store = build_s3_blob_store(&blob_config).await?;
            Ok(ConfiguredChainDataReader::FjallS3(Arc::new(
                MonadChainDataService::new_reader_only(
                    meta_store,
                    blob_store,
                    limits,
                    cache_config,
                ),
            )))
        }
        #[cfg(feature = "dynamo")]
        ChainDataBlobBackendConfig::Dynamo(blob_config) => {
            let blob_store = build_dynamo_blob_store(&blob_config, dynamo_client).await?;
            Ok(ConfiguredChainDataReader::FjallDynamo(Arc::new(
                MonadChainDataService::new_reader_only(
                    meta_store,
                    blob_store,
                    limits,
                    cache_config,
                ),
            )))
        }
    }
}

#[cfg(feature = "dynamo")]
async fn dispatch_dynamo_reader_blob(
    store_config: &ChainDataStoreConfig,
    limits: QueryLimits,
    meta_store: crate::store::DynamoMetaStore,
    #[allow(unused_variables)] dynamo_client: Option<SharedDynamoClient>,
) -> Result<ConfiguredChainDataReader> {
    let cache_config = resolve_cache_config(store_config.cache_mib);
    match store_config.blob.clone() {
        ChainDataBlobBackendConfig::Fjall(blob_config) => {
            let blob_store = open_fjall(
                blob_config
                    .data_dir
                    .as_ref()
                    .expect("validated fjall blob data_dir"),
                blob_config.tuning.to_tuning()?,
                "blob",
            )?;
            Ok(ConfiguredChainDataReader::DynamoFjall(Arc::new(
                MonadChainDataService::new_reader_only(
                    meta_store,
                    blob_store,
                    limits,
                    cache_config,
                ),
            )))
        }
        #[cfg(feature = "s3")]
        ChainDataBlobBackendConfig::S3(blob_config) => {
            let blob_store = build_s3_blob_store(&blob_config).await?;
            Ok(ConfiguredChainDataReader::DynamoS3(Arc::new(
                MonadChainDataService::new_reader_only(
                    meta_store,
                    blob_store,
                    limits,
                    cache_config,
                ),
            )))
        }
        ChainDataBlobBackendConfig::Dynamo(blob_config) => {
            let blob_store = build_dynamo_blob_store(&blob_config, dynamo_client).await?;
            Ok(ConfiguredChainDataReader::DynamoDynamo(Arc::new(
                MonadChainDataService::new_reader_only(
                    meta_store,
                    blob_store,
                    limits,
                    cache_config,
                ),
            )))
        }
    }
}

async fn dispatch_blob<M, S>(
    store_config: &ChainDataStoreConfig,
    ingest_config: ChainDataIngestConfig,
    source: S,
    observed_upstream: Arc<AtomicU64>,
    meta_store: M,
    #[allow(unused_variables)] dynamo_client: Option<SharedDynamoClient>,
) -> Result<()>
where
    M: MetaStoreCas,
    S: ChainDataIngestSource,
{
    match store_config.blob.clone() {
        ChainDataBlobBackendConfig::Fjall(blob_config) => {
            let blob_store = open_fjall(
                blob_config
                    .data_dir
                    .as_ref()
                    .expect("validated fjall blob data_dir"),
                blob_config.tuning.to_tuning()?,
                "blob",
            )?;
            run_with_store(
                store_config,
                ingest_config,
                source,
                observed_upstream,
                meta_store,
                blob_store,
            )
            .await
        }
        #[cfg(feature = "s3")]
        ChainDataBlobBackendConfig::S3(blob_config) => {
            let blob_store = build_s3_blob_store(&blob_config).await?;
            run_with_store(
                store_config,
                ingest_config,
                source,
                observed_upstream,
                meta_store,
                blob_store,
            )
            .await
        }
        #[cfg(feature = "dynamo")]
        ChainDataBlobBackendConfig::Dynamo(blob_config) => {
            let blob_store = build_dynamo_blob_store(&blob_config, dynamo_client).await?;
            run_with_store(
                store_config,
                ingest_config,
                source,
                observed_upstream,
                meta_store,
                blob_store,
            )
            .await
        }
    }
}

async fn run_with_store<M, B, S>(
    store_config: &ChainDataStoreConfig,
    ingest_config: ChainDataIngestConfig,
    source: S,
    observed_upstream: Arc<AtomicU64>,
    meta_store: M,
    blob_store: B,
) -> Result<()>
where
    M: MetaStoreCas,
    B: BlobStore,
    S: ChainDataIngestSource,
{
    let cache_config = resolve_cache_config(store_config.cache_mib);
    let service = if store_config.single_writer {
        Arc::new(MonadChainDataService::with_cache_config(
            meta_store,
            blob_store,
            QueryLimits::UNLIMITED,
            cache_config,
        ))
    } else {
        build_reader_writer_service(
            meta_store,
            blob_store,
            cache_config,
            store_config.owner_id,
            store_config.lease_blocks,
            store_config.renew_threshold_blocks,
            observed_upstream.clone(),
        )
    };
    run_chain_data_ingest(service, source, ingest_config, Some(observed_upstream)).await
}

fn resolve_cache_config(cache_mib: Option<usize>) -> CacheConfig {
    let baseline = CacheConfig::default();
    let baseline_total_mib = baseline.approx_total_mib().max(1);
    match cache_mib {
        None => baseline,
        Some(0) => baseline.scale(0, 1),
        Some(target_mib) => baseline.scale(target_mib, baseline_total_mib),
    }
}

fn open_fjall(path: &Path, tuning: FjallTuning, label: &'static str) -> Result<FjallStore> {
    let fresh = data_dir_fresh(path);
    info!(
        path = %path.display(),
        fresh,
        ?tuning,
        "opening chain-data fjall {label} store"
    );
    FjallStore::open(path, tuning).with_context(|| {
        format!(
            "opening chain-data fjall {label} store at {}",
            path.display()
        )
    })
}

fn data_dir_fresh(path: &Path) -> bool {
    !path.exists()
        || path
            .read_dir()
            .map(|mut d| d.next().is_none())
            .unwrap_or(true)
}

#[cfg(feature = "s3")]
async fn build_s3_blob_store(config: &ChainDataS3BlobConfig) -> Result<crate::store::S3BlobStore> {
    use crate::store::{S3BlobStore, S3BlobStoreConfig, S3Credentials};

    let credentials = match (&config.access_key_id, &config.secret_access_key) {
        (Some(access_key_id), Some(secret_access_key)) => Some(S3Credentials {
            access_key_id: access_key_id.clone(),
            secret_access_key: secret_access_key.clone(),
            session_token: None,
        }),
        _ => None,
    };
    let store_config = S3BlobStoreConfig {
        bucket: config.bucket.clone().expect("validated s3 bucket"),
        root_prefix: config.prefix.clone(),
        endpoint_urls: config.endpoint_urls.clone(),
        region: config.region.clone(),
        force_path_style: config.force_path_style,
        max_concurrency: config.max_concurrency,
        create_bucket: config.create_bucket,
        credentials,
    };
    S3BlobStore::new(store_config)
        .await
        .context("building chain-data S3 blob store")
}

#[cfg(feature = "dynamo")]
async fn build_dynamo_meta_store(
    config: &ChainDataDynamoMetaConfig,
) -> Result<crate::store::DynamoMetaStore> {
    use crate::store::{DynamoMetaStore, DynamoMetaStoreConfig, DynamoTableLayout};

    let table_layout = match config.effective_table_layout() {
        ChainDataDynamoTableLayoutConfig::Single => {
            DynamoTableLayout::single(config.table.clone().expect("validated dynamo table"))
        }
        ChainDataDynamoTableLayoutConfig::PerLogicalTable => DynamoTableLayout::PerLogicalTable {
            prefix: config
                .table_prefix
                .clone()
                .expect("validated dynamo table_prefix"),
        },
    };
    let credentials = dynamo_credentials(
        &config.access_key_id,
        &config.secret_access_key,
        &config.session_token,
    );
    let store_config = DynamoMetaStoreConfig {
        table_layout,
        endpoint_url: config.endpoint_url.clone(),
        region: config.region.clone(),
        batch_max_concurrency: config.effective_max_concurrency(),
        batch_table_max_concurrency: config.effective_table_max_concurrency(),
        credentials,
    };
    let store = DynamoMetaStore::new(store_config)
        .await
        .context("building chain-data Dynamo meta store")?;
    if config.create_table {
        store
            .create_table()
            .await
            .context("creating chain-data Dynamo meta table(s)")?;
    }
    store
        .validate_table()
        .await
        .context("validating chain-data Dynamo meta table(s)")?;
    Ok(store)
}

#[cfg(feature = "dynamo")]
type SharedDynamoClient = aws_sdk_dynamodb::Client;
#[cfg(not(feature = "dynamo"))]
type SharedDynamoClient = ();

#[cfg(feature = "dynamo")]
async fn build_dynamo_blob_store(
    config: &ChainDataDynamoBlobConfig,
    shared_client: Option<aws_sdk_dynamodb::Client>,
) -> Result<crate::store::DynamoBlobStore> {
    use crate::store::{DynamoBlobStore, DynamoBlobStoreConfig};

    let mut store_config =
        DynamoBlobStoreConfig::new(config.table.clone().expect("validated table"));
    store_config.endpoint_url = config.endpoint_url.clone();
    store_config.region = config.region.clone();
    store_config.batch_max_concurrency = config.max_concurrency;
    store_config.credentials = dynamo_credentials(
        &config.access_key_id,
        &config.secret_access_key,
        &config.session_token,
    );
    if let Some(chunk_size) = config.chunk_size {
        store_config.chunk_size = chunk_size;
    }
    let store = match shared_client {
        Some(client) => DynamoBlobStore::new_with_client(
            client,
            store_config.table_name.clone(),
            store_config.batch_max_concurrency,
            store_config.chunk_size,
        ),
        None => DynamoBlobStore::new(store_config)
            .await
            .context("building chain-data Dynamo blob store")?,
    };
    if config.create_table {
        store
            .create_table()
            .await
            .context("creating chain-data Dynamo blob table")?;
    }
    store
        .validate_table()
        .await
        .context("validating chain-data Dynamo blob table")?;
    Ok(store)
}

#[cfg(feature = "dynamo")]
fn dynamo_credentials(
    access_key_id: &Option<String>,
    secret_access_key: &Option<String>,
    session_token: &Option<String>,
) -> Option<crate::store::DynamoCredentials> {
    match (access_key_id, secret_access_key) {
        (Some(access_key_id), Some(secret_access_key)) => Some(crate::store::DynamoCredentials {
            access_key_id: access_key_id.clone(),
            secret_access_key: secret_access_key.clone(),
            session_token: session_token.clone(),
        }),
        _ => None,
    }
}

fn validate_pair(
    left: &Option<String>,
    right: &Option<String>,
    left_name: &'static str,
    right_name: &'static str,
) -> Result<()> {
    match (left, right) {
        (Some(_), Some(_)) | (None, None) => Ok(()),
        (Some(_), None) => {
            warn!("{left_name} was set without {right_name}");
            bail!("{left_name} requires {right_name}")
        }
        (None, Some(_)) => {
            warn!("{right_name} was set without {left_name}");
            bail!("{right_name} requires {left_name}")
        }
    }
}
