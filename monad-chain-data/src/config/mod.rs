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

//! Operator-facing configuration for the chain-data stores: backend selection
//! (S3 / DynamoDB-or-Scylla), cache and query-runtime budgets, and the store
//! builders shared by the [`ingest`] entry point and the [`reader`].

#[cfg(any(feature = "dynamo", feature = "mongo"))]
use eyre::Context;
use eyre::{bail, Result};
#[cfg(feature = "dynamo")]
use tracing::{info, warn};

#[cfg(any(feature = "dynamo", feature = "mongo"))]
use crate::{engine::tables::QueryRuntimeConfig, store::CacheConfig};

pub mod ingest;
pub mod reader;

pub use ingest::{
    run_configured_chain_data_engine_ingest, ChainDataEngineConfig, ChainDataPayloadConfig,
};
pub use reader::{open_configured_chain_data_reader, ConfiguredChainDataReader};

/// An optional secret (credential) that never renders its value via `Debug`, so
/// config dumps cannot leak it. `#[serde(transparent)]` keeps the TOML wire
/// shape identical to a plain `Option<String>`.
#[derive(Clone, Default, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct Redacted(pub Option<String>);

impl std::fmt::Debug for Redacted {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.0 {
            Some(_) => f.write_str("[REDACTED]"),
            None => f.write_str("None"),
        }
    }
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct ChainDataStoreConfig {
    pub meta: ChainDataMetaBackendConfig,
    /// Blob backend for pack blobs and checkpoint-snapshot payloads. `None`
    /// (the `[store.blob]` TOML section omitted) means NO blob store: only
    /// valid for `payload = "external-archive"` stores, whose row payloads
    /// live in the monad-archive objects. Ingest then auto-disables
    /// checkpoints (snapshot payloads are blob objects) and recovers from
    /// fragments; any blob access errors loudly (see
    /// [`crate::store::NullBlobStore`]).
    pub blob: Option<ChainDataBlobBackendConfig>,
    pub cache: ChainDataCacheConfig,
    /// Reader-side query-runtime limits (fan-out + decode budget).
    pub query: ChainDataQueryConfig,
    pub reader_only: bool,
    /// Read-only access to the monad-archive object store holding external
    /// payloads (`payload = "external-archive"` ingest). Required on readers
    /// of an externally-ingested store; ignored for fully native stores.
    pub archive: Option<ChainDataArchiveBackendConfig>,
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct ChainDataCacheConfig {
    /// Total cache budget in MiB. If absent, the caller's mode default applies:
    /// ingest resolves to 0 MiB, reader resolves to 8192 MiB.
    pub total_mib: Option<usize>,
    /// Per-table budget overrides in MiB. Unspecified tables get the default
    /// ratio of the total budget.
    pub tables: ChainDataCacheTableBudgets,
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct ChainDataCacheTableBudgets {
    pub dir_by_block_mib: Option<usize>,
    pub dir_bucket_mib: Option<usize>,
    pub bitmap_by_block_mib: Option<usize>,
    pub bitmap_page_blob_mib: Option<usize>,
    pub bitmap_page_counts_mib: Option<usize>,
    pub open_bitmap_stream_mib: Option<usize>,
    pub block_header_mib: Option<usize>,
    pub block_hash_to_number_mib: Option<usize>,
    pub tx_hash_index_mib: Option<usize>,
    /// Decoded-row cache byte budget (split across the three families).
    pub row_cache_mib: Option<usize>,
}

/// Reader-side query-runtime limits; each set field overrides the matching
/// [`QueryRuntimeConfig`] default. Raise the concurrency knobs for
/// high-latency backends (S3/Dynamo); fast local backends suit the defaults.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct ChainDataQueryConfig {
    pub blob_io_concurrency: Option<usize>,
    pub materialize_concurrency: Option<usize>,
    /// Concurrent page intersections (stage-1 bitmap fetches) per query.
    pub page_intersect_concurrency: Option<usize>,
    pub materialize_budget_permits: Option<usize>,
    /// Decode-budget permit unit, in KiB.
    pub materialize_permit_kib: Option<usize>,
    /// Maximum inter-frame gap (KiB) for span coalescing (wasted gap bytes vs
    /// an extra range read). Raise for high-latency backends.
    pub materialize_span_max_gap_kib: Option<usize>,
    /// Maximum length (KiB) of one coalesced read span.
    pub materialize_span_max_kib: Option<usize>,
    /// Span count above which a dense batch reads the whole region.
    pub materialize_whole_region_span_threshold: Option<usize>,
    /// Largest region (MiB) the dense-selection whole-region read will pull.
    pub materialize_whole_region_max_mib: Option<usize>,
}

impl ChainDataQueryConfig {
    // Only consumed when assembling a configured reader.
    #[cfg(any(feature = "dynamo", feature = "mongo"))]
    pub(crate) fn to_runtime(&self) -> QueryRuntimeConfig {
        let d = QueryRuntimeConfig::default();
        let kib_to_bytes =
            |kib: Option<usize>, default: usize| kib.map(|k| k * 1024).unwrap_or(default);
        QueryRuntimeConfig {
            blob_io_concurrency: self.blob_io_concurrency.unwrap_or(d.blob_io_concurrency),
            materialize_concurrency: self
                .materialize_concurrency
                .unwrap_or(d.materialize_concurrency),
            page_intersect_concurrency: self
                .page_intersect_concurrency
                .unwrap_or(d.page_intersect_concurrency),
            materialize_budget_permits: self
                .materialize_budget_permits
                .unwrap_or(d.materialize_budget_permits),
            materialize_permit_bytes: kib_to_bytes(
                self.materialize_permit_kib,
                d.materialize_permit_bytes,
            ),
            materialize_span_max_gap_bytes: kib_to_bytes(
                self.materialize_span_max_gap_kib,
                d.materialize_span_max_gap_bytes,
            ),
            materialize_span_max_bytes: kib_to_bytes(
                self.materialize_span_max_kib,
                d.materialize_span_max_bytes,
            ),
            materialize_whole_region_span_threshold: self
                .materialize_whole_region_span_threshold
                .unwrap_or(d.materialize_whole_region_span_threshold),
            materialize_whole_region_max_bytes: self
                .materialize_whole_region_max_mib
                .map(CacheConfig::budget_mib_to_bytes)
                .unwrap_or(d.materialize_whole_region_max_bytes),
        }
    }
}

impl ChainDataStoreConfig {
    pub fn validate_ingest(&self) -> Result<()> {
        if self.reader_only {
            bail!("chain-data embedded ingest cannot run with reader_only=true");
        }
        self.validate_reader()
    }

    pub fn validate_reader(&self) -> Result<()> {
        self.meta.validate()?;
        // A missing blob backend is valid for readers (external-archive
        // stores); ingest-side payload/seed constraints live in `ingest`.
        if let Some(blob) = &self.blob {
            blob.validate()?;
        }
        // The mongo meta backend ships blob-less only: its migration story is
        // "indexes next to an existing archive", where row payloads stay in
        // the archive's own collections (external-archive payload mode).
        #[cfg(feature = "mongo")]
        if matches!(&self.meta, ChainDataMetaBackendConfig::Mongo(_)) && self.blob.is_some() {
            bail!(
                "chain-data mongo meta backend supports only blob-less stores: omit \
                 [store.blob] and ingest with payload = \"external-archive\""
            );
        }
        if let Some(archive) = &self.archive {
            archive.validate()?;
        }
        if self.blob.is_none() && self.archive.is_none() {
            bail!(
                "a store without [store.blob] keeps its row payloads in the monad-archive \
                 objects and requires [store.archive] to read them"
            );
        }
        Ok(())
    }
}

/// Backend selection for the external archive's object store. Keys are RAW
/// archive object keys (e.g. `block/000000000123`) — no chain-data
/// prefix/table/hex layout applies.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum ChainDataArchiveBackendConfig {
    #[cfg(feature = "s3")]
    S3(ChainDataArchiveS3Config),
    #[cfg(feature = "mongo")]
    Mongo(ChainDataArchiveMongoConfig),
    #[cfg(feature = "dynamo")]
    Dynamo(ChainDataArchiveDynamoConfig),
    #[cfg(not(any(feature = "s3", feature = "mongo", feature = "dynamo")))]
    Unavailable,
}

impl ChainDataArchiveBackendConfig {
    fn validate(&self) -> Result<()> {
        match self {
            #[cfg(feature = "s3")]
            Self::S3(config) => config.validate(),
            #[cfg(feature = "mongo")]
            Self::Mongo(config) => config.validate(),
            #[cfg(feature = "dynamo")]
            Self::Dynamo(config) => config.validate(),
            #[cfg(not(any(feature = "s3", feature = "mongo", feature = "dynamo")))]
            Self::Unavailable => {
                bail!("chain-data archive access requires the s3, mongo, or dynamo feature")
            }
        }
    }
}

/// Read-only access to a monad-archive block-data table on a
/// DynamoDB-API-compatible backend (a Scylla Alternator archive replica).
#[cfg(feature = "dynamo")]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct ChainDataArchiveDynamoConfig {
    /// The archive's block-data table (e.g. `{namespace}_block_level`).
    pub table: Option<String>,
    pub endpoint_url: Option<String>,
    pub region: Option<String>,
    pub profile: Option<String>,
    pub access_key_id: Redacted,
    pub secret_access_key: Redacted,
    pub session_token: Redacted,
    pub max_concurrency: usize,
}

#[cfg(feature = "dynamo")]
impl Default for ChainDataArchiveDynamoConfig {
    fn default() -> Self {
        Self {
            table: None,
            endpoint_url: None,
            region: None,
            profile: None,
            access_key_id: Redacted(None),
            secret_access_key: Redacted(None),
            session_token: Redacted(None),
            max_concurrency: 256,
        }
    }
}

#[cfg(feature = "dynamo")]
impl ChainDataArchiveDynamoConfig {
    fn validate(&self) -> Result<()> {
        if self.table.is_none() {
            bail!("chain-data archive dynamo requires table");
        }
        if self.max_concurrency == 0 {
            bail!("chain-data archive dynamo max_concurrency must be >= 1");
        }
        validate_pair(
            &self.access_key_id.0,
            &self.secret_access_key.0,
            "archive dynamo access_key_id",
            "archive dynamo secret_access_key",
        )
    }
}

/// Read-only access to a monad-archive MongoDB block-data collection.
#[cfg(feature = "mongo")]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct ChainDataArchiveMongoConfig {
    /// MongoDB connection string; redacted because it may embed credentials.
    pub url: Redacted,
    pub database: Option<String>,
    /// The archive's block-data collection.
    pub collection: String,
    pub max_pool_size: u32,
}

#[cfg(feature = "mongo")]
impl Default for ChainDataArchiveMongoConfig {
    fn default() -> Self {
        Self {
            url: Redacted(None),
            database: None,
            collection: "block_level".to_string(),
            max_pool_size: 64,
        }
    }
}

#[cfg(feature = "mongo")]
impl ChainDataArchiveMongoConfig {
    fn validate(&self) -> Result<()> {
        if self.url.0.is_none() {
            bail!("chain-data archive mongo requires url");
        }
        if self.database.is_none() {
            bail!("chain-data archive mongo requires database");
        }
        if self.collection.is_empty() {
            bail!("chain-data archive mongo requires collection");
        }
        if self.max_pool_size == 0 {
            bail!("chain-data archive mongo max_pool_size must be >= 1");
        }
        Ok(())
    }
}

/// Read-only S3 access to a monad-archive bucket.
#[cfg(feature = "s3")]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct ChainDataArchiveS3Config {
    pub bucket: Option<String>,
    pub region: Option<String>,
    pub profile: Option<String>,
    pub endpoint_urls: Vec<String>,
    pub force_path_style: bool,
    pub max_concurrency: usize,
    pub access_key_id: Redacted,
    pub secret_access_key: Redacted,
}

#[cfg(feature = "s3")]
impl Default for ChainDataArchiveS3Config {
    fn default() -> Self {
        Self {
            bucket: None,
            region: None,
            profile: None,
            endpoint_urls: Vec::new(),
            force_path_style: false,
            max_concurrency: 64,
            access_key_id: Redacted(None),
            secret_access_key: Redacted(None),
        }
    }
}

#[cfg(feature = "s3")]
impl ChainDataArchiveS3Config {
    fn validate(&self) -> Result<()> {
        if self.bucket.is_none() {
            bail!("chain-data archive s3 requires bucket");
        }
        if self.max_concurrency == 0 {
            bail!("chain-data archive s3 max_concurrency must be >= 1");
        }
        validate_pair(
            &self.access_key_id.0,
            &self.secret_access_key.0,
            "archive s3 access_key_id",
            "archive s3 secret_access_key",
        )
    }
}

/// Builds the configured external archive reader, or `None` when no archive
/// access is configured. Shared by the ingest assembly and the reader.
///
/// Gated on the meta-backend features because only the configured assembly
/// paths construct readers, not because the reader needs those backends.
#[cfg(any(feature = "dynamo", feature = "mongo"))]
pub(crate) async fn build_external_payload_reader(
    config: &Option<ChainDataArchiveBackendConfig>,
) -> Result<Option<std::sync::Arc<dyn crate::external::ExternalBlobReader>>> {
    match config {
        None => Ok(None),
        #[cfg(feature = "s3")]
        Some(ChainDataArchiveBackendConfig::S3(s3)) => {
            use crate::store::{S3BlobStoreConfig, S3Credentials, S3ExternalBlobReader};

            let credentials = match (&s3.access_key_id.0, &s3.secret_access_key.0) {
                (Some(access_key_id), Some(secret_access_key)) => Some(S3Credentials {
                    access_key_id: access_key_id.clone(),
                    secret_access_key: secret_access_key.clone(),
                    session_token: None,
                }),
                _ => None,
            };
            let store_config = S3BlobStoreConfig {
                bucket: s3.bucket.clone().expect("validated archive s3 bucket"),
                root_prefix: String::new(),
                endpoint_urls: s3.endpoint_urls.clone(),
                region: s3.region.clone(),
                profile: s3.profile.clone(),
                force_path_style: s3.force_path_style,
                max_concurrency: s3.max_concurrency,
                create_bucket: false,
                credentials,
            };
            let reader = S3ExternalBlobReader::new(store_config)
                .await
                .context("building chain-data archive S3 reader")?;
            Ok(Some(std::sync::Arc::new(reader)))
        }
        #[cfg(feature = "mongo")]
        Some(ChainDataArchiveBackendConfig::Mongo(mongo)) => {
            use crate::store::{MongoExternalBlobReader, MongoExternalBlobReaderConfig};

            let reader_config = MongoExternalBlobReaderConfig {
                connection_string: mongo.url.0.clone().expect("validated archive mongo url"),
                database: mongo
                    .database
                    .clone()
                    .expect("validated archive mongo database"),
                collection: mongo.collection.clone(),
                max_pool_size: mongo.max_pool_size,
            };
            let reader = MongoExternalBlobReader::new(reader_config)
                .await
                .context("building chain-data archive Mongo reader")?;
            Ok(Some(std::sync::Arc::new(reader)))
        }
        #[cfg(feature = "dynamo")]
        Some(ChainDataArchiveBackendConfig::Dynamo(dynamo)) => {
            use crate::store::{DynamoExternalBlobReader, DynamoExternalBlobReaderConfig};

            let reader_config = DynamoExternalBlobReaderConfig {
                table: dynamo
                    .table
                    .clone()
                    .expect("validated archive dynamo table"),
                endpoint_url: dynamo.endpoint_url.clone(),
                region: dynamo.region.clone(),
                profile: dynamo.profile.clone(),
                credentials: dynamo_credentials(
                    &dynamo.access_key_id.0,
                    &dynamo.secret_access_key.0,
                    &dynamo.session_token.0,
                ),
                max_concurrency: dynamo.max_concurrency,
            };
            let reader = DynamoExternalBlobReader::new(reader_config)
                .await
                .context("building chain-data archive Dynamo reader")?;
            Ok(Some(std::sync::Arc::new(reader)))
        }
        #[cfg(not(any(feature = "s3", feature = "mongo", feature = "dynamo")))]
        Some(ChainDataArchiveBackendConfig::Unavailable) => {
            bail!("chain-data archive access requires the s3, mongo, or dynamo feature")
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum ChainDataMetaBackendConfig {
    #[cfg(feature = "dynamo")]
    Dynamo(ChainDataDynamoMetaConfig),
    #[cfg(feature = "mongo")]
    Mongo(ChainDataMongoMetaConfig),
    #[cfg(not(any(feature = "dynamo", feature = "mongo")))]
    Unavailable,
}

impl Default for ChainDataMetaBackendConfig {
    fn default() -> Self {
        #[cfg(feature = "dynamo")]
        {
            Self::Dynamo(ChainDataDynamoMetaConfig::default())
        }
        #[cfg(all(not(feature = "dynamo"), feature = "mongo"))]
        {
            Self::Mongo(ChainDataMongoMetaConfig::default())
        }
        #[cfg(not(any(feature = "dynamo", feature = "mongo")))]
        {
            Self::Unavailable
        }
    }
}

impl ChainDataMetaBackendConfig {
    fn validate(&self) -> Result<()> {
        match self {
            #[cfg(feature = "dynamo")]
            Self::Dynamo(config) => config.validate(),
            #[cfg(feature = "mongo")]
            Self::Mongo(config) => config.validate(),
            #[cfg(not(any(feature = "dynamo", feature = "mongo")))]
            Self::Unavailable => {
                bail!("chain-data configured storage requires the dynamo or mongo feature")
            }
        }
    }
}

/// MongoDB meta backend: chain-data meta rows in one dedicated collection of
/// an (operator's existing) MongoDB database. Blob-less only — pair with
/// `payload = "external-archive"` ingest and `[store.archive]` so row
/// payloads stay in the archive's own collections.
#[cfg(feature = "mongo")]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct ChainDataMongoMetaConfig {
    /// MongoDB connection string; redacted because it may embed credentials.
    pub url: Redacted,
    pub database: Option<String>,
    /// Collection holding every chain-data meta row. Distinct from the
    /// archive's `block_level`/`tx_index` collections, which stay untouched.
    pub collection: String,
    pub max_pool_size: u32,
    /// Concurrent single-document writes per ingest commit batch.
    pub write_concurrency: usize,
}

#[cfg(feature = "mongo")]
impl Default for ChainDataMongoMetaConfig {
    fn default() -> Self {
        Self {
            url: Redacted(None),
            database: None,
            collection: "chain_data_meta".to_string(),
            max_pool_size: 64,
            write_concurrency: 64,
        }
    }
}

#[cfg(feature = "mongo")]
impl ChainDataMongoMetaConfig {
    fn validate(&self) -> Result<()> {
        if self.url.0.is_none() {
            bail!("chain-data mongo meta requires url");
        }
        if self.database.is_none() {
            bail!("chain-data mongo meta requires database");
        }
        if self.collection.is_empty() {
            bail!("chain-data mongo meta requires collection");
        }
        if self.max_pool_size == 0 || self.write_concurrency == 0 {
            bail!("chain-data mongo meta pool size and write concurrency must be >= 1");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum ChainDataBlobBackendConfig {
    #[cfg(feature = "s3")]
    S3(ChainDataS3BlobConfig),
    #[cfg(feature = "dynamo")]
    Dynamo(ChainDataDynamoBlobConfig),
    #[cfg(not(any(feature = "s3", feature = "dynamo")))]
    Unavailable,
}

impl ChainDataBlobBackendConfig {
    fn validate(&self) -> Result<()> {
        match self {
            #[cfg(feature = "s3")]
            Self::S3(config) => config.validate(),
            #[cfg(feature = "dynamo")]
            Self::Dynamo(config) => config.validate(),
            #[cfg(not(any(feature = "s3", feature = "dynamo")))]
            Self::Unavailable => bail!("chain-data configured blob storage requires s3 or dynamo"),
        }
    }
}

#[cfg(feature = "s3")]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct ChainDataS3BlobConfig {
    pub bucket: Option<String>,
    pub region: Option<String>,
    pub profile: Option<String>,
    pub endpoint_urls: Vec<String>,
    pub prefix: String,
    pub force_path_style: bool,
    pub max_concurrency: usize,
    pub create_bucket: bool,
    pub access_key_id: Redacted,
    pub secret_access_key: Redacted,
}

#[cfg(feature = "s3")]
impl Default for ChainDataS3BlobConfig {
    fn default() -> Self {
        Self {
            bucket: None,
            region: None,
            profile: None,
            endpoint_urls: Vec::new(),
            prefix: String::new(),
            force_path_style: false,
            max_concurrency: 64,
            create_bucket: false,
            access_key_id: Redacted(None),
            secret_access_key: Redacted(None),
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
            &self.access_key_id.0,
            &self.secret_access_key.0,
            "s3 access_key_id",
            "s3 secret_access_key",
        )
    }
}

#[cfg(feature = "dynamo")]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ChainDataDynamoTableLayoutConfig {
    #[default]
    Single,
    PerLogicalTable,
}

#[cfg(feature = "dynamo")]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct ChainDataDynamoMetaConfig {
    pub table: Option<String>,
    pub table_prefix: Option<String>,
    pub table_layout: ChainDataDynamoTableLayoutConfig,
    /// Single Alternator/DynamoDB endpoint. Use `endpoint_urls` to drive
    /// multiple nodes.
    pub endpoint_url: Option<String>,
    /// Multiple endpoints (e.g. all Alternator nodes); requests round-robin
    /// across them to spread coordinator load. Takes precedence over
    /// `endpoint_url` when non-empty.
    pub endpoint_urls: Vec<String>,
    pub region: Option<String>,
    pub profile: Option<String>,
    pub access_key_id: Redacted,
    pub secret_access_key: Redacted,
    pub session_token: Redacted,
    pub create_table: bool,
    pub max_concurrency: usize,
    pub table_max_concurrency: usize,
    pub scylla_profile: bool,
    pub scylla_concurrency: usize,
    /// Desired max items per `BatchWriteItem`. Verified by a startup probe before
    /// use; if the backend rejects it the store falls back to 25. Unset →
    /// `ALTERNATOR_DEFAULT_BATCH_WRITE_ITEMS` (100) under `scylla_profile`, else
    /// 25. DynamoDB caps at 25; Alternator defaults to 100. Must be within
    /// 1..=1000: the probe materializes this many requests up front, so the
    /// bound keeps a typo from ballooning into a giant allocation.
    pub batch_write_max_items: Option<usize>,
}

#[cfg(feature = "dynamo")]
impl Default for ChainDataDynamoMetaConfig {
    fn default() -> Self {
        Self {
            table: None,
            table_prefix: None,
            table_layout: ChainDataDynamoTableLayoutConfig::Single,
            endpoint_url: None,
            endpoint_urls: Vec::new(),
            region: None,
            profile: None,
            access_key_id: Redacted(None),
            secret_access_key: Redacted(None),
            session_token: Redacted(None),
            create_table: false,
            max_concurrency: 256,
            table_max_concurrency: 256,
            scylla_profile: false,
            scylla_concurrency: 1024,
            batch_write_max_items: None,
        }
    }
}

/// Default `BatchWriteItem` item count to probe for under `scylla_profile` —
/// ScyllaDB Alternator's default `alternator_max_items_in_batch_write`.
#[cfg(feature = "dynamo")]
const ALTERNATOR_DEFAULT_BATCH_WRITE_ITEMS: usize = 100;
/// DynamoDB's fixed `BatchWriteItem` item cap (and the safe fallback).
#[cfg(feature = "dynamo")]
const DYNAMO_BATCH_WRITE_ITEMS: usize = 25;
/// Upper bound on configured `batch_write_max_items`. The startup probe
/// materializes candidate-many `WriteRequest`s before the backend can reject
/// them, so an absurd value (an items/bytes mix-up, say) must fail validation
/// instead of allocating at startup. 10x Alternator's default leaves ample
/// headroom for raised `alternator_max_items_in_batch_write` deployments.
#[cfg(feature = "dynamo")]
const MAX_BATCH_WRITE_ITEMS: usize = 1000;

#[cfg(feature = "dynamo")]
impl ChainDataDynamoMetaConfig {
    fn validate(&self) -> Result<()> {
        if self.effective_max_concurrency() == 0 || self.effective_table_max_concurrency() == 0 {
            bail!("chain-data dynamo concurrency must be >= 1");
        }
        if let Some(items) = self.batch_write_max_items {
            if items == 0 || items > MAX_BATCH_WRITE_ITEMS {
                bail!(
                    "chain-data dynamo batch_write_max_items must be within \
                     1..={MAX_BATCH_WRITE_ITEMS} (items per BatchWriteItem, not bytes)"
                );
            }
        }
        validate_pair(
            &self.access_key_id.0,
            &self.secret_access_key.0,
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

    /// Endpoints to use: `endpoint_urls` if set, else the single `endpoint_url`,
    /// else empty (default AWS resolver).
    fn effective_endpoint_urls(&self) -> Vec<String> {
        if !self.endpoint_urls.is_empty() {
            self.endpoint_urls.clone()
        } else {
            self.endpoint_url.clone().into_iter().collect()
        }
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
    pub profile: Option<String>,
    pub access_key_id: Redacted,
    pub secret_access_key: Redacted,
    pub session_token: Redacted,
    pub create_table: bool,
    pub max_concurrency: usize,
    /// Bytes per blob chunk. A wire contract: it sets the byte->chunk-index
    /// mapping, so it must match the size the table's existing data was
    /// written with -- a mismatched reader silently returns wrong bytes.
    /// Unset uses the store default (64 KiB). Must be within 1..=350 KiB
    /// (DynamoDB's item ceiling with framing margin). Provisioning persists
    /// the size as a table marker that startup validation checks.
    pub chunk_size: Option<usize>,
}

#[cfg(feature = "dynamo")]
impl Default for ChainDataDynamoBlobConfig {
    fn default() -> Self {
        Self {
            table: None,
            endpoint_url: None,
            region: None,
            profile: None,
            access_key_id: Redacted(None),
            secret_access_key: Redacted(None),
            session_token: Redacted(None),
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
        // Reject loudly rather than clamp: the configured number must be the
        // number used, or the byte->chunk-index wire contract drifts.
        if let Some(chunk_size) = self.chunk_size {
            if chunk_size == 0 || chunk_size > crate::store::blob::MAX_CHUNK_SIZE {
                bail!(
                    "chain-data dynamo blob chunk_size must be within 1..={} bytes \
                     (and must match the size existing table data was written with)",
                    crate::store::blob::MAX_CHUNK_SIZE
                );
            }
        }
        validate_pair(
            &self.access_key_id.0,
            &self.secret_access_key.0,
            "dynamo access_key_id",
            "dynamo secret_access_key",
        )
    }
}

/// Which side is opening the stores; selects the default cache budget
/// (ingest runs cache-less, readers default to [`READER_DEFAULT_CACHE_MIB`]).
#[cfg(any(feature = "dynamo", feature = "mongo"))]
#[derive(Debug, Clone, Copy)]
pub(crate) enum ChainDataCacheMode {
    Ingest,
    Reader,
}

#[cfg(any(feature = "dynamo", feature = "mongo"))]
const READER_DEFAULT_CACHE_MIB: usize = 8192;

#[cfg(any(feature = "dynamo", feature = "mongo"))]
pub(crate) fn resolve_cache_config(
    store_config: &ChainDataStoreConfig,
    mode: ChainDataCacheMode,
) -> CacheConfig {
    let mode_default_mib = match mode {
        ChainDataCacheMode::Ingest => 0,
        ChainDataCacheMode::Reader => READER_DEFAULT_CACHE_MIB,
    };
    let total_mib = store_config.cache.total_mib.unwrap_or(mode_default_mib);

    let mut config = CacheConfig::from_total_mib(total_mib);
    apply_cache_table_overrides(&mut config, &store_config.cache.tables);
    config
}

#[cfg(any(feature = "dynamo", feature = "mongo"))]
fn apply_cache_table_overrides(config: &mut CacheConfig, overrides: &ChainDataCacheTableBudgets) {
    // One row per cache: (MiB override, target byte budget).
    #[rustfmt::skip]
    let slots = [
        (overrides.dir_by_block_mib, &mut config.dir_by_block_cache_bytes),
        (overrides.dir_bucket_mib, &mut config.dir_bucket_cache_bytes),
        (overrides.bitmap_by_block_mib, &mut config.bitmap_by_block_cache_bytes),
        (overrides.bitmap_page_blob_mib, &mut config.bitmap_page_blob_cache_bytes),
        (overrides.bitmap_page_counts_mib, &mut config.bitmap_page_counts_cache_bytes),
        (overrides.open_bitmap_stream_mib, &mut config.open_bitmap_stream_cache_bytes),
        (overrides.block_header_mib, &mut config.block_header_cache_bytes),
        (overrides.block_hash_to_number_mib, &mut config.block_hash_to_number_cache_bytes),
        (overrides.tx_hash_index_mib, &mut config.tx_hash_index_cache_bytes),
        (overrides.row_cache_mib, &mut config.row_cache_bytes),
    ];
    for (override_mib, target_bytes) in slots {
        if let Some(mib) = override_mib {
            *target_bytes = CacheConfig::budget_mib_to_bytes(mib);
        }
    }
}

// Configured store assembly only happens behind a dynamo meta backend, so the
// S3 blob builder is dead unless both features are on.
#[cfg(all(feature = "s3", feature = "dynamo"))]
pub(crate) async fn build_s3_blob_store(
    config: &ChainDataS3BlobConfig,
) -> Result<crate::store::S3BlobStore> {
    use crate::store::{S3BlobStore, S3BlobStoreConfig, S3Credentials};

    let credentials = match (&config.access_key_id.0, &config.secret_access_key.0) {
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
        profile: config.profile.clone(),
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
pub(crate) async fn build_dynamo_meta_store(
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
        &config.access_key_id.0,
        &config.secret_access_key.0,
        &config.session_token.0,
    );
    let store_config = DynamoMetaStoreConfig {
        table_layout,
        endpoint_urls: config.effective_endpoint_urls(),
        region: config.region.clone(),
        profile: config.profile.clone(),
        batch_max_concurrency: config.effective_max_concurrency(),
        batch_table_max_concurrency: config.effective_table_max_concurrency(),
        // Start at the DynamoDB-safe limit; a startup probe raises it if the
        // backend (Alternator) accepts larger batches.
        batch_write_max_items: DYNAMO_BATCH_WRITE_ITEMS,
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
    // Probe the effective BatchWriteItem limit (Alternator allows >25) with one
    // delete-batch of non-existent keys; fall back to 25 if rejected.
    let batch_candidate = config
        .batch_write_max_items
        .unwrap_or(if config.scylla_profile {
            ALTERNATOR_DEFAULT_BATCH_WRITE_ITEMS
        } else {
            DYNAMO_BATCH_WRITE_ITEMS
        });
    let effective = store
        .discover_batch_write_limit(batch_candidate)
        .await
        .context("probing dynamo BatchWriteItem item limit")?;
    info!(
        candidate = batch_candidate,
        effective, "resolved dynamo BatchWriteItem item limit"
    );
    Ok(store)
}

#[cfg(feature = "mongo")]
pub(crate) async fn build_mongo_meta_store(
    config: &ChainDataMongoMetaConfig,
) -> Result<crate::store::MongoMetaStore> {
    use crate::store::{MongoMetaStore, MongoMetaStoreConfig};

    let store_config = MongoMetaStoreConfig {
        connection_string: config.url.0.clone().expect("validated mongo url"),
        database: config.database.clone().expect("validated mongo database"),
        collection: config.collection.clone(),
        max_pool_size: config.max_pool_size,
        write_concurrency: config.write_concurrency,
    };
    MongoMetaStore::new(store_config)
        .await
        .context("building chain-data Mongo meta store")
}

/// The meta store's connection state (client ring + probed batch-write limit)
/// handed to a co-deployed dynamo blob store.
#[cfg(feature = "dynamo")]
pub(crate) type SharedDynamoConnection = crate::store::dynamo_common::SharedDynamoConnection;

#[cfg(feature = "dynamo")]
pub(crate) async fn build_dynamo_blob_store(
    config: &ChainDataDynamoBlobConfig,
    shared: Option<SharedDynamoConnection>,
) -> Result<crate::store::DynamoBlobStore> {
    use crate::store::{DynamoBlobStore, DynamoBlobStoreConfig};

    let defaults = DynamoBlobStoreConfig::new(config.table.clone().expect("validated table"));
    let store_config = DynamoBlobStoreConfig {
        endpoint_url: config.endpoint_url.clone(),
        region: config.region.clone(),
        profile: config.profile.clone(),
        batch_max_concurrency: config.max_concurrency,
        credentials: dynamo_credentials(
            &config.access_key_id.0,
            &config.secret_access_key.0,
            &config.session_token.0,
        ),
        chunk_size: config.chunk_size.unwrap_or(defaults.chunk_size),
        ..defaults
    };
    let chunk_size = store_config.chunk_size;
    let store = match shared {
        Some(shared) => {
            // The blob store reuses the meta store's client ring, so any of these
            // settings is silently overridden. Suppress the warn in the common
            // DynamoDynamo case where the blob endpoint already matches the meta
            // store's (and nothing else diverges) to avoid a spurious startup
            // warn on every co-deployed instance.
            let endpoint_matches_meta = match &store_config.endpoint_url {
                Some(endpoint) => shared.endpoint_urls.iter().any(|e| e == endpoint),
                None => true,
            };
            let other_overrides = store_config.region.is_some()
                || store_config.profile.is_some()
                || store_config.credentials.is_some();
            if !endpoint_matches_meta || other_overrides {
                warn!(
                    "chain-data dynamo blob endpoint/region/profile/credential settings are \
                     ignored: the blob store shares the meta store's client(s)"
                );
            }
            DynamoBlobStore::with_connection(
                shared,
                store_config.table_name.clone(),
                store_config.batch_max_concurrency,
                store_config.chunk_size,
            )
        }
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
    info!(chunk_size, "resolved dynamo blob chunk size");
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

#[cfg(any(feature = "s3", feature = "dynamo"))]
fn validate_pair(
    left: &Option<String>,
    right: &Option<String>,
    left_name: &'static str,
    right_name: &'static str,
) -> Result<()> {
    match (left, right) {
        (Some(_), Some(_)) | (None, None) => Ok(()),
        (Some(_), None) => bail!("{left_name} requires {right_name}"),
        (None, Some(_)) => bail!("{right_name} requires {left_name}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redacted_debug_hides_value_but_marks_presence() {
        // Some(_) renders the marker, never the secret; None renders None.
        assert_eq!(
            format!("{:?}", Redacted(Some("super-secret".to_string()))),
            "[REDACTED]"
        );
        assert_eq!(format!("{:?}", Redacted(None)), "None");
    }

    /// `#[serde(transparent)]` keeps the TOML wire shape identical to the
    /// plain `Option<String>` fields this replaced: a present key parses into
    /// `Some`, an absent key falls back to the container default (`None`).
    #[cfg(feature = "s3")]
    #[test]
    fn redacted_serde_is_transparent_to_option_string() {
        let parsed: ChainDataS3BlobConfig = toml::from_str(
            "bucket = \"b\"\naccess_key_id = \"ak-123\"\nsecret_access_key = \"sk-456\"\n",
        )
        .unwrap();
        assert_eq!(parsed.access_key_id.0.as_deref(), Some("ak-123"));
        assert_eq!(parsed.secret_access_key.0.as_deref(), Some("sk-456"));
        // The secret never reaches a debug rendering of the config.
        let dump = format!("{parsed:?}");
        assert!(!dump.contains("sk-456"));
        assert!(dump.contains("[REDACTED]"));

        let absent: ChainDataS3BlobConfig = toml::from_str("bucket = \"b\"\n").unwrap();
        assert!(absent.access_key_id.0.is_none());
        assert!(absent.secret_access_key.0.is_none());
    }

    /// chunk_size is a wire contract (byte->chunk-index mapping); out-of-range
    /// values must fail validation loudly rather than be silently clamped into
    /// a different mapping than the operator configured.
    #[cfg(feature = "dynamo")]
    #[test]
    fn dynamo_blob_chunk_size_must_be_within_wire_bounds() {
        use crate::store::blob::MAX_CHUNK_SIZE;

        let with_chunk_size = |chunk_size| ChainDataDynamoBlobConfig {
            table: Some("t".to_string()),
            chunk_size,
            ..ChainDataDynamoBlobConfig::default()
        };
        assert!(with_chunk_size(None).validate().is_ok());
        assert!(with_chunk_size(Some(64 * 1024)).validate().is_ok());
        assert!(with_chunk_size(Some(MAX_CHUNK_SIZE)).validate().is_ok());
        assert!(with_chunk_size(Some(0)).validate().is_err());
        assert!(with_chunk_size(Some(MAX_CHUNK_SIZE + 1))
            .validate()
            .is_err());
    }

    /// The startup probe materializes batch_write_max_items-many WriteRequests
    /// before the backend can reject them, so an absurd configured value must
    /// fail validation instead of allocating (or OOMing) at startup.
    #[cfg(feature = "dynamo")]
    #[test]
    fn dynamo_meta_batch_write_max_items_is_bounded() {
        let with_items = |items| ChainDataDynamoMetaConfig {
            table: Some("t".to_string()),
            batch_write_max_items: items,
            ..ChainDataDynamoMetaConfig::default()
        };
        assert!(with_items(None).validate().is_ok());
        assert!(with_items(Some(25)).validate().is_ok());
        assert!(with_items(Some(MAX_BATCH_WRITE_ITEMS)).validate().is_ok());
        assert!(with_items(Some(0)).validate().is_err());
        assert!(with_items(Some(100_000_000)).validate().is_err());
    }

    #[test]
    fn ingest_cache_default_is_disabled() {
        let config =
            resolve_cache_config(&ChainDataStoreConfig::default(), ChainDataCacheMode::Ingest);
        assert_eq!(config.total_bytes(), 0);
        assert_eq!(config.row_cache_bytes, 0);
    }

    #[test]
    fn reader_cache_default_is_eight_gib_ratio_based() {
        let config =
            resolve_cache_config(&ChainDataStoreConfig::default(), ChainDataCacheMode::Reader);
        // 464/1024 of 8192 MiB, as a byte budget.
        assert_eq!(config.row_cache_bytes, 3712 * 1024 * 1024);
        // 256/1024 of 8192 MiB.
        assert_eq!(config.bitmap_by_block_cache_bytes, 2048 * 1024 * 1024);
        // 64/1024 of 8192 MiB: block metadata sits on every materialization
        // path, so it must not be starved (see `CacheConfig` ratios).
        assert_eq!(config.block_header_cache_bytes, 512 * 1024 * 1024);
    }

    #[test]
    fn nested_zero_total_disables_reader_cache() {
        let store_config = ChainDataStoreConfig {
            cache: ChainDataCacheConfig {
                total_mib: Some(0),
                ..ChainDataCacheConfig::default()
            },
            ..ChainDataStoreConfig::default()
        };
        let config = resolve_cache_config(&store_config, ChainDataCacheMode::Reader);
        assert_eq!(config.total_bytes(), 0);
        assert_eq!(config.row_cache_bytes, 0);
    }

    #[test]
    fn per_table_budget_overrides_ratio_budget() {
        let store_config = ChainDataStoreConfig {
            cache: ChainDataCacheConfig {
                total_mib: Some(2048),
                tables: ChainDataCacheTableBudgets {
                    row_cache_mib: Some(64),
                    bitmap_page_blob_mib: Some(32),
                    ..ChainDataCacheTableBudgets::default()
                },
                ..ChainDataCacheConfig::default()
            },
            ..ChainDataStoreConfig::default()
        };
        let config = resolve_cache_config(&store_config, ChainDataCacheMode::Reader);
        assert_eq!(config.row_cache_bytes, 64 * 1024 * 1024);
        assert_eq!(config.bitmap_page_blob_cache_bytes, 32 * 1024 * 1024);
    }
}
