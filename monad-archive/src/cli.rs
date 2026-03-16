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

use std::str::FromStr;

use aws_config::{
    meta::{credentials::CredentialsProviderChain, region::RegionProviderChain},
    profile::{ProfileFileCredentialsProvider, ProfileFileRegionProvider},
    retry::RetryConfig,
    timeout::TimeoutConfig,
    BehaviorVersion, Region, SdkConfig,
};
use aws_sdk_s3::config::{Credentials, SharedCredentialsProvider};
use eyre::{bail, OptionExt};
use serde::{
    de::{self, DeserializeOwned},
    Deserialize, Serialize,
};
use serde_json::{Map as JsonMap, Value as JsonValue};

use crate::{archive_reader::redact_mongo_url, kvstore::mongo::MongoDbStorage, prelude::*};

const DEFAULT_BUCKET_TIMEOUT: u64 = 10;
const DEFAULT_OPERATION_TIMEOUT: u64 = 40;
const DEFAULT_CONCURRENCY: usize = 50;
const DEFAULT_TRIEDB_NODE_LRU_MAX_MEM: u64 = 50 << 20;
const DEFAULT_MAX_BUFFERED_READ_REQUESTS: usize = 5000;
const DEFAULT_MAX_TRIEDB_ASYNC_READ_CONCURRENCY: usize = 10000;
const DEFAULT_MAX_BUFFERED_TRAVERSE_REQUESTS: usize = 200;
const DEFAULT_MAX_TRIEDB_ASYNC_TRAVERSE_CONCURRENCY: usize = 20;
const DEFAULT_MAX_FINALIZED_BLOCK_CACHE_LEN: usize = 200;
const DEFAULT_MAX_VOTED_BLOCK_CACHE_LEN: usize = 3;

fn default_aws_concurrency() -> usize {
    DEFAULT_CONCURRENCY
}

fn default_triedb_node_lru_max_mem() -> u64 {
    DEFAULT_TRIEDB_NODE_LRU_MAX_MEM
}

fn default_triedb_max_buffered_read_requests() -> usize {
    DEFAULT_MAX_BUFFERED_READ_REQUESTS
}

fn default_triedb_max_async_read_concurrency() -> usize {
    DEFAULT_MAX_TRIEDB_ASYNC_READ_CONCURRENCY
}

fn default_triedb_max_buffered_traverse_requests() -> usize {
    DEFAULT_MAX_BUFFERED_TRAVERSE_REQUESTS
}

fn default_triedb_max_async_traverse_concurrency() -> usize {
    DEFAULT_MAX_TRIEDB_ASYNC_TRAVERSE_CONCURRENCY
}

fn default_triedb_max_finalized_block_cache_len() -> usize {
    DEFAULT_MAX_FINALIZED_BLOCK_CACHE_LEN
}

fn default_triedb_max_voted_block_cache_len() -> usize {
    DEFAULT_MAX_VOTED_BLOCK_CACHE_LEN
}

pub fn get_default_bucket_timeout() -> u64 {
    DEFAULT_BUCKET_TIMEOUT
}

fn get_default_operation_timeout() -> u64 {
    DEFAULT_OPERATION_TIMEOUT
}

pub fn set_source_and_sink_metrics(
    sink: &ArchiveArgs,
    source: &BlockDataReaderArgs,
    metrics: &Metrics,
) {
    match sink {
        ArchiveArgs::Aws(_) => {
            metrics.periodic_gauge_with_attrs(
                MetricNames::SINK_STORE_TYPE,
                1,
                vec![opentelemetry::KeyValue::new("sink_store_type", "aws")],
            );
        }
        ArchiveArgs::MongoDb(_) => {
            metrics.periodic_gauge_with_attrs(
                MetricNames::SINK_STORE_TYPE,
                2,
                vec![opentelemetry::KeyValue::new("sink_store_type", "mongodb")],
            );
        }
        ArchiveArgs::Fs(_) => {
            metrics.periodic_gauge_with_attrs(
                MetricNames::SINK_STORE_TYPE,
                3,
                vec![opentelemetry::KeyValue::new("sink_store_type", "fs")],
            );
        }
    }

    match source {
        BlockDataReaderArgs::Aws(_) => {
            metrics.periodic_gauge_with_attrs(
                MetricNames::SOURCE_STORE_TYPE,
                1,
                vec![opentelemetry::KeyValue::new("source_store_type", "aws")],
            );
        }
        BlockDataReaderArgs::MongoDb(_) => {
            metrics.periodic_gauge_with_attrs(
                MetricNames::SOURCE_STORE_TYPE,
                2,
                vec![opentelemetry::KeyValue::new("source_store_type", "mongodb")],
            );
        }
        BlockDataReaderArgs::Fs(_) => {
            metrics.periodic_gauge_with_attrs(
                MetricNames::SOURCE_STORE_TYPE,
                4,
                vec![opentelemetry::KeyValue::new("source_store_type", "fs")],
            );
        }
        BlockDataReaderArgs::Triedb(_) => {
            metrics.periodic_gauge_with_attrs(
                MetricNames::SOURCE_STORE_TYPE,
                3,
                vec![opentelemetry::KeyValue::new("source_store_type", "triedb")],
            );
        }
    }
}

pub async fn get_aws_config(region: Option<String>, timeout_secs: u64) -> SdkConfig {
    let region_provider = RegionProviderChain::default_provider().or_else(
        region
            .map(Region::new)
            .unwrap_or_else(|| Region::new("us-east-2")),
    );

    info!(
        "Running in region: {}",
        region_provider
            .region()
            .await
            .map(|r| r.to_string())
            .unwrap_or("No region found".into())
    );

    aws_config::defaults(BehaviorVersion::latest())
        .region(region_provider)
        .timeout_config(
            TimeoutConfig::builder()
                .operation_timeout(Duration::from_secs(4 * timeout_secs))
                .operation_attempt_timeout(Duration::from_secs(timeout_secs))
                .read_timeout(Duration::from_secs(timeout_secs))
                .build(),
        )
        .retry_config(RetryConfig::standard().with_max_attempts(3))
        .load()
        .await
}

#[derive(Debug, Clone, Serialize, Eq, PartialEq, Hash)]
pub enum BlockDataReaderArgs {
    Aws(AwsCliArgs),
    Triedb(TrieDbCliArgs),
    MongoDb(MongoDbCliArgs),
    Fs(FsCliArgs),
}

#[derive(Debug, Clone, Serialize, Eq, PartialEq, Hash)]
pub enum ArchiveArgs {
    Aws(AwsCliArgs),
    MongoDb(MongoDbCliArgs),
    Fs(FsCliArgs),
}

impl FromStr for BlockDataReaderArgs {
    type Err = eyre::Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        use BlockDataReaderArgs::*;
        let (storage_type, args) = s.split_once(' ').ok_or_eyre("Storage args string empty")?;

        Ok(match storage_type.to_lowercase().as_str() {
            "aws" => Aws(AwsCliArgs::parse(args)?),
            "triedb" => Triedb(TrieDbCliArgs::parse(args)?),
            "mongodb" => MongoDb(MongoDbCliArgs::parse(args)?),
            "fs" => Fs(FsCliArgs::parse(args)?),
            _ => {
                bail!("Unrecognized storage args variant: {storage_type}");
            }
        })
    }
}

impl FromStr for ArchiveArgs {
    type Err = eyre::Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        use ArchiveArgs::*;
        let (storage_type, args) = s.split_once(' ').ok_or_eyre("Storage args string empty")?;

        Ok(match storage_type.to_lowercase().as_str() {
            "aws" => Aws(AwsCliArgs::parse(args)?),
            "mongodb" => MongoDb(MongoDbCliArgs::parse(args)?),
            "fs" => Fs(FsCliArgs::parse(args)?),
            _ => {
                bail!("Unrecognized storage args variant: {storage_type}");
            }
        })
    }
}

impl<'de> Deserialize<'de> for BlockDataReaderArgs {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = JsonValue::deserialize(deserializer)?;
        parse_block_data_reader_args(value)
    }
}

impl<'de> Deserialize<'de> for ArchiveArgs {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = JsonValue::deserialize(deserializer)?;
        parse_archive_args(value)
    }
}

impl BlockDataReaderArgs {
    pub async fn build(&self, metrics: &Metrics) -> Result<BlockDataReaderErased> {
        use BlockDataReaderArgs::*;
        Ok(match self {
            Triedb(args) => TriedbReader::new(args).into(),
            Aws(args) => {
                let config = args.config().await?;
                let bucket = Bucket::new(args.bucket.clone(), &config, metrics.clone());
                BlockDataArchive::new(bucket).into()
            }
            MongoDb(args) => BlockDataArchive::new(
                MongoDbStorage::new_block_store(&args.url, &args.db, metrics.clone()).await?,
            )
            .into(),
            Fs(fs_args) => {
                BlockDataArchive::new(FsStorage::new(fs_args.block_store_path(), metrics.clone())?)
                    .into()
            }
        })
    }

    pub fn replica_name(&self) -> String {
        use BlockDataReaderArgs::*;
        match self {
            Aws(aws_cli_args) => aws_cli_args.bucket.clone(),
            Triedb(trie_db_cli_args) => trie_db_cli_args.triedb_path.clone(),
            MongoDb(mongo_db_cli_args) => mongo_db_cli_args.replica_name(),
            Fs(fs_cli_args) => fs_cli_args.path.to_string_lossy().into_owned(),
        }
    }
}

impl ArchiveArgs {
    pub async fn build_block_data_archive(&self, metrics: &Metrics) -> Result<BlockDataArchive> {
        let store: KVStoreErased = match self {
            ArchiveArgs::Aws(args) => {
                let config = args.config().await?;
                Bucket::new(args.bucket.clone(), &config, metrics.clone()).into()
            }
            ArchiveArgs::MongoDb(args) => {
                MongoDbStorage::new_block_store(&args.url, &args.db, metrics.clone())
                    .await?
                    .into()
            }
            ArchiveArgs::Fs(args) => {
                FsStorage::new(args.block_store_path(), metrics.clone())?.into()
            }
        };
        Ok(BlockDataArchive::new(store))
    }

    pub async fn build_index_archive(
        &self,
        metrics: &Metrics,
        max_inline_encoded_len: usize,
    ) -> Result<TxIndexArchiver> {
        let (blob, index): (KVStoreErased, KVStoreErased) = match self {
            ArchiveArgs::Aws(args) => {
                let config = args.config().await?;
                let bucket = Bucket::new(args.bucket.clone(), &config, metrics.clone());
                let index = DynamoDBArchive::new(
                    bucket.clone(),
                    args.bucket.clone(),
                    &config,
                    args.concurrency,
                    metrics.clone(),
                );
                (bucket.into(), index.into())
            }
            ArchiveArgs::MongoDb(args) => (
                MongoDbStorage::new_block_store(&args.url, &args.db, metrics.clone())
                    .await?
                    .into(),
                MongoDbStorage::new_index_store(&args.url, &args.db, metrics.clone())
                    .await?
                    .into(),
            ),
            ArchiveArgs::Fs(args) => (
                FsStorage::new(args.block_store_path(), metrics.clone())?.into(),
                FsStorage::new(args.index_store_path(), metrics.clone())?.into(),
            ),
        };
        Ok(TxIndexArchiver::new(
            index,
            BlockDataArchive::new(blob),
            max_inline_encoded_len,
        ))
    }

    pub async fn build_archive_reader(&self, metrics: &Metrics) -> Result<ArchiveReader> {
        let (blob, index): (KVStoreErased, KVStoreErased) = match self {
            ArchiveArgs::Aws(args) => {
                let config = args.config().await?;
                let bucket = Bucket::new(args.bucket.clone(), &config, metrics.clone());
                let index = DynamoDBArchive::new(
                    bucket.clone(),
                    args.bucket.clone(),
                    &config,
                    // TODO: remove me, concurrency should be handled elsewhere
                    args.concurrency,
                    metrics.clone(),
                );
                (bucket.into(), index.into())
            }
            ArchiveArgs::MongoDb(args) => (
                MongoDbStorage::new_block_store(&args.url, &args.db, metrics.clone())
                    .await?
                    .into(),
                MongoDbStorage::new_index_store(&args.url, &args.db, metrics.clone())
                    .await?
                    .into(),
            ),
            ArchiveArgs::Fs(args) => (
                FsStorage::new(args.block_store_path(), metrics.clone())?.into(),
                FsStorage::new(args.index_store_path(), metrics.clone())?.into(),
            ),
        };
        let bdr = BlockDataReaderErased::from(BlockDataArchive::new(blob));
        Ok(ArchiveReader::new(
            bdr.clone(),
            IndexReaderImpl::new(index, bdr),
            None,
            None,
        ))
    }

    pub fn replica_name(&self) -> String {
        match self {
            ArchiveArgs::Aws(aws_cli_args) => aws_cli_args.bucket.clone(),
            ArchiveArgs::MongoDb(mongo_db_cli_args) => mongo_db_cli_args.replica_name(),
            ArchiveArgs::Fs(fs_cli_args) => fs_cli_args.path.to_string_lossy().into_owned(),
        }
    }
}

fn parse_block_data_reader_args<E: de::Error>(value: JsonValue) -> Result<BlockDataReaderArgs, E> {
    let map = match value {
        JsonValue::Object(map) => map,
        other => {
            return Err(E::custom(format!(
                "block_data_source must be a table, got {other:?}",
            )))
        }
    };

    parse_type_or_legacy(
        map,
        "block_data_source",
        |ty, cfg| match ty.as_str() {
            "aws" => {
                deserialize_variant(cfg, "aws block_data_source").map(BlockDataReaderArgs::Aws)
            }
            "triedb" => deserialize_variant(cfg, "triedb block_data_source")
                .map(BlockDataReaderArgs::Triedb),
            "mongodb" => deserialize_variant(cfg, "mongodb block_data_source")
                .map(BlockDataReaderArgs::MongoDb),
            "fs" => deserialize_variant(cfg, "fs block_data_source").map(BlockDataReaderArgs::Fs),
            other => Err(E::custom(format!(
                "unsupported block_data_source type '{other}'",
            ))),
        },
        |variant, cfg| match variant.as_str() {
            "Aws" => {
                deserialize_variant(cfg, "Aws block_data_source").map(BlockDataReaderArgs::Aws)
            }
            "Triedb" => deserialize_variant(cfg, "Triedb block_data_source")
                .map(BlockDataReaderArgs::Triedb),
            "MongoDb" => deserialize_variant(cfg, "MongoDb block_data_source")
                .map(BlockDataReaderArgs::MongoDb),
            "Fs" => deserialize_variant(cfg, "Fs block_data_source").map(BlockDataReaderArgs::Fs),
            other => Err(E::custom(format!(
                "unsupported block_data_source variant '{other}'",
            ))),
        },
    )
}

fn parse_archive_args<E: de::Error>(value: JsonValue) -> Result<ArchiveArgs, E> {
    let map = match value {
        JsonValue::Object(map) => map,
        other => {
            return Err(E::custom(format!(
                "archive_sink must be a table, got {other:?}",
            )))
        }
    };

    parse_type_or_legacy(
        map,
        "archive_sink",
        |ty, cfg| match ty.as_str() {
            "aws" => deserialize_variant(cfg, "aws archive_sink").map(ArchiveArgs::Aws),
            "mongodb" => deserialize_variant(cfg, "mongodb archive_sink").map(ArchiveArgs::MongoDb),
            "fs" => deserialize_variant(cfg, "fs archive_sink").map(ArchiveArgs::Fs),
            other => Err(E::custom(format!(
                "unsupported archive_sink type '{other}'",
            ))),
        },
        |variant, cfg| match variant.as_str() {
            "Aws" => deserialize_variant(cfg, "Aws archive_sink").map(ArchiveArgs::Aws),
            "MongoDb" => deserialize_variant(cfg, "MongoDb archive_sink").map(ArchiveArgs::MongoDb),
            "Fs" => deserialize_variant(cfg, "Fs archive_sink").map(ArchiveArgs::Fs),
            other => Err(E::custom(format!(
                "unsupported archive_sink variant '{other}'",
            ))),
        },
    )
}

fn parse_type_or_legacy<E, FTyped, FLegacy, R>(
    mut map: JsonMap<String, JsonValue>,
    context: &str,
    mut typed: FTyped,
    mut legacy: FLegacy,
) -> Result<R, E>
where
    E: de::Error,
    FTyped: FnMut(String, JsonValue) -> Result<R, E>,
    FLegacy: FnMut(String, JsonValue) -> Result<R, E>,
{
    if let Some(type_value) = map.remove("type") {
        let type_name = extract_type::<E>(type_value, context)?;
        let cfg = JsonValue::Object(map);
        return typed(type_name, cfg);
    }

    if map.len() != 1 {
        return Err(E::custom(format!(
            "{context} must contain a 'type' field or a single legacy variant",
        )));
    }

    let (variant, value) = map.into_iter().next().unwrap();
    legacy(variant, value)
}

fn extract_type<E: de::Error>(value: JsonValue, context: &str) -> Result<String, E> {
    value
        .as_str()
        .map(|s| s.to_lowercase())
        .ok_or_else(|| E::custom(format!("{context}.type must be a string")))
}

fn deserialize_variant<E: de::Error, T: DeserializeOwned>(
    value: JsonValue,
    label: &str,
) -> Result<T, E> {
    serde_json::from_value(value)
        .map_err(|err| E::custom(format!("failed to parse {label}: {err}")))
}

#[derive(Clone, Serialize, Deserialize, Default, Eq, PartialEq, Hash)]
pub struct AwsCliArgs {
    pub bucket: String,
    pub region: Option<String>,
    pub endpoint: Option<String>,
    pub profile: Option<String>,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    // TODO: remove me, concurrency should be handled elsewhere
    #[serde(default = "default_aws_concurrency")]
    pub concurrency: usize,
    // If these are not provided, uses timeout_secs for all
    #[serde(default = "get_default_operation_timeout")]
    pub operation_timeout_secs: u64,
    #[serde(default = "get_default_bucket_timeout")]
    pub operation_attempt_timeout_secs: u64,
    #[serde(default = "get_default_bucket_timeout")]
    pub read_timeout_secs: u64,
}

impl std::fmt::Debug for AwsCliArgs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AwsCliArgs")
            .field("bucket", &self.bucket)
            .field("region", &self.region)
            .field("endpoint", &self.endpoint)
            .field("profile", &self.profile)
            .field(
                "access_key_id",
                &self.access_key_id.as_ref().map(|_| "[REDACTED]"),
            )
            .field(
                "secret_access_key",
                &self.secret_access_key.as_ref().map(|_| "[REDACTED]"),
            )
            .field("concurrency", &self.concurrency)
            .field("operation_timeout_secs", &self.operation_timeout_secs)
            .field(
                "operation_attempt_timeout_secs",
                &self.operation_attempt_timeout_secs,
            )
            .field("read_timeout_secs", &self.read_timeout_secs)
            .finish()
    }
}

impl AwsCliArgs {
    pub fn parse(s: &str) -> Result<Self> {
        let (mut positional, mut kv) = parse_str_positional_and_kv(s)?;

        let get_u64 = |kv: &HashMap<String, String>, key: &str, default: u64| -> u64 {
            kv.get(key)
                .and_then(|s| u64::from_str(s).ok())
                .unwrap_or(default)
        };

        let timeout_secs = get_u64(&kv, "timeout-secs", DEFAULT_BUCKET_TIMEOUT);
        info!("Using timeout_secs: {}", timeout_secs);

        let args = Self {
            // prefer positional, fallback to kv
            bucket: positional
                .first_mut()
                .map(std::mem::take)
                .or_else(|| kv.remove("bucket"))
                .ok_or_eyre("storage args missing bucket")?,
            region: kv.remove("region"),
            endpoint: kv.remove("endpoint"),
            profile: kv.remove("profile"),
            access_key_id: kv.remove("access-key-id"),
            secret_access_key: kv.remove("secret-access-key"),
            concurrency: kv
                .remove("concurrency")
                .and_then(|s| usize::from_str(&s).ok())
                // TODO: remove me, concurrency should be handled elsewhere
                .unwrap_or(DEFAULT_CONCURRENCY),
            // If these are not provided, uses timeout_secs for all
            operation_timeout_secs: get_u64(&kv, "operation-timeout-secs", 4 * timeout_secs),
            operation_attempt_timeout_secs: get_u64(
                &kv,
                "operation-attempt-timeout-secs",
                timeout_secs,
            ),
            read_timeout_secs: get_u64(&kv, "read-timeout-secs", timeout_secs),
        };

        args.validate_credentials_config()?;
        Ok(args)
    }

    pub(crate) async fn config(&self) -> Result<SdkConfig> {
        self.validate_credentials_config()?;

        let mut config = aws_config::defaults(BehaviorVersion::latest());

        if let Some(profile) = &self.profile {
            config = config.profile_name(profile);
        }

        config = config.region(self.region_provider());

        if let Some(credentials_provider) = self.credentials_provider().await? {
            config = config.credentials_provider(credentials_provider);
        }

        let mut config = config
            .timeout_config(
                TimeoutConfig::builder()
                    .operation_timeout(Duration::from_secs(self.operation_timeout_secs))
                    .operation_attempt_timeout(Duration::from_secs(
                        self.operation_attempt_timeout_secs,
                    ))
                    .read_timeout(Duration::from_secs(self.read_timeout_secs))
                    .build(),
            )
            .retry_config(RetryConfig::standard().with_max_attempts(3));

        if let Some(endpoint) = &self.endpoint {
            config = config.endpoint_url(endpoint);
        }

        let sdk_config = config.load().await;
        let region = sdk_config
            .region()
            .map(|region| region.as_ref())
            .unwrap_or("unknown");
        info!("Bucket {} running in region: {}", self.bucket, region);

        Ok(sdk_config)
    }

    fn validate_credentials_config(&self) -> Result<()> {
        match (&self.access_key_id, &self.secret_access_key) {
            (Some(_), None) => bail!(
                "aws config for bucket '{}' must set --secret-access-key when --access-key-id is provided",
                self.bucket
            ),
            (None, Some(_)) => bail!(
                "aws config for bucket '{}' must set --access-key-id when --secret-access-key is provided",
                self.bucket
            ),
            _ => Ok(()),
        }
    }

    async fn credentials_provider(&self) -> Result<Option<SharedCredentialsProvider>> {
        if let Some(provider) = self.static_credentials_provider()? {
            return Ok(Some(provider));
        }

        if let Some(profile) = &self.profile {
            let provider = CredentialsProviderChain::first_try(
                "Profile",
                ProfileFileCredentialsProvider::builder()
                    .profile_name(profile)
                    .build(),
            )
            .or_default_provider()
            .await;
            return Ok(Some(SharedCredentialsProvider::new(provider)));
        }

        Ok(None)
    }

    fn static_credentials_provider(&self) -> Result<Option<SharedCredentialsProvider>> {
        match (&self.access_key_id, &self.secret_access_key) {
            (Some(access_key_id), Some(secret_access_key)) => {
                Ok(Some(SharedCredentialsProvider::new(Credentials::new(
                    access_key_id,
                    secret_access_key,
                    None,
                    None,
                    "minio",
                ))))
            }
            (None, None) => Ok(None),
            _ => {
                self.validate_credentials_config()?;
                unreachable!("invalid partial credentials should be rejected")
            }
        }
    }

    fn region_provider(&self) -> RegionProviderChain {
        if let Some(region) = &self.region {
            return RegionProviderChain::first_try(Region::new(region.clone()));
        }

        if let Some(profile) = &self.profile {
            return RegionProviderChain::first_try(
                ProfileFileRegionProvider::builder()
                    .profile_name(profile)
                    .build(),
            )
            .or_default_provider()
            .or_else(Region::new("us-east-2"));
        }

        RegionProviderChain::default_provider().or_else(Region::new("us-east-2"))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Hash, PartialEq, Eq)]
pub struct TrieDbCliArgs {
    pub triedb_path: String,
    #[serde(default = "default_triedb_node_lru_max_mem")]
    pub triedb_node_lru_max_mem: u64,
    #[serde(default = "default_triedb_max_buffered_read_requests")]
    pub max_buffered_read_requests: usize,
    #[serde(default = "default_triedb_max_async_read_concurrency")]
    pub max_triedb_async_read_concurrency: usize,
    #[serde(default = "default_triedb_max_buffered_traverse_requests")]
    pub max_buffered_traverse_requests: usize,
    #[serde(default = "default_triedb_max_async_traverse_concurrency")]
    pub max_triedb_async_traverse_concurrency: usize,
    #[serde(default = "default_triedb_max_finalized_block_cache_len")]
    pub max_finalized_block_cache_len: usize,
    #[serde(default = "default_triedb_max_voted_block_cache_len")]
    pub max_voted_block_cache_len: usize,
}

impl TrieDbCliArgs {
    pub fn parse(s: &str) -> Result<TrieDbCliArgs> {
        let (positional, kv) = parse_str_positional_and_kv(s)?;
        let triedb_path = positional
            .first()
            .ok_or_eyre("storage args missing db path")?
            .to_string();

        // get a usize from kv or use default value
        let get = |key: &str, default: usize| -> usize {
            kv.get(key)
                .and_then(|s| usize::from_str(s).ok())
                .unwrap_or(default)
        };

        // only this first one should be positional for backcompat
        let max_buffered_read_requests = positional
            .get(1)
            .and_then(|s| usize::from_str(s).ok())
            .unwrap_or_else(|| {
                get(
                    "max-buffered-read-requests",
                    DEFAULT_MAX_BUFFERED_READ_REQUESTS,
                )
            });

        Ok(TrieDbCliArgs {
            triedb_path,
            max_buffered_read_requests,
            max_triedb_async_read_concurrency: get(
                "max-triedb-async-read-concurrency",
                DEFAULT_MAX_TRIEDB_ASYNC_READ_CONCURRENCY,
            ),
            max_buffered_traverse_requests: get(
                "max-buffered-traverse-requests",
                DEFAULT_MAX_BUFFERED_TRAVERSE_REQUESTS,
            ),
            max_triedb_async_traverse_concurrency: get(
                "max-triedb-async-traverse-concurrency",
                DEFAULT_MAX_TRIEDB_ASYNC_TRAVERSE_CONCURRENCY,
            ),
            max_finalized_block_cache_len: get(
                "max-finalized-block-cache-len",
                DEFAULT_MAX_FINALIZED_BLOCK_CACHE_LEN,
            ),
            max_voted_block_cache_len: get(
                "max-voted-block-cache-len",
                DEFAULT_MAX_VOTED_BLOCK_CACHE_LEN,
            ),
            triedb_node_lru_max_mem: DEFAULT_TRIEDB_NODE_LRU_MAX_MEM, // 50MB
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Hash, PartialEq, Eq)]
pub struct MongoDbCliArgs {
    pub url: String,
    pub db: String,
    /// Optional explicit replica name. If not set, derived from redacted URL and db name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replica_name: Option<String>,
}

impl MongoDbCliArgs {
    pub fn parse(s: &str) -> Result<Self> {
        let (positional, mut kv) = parse_str_positional_and_kv(s)?;
        Ok(Self {
            url: kv
                .remove("url")
                .or_else(|| positional.first().cloned())
                .ok_or_eyre("storage args missing mongo url")?,
            db: kv
                .remove("db")
                .or_else(|| positional.get(1).cloned())
                .ok_or_eyre("storage args missing mongo db name")?,
            replica_name: kv.remove("replica-name"),
        })
    }

    /// Returns the replica name for this MongoDB connection.
    /// Uses explicit replica_name if set, otherwise derives from redacted URL and db name.
    pub fn replica_name(&self) -> String {
        self.replica_name
            .clone()
            .unwrap_or_else(|| format!("{}:{}", redact_mongo_url(&self.url), self.db))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Hash, PartialEq, Eq)]
pub struct FsCliArgs {
    pub path: PathBuf,
}

impl FsCliArgs {
    pub fn parse(s: &str) -> Result<Self> {
        let (positional, mut kv) = parse_str_positional_and_kv(s)?;
        Ok(Self {
            path: kv
                .remove("path")
                .or_else(|| positional.first().cloned())
                .map(PathBuf::from)
                .ok_or_eyre("storage args missing path")?,
        })
    }

    pub fn block_store_path(&self) -> PathBuf {
        self.path.join("blocks")
    }

    pub fn index_store_path(&self) -> PathBuf {
        self.path.join("index")
    }
}

// Parse a string into a list of positional arguments and a map of key-value pairs.
// Example: "aws s3://bucket/path --concurrency 10" -> (["aws", "s3://bucket/path"], {"concurrency": "10"})
fn parse_str_positional_and_kv(s: &str) -> Result<(Vec<String>, HashMap<String, String>)> {
    let mut positional = Vec::new();
    let mut kv_pairs = HashMap::new();

    let mut parts = s.split_whitespace().peekable();

    while let Some(part) = parts.next() {
        if part.starts_with("--") {
            // This is a key-value flag
            let key = part.trim_start_matches("--");

            // Check if there's a value following this flag
            if let Some(next) = parts.peek() {
                if !next.starts_with("--") {
                    // The next item is the value for this flag
                    let value = parts.next().ok_or_eyre("Flag requires a value")?;
                    kv_pairs.insert(key.to_string(), value.to_string());
                } else {
                    // No value provided for this flag
                    bail!("Flag --{} requires a value", key)
                }
            } else {
                // Flag at the end with no value
                bail!("Flag --{} requires a value", key);
            }
        } else {
            // This is a positional argument
            positional.push(part.to_string());
        }
    }

    Ok((positional, kv_pairs))
}

#[cfg(test)]
mod tests {
    use std::{
        env, fs,
        path::PathBuf,
        str::FromStr,
        sync::{Mutex, MutexGuard, OnceLock},
    };

    use aws_sdk_s3::config::ProvideCredentials;
    use tempfile::tempdir;

    use super::*;

    static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    struct EnvGuard {
        _guard: MutexGuard<'static, ()>,
        originals: Vec<(&'static str, Option<String>)>,
    }

    impl EnvGuard {
        fn set(vars: &[(&'static str, &str)]) -> Self {
            let guard = ENV_LOCK
                .get_or_init(|| Mutex::new(()))
                .lock()
                .unwrap_or_else(|err| err.into_inner());

            let originals = vars
                .iter()
                .map(|(key, _)| (*key, env::var(key).ok()))
                .collect::<Vec<_>>();

            for (key, value) in vars {
                // Tests serialize env mutations through ENV_LOCK.
                unsafe { env::set_var(key, value) };
            }

            Self {
                _guard: guard,
                originals,
            }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            for (key, value) in self.originals.drain(..) {
                match value {
                    Some(value) => {
                        // Tests serialize env mutations through ENV_LOCK.
                        unsafe { env::set_var(key, value) };
                    }
                    None => {
                        // Tests serialize env mutations through ENV_LOCK.
                        unsafe { env::remove_var(key) };
                    }
                }
            }
        }
    }

    fn write_profile_files(
        config_contents: &str,
        credentials_contents: &str,
    ) -> (tempfile::TempDir, PathBuf, PathBuf) {
        let dir = tempdir().unwrap();
        let config_path = dir.path().join("config");
        let credentials_path = dir.path().join("credentials");
        fs::write(&config_path, config_contents).unwrap();
        fs::write(&credentials_path, credentials_contents).unwrap();
        (dir, config_path, credentials_path)
    }

    #[test]
    fn parse_str_positional_and_kv_basic() {
        let s = "object-store s3://bucket/path --env-prefix FOO";
        let (pos, kv) = parse_str_positional_and_kv(s).unwrap();
        assert_eq!(pos, vec!["object-store", "s3://bucket/path"]);
        assert_eq!(kv.get("env-prefix").map(String::as_str), Some("FOO"));
    }

    #[test]
    fn parse_str_positional_and_kv_many_flags_and_positional() {
        let s = "triedb /db/path \
                 --max-triedb-async-read-concurrency 100 \
                 --max-buffered-traverse-requests 300 \
                 --max-triedb-async-traverse-concurrency 30 \
                 --max-finalized-block-cache-len 250 \
                 --max-voted-block-cache-len 5";
        let (pos, kv) = parse_str_positional_and_kv(s).unwrap();
        assert_eq!(pos, vec!["triedb", "/db/path"]);
        assert_eq!(kv.get("max-triedb-async-read-concurrency").unwrap(), "100");
        assert_eq!(kv.get("max-buffered-traverse-requests").unwrap(), "300");
        assert_eq!(
            kv.get("max-triedb-async-traverse-concurrency").unwrap(),
            "30"
        );
        assert_eq!(kv.get("max-finalized-block-cache-len").unwrap(), "250");
        assert_eq!(kv.get("max-voted-block-cache-len").unwrap(), "5");
    }

    #[test]
    fn parse_str_positional_and_kv_missing_flag_value_errors() {
        let s = "aws s3://bucket --concurrency";
        let err = parse_str_positional_and_kv(s).unwrap_err().to_string();
        assert!(err.contains("requires a value"));
    }

    #[test]
    fn aws_fromstr_defaults() {
        let a = BlockDataReaderArgs::from_str("aws my-bucket").unwrap();
        match a {
            BlockDataReaderArgs::Aws(args) => {
                assert_eq!(args.bucket, "my-bucket");
                assert_eq!(args.concurrency, DEFAULT_CONCURRENCY); // default
                assert_eq!(args.region, None);
                assert_eq!(args.profile, None);
            }
            _ => panic!("expected Aws variant"),
        }
    }

    #[test]
    fn aws_fromstr_overrides() {
        let a = BlockDataReaderArgs::from_str("aws my-bucket --concurrency 64 --region us-west-2")
            .unwrap();
        match a {
            BlockDataReaderArgs::Aws(args) => {
                assert_eq!(args.bucket, "my-bucket");
                assert_eq!(args.concurrency, 64);
                assert_eq!(args.region.as_deref(), Some("us-west-2"));
                assert_eq!(args.profile, None);
            }
            _ => panic!("expected Aws variant"),
        }

        let a = BlockDataReaderArgs::from_str("aws my-bucket --profile mainnet").unwrap();
        match a {
            BlockDataReaderArgs::Aws(args) => {
                assert_eq!(args.bucket, "my-bucket");
                assert_eq!(args.profile.as_deref(), Some("mainnet"));
            }
            _ => panic!("expected Aws variant"),
        }

        let a = BlockDataReaderArgs::from_str("aws my-bucket 64 us-west-2").unwrap();
        match a {
            BlockDataReaderArgs::Aws(args) => {
                assert_eq!(args.bucket, "my-bucket");
                assert_eq!(args.concurrency, DEFAULT_CONCURRENCY);
                assert_eq!(args.region.as_deref(), None);
            }
            _ => panic!("expected Aws variant"),
        }

        let err = BlockDataReaderArgs::from_str("aws my-bucket --access-key-id only-id")
            .unwrap_err()
            .to_string();
        assert!(err.contains("--secret-access-key"));
    }

    #[tokio::test]
    async fn aws_profile_credentials_ignore_environment_credentials() {
        let (_dir, config_path, credentials_path) = write_profile_files(
            "[profile isolated]\nregion = us-west-2\n",
            "[isolated]\naws_access_key_id = PROFILE_KEY\naws_secret_access_key = PROFILE_SECRET\n",
        );

        let _env = EnvGuard::set(&[
            ("AWS_CONFIG_FILE", config_path.to_str().unwrap()),
            (
                "AWS_SHARED_CREDENTIALS_FILE",
                credentials_path.to_str().unwrap(),
            ),
            ("AWS_ACCESS_KEY_ID", "ENV_KEY"),
            ("AWS_SECRET_ACCESS_KEY", "ENV_SECRET"),
            ("AWS_REGION", "eu-central-1"),
        ]);

        let config = AwsCliArgs {
            bucket: "my-bucket".to_string(),
            profile: Some("isolated".to_string()),
            ..Default::default()
        }
        .config()
        .await
        .unwrap();

        let credentials = config
            .credentials_provider()
            .unwrap()
            .provide_credentials()
            .await
            .unwrap();
        assert_eq!(credentials.access_key_id(), "PROFILE_KEY");
        assert_eq!(credentials.secret_access_key(), "PROFILE_SECRET");
    }

    #[tokio::test]
    async fn aws_profile_region_ignores_environment_region() {
        let (_dir, config_path, credentials_path) = write_profile_files(
            "[profile isolated]\nregion = us-west-2\n",
            "[isolated]\naws_access_key_id = PROFILE_KEY\naws_secret_access_key = PROFILE_SECRET\n",
        );

        let _env = EnvGuard::set(&[
            ("AWS_CONFIG_FILE", config_path.to_str().unwrap()),
            (
                "AWS_SHARED_CREDENTIALS_FILE",
                credentials_path.to_str().unwrap(),
            ),
            ("AWS_REGION", "eu-central-1"),
        ]);

        let config = AwsCliArgs {
            bucket: "my-bucket".to_string(),
            profile: Some("isolated".to_string()),
            ..Default::default()
        }
        .config()
        .await
        .unwrap();

        assert_eq!(
            config.region().map(|region| region.as_ref()),
            Some("us-west-2")
        );
    }

    #[tokio::test]
    async fn aws_profile_without_region_falls_back_to_default_chain_region() {
        let (_dir, config_path, credentials_path) = write_profile_files(
            "[profile isolated]\n",
            "[isolated]\naws_access_key_id = PROFILE_KEY\naws_secret_access_key = PROFILE_SECRET\n",
        );

        let _env = EnvGuard::set(&[
            ("AWS_CONFIG_FILE", config_path.to_str().unwrap()),
            (
                "AWS_SHARED_CREDENTIALS_FILE",
                credentials_path.to_str().unwrap(),
            ),
            ("AWS_REGION", "eu-central-1"),
        ]);

        let config = AwsCliArgs {
            bucket: "my-bucket".to_string(),
            profile: Some("isolated".to_string()),
            ..Default::default()
        }
        .config()
        .await
        .unwrap();

        assert_eq!(
            config.region().map(|region| region.as_ref()),
            Some("eu-central-1")
        );
    }

    #[tokio::test]
    async fn aws_explicit_region_beats_profile_and_environment_region() {
        let (_dir, config_path, credentials_path) = write_profile_files(
            "[profile isolated]\nregion = us-west-2\n",
            "[isolated]\naws_access_key_id = PROFILE_KEY\naws_secret_access_key = PROFILE_SECRET\n",
        );

        let _env = EnvGuard::set(&[
            ("AWS_CONFIG_FILE", config_path.to_str().unwrap()),
            (
                "AWS_SHARED_CREDENTIALS_FILE",
                credentials_path.to_str().unwrap(),
            ),
            ("AWS_REGION", "eu-central-1"),
        ]);

        let config = AwsCliArgs {
            bucket: "my-bucket".to_string(),
            profile: Some("isolated".to_string()),
            region: Some("ap-southeast-1".to_string()),
            ..Default::default()
        }
        .config()
        .await
        .unwrap();

        assert_eq!(
            config.region().map(|region| region.as_ref()),
            Some("ap-southeast-1")
        );
    }

    #[tokio::test]
    async fn aws_explicit_static_credentials_beat_profile_credentials() {
        let (_dir, config_path, credentials_path) = write_profile_files(
            "[profile isolated]\nregion = us-west-2\n",
            "[isolated]\naws_access_key_id = PROFILE_KEY\naws_secret_access_key = PROFILE_SECRET\n",
        );

        let _env = EnvGuard::set(&[
            ("AWS_CONFIG_FILE", config_path.to_str().unwrap()),
            (
                "AWS_SHARED_CREDENTIALS_FILE",
                credentials_path.to_str().unwrap(),
            ),
            ("AWS_ACCESS_KEY_ID", "ENV_KEY"),
            ("AWS_SECRET_ACCESS_KEY", "ENV_SECRET"),
        ]);

        let config = AwsCliArgs {
            bucket: "my-bucket".to_string(),
            profile: Some("isolated".to_string()),
            access_key_id: Some("STATIC_KEY".to_string()),
            secret_access_key: Some("STATIC_SECRET".to_string()),
            ..Default::default()
        }
        .config()
        .await
        .unwrap();

        let credentials = config
            .credentials_provider()
            .unwrap()
            .provide_credentials()
            .await
            .unwrap();
        assert_eq!(credentials.access_key_id(), "STATIC_KEY");
        assert_eq!(credentials.secret_access_key(), "STATIC_SECRET");
    }

    #[tokio::test]
    async fn aws_partial_static_credentials_are_rejected() {
        let err = AwsCliArgs {
            bucket: "my-bucket".to_string(),
            access_key_id: Some("STATIC_KEY".to_string()),
            ..Default::default()
        }
        .config()
        .await
        .unwrap_err()
        .to_string();

        assert!(err.contains("--secret-access-key"));
    }

    #[test]
    fn mongodb_fromstr_basic() {
        let a = ArchiveArgs::from_str("mongodb mongodb://localhost:27017 mydb").unwrap();
        match a {
            ArchiveArgs::MongoDb(args) => {
                assert_eq!(args.url, "mongodb://localhost:27017");
                assert_eq!(args.db, "mydb");
            }
            _ => panic!("expected MongoDb variant"),
        }
    }

    #[test]
    fn fs_fromstr_block_reader() {
        let args = BlockDataReaderArgs::from_str("fs /tmp/archive").unwrap();
        match args {
            BlockDataReaderArgs::Fs(fs_args) => {
                assert_eq!(fs_args.path, PathBuf::from("/tmp/archive"));
                assert_eq!(
                    fs_args.block_store_path(),
                    PathBuf::from("/tmp/archive/blocks")
                );
                assert_eq!(
                    fs_args.index_store_path(),
                    PathBuf::from("/tmp/archive/index")
                );
            }
            _ => panic!("expected Fs variant"),
        }
    }

    #[test]
    fn fs_fromstr_archive() {
        let args = ArchiveArgs::from_str("fs /tmp/archive").unwrap();
        match args {
            ArchiveArgs::Fs(fs_args) => {
                assert_eq!(fs_args.path, PathBuf::from("/tmp/archive"));
            }
            _ => panic!("expected Fs variant"),
        }
    }

    #[test]
    fn mongodb_fromstr_ignores_deprecated_capped_size_arg() {
        // Third positional numeric should be ignored with a warning
        let a = BlockDataReaderArgs::from_str("mongodb mongodb://host:27017 mydb 10").unwrap();
        match a {
            BlockDataReaderArgs::MongoDb(args) => {
                assert_eq!(args.url, "mongodb://host:27017");
                assert_eq!(args.db, "mydb");
            }
            _ => panic!("expected MongoDb variant"),
        }
    }

    #[test]
    fn triedb_parse_minimal_defaults() {
        // Direct parser expects just the args string (no leading type token)
        let t = TrieDbCliArgs::parse("/data/triedb").unwrap();
        assert_eq!(t.triedb_path, "/data/triedb");
        assert_eq!(t.max_buffered_read_requests, 5000);
        assert_eq!(t.max_triedb_async_read_concurrency, 10000);
        assert_eq!(t.max_buffered_traverse_requests, 200);
        assert_eq!(t.max_triedb_async_traverse_concurrency, 20);
        assert_eq!(t.max_finalized_block_cache_len, 200);
        assert_eq!(t.max_voted_block_cache_len, 3);
    }

    #[test]
    fn triedb_parse_with_overrides() {
        let t = TrieDbCliArgs::parse(
            "/db 4000 \
             --max-triedb-async-read-concurrency 100 \
             --max-buffered-traverse-requests 300 \
             --max-triedb-async-traverse-concurrency 30 \
             --max-finalized-block-cache-len 250 \
             --max-voted-block-cache-len 5",
        )
        .unwrap();
        assert_eq!(t.triedb_path, "/db");
        assert_eq!(t.max_buffered_read_requests, 4000);
        assert_eq!(t.max_triedb_async_read_concurrency, 100);
        assert_eq!(t.max_buffered_traverse_requests, 300);
        assert_eq!(t.max_triedb_async_traverse_concurrency, 30);
        assert_eq!(t.max_finalized_block_cache_len, 250);
        assert_eq!(t.max_voted_block_cache_len, 5);
    }

    #[test]
    fn triedb_fromstr_roundtrip() {
        let r = BlockDataReaderArgs::from_str(
            "triedb /db/path \
             --max-triedb-async-read-concurrency 42",
        )
        .unwrap();
        match r {
            BlockDataReaderArgs::Triedb(t) => {
                assert_eq!(t.triedb_path, "/db/path");
                assert_eq!(t.max_triedb_async_read_concurrency, 42);
            }
            _ => panic!("expected Triedb variant"),
        }
    }

    #[test]
    fn unrecognized_variant_is_error() {
        assert!(BlockDataReaderArgs::from_str("foo bar baz").is_err());
        assert!(ArchiveArgs::from_str("nope something").is_err());
    }

    #[test]
    fn missing_args_is_error() {
        let err = BlockDataReaderArgs::from_str("aws")
            .unwrap_err()
            .to_string();
        assert!(err.contains("Storage args string empty"));
    }

    #[test]
    fn replica_name_roundtrip() {
        let aws = ArchiveArgs::from_str("aws my-bucket 10 us-west-1").unwrap();
        assert_eq!(aws.replica_name(), "my-bucket");

        // MongoDB without explicit replica name derives from redacted URL and db
        let mongo = ArchiveArgs::from_str("mongodb mongodb://h:27017 mydb").unwrap();
        assert_eq!(mongo.replica_name(), "mongodb://h:27017:mydb");

        let local = ArchiveArgs::from_str("fs /tmp/archive").unwrap();
        assert_eq!(local.replica_name(), "/tmp/archive");
    }

    #[test]
    fn mongodb_explicit_replica_name() {
        // Explicit replica name takes precedence
        let mongo =
            ArchiveArgs::from_str("mongodb mongodb://h:27017 mydb --replica-name my-replica")
                .unwrap();
        assert_eq!(mongo.replica_name(), "my-replica");

        // BlockDataReaderArgs also supports explicit replica name
        let mongo_reader = BlockDataReaderArgs::from_str(
            "mongodb mongodb://h:27017 mydb --replica-name reader-replica",
        )
        .unwrap();
        assert_eq!(mongo_reader.replica_name(), "reader-replica");
    }

    #[test]
    fn mongodb_replica_name_includes_host() {
        // Different hosts should produce different replica names
        let mongo1 = ArchiveArgs::from_str("mongodb mongodb://host1:27017 mydb").unwrap();
        let mongo2 = ArchiveArgs::from_str("mongodb mongodb://host2:27017 mydb").unwrap();

        assert_ne!(mongo1.replica_name(), mongo2.replica_name());
        assert_eq!(mongo1.replica_name(), "mongodb://host1:27017:mydb");
        assert_eq!(mongo2.replica_name(), "mongodb://host2:27017:mydb");
    }

    #[test]
    fn mongodb_replica_name_redacts_credentials() {
        let mongo =
            ArchiveArgs::from_str("mongodb mongodb://user:password@host:27017 mydb").unwrap();
        let replica_name = mongo.replica_name();

        // Should not contain credentials
        assert!(!replica_name.contains("user"));
        assert!(!replica_name.contains("password"));
        // Should contain redacted marker and host
        assert!(replica_name.contains("***"));
        assert!(replica_name.contains("host:27017"));
        assert!(replica_name.contains("mydb"));
    }

    #[test]
    fn aws_debug_redacts_secrets_when_present() {
        let args = AwsCliArgs {
            bucket: "my-bucket".to_string(),
            region: Some("us-east-1".to_string()),
            endpoint: Some("https://s3.amazonaws.com".to_string()),
            profile: Some("mainnet".to_string()),
            access_key_id: Some("AKIAIOSFODNN7EXAMPLE".to_string()),
            secret_access_key: Some("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string()),
            concurrency: 10,
            operation_timeout_secs: 30,
            operation_attempt_timeout_secs: 30,
            read_timeout_secs: 30,
        };

        let debug_output = format!("{:?}", args);

        // Should NOT contain actual secrets
        assert!(!debug_output.contains("AKIAIOSFODNN7EXAMPLE"));
        assert!(!debug_output.contains("wJalrXUtnFEMI"));
        assert!(!debug_output.contains("bPxRfiCYEXAMPLEKEY"));

        // Should contain [REDACTED] for secret fields
        assert!(debug_output.contains("[REDACTED]"));

        // Should still contain non-secret fields
        assert!(debug_output.contains("my-bucket"));
        assert!(debug_output.contains("us-east-1"));
    }

    #[test]
    fn aws_debug_shows_none_when_secrets_absent() {
        let args = AwsCliArgs {
            bucket: "my-bucket".to_string(),
            region: None,
            endpoint: None,
            profile: None,
            access_key_id: None,
            secret_access_key: None,
            concurrency: 10,
            operation_timeout_secs: 30,
            operation_attempt_timeout_secs: 30,
            read_timeout_secs: 30,
        };

        let debug_output = format!("{:?}", args);

        // When secrets are None, should show None (not [REDACTED])
        assert!(debug_output.contains("access_key_id: None"));
        assert!(debug_output.contains("secret_access_key: None"));
    }

    #[test]
    fn aws_debug_does_not_leak_secrets_in_alternate_format() {
        let args = AwsCliArgs {
            bucket: "test".to_string(),
            region: None,
            endpoint: None,
            profile: None,
            access_key_id: Some("SECRET_KEY_ID".to_string()),
            secret_access_key: Some("SECRET_ACCESS_KEY".to_string()),
            concurrency: default_aws_concurrency(),
            operation_timeout_secs: get_default_bucket_timeout(),
            operation_attempt_timeout_secs: get_default_bucket_timeout(),
            read_timeout_secs: get_default_bucket_timeout(),
        };

        // Test both {:?} and {:#?} (pretty print)
        let debug_output = format!("{:?}", args);
        let pretty_output = format!("{:#?}", args);

        assert!(!debug_output.contains("SECRET_KEY_ID"));
        assert!(!debug_output.contains("SECRET_ACCESS_KEY"));
        assert!(!pretty_output.contains("SECRET_KEY_ID"));
        assert!(!pretty_output.contains("SECRET_ACCESS_KEY"));
    }
}
