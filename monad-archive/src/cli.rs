use std::str::FromStr;

use aws_config::{
    meta::region::RegionProviderChain, timeout::TimeoutConfig, BehaviorVersion, Region, SdkConfig,
};
use clap::Parser;
use eyre::{bail, OptionExt};
use futures::join;

use crate::{
    kvstore::{mongo::MongoDbStorage, rocksdb_storage::RocksDbClient},
    prelude::*,
};

async fn get_aws_config(region: Option<String>) -> SdkConfig {
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
                .operation_timeout(Duration::from_secs(5))
                .read_timeout(Duration::from_secs(2))
                .build(),
        )
        .load()
        .await
}

#[derive(Debug, Clone)]
pub enum BlockDataReaderArgs {
    Aws(AwsCliArgs),
    RocksDb(RocksDbCliArgs),
    Triedb(TrieDbCliArgs),
    MongoDb(MongoDbCliArgs),
}

#[derive(Debug, Clone)]
pub enum ArchiveArgs {
    Aws(AwsCliArgs),
    RocksDb(RocksDbCliArgs),
    MongoDb(MongoDbCliArgs),
}

impl FromStr for BlockDataReaderArgs {
    type Err = eyre::Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        use BlockDataReaderArgs::*;
        let mut words = s.split(' ');
        let Some(first) = words.next() else {
            bail!("Storage args string empty");
        };

        let next =
            |str: &'static str| -> Result<String> { Ok(words.next().ok_or_eyre(str)?.to_owned()) };

        Ok(match first.to_lowercase().as_str() {
            "aws" => Aws(AwsCliArgs::parse(next)?),
            "rocksdb" => RocksDb(RocksDbCliArgs::parse(next)?),
            "triedb" => Triedb(TrieDbCliArgs::parse(next)?),
            "mongodb" => MongoDb(MongoDbCliArgs::parse(next)?),
            _ => {
                bail!("Unrecognized storage args variant: {first}");
            }
        })
    }
}

impl FromStr for ArchiveArgs {
    type Err = eyre::Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        use ArchiveArgs::*;
        let mut words = s.split(' ');
        let Some(first) = words.next() else {
            bail!("Storage args string empty");
        };

        let next =
            |str: &'static str| -> Result<String> { Ok(words.next().ok_or_eyre(str)?.to_owned()) };

        Ok(match first.to_lowercase().as_str() {
            "aws" => Aws(AwsCliArgs::parse(next)?),
            "rocksdb" => RocksDb(RocksDbCliArgs::parse(next)?),
            "mongodb" => MongoDb(MongoDbCliArgs::parse(next)?),
            _ => {
                bail!("Unrecognized storage args variant: {first}");
            }
        })
    }
}

impl BlockDataReaderArgs {
    pub async fn build(&self, metrics: &Metrics) -> Result<BlockDataReaderErased> {
        use BlockDataReaderArgs::*;
        Ok(match self {
            Aws(args) => BlockDataArchive::new(args.build_blob_store(metrics).await).into(),
            RocksDb(args) => BlockDataArchive::new(RocksDbClient::try_from(args)?).into(),
            Triedb(args) => TriedbReader::new(args).into(),
            MongoDb(args) => BlockDataArchive::new(
                MongoDbStorage::new_block_store(&args.url, &args.db, None).await?,
            )
            .into(),
        })
    }

    pub fn replica_name(&self) -> String {
        use BlockDataReaderArgs::*;
        match self {
            Aws(aws_cli_args) => aws_cli_args.bucket.clone(),
            RocksDb(rocks_db_cli_args) => rocks_db_cli_args.db_path.clone(),
            Triedb(trie_db_cli_args) => trie_db_cli_args.triedb_path.clone(),
            MongoDb(mongo_db_cli_args) => {
                format!("{}:{}", mongo_db_cli_args.url, mongo_db_cli_args.db)
            }
        }
    }
}

impl ArchiveArgs {
    pub async fn build_block_data_archive(&self, metrics: &Metrics) -> Result<BlockDataArchive> {
        let store = match self {
            ArchiveArgs::Aws(args) => args.build_blob_store(metrics).await,
            ArchiveArgs::RocksDb(args) => RocksDbClient::try_from(args)?.into(),
            ArchiveArgs::MongoDb(args) => {
                MongoDbStorage::new_block_store(&args.url, &args.db, args.capped_size_gb)
                    .await?
                    .into()
            }
        };
        Ok(BlockDataArchive::new(store))
    }

    pub async fn build_index_archive(
        &self,
        metrics: &Metrics,
        max_inline_encoded_len: usize,
    ) -> Result<TxIndexArchiver> {
        let (blob, index) = match self {
            ArchiveArgs::Aws(args) => {
                join!(
                    args.build_blob_store(metrics),
                    args.build_index_store(metrics)
                )
            }
            ArchiveArgs::RocksDb(args) => {
                let store = KVStoreErased::from(args.build()?);
                (store.clone(), store)
            }
            ArchiveArgs::MongoDb(args) => (
                MongoDbStorage::new_block_store(&args.url, &args.db, None)
                    .await?
                    .into(),
                MongoDbStorage::new_index_store(&args.url, &args.db, args.capped_size_gb)
                    .await?
                    .into(),
            ),
        };
        Ok(TxIndexArchiver::new(
            index,
            BlockDataArchive::new(blob),
            max_inline_encoded_len,
        ))
    }

    pub async fn build_archive_reader(&self, metrics: &Metrics) -> Result<ArchiveReader> {
        let (blob, index) = match self {
            ArchiveArgs::Aws(args) => {
                join!(
                    args.build_blob_store(metrics),
                    args.build_index_store(metrics)
                )
            }
            ArchiveArgs::RocksDb(args) => {
                let store = KVStoreErased::from(args.build()?);
                (store.clone(), store)
            }
            ArchiveArgs::MongoDb(args) => (
                MongoDbStorage::new_block_store(&args.url, &args.db, None)
                    .await?
                    .into(),
                MongoDbStorage::new_index_store(&args.url, &args.db, args.capped_size_gb)
                    .await?
                    .into(),
            ),
        };
        let bdr = BlockDataReaderErased::from(BlockDataArchive::new(blob));
        // TODO: Fixme
        Ok(ArchiveReader::new(bdr, index, None))
    }

    pub fn replica_name(&self) -> String {
        match self {
            ArchiveArgs::Aws(aws_cli_args) => aws_cli_args.bucket.clone(),
            ArchiveArgs::RocksDb(rocks_db_cli_args) => rocks_db_cli_args.db_path.clone(),
            ArchiveArgs::MongoDb(mongo_db_cli_args) => {
                format!("{}:{}", mongo_db_cli_args.url, mongo_db_cli_args.db)
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct AwsCliArgs {
    pub bucket: String,
    pub concurrency: usize,
    pub region: Option<String>,
}

impl AwsCliArgs {
    pub fn parse(mut next: impl FnMut(&'static str) -> Result<String>) -> Result<Self> {
        Ok(Self {
            bucket: next("args missing bucket")?,
            concurrency: usize::from_str(&next("args missing concurrency")?)?,
            region: next("").ok(),
        })
    }

    pub async fn build_blob_store(&self, metrics: &Metrics) -> KVStoreErased {
        S3Bucket::new(
            self.bucket.clone(),
            &get_aws_config(self.region.clone()).await,
            metrics.clone(),
        )
        .into()
    }

    pub async fn build_index_store(&self, metrics: &Metrics) -> KVStoreErased {
        let config = &get_aws_config(self.region.clone()).await;

        DynamoDBArchive::new(
            S3Bucket::new(self.bucket.clone(), config, metrics.clone()),
            self.bucket.clone(),
            config,
            self.concurrency,
            metrics.clone(),
        )
        .into()
    }
}

#[derive(Clone, Debug, Parser)]
pub struct RocksDbCliArgs {
    pub db_path: String,
}

impl RocksDbCliArgs {
    pub fn parse(mut next: impl FnMut(&'static str) -> Result<String>) -> Result<Self> {
        Ok(Self {
            db_path: next("rocksdb args missing db path")?,
        })
    }

    pub fn build(&self) -> Result<RocksDbClient> {
        RocksDbClient::new(&self.db_path)
    }
}

#[derive(Clone, Debug)]
pub struct TrieDbCliArgs {
    pub triedb_path: String,
    pub max_buffered_read_requests: usize,
    pub max_triedb_async_read_concurrency: usize,
    pub max_buffered_traverse_requests: usize,
    pub max_triedb_async_traverse_concurrency: usize,
    pub max_finalized_block_cache_len: usize,
    pub max_voted_block_cache_len: usize,
}

impl TrieDbCliArgs {
    pub fn parse(mut next: impl FnMut(&'static str) -> Result<String>) -> Result<TrieDbCliArgs> {
        Ok(TrieDbCliArgs {
            triedb_path: next("storage args missing db path")?,
            max_buffered_read_requests: usize::from_str(&next(
                "args missing max_buffered_read_requests",
            )?)?,
            max_triedb_async_read_concurrency: 10000,
            max_buffered_traverse_requests: 200,
            max_triedb_async_traverse_concurrency: 20,
            max_finalized_block_cache_len: 200,
            max_voted_block_cache_len: 3,
        })
    }
}

#[derive(Clone, Debug)]
pub struct MongoDbCliArgs {
    pub url: String,
    pub db: String,
    pub capped_size_gb: Option<u64>,
}

impl MongoDbCliArgs {
    pub fn parse(mut next: impl FnMut(&'static str) -> Result<String>) -> Result<Self> {
        Ok(MongoDbCliArgs {
            url: next("storage args missing mongo url")?,
            db: next("storage args missing mongo db name")?,
            capped_size_gb: next("").ok().and_then(|s| u64::from_str(&s).ok()),
        })
    }
}
