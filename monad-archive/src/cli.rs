use std::str::FromStr;

use aws_config::{
    meta::region::RegionProviderChain, timeout::TimeoutConfig, BehaviorVersion, Region, SdkConfig,
};
use clap::Parser;
use eyre::{bail, OptionExt};
use futures::join;

use crate::{
    prelude::*,
    storage::{triedb_reader::TriedbReader, DynamoDBArchive, RocksDbClient},
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
}

#[derive(Debug, Clone)]
pub enum ArchiveArgs {
    Aws(AwsCliArgs),
    RocksDb(RocksDbCliArgs),
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
            RocksDb(args) => BlockDataArchive::new(RocksDbClient::try_from(args)?.into()).into(),
            Triedb(args) => TriedbReader::new(args).into(),
        })
    }

    pub fn replica_name(&self) -> String {
        use BlockDataReaderArgs::*;
        match self {
            Aws(aws_cli_args) => aws_cli_args.bucket.clone(),
            RocksDb(rocks_db_cli_args) => rocks_db_cli_args.db_path.clone(),
            Triedb(trie_db_cli_args) => trie_db_cli_args.triedb_path.clone(),
        }
    }
}

impl ArchiveArgs {
    pub async fn build_block_data_archive(&self, metrics: &Metrics) -> Result<BlockDataArchive> {
        let store: BlobStoreErased = match self {
            ArchiveArgs::Aws(args) => args.build_blob_store(metrics).await,
            ArchiveArgs::RocksDb(args) => RocksDbClient::try_from(args)?.into(),
        };
        Ok(BlockDataArchive::new(store))
    }

    pub async fn build_index_archive(&self, metrics: &Metrics) -> Result<TxIndexArchiver> {
        let (bstore, istore) = self.build_both(metrics).await?;
        Ok(TxIndexArchiver::new(istore, BlockDataArchive::new(bstore)))
    }

    async fn build_both(&self, metrics: &Metrics) -> Result<(BlobStoreErased, IndexStoreErased)> {
        Ok(match self {
            ArchiveArgs::Aws(args) => {
                let (b, s) = join!(
                    args.build_blob_store(metrics),
                    args.build_index_store(metrics)
                );
                (b, s.into())
            }
            ArchiveArgs::RocksDb(args) => {
                let store = args.build()?;
                (store.clone().into(), store.into())
            }
        })
    }

    pub async fn build_archive_reader(&self, metrics: &Metrics) -> Result<ArchiveReader> {
        let (b_reader, i_reader) = self.build_both(metrics).await?;
        Ok(ArchiveReader::new(
            BlockDataArchive::new(b_reader).into(),
            i_reader,
        ))
    }

    pub fn replica_name(&self) -> String {
        match self {
            ArchiveArgs::Aws(aws_cli_args) => aws_cli_args.bucket.clone(),
            ArchiveArgs::RocksDb(rocks_db_cli_args) => rocks_db_cli_args.db_path.clone(),
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

    pub async fn build_blob_store(&self, metrics: &Metrics) -> BlobStoreErased {
        S3Bucket::new(
            self.bucket.clone(),
            &get_aws_config(self.region.clone()).await,
            metrics.clone(),
        )
        .into()
    }

    pub async fn build_index_store(&self, metrics: &Metrics) -> DynamoDBArchive {
        let config = &get_aws_config(self.region.clone()).await;

        DynamoDBArchive::new(
            BlockDataArchive::new(
                S3Bucket::new(self.bucket.clone(), config, metrics.clone()).into(),
            )
            .into(),
            self.bucket.clone(),
            config,
            self.concurrency,
            metrics.clone(),
        )
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
    pub max_concurrent_requests: usize,
}

impl TrieDbCliArgs {
    pub fn parse(mut next: impl FnMut(&'static str) -> Result<String>) -> Result<TrieDbCliArgs> {
        Ok(TrieDbCliArgs {
            triedb_path: next("storage args missing db path")?,
            max_concurrent_requests: usize::from_str(&next(
                "args missing max_concurrent_requests",
            )?)?,
        })
    }
}
