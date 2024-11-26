use std::collections::HashMap;

use eyre::Result;
use reth_primitives::{Block, ReceiptWithBloom};

use crate::{
    dynamodb::{DynamoDBArchive, TxIndexedData},
    metrics::Metrics,
    s3_archive::{get_aws_config, S3Archive, S3Bucket},
};

pub enum LatestKind {
    Uploaded,
    Indexed,
}

#[derive(Clone)]
pub struct ArchiveReader {
    s3: S3Archive,
    dynamodb: DynamoDBArchive,
}

impl ArchiveReader {
    pub async fn new(
        bucket: String,
        table: String,
        region: Option<String>,
        concurrency: usize,
        metrics: &Metrics,
    ) -> ArchiveReader {
        let sdk_config = get_aws_config(region).await;
        ArchiveReader {
            s3: S3Archive::new(S3Bucket::new(bucket, &sdk_config, metrics.clone())),
            dynamodb: DynamoDBArchive::new(table, &sdk_config, concurrency, metrics.clone()),
        }
    }

    pub fn bucket(&self) -> &str {
        &self.s3.bucket.bucket
    }

    pub async fn batch_get_txdata(
        &self,
        keys: &[String],
    ) -> Result<HashMap<String, TxIndexedData>> {
        self.dynamodb.batch_get_txdata(keys).await
    }
    pub async fn get_txdata(&self, key: impl Into<String>) -> Result<Option<TxIndexedData>> {
        self.dynamodb.get_txdata(key).await
    }

    // Get the latest stored block
    pub async fn get_latest(&self, latest_kind: LatestKind) -> Result<u64> {
        self.s3.get_latest(latest_kind).await
    }

    pub async fn get_block_by_hash(&self, block_hash: &[u8; 32]) -> Result<Block> {
        self.s3.get_block_by_hash(block_hash).await
    }

    pub async fn get_block_by_number(&self, block_num: u64) -> Result<Block> {
        self.s3.get_block_by_number(block_num).await
    }

    pub async fn get_block_receipts(&self, block_number: u64) -> Result<Vec<ReceiptWithBloom>> {
        self.s3.get_block_receipts(block_number).await
    }

    pub async fn get_block_traces(&self, block_number: u64) -> Result<Vec<Vec<u8>>> {
        self.s3.get_block_traces(block_number).await
    }
}
