use core::str;
use std::sync::Arc;

use alloy_rlp::{Decodable, Encodable};
use aws_config::{meta::region::RegionProviderChain, SdkConfig};
use aws_sdk_s3::{
    config::{BehaviorVersion, Region},
    primitives::ByteStream,
    Client,
};
use bytes::Bytes;
use eyre::{Context, Result};
use futures::try_join;
use reth_primitives::{Block, ReceiptWithBloom, TransactionSigned};
use tokio::time::Duration;
use tokio_retry::{
    strategy::{jitter, ExponentialBackoff},
    Retry,
};
use tracing::info;

use crate::{
    archive_interface::{ArchiveReader, ArchiveWriter, LatestKind},
    metrics::Metrics,
};

use monad_triedb_utils::triedb_env::BlockHeader;

const AWS_S3_ERRORS: &'static str = "aws_s3_errors";
const AWS_S3_READS: &'static str = "aws_s3_reads";
const AWS_S3_WRITES: &'static str = "aws_s3_writes";

const BLOCK_PADDING_WIDTH: usize = 12;

#[derive(Clone)]
pub struct S3Bucket {
    pub client: Client,
    pub bucket: String,
    pub metrics: Metrics,
}

pub async fn get_aws_config(region: Option<String>) -> SdkConfig {
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
        .load()
        .await
}

impl S3Bucket {
    pub fn new(bucket: String, sdk_config: &SdkConfig, metrics: Metrics) -> Self {
        S3Bucket::from_client(bucket, Client::new(sdk_config), metrics)
    }

    pub fn from_client(bucket: String, client: Client, metrics: Metrics) -> Self {
        S3Bucket {
            bucket,
            client,
            metrics,
        }
    }

    // Upload rlp-encoded bytes with retry
    pub async fn upload(&self, key: &str, data: Vec<u8>) -> Result<()> {
        let retry_strategy = ExponentialBackoff::from_millis(10)
            .max_delay(Duration::from_secs(1))
            .map(jitter);

        Retry::spawn(retry_strategy, || {
            let client = &self.client;
            let bucket = &self.bucket;
            let key = key.to_string();
            let body = ByteStream::from(data.clone());
            let metrics = &self.metrics;

            async move {
                client
                    .put_object()
                    .bucket(bucket)
                    .key(&key)
                    .body(body)
                    .send()
                    .await
                    .wrap_err_with(|| {
                        metrics.inc_counter(AWS_S3_ERRORS);
                        format!("Failed to upload {}. Retrying...", key)
                    })
            }
        })
        .await
        .map(|_| ())
        .wrap_err_with(|| format!("Failed to upload {}. Retrying...", key))?;

        self.metrics.counter(AWS_S3_WRITES, 1);
        Ok(())
    }

    pub async fn read(&self, key: &str) -> Result<Bytes> {
        let resp = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .wrap_err_with(|| {
                self.metrics.inc_counter(AWS_S3_ERRORS);
                format!("Failed to read key from s3 {key}")
            })?;

        let data = resp.body.collect().await.wrap_err_with(|| {
            self.metrics.inc_counter(AWS_S3_ERRORS);
            "Unable to collect response data"
        })?;

        self.metrics.counter(AWS_S3_READS, 1);
        Ok(data.into_bytes())
    }
}

#[derive(Clone)]
pub struct S3Archive {
    pub bucket: Arc<S3Bucket>,

    pub latest_uploaded_table_key: &'static str,
    pub latest_indexed_table_key: &'static str,

    // key =  {block}/{block_number}, value = {RLP(Block)}
    pub block_table_prefix: &'static str,

    // key = {block_hash}/{$block_hash}, value = {str(block_number)}
    pub block_hash_table_prefix: &'static str,

    // key = {receipts}/{block_number}, value = {RLP(Vec<Receipt>)}
    pub receipts_table_prefix: &'static str,

    // key = {traces}/{block_number}, value = {RLP(Vec<Vec<u8>>)}
    pub traces_table_prefix: &'static str,
}

impl S3Archive {
    pub async fn new(archive: S3Bucket) -> Result<Self> {
        Ok(S3Archive {
            bucket: Arc::new(archive),
            block_table_prefix: "block",
            block_hash_table_prefix: "block_hash",
            receipts_table_prefix: "receipts",
            traces_table_prefix: "traces",
            latest_uploaded_table_key: "latest",
            latest_indexed_table_key: "latest_indexed",
        })
    }

    pub fn as_reader(self) -> impl ArchiveReader {
        self
    }

    pub async fn read_block(&self, block_num: u64) -> Result<Block> {
        let bytes = self.bucket.read(&self.block_key(block_num)).await?;
        let mut bytes: &[u8] = &bytes;
        let block = Block::decode(&mut bytes)?;
        Ok(block)
    }

    pub fn block_key(&self, block_num: u64) -> String {
        format!(
            "{}/{:0width$}",
            self.block_table_prefix,
            block_num,
            width = BLOCK_PADDING_WIDTH
        )
    }

    pub fn receipts_key(&self, block_num: u64) -> String {
        format!(
            "{}/{:0width$}",
            self.receipts_table_prefix,
            block_num,
            width = BLOCK_PADDING_WIDTH
        )
    }

    pub fn traces_key(&self, block_num: u64) -> String {
        format!(
            "{}/{:0width$}",
            self.traces_table_prefix,
            block_num,
            width = BLOCK_PADDING_WIDTH
        )
    }
}

impl ArchiveWriter for S3Archive {
    async fn update_latest(&self, block_num: u64, latest_kind: LatestKind) -> Result<()> {
        let key = match latest_kind {
            LatestKind::Uploaded => &self.latest_uploaded_table_key,
            LatestKind::Indexed => &self.latest_indexed_table_key,
        };
        let latest_value = format!("{:0width$}", block_num, width = BLOCK_PADDING_WIDTH);
        self.bucket
            .upload(key, latest_value.as_bytes().to_vec())
            .await
    }

    async fn archive_block(
        &self,
        block_header: BlockHeader,
        transactions: Vec<TransactionSigned>,
        block_num: u64,
    ) -> Result<()> {
        // 1) Insert into block table
        let block_key = self.block_key(block_num);

        let block = make_block(block_header.clone(), transactions.clone());
        let mut rlp_block = Vec::with_capacity(8096);
        block.encode(&mut rlp_block);

        // 2) Insert into block_hash table
        let block_hash_key_suffix = hex::encode(block_header.hash);
        let block_hash_key = format!("{}/{}", self.block_hash_table_prefix, block_hash_key_suffix);
        let block_hash_value_string = block_num.to_string();
        let block_hash_value = block_hash_value_string.as_bytes();

        // 3) Join futures
        try_join!(
            self.bucket.upload(&block_key, rlp_block),
            self.bucket
                .upload(&block_hash_key, block_hash_value.to_vec())
        )?;
        Ok(())
    }

    async fn archive_receipts(
        &self,
        receipts: Vec<ReceiptWithBloom>,
        block_num: u64,
    ) -> Result<()> {
        // 1) Prepare the receipts upload
        let receipts_key = format!(
            "{}/{:0width$}",
            self.receipts_table_prefix,
            block_num,
            width = BLOCK_PADDING_WIDTH
        );

        let mut rlp_receipts = Vec::new();
        receipts.encode(&mut rlp_receipts);
        self.bucket.upload(&receipts_key, rlp_receipts).await
    }

    async fn archive_traces(&self, traces: Vec<Vec<u8>>, block_num: u64) -> Result<()> {
        let traces_key = format!(
            "{}/{:0width$}",
            self.traces_table_prefix,
            block_num,
            width = BLOCK_PADDING_WIDTH
        );

        let mut rlp_traces = vec![];
        traces.encode(&mut rlp_traces);

        self.bucket.upload(&traces_key, rlp_traces).await
    }
}

impl ArchiveReader for S3Archive {
    async fn get_latest(&self, latest_kind: LatestKind) -> Result<u64> {
        let key = match latest_kind {
            LatestKind::Uploaded => &self.latest_uploaded_table_key,
            LatestKind::Indexed => &self.latest_indexed_table_key,
        };

        let value = self.bucket.read(key).await?;

        let value_str = String::from_utf8(value.to_vec()).wrap_err("Invalid UTF-8 sequence")?;

        // Parse the string as u64
        value_str.parse::<u64>().wrap_err_with(|| {
            format!("Unable to convert block_number string to number (u64), value: {value_str}")
        })
    }

    async fn get_block_by_number(&self, block_num: u64) -> Result<Block> {
        self.read_block(block_num).await
    }

    async fn get_block_by_hash(&self, block_hash: &[u8; 32]) -> Result<Block> {
        let block_hash_key_suffix = hex::encode(block_hash);
        let block_hash_key = format!("{}/{}", self.block_hash_table_prefix, block_hash_key_suffix);

        let block_num_bytes = self.bucket.read(&block_hash_key).await?;

        let block_num_str =
            String::from_utf8(block_num_bytes.to_vec()).wrap_err("Invalid UTF-8 sequence")?;

        let block_num = block_num_str.parse::<u64>().wrap_err_with(|| {
            format!("Unable to convert block_number string to number (u64), value: {block_num_str}")
        })?;

        self.get_block_by_number(block_num).await
    }

    async fn get_block_receipts(&self, block_number: u64) -> Result<Vec<ReceiptWithBloom>> {
        let receipts_key = self.receipts_key(block_number);

        let rlp_receipts = self.bucket.read(&receipts_key).await?;
        let mut rlp_receipts_slice: &[u8] = &rlp_receipts;

        let receipts = Vec::decode(&mut rlp_receipts_slice).wrap_err("Cannot decode block")?;

        Ok(receipts)
    }

    async fn get_block_traces(&self, block_number: u64) -> Result<Vec<Vec<u8>>> {
        let traces_key = self.traces_key(block_number);

        let rlp_traces = self.bucket.read(&traces_key).await?;
        let mut rlp_traces_slice: &[u8] = &rlp_traces;

        let traces = Vec::decode(&mut rlp_traces_slice).wrap_err("Cannot decode block")?;

        Ok(traces)
    }
}

pub fn make_block(block_header: BlockHeader, transactions: Vec<TransactionSigned>) -> Block {
    Block {
        header: block_header.header,
        body: transactions,
        ..Default::default()
    }
}
