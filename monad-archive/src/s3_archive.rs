use core::str;
use std::sync::Arc;

use alloy_rlp::Encodable;
use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::{
    config::{BehaviorVersion, Region},
    primitives::ByteStream,
    Client,
};
use bytes::Bytes;
use eyre::Context;
use futures::try_join;
use reth_primitives::{Block, ReceiptWithBloom, TransactionSigned};
use tokio::time::Duration;
use tokio_retry::{
    strategy::{jitter, ExponentialBackoff},
    Retry,
};

use crate::{archive_interface::ArchiveWriterInterface, errors::ArchiveError, triedb::BlockHeader};

const BLOCK_PADDING_WIDTH: usize = 12;

#[derive(Clone)]
pub struct S3Archive {
    pub client: Client,
    pub bucket: String,

    pub latest_table_key: &'static str,

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
    pub async fn new(bucket: String, region: Option<String>) -> Result<Self, ArchiveError> {
        let region_provider = RegionProviderChain::default_provider().or_else(
            region
                .map(Region::new)
                .unwrap_or_else(|| Region::new("us-east-2")),
        );
        let config = aws_config::defaults(BehaviorVersion::latest())
            .region(region_provider)
            .load()
            .await;
        let client = Client::new(&config);
        Ok(S3Archive {
            client,
            bucket,
            block_table_prefix: "block",
            block_hash_table_prefix: "block_hash",
            receipts_table_prefix: "receipts",
            traces_table_prefix: "traces",
            latest_table_key: "latest",
        })
    }

    // Upload rlp-encoded bytes with retry
    pub async fn upload(&self, key: &str, data: Vec<u8>) -> Result<(), ArchiveError> {
        let retry_strategy = ExponentialBackoff::from_millis(10)
            .max_delay(Duration::from_secs(1))
            .map(jitter);

        Retry::spawn(retry_strategy, || {
            let client = &self.client;
            let bucket = &self.bucket;
            let key = key.to_string();
            let body = ByteStream::from(data.clone());

            async move {
                client
                    .put_object()
                    .bucket(bucket)
                    .key(&key)
                    .body(body)
                    .send()
                    .await
                    .wrap_err_with(|| format!("Failed to upload {}. Retrying...", key))
            }
        })
        .await
        .map(|_| ())
        .wrap_err_with(|| format!("Failed to upload {}. Retrying...", key))?;

        Ok(())
    }

    pub async fn read(&self, key: &str) -> Result<Bytes, ArchiveError> {
        let resp = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .wrap_err_with(|| format!("Failed to read key from s3 {key}"))?;

        let data = resp
            .body
            .collect()
            .await
            .wrap_err("Unable to collect response data")?;

        Ok(data.into_bytes())
    }
}

#[derive(Clone)]
pub struct S3ArchiveWriter {
    archive: Arc<S3Archive>,
}

impl S3ArchiveWriter {
    pub async fn new(archive: S3Archive) -> Result<Self, ArchiveError> {
        Ok(S3ArchiveWriter {
            archive: Arc::new(archive),
        })
    }
}

impl ArchiveWriterInterface for S3ArchiveWriter {
    async fn get_latest(&self) -> Result<u64, ArchiveError> {
        let key = &self.archive.latest_table_key;

        let value = self.archive.read(key).await?;

        let value_str = String::from_utf8(value.to_vec()).wrap_err("Invalid UTF-8 sequence")?;

        // Parse the string as u64
        value_str.parse::<u64>().wrap_err_with(|| {
            format!("Unable to convert block_number string to number (u64), value: {value_str}")
        })
    }

    async fn update_latest(&self, block_num: u64) -> Result<(), ArchiveError> {
        let key = &self.archive.latest_table_key;
        let latest_value = format!("{:0width$}", block_num, width = BLOCK_PADDING_WIDTH);
        self.archive
            .upload(key, latest_value.as_bytes().to_vec())
            .await
    }

    async fn archive_block(
        &self,
        block_header: BlockHeader,
        transactions: Vec<TransactionSigned>,
        block_num: u64,
    ) -> Result<(), ArchiveError> {
        // 1) Insert into block table
        let block_key = format!(
            "{}/{:0width$}",
            self.archive.block_table_prefix,
            block_num,
            width = BLOCK_PADDING_WIDTH
        );

        let block = make_block(block_header.clone(), transactions.clone());
        let mut rlp_block = Vec::with_capacity(8096);
        block.encode(&mut rlp_block);

        // 2) Insert into block_hash table
        let block_hash_key_suffix = hex::encode(block_header.hash);
        let block_hash_key = format!(
            "{}/{}",
            self.archive.block_hash_table_prefix, block_hash_key_suffix
        );
        let block_hash_value_string = block_num.to_string();
        let block_hash_value = block_hash_value_string.as_bytes();

        // 3) Join futures
        try_join!(
            self.archive.upload(&block_key, rlp_block),
            self.archive
                .upload(&block_hash_key, block_hash_value.to_vec())
        )?;

        Ok(())
    }

    async fn archive_receipts(
        &self,
        receipts: Vec<ReceiptWithBloom>,
        block_num: u64,
    ) -> Result<(), ArchiveError> {
        // 1) Prepare the receipts upload
        let receipts_key = format!(
            "{}/{:0width$}",
            self.archive.receipts_table_prefix,
            block_num,
            width = BLOCK_PADDING_WIDTH
        );

        let mut rlp_receipts = Vec::new();
        receipts.encode(&mut rlp_receipts);
        self.archive.upload(&receipts_key, rlp_receipts).await
    }

    async fn archive_traces(
        &self,
        traces: Vec<Vec<u8>>,
        block_num: u64,
    ) -> Result<(), ArchiveError> {
        let traces_key = format!(
            "{}/{:0width$}",
            self.archive.traces_table_prefix,
            block_num,
            width = BLOCK_PADDING_WIDTH
        );

        let mut rlp_traces = vec![];
        traces.encode(&mut rlp_traces);

        self.archive.upload(&traces_key, rlp_traces).await
    }
}

pub fn make_block(block_header: BlockHeader, transactions: Vec<TransactionSigned>) -> Block {
    Block {
        header: block_header.header,
        body: transactions,
        ..Default::default()
    }
}
