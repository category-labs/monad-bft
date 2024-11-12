use core::str;
use std::sync::Arc;

use alloy_rlp::{Encodable, RlpDecodable, RlpEncodable};
use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::{
    config::{BehaviorVersion, Region},
    primitives::ByteStream,
    Client,
};
use bytes::Bytes;
use futures::{future::join_all, try_join};
use reth_primitives::{Block, ReceiptWithBloom, TransactionSigned};
use tokio::{sync::Semaphore, time::Duration};
use tokio_retry::{
    strategy::{jitter, ExponentialBackoff},
    Retry,
};
use tracing::{error, warn};

use crate::{archive_interface::ArchiveWriterInterface, errors::ArchiveError, triedb::BlockHeader};

const BLOCK_PADDING_WIDTH: usize = 12;

#[derive(Debug, Clone, PartialEq, Eq, Default, RlpEncodable, RlpDecodable)]
pub struct HashTable {
    pub tx: TransactionSigned,
    pub receipt: ReceiptWithBloom,
    pub trace: Vec<u8>,
}

impl HashTable {
    pub fn new(tx: TransactionSigned, receipt: ReceiptWithBloom, trace: Vec<u8>) -> Self {
        HashTable { tx, receipt, trace }
    }
}

#[derive(Clone)]
pub struct S3Archive {
    pub client: Client,
    pub bucket: String,
    pub semaphore: Arc<Semaphore>,

    pub latest_table_key: String,

    // key =  {block}/{block_number}, value = {RLP(Block)}
    pub block_table_prefix: String,

    // key = {block_hash}/{$block_hash}, value = {str(block_number)}
    pub block_hash_table_prefix: String,

    // key = {receipts}/{block_number}, value = {RLP(Vec<Receipt>)}
    pub receipts_table_prefix: String,

    // key = {traces}/{block_number}, value = {RLP(Vec<Vec<u8>>)}
    pub traces_table_prefix: String,

    // key = {hash}/{tx_hash}, value = {RLP(TransactionSigned, ReceiptWithBloom, Vec<u8>)}
    pub hash_table_prefix: String,
}

impl S3Archive {
    pub async fn new(
        bucket: String,
        region: Option<String>,
        concurrency_level: usize,
    ) -> Result<Self, ArchiveError> {
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
            semaphore: Arc::new(Semaphore::new(concurrency_level)),
            block_table_prefix: "block".to_string(),
            block_hash_table_prefix: "block_hash".to_string(),
            receipts_table_prefix: "receipts".to_string(),
            traces_table_prefix: "traces".to_string(),
            hash_table_prefix: "hash".to_string(),
            latest_table_key: "latest".to_string(),
        })
    }

    // Upload rlp-encoded bytes with retry
    pub async fn upload(&self, key: String, data: Vec<u8>) -> Result<(), ArchiveError> {
        let retry_strategy = ExponentialBackoff::from_millis(10)
            .max_delay(Duration::from_secs(1))
            .map(jitter);

        let permit = self
            .semaphore
            .acquire()
            .await
            .map_err(|_| ArchiveError::custom_error("Semaphore was closed".into()))?;

        Retry::spawn(retry_strategy, || {
            let client = &self.client;
            let bucket = &self.bucket;
            let key = key.to_string();
            let body = ByteStream::from(data.to_vec());

            async move {
                client
                    .put_object()
                    .bucket(bucket)
                    .key(&key)
                    .body(body)
                    .send()
                    .await
                    .map_err(|e| {
                        warn!("Failed to upload {}: {}. Retrying...", key, e);
                        ArchiveError::custom_error(format!("Failed to upload {}: {}", key, e))
                    })
            }
        })
        .await
        .map(|_| ())
        .map_err(|e| {
            error!("Failed to upload after retries {}: {:?}", key, e);
            ArchiveError::custom_error(format!("Failed to upload after retries {}: {:?}", key, e))
        })?;

        std::mem::drop(permit);

        Ok(())
    }

    pub async fn read(&self, key: &str) -> Result<Bytes, ArchiveError> {
        let resp = match self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
        {
            Ok(output) => output,
            Err(e) => {
                warn!("Fail to read from S3: {:?}", e);
                return Err(ArchiveError::custom_error(format!(
                    "Fail to read from S3: {:?}",
                    e
                )));
            }
        };

        let data = resp.body.collect().await.map_err(|e| {
            error!("Unable to collect response data: {:?}", e);
            ArchiveError::custom_error(format!("Unable to collect response data: {:?}", e))
        })?;
        let data_bytes = data.into_bytes();

        Ok(data_bytes)
    }
}

#[derive(Clone)]
pub struct S3ArchiveWriter {
    archive: S3Archive,
}

impl S3ArchiveWriter {
    pub async fn new(archive: S3Archive) -> Result<Self, ArchiveError> {
        Ok(S3ArchiveWriter { archive })
    }
}

impl ArchiveWriterInterface for S3ArchiveWriter {
    async fn get_latest(&self) -> Result<u64, ArchiveError> {
        let key = &self.archive.latest_table_key;

        let value = self.archive.read(key).await?;

        let value_str = String::from_utf8(value.to_vec()).map_err(|e| {
            error!("Invalid UTF-8 sequence: {}", e);
            ArchiveError::custom_error("Invalid UTF-8 sequence".into())
        })?;

        // Parse the string as u64
        value_str.parse::<u64>().map_err(|_| {
            error!(
                "Unable to convert block_number string to number (u64), value: {}",
                value_str
            );
            ArchiveError::custom_error(
                "Unable to convert block_number string to number (u64)".into(),
            )
        })
    }

    async fn update_latest(&self, block_num: u64) -> Result<(), ArchiveError> {
        let key = &self.archive.latest_table_key;
        let latest_value = format!("{:0width$}", block_num, width = BLOCK_PADDING_WIDTH);
        self.archive
            .upload(key.clone(), latest_value.as_bytes().to_vec())
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
        let mut rlp_block = vec![];
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
            self.archive.upload(block_key, rlp_block),
            self.archive
                .upload(block_hash_key, block_hash_value.to_vec())
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
        self.archive.upload(receipts_key, rlp_receipts).await
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

        self.archive.upload(traces_key, rlp_traces).await
    }

    async fn archive_hashes(
        &self,
        transactions: Vec<TransactionSigned>,
        receipts: Vec<ReceiptWithBloom>,
        traces: Vec<Vec<u8>>,
        tx_hashes: Vec<[u8; 32]>,
    ) -> Result<(), ArchiveError> {
        let mut handles = Vec::with_capacity(transactions.len());

        for i in 0..transactions.len() {
            let archive = self.archive.clone();
            let tx_hash = tx_hashes[i].clone();

            let hash = HashTable::new(
                transactions[i].clone(),
                receipts[i].clone(),
                traces[i].clone(),
            );
            let mut rlp_hash = Vec::new();
            hash.encode(&mut rlp_hash);

            let handle = tokio::spawn(async move {
                let hash_key_suffix = hex::encode(tx_hash);
                let hash_key = format!("{}/{}", archive.hash_table_prefix, hash_key_suffix);
                archive.upload(hash_key, rlp_hash).await
            });

            handles.push(handle);
        }

        let results = join_all(handles).await;

        for (idx, upload_result) in results.into_iter().enumerate() {
            if let Err(e) = upload_result {
                error!("Failed to upload index: {}, {:?}", idx, e);
                return Err(ArchiveError::custom_error(format!(
                    "Failed to upload index: {}, {:?}",
                    idx, e
                )));
            }
        }

        Ok(())
    }
}

pub fn make_block(block_header: BlockHeader, transactions: Vec<TransactionSigned>) -> Block {
    Block {
        header: block_header.header,
        body: transactions,
        ..Default::default()
    }
}
