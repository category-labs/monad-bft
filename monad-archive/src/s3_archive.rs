use core::str;
use std::{collections::HashMap, sync::Arc};

use alloy_rlp::Encodable;
use aws_config::{meta::region::RegionProviderChain, SdkConfig};
use aws_sdk_dynamodb::{
    types::{AttributeValue, PutRequest, WriteRequest},
    Client as DbClient,
};
use aws_sdk_s3::{
    config::{BehaviorVersion, Region},
    primitives::ByteStream,
    Client as S3Client,
};
use bytes::Bytes;
use futures::{future::join_all, try_join};
use reth_primitives::{Block, ReceiptWithBloom, TransactionSigned, TxHash};
use tokio::time::Duration;
use tokio_retry::{
    strategy::{jitter, ExponentialBackoff},
    Retry,
};
use tracing::{error, warn};

use crate::{archive_interface::ArchiveWriterInterface, errors::ArchiveError, triedb::BlockHeader};

const BLOCK_PADDING_WIDTH: usize = 12;
const MAX_BATCH_SIZE: usize = 25;

#[derive(Clone)]
pub struct S3Archive {
    pub s3_client: S3Client,
    pub bucket: String,

    pub latest_table_key: String,

    // key =  {block}/{block_number}, value = {RLP(Block)}
    pub block_table_prefix: String,

    // key = {block_hash}/{$block_hash}, value = {str(block_number)}
    pub block_hash_table_prefix: String,

    // key = {receipts}/{block_number}, value = {RLP(Vec<Receipt>)}
    pub receipts_table_prefix: String,

    // key = {traces}/{block_number}, value = {RLP(Vec<Vec<u8>>)}
    pub traces_table_prefix: String,
}

#[derive(Clone)]
pub struct DynamoDBArchive {
    // For DynamoDB
    pub client: DbClient,
    pub table: String,
}

impl DynamoDBArchive {
    pub fn new(table: String, config: SdkConfig) -> Self {
        let client = DbClient::new(&config);
        Self { client, table }
    }

    pub async fn index_block(
        &self,
        hashes: Vec<TxHash>,
        block_num: u64,
    ) -> Result<(), ArchiveError> {
        let mut requests = Vec::new();

        for hash in hashes {
            let mut attribute_map = HashMap::new();
            attribute_map.insert("tx_hash".to_string(), AttributeValue::S(hex::encode(hash)));
            attribute_map.insert(
                "block_number".to_string(),
                AttributeValue::S(block_num.to_string()),
            );

            let put_request = PutRequest::builder()
                .set_item(Some(attribute_map))
                .build()
                .expect("Build successful");

            let write_request = WriteRequest::builder().put_request(put_request).build();

            requests.push(write_request);
        }

        let batch_writes = split_into_batches(requests, MAX_BATCH_SIZE);
        let mut batch_write_handles = Vec::new();
        for batch_write in batch_writes {
            let batch_write = batch_write.clone();

            let this = (*self).clone();
            let handle = tokio::spawn(async move { this.upload_to_db(batch_write).await });

            batch_write_handles.push(handle);
        }

        let results = join_all(batch_write_handles).await;

        for (idx, batch_write_result) in results.into_iter().enumerate() {
            if let Err(e) = batch_write_result {
                error!("Failed to upload index: {}, {:?}", idx, e);
                return Err(ArchiveError::custom_error(format!(
                    "Failed to upload index: {}, {:?}",
                    idx, e
                )));
            }
        }
        Ok(())
    }

    pub async fn upload_to_db(&self, values: Vec<WriteRequest>) -> Result<(), ArchiveError> {
        if values.len() > 25 {
            panic!("Batch size larger than limit = 25")
        }

        let retry_strategy = ExponentialBackoff::from_millis(10)
            .max_delay(Duration::from_secs(1))
            .map(jitter);

        // TODO: Only deal with unprocessed items, but it's pretty complicated
        Retry::spawn(retry_strategy, || {
            let values = values.clone();
            // let client = client;
            let client = &self.client;
            let table = self.table.clone();

            async move {
                let mut batch_write: HashMap<String, Vec<WriteRequest>> = HashMap::new();
                batch_write.insert(table.clone(), values.clone());

                let response = client
                    .batch_write_item()
                    .set_request_items(Some(batch_write.clone()))
                    .send()
                    .await
                    .map_err(|e| {
                        warn!("Failed to upload to table {}: {}. Retrying...", table, e);
                        ArchiveError::custom_error(format!(
                            "Failed to upload to table {}: {}",
                            table, e
                        ))
                    })?;

                // Check for unprocessed items
                if let Some(unprocessed) = response.unprocessed_items() {
                    if !unprocessed.is_empty() {
                        warn!(
                            "Unprocessed items detected for table {}: {}. Retrying...",
                            table,
                            unprocessed.get(&table).map(|v| v.len()).unwrap_or(0)
                        );
                        return Err(ArchiveError::custom_error(
                            "Unprocessed items detected".into(),
                        ));
                    }
                }

                Ok(())
            }
        })
        .await
        .map(|_| ())
        .map_err(|e| {
            error!(
                "Failed to upload to table {} after retries: {:?}",
                self.table, e
            );
            ArchiveError::custom_error(format!(
                "Failed to upload to table {} after retries: {:?}",
                self.table, e
            ))
        })
    }
}

impl S3Archive {
    pub async fn new(
        bucket: String,
        table: String,
        region: Option<String>,
    ) -> Result<(Self, DynamoDBArchive), ArchiveError> {
        let region_provider = RegionProviderChain::default_provider().or_else(
            region
                .map(Region::new)
                .unwrap_or_else(|| Region::new("us-east-2")),
        );

        let config = aws_config::defaults(BehaviorVersion::latest())
            .region(region_provider)
            .load()
            .await;
        let s3_client = S3Client::new(&config);

        Ok((
            S3Archive {
                s3_client,
                bucket,
                block_table_prefix: "block".to_string(),
                block_hash_table_prefix: "block_hash".to_string(),
                receipts_table_prefix: "receipts".to_string(),
                traces_table_prefix: "traces".to_string(),
                latest_table_key: "latest".to_string(),
            },
            DynamoDBArchive::new(table, config),
        ))
    }

    // Upload rlp-encoded bytes with retry
    pub async fn upload_to_s3(&self, key: &str, data: Vec<u8>) -> Result<(), ArchiveError> {
        let retry_strategy = ExponentialBackoff::from_millis(10)
            .max_delay(Duration::from_secs(1))
            .map(jitter);

        Retry::spawn(retry_strategy, || {
            let client = &self.s3_client;
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

        Ok(())
    }

    pub async fn read_from_s3(&self, key: &str) -> Result<Bytes, ArchiveError> {
        let resp = match self
            .s3_client
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

        let value = self.archive.read_from_s3(key).await?;

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
            .upload_to_s3(key, latest_value.as_bytes().to_vec())
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
            self.archive.upload_to_s3(&block_key, rlp_block),
            self.archive
                .upload_to_s3(&block_hash_key, block_hash_value.to_vec())
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
        self.archive.upload_to_s3(&receipts_key, rlp_receipts).await
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

        self.archive.upload_to_s3(&traces_key, rlp_traces).await
    }
}

pub fn make_block(block_header: BlockHeader, transactions: Vec<TransactionSigned>) -> Block {
    Block {
        header: block_header.header,
        body: transactions,
        ..Default::default()
    }
}

fn split_into_batches(values: Vec<WriteRequest>, batch_size: usize) -> Vec<Vec<WriteRequest>> {
    values
        .chunks(batch_size)
        .map(|chunk| chunk.to_vec())
        .collect()
}
