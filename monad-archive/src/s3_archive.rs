use core::str;
use std::{collections::HashMap, sync::Arc};

use alloy_rlp::Encodable;
use aws_config::meta::region::RegionProviderChain;
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
use futures::try_join;
use reth_primitives::{Block, ReceiptWithBloom, TransactionSigned};
use tokio::time::Duration;
use tokio_retry::{
    strategy::{jitter, ExponentialBackoff},
    Retry,
};
use tracing::{error, warn};

use crate::{archive_interface::ArchiveWriterInterface, errors::ArchiveError, triedb::BlockHeader};

const BLOCK_PADDING_WIDTH: usize = 12;

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

    // For DynamoDB
    pub db_client: DbClient,
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
        let s3_client = S3Client::new(&config);
        let db_client = DbClient::new(&config);

        Ok(S3Archive {
            s3_client,
            bucket,
            block_table_prefix: "block".to_string(),
            block_hash_table_prefix: "block_hash".to_string(),
            receipts_table_prefix: "receipts".to_string(),
            traces_table_prefix: "traces".to_string(),
            latest_table_key: "latest".to_string(),
            db_client,
        })
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

    pub async fn upload_to_db(
        &self,
        table: String,
        values: Vec<WriteRequest>,
    ) -> Result<(), ArchiveError> {
        let chunk_size = 25;
        let value_chunks = values.chunks(chunk_size);

        for value_chunk in value_chunks {
            let mut request_items = HashMap::new();
            request_items.insert(table.clone(), value_chunk.to_vec());

            let batch_write = self
                .db_client
                .batch_write_item()
                .set_request_items(Some(request_items.clone()))
                .send()
                .await;

            if batch_write.is_err() {
                error!("Error is batch writing");
                return Err(ArchiveError::custom_error("error in batch writing".into()));
            }

            // let retry_strategy = ExponentialBackoff::from_millis(10)
            // .max_delay(Duration::from_secs(1))
            // .map(jitter);
            // Retry::spawn(retry_strategy, || {
            //     match batch_write {
            //         Ok(output) => {
            //             if let Some(unprocessed_items) = output.unprocessed_items().and_then(|items| items.get(&table.clone())) {
            //                 if !unprocessed_items.is_empty() {
            //                     remaining_items.insert(table.clone(), unprocessed_items.clone());
            //                     warn!("Failed to upload all items");
            //                     Err(ArchiveError::custom_error("Failed to upload all items".into()))
            //                 }else{
            //                     Ok(())
            //                 }
            //             }else{
            //                 Ok(())
            //             }
            //         },
            //         Err(e) => {
            //             Err(ArchiveError::custom_error("Failed to upload all items".into()))
            //         }
            //     }
            // }).await
            // .map(|_| ())
            // .map_err(|_| {
            //     ArchiveError::custom_error("message".into())
            // })?;
        }

        Ok(())
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

        // 4) Update DynamoDB
        let mut requests = Vec::new();

        for tx in transactions {
            let mut attribute_map = HashMap::new();
            attribute_map.insert(
                "tx_hash".to_string(),
                AttributeValue::S(hex::encode(tx.hash)),
            );
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

        self.archive.upload_to_db("tx-hash".into(), requests).await
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
