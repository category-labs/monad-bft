use core::str;

use alloy_rlp::{Decodable, Encodable};
use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::{
    config::{BehaviorVersion, Region},
    primitives::ByteStream,
    Client,
};
use bytes::Bytes;
use futures::stream::{self, StreamExt};
use reth_primitives::{Block, ReceiptWithBloom, TransactionSigned};
use tokio::time::Duration;
use tokio_retry::{
    strategy::{jitter, ExponentialBackoff},
    Retry,
};
use tracing::{error, warn};

use crate::{
    archive_interface::{ArchiveReaderInterface, ArchiveWriterInterface},
    errors::ArchiveError,
    triedb::BlockHeader,
};

const BLOCK_PADDING_WIDTH: usize = 12;

#[derive(Clone)]
pub struct S3Archive {
    pub client: Client,
    pub bucket: String,
    pub max_concurrent_upload: usize,

    pub latest_table_key: String,

    // key =  {block}/{block_number}, value = {RLP(Block)}
    pub block_table_prefix: String,

    // key = {block_hash}/{$block_hash}, value = {str(block_number)}
    pub block_hash_table_prefix: String,

    // key = {tx_hash}/{$tx_hash}, value = {RLP(Transaction)}
    pub tx_hash_table_prefix: String,

    // key = {receipts}/{block_number}, value = {RLP(Vec<Receipt>)}
    pub receipts_table_prefix: String,

    // key = {receipt_hash}/{$receipt_hash}, value = {RLP(Receipt)}
    pub receipt_hash_table_prefix: String,
    // TODO: traces
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
            max_concurrent_upload: 30,
            block_table_prefix: "block".to_string(),
            block_hash_table_prefix: "block_hash".to_string(),
            tx_hash_table_prefix: "tx_hash".to_string(),
            receipts_table_prefix: "receipts".to_string(),
            receipt_hash_table_prefix: "receipt_hash".to_string(),
            latest_table_key: "latest".to_string(),
        })
    }

    // Upload rlp-encoded bytes with retry
    pub async fn upload(&self, key: String, data: Vec<u8>) -> Result<(), ArchiveError> {
        let retry_strategy = ExponentialBackoff::from_millis(10)
            .max_delay(Duration::from_secs(1))
            .map(jitter);

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
        })
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

        // 3) Insert into transaction table
        let tx_uploads_f = transactions.into_iter().map(|transaction| {
            let tx_hash_key_suffix = hex::encode(transaction.hash);
            let tx_hash_key = format!(
                "{}/{}",
                self.archive.tx_hash_table_prefix, tx_hash_key_suffix
            );
            let mut rlp_tx = vec![];
            transaction.encode(&mut rlp_tx);
            self.archive.upload(tx_hash_key, rlp_tx)
        });

        let tx_uploads = futures::future::join_all(tx_uploads_f).await;
        for upload_result in tx_uploads {
            if upload_result.is_err() {
                error!(
                    "Failed to upload transaction: {:?}",
                    upload_result.clone().err()
                );
                return Err(ArchiveError::custom_error(format!(
                    "Failed to upload transaction: {:?}",
                    upload_result.err()
                )));
            }
        }

        futures::try_join!(
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
        tx_hashes: Vec<[u8; 32]>,
    ) -> Result<(), ArchiveError> {
        // 1) Insert into receipts table
        let receipts_key = format!(
            "{}/{:0width$}",
            self.archive.receipts_table_prefix,
            block_num,
            width = BLOCK_PADDING_WIDTH
        );

        let mut rlp_receipts = vec![];
        receipts.encode(&mut rlp_receipts);

        self.archive.upload(receipts_key, rlp_receipts).await?;

        let upload_stream = stream::iter(receipts.into_iter().enumerate().map(|(idx, receipt)| {
            let receipt_hash_key_suffix = hex::encode(tx_hashes[idx]); // Use hex encoding for consistency
            let receipt_hash_key = format!(
                "{}/{}",
                self.archive.receipt_hash_table_prefix, receipt_hash_key_suffix
            );
            let mut rlp_receipt = vec![];
            receipt.encode(&mut rlp_receipt);
            self.archive.upload(receipt_hash_key, rlp_receipt)
        }));

        // Process the stream with limited concurrency
        let results = upload_stream
            .buffer_unordered(self.archive.max_concurrent_upload)
            .collect::<Vec<Result<(), ArchiveError>>>()
            .await;

        // Handle any upload errors
        for upload_result in results {
            if let Err(e) = upload_result {
                error!("Failed to upload receipt: {:?}", e);
                return Err(ArchiveError::custom_error(format!(
                    "Failed to upload receipt: {:?}",
                    e
                )));
            }
        }

        Ok(())
    }

    async fn archive_traces(
        &self,
        traces: Bytes,
        block_num: u64,
        tx_hashes: Vec<[u8; 32]>,
    ) -> Result<(), ArchiveError> {
        Err(ArchiveError::custom_error("Not implemented".into()))
    }
}

#[derive(Clone)]
pub struct S3ArchiveReader {
    archive: S3Archive,
}

impl S3ArchiveReader {
    pub async fn new(archive: S3Archive) -> Result<Self, ArchiveError> {
        Ok(S3ArchiveReader { archive })
    }
}

impl ArchiveReaderInterface for S3ArchiveReader {
    async fn get_latest(&self) -> Result<u64, ArchiveError> {
        let key = &self.archive.latest_table_key;

        let value = self.archive.read(key).await?;
        let value_str = hex::encode(value.to_vec());

        value_str.parse::<u64>().map_err(|_| {
            error!("Unable to convert block_number string to number (u64)");
            ArchiveError::custom_error(
                "Unable to convert block_number string to number (u64)".into(),
            )
        })
    }

    /*
        Block Methods
    */

    // eth_getBlockByHash
    async fn get_block_by_hash(&self, block_hash: &[u8; 32]) -> Result<Block, ArchiveError> {
        let block_hash_key_suffix = hex::encode(block_hash);
        let block_hash_key = format!(
            "{}/{}",
            self.archive.block_hash_table_prefix, block_hash_key_suffix
        );

        let block_num_bytes = self.archive.read(&block_hash_key).await?;

        let block_num_str = String::from_utf8(block_num_bytes.to_vec()).map_err(|e| {
            error!("Invalid UTF-8 sequence: {}", e);
            ArchiveError::custom_error("Invalid UTF-8 sequence".into())
        })?;

        let block_num = block_num_str.parse::<u64>().map_err(|_| {
            error!(
                "Unable to convert block_number string to number (u64), value: {}",
                block_num_str
            );
            ArchiveError::custom_error(
                "Unable to convert block_number string to number (u64)".into(),
            )
        })?;

        self.get_block_by_number(block_num).await
    }

    // eth_getBlockByNumber
    // eth_getRawBlock
    // eth_getRawHeader
    async fn get_block_by_number(&self, block_num: u64) -> Result<Block, ArchiveError> {
        let block_key = format!("block/{:0width$}", block_num, width = BLOCK_PADDING_WIDTH);

        let rlp_block = self.archive.read(&block_key).await?;
        let mut rlp_block_slice: &[u8] = &rlp_block;
        let block = Block::decode(&mut rlp_block_slice)
            .map_err(|_| ArchiveError::custom_error("Cannot decode block".into()))?;

        Ok(block)
    }

    //eth_getBlockTransactionCountByHash
    async fn get_block_transaction_count_by_hash(
        &self,
        block_hash: &[u8; 32],
    ) -> Result<usize, ArchiveError> {
        let block = self.get_block_by_hash(block_hash).await?;

        Ok(block.body.length())
    }

    //eth_getBlockTransactionCountByNumber
    async fn get_block_transaction_count_by_number(
        &self,
        block_num: u64,
    ) -> Result<usize, ArchiveError> {
        let block = self.get_block_by_number(block_num).await?;

        Ok(block.body.length())
    }

    /*
        Transaction Methods
    */

    //eth_getTransactionByBlockHashAndIndex
    async fn get_transaction_by_block_hash_and_index(
        &self,
        block_hash: &[u8; 32],
        tx_index: u64,
    ) -> Result<TransactionSigned, ArchiveError> {
        let block = self.get_block_by_hash(block_hash).await?;

        Ok(block.body[tx_index as usize].clone())
    }

    // eth_getTransactionByBlockNumberAndIndex
    async fn get_transaction_by_block_number_and_index(
        &self,
        block_num: u64,
        tx_index: u64,
    ) -> Result<TransactionSigned, ArchiveError> {
        let block = self.get_block_by_number(block_num).await?;

        Ok(block.body[tx_index as usize].clone())
    }

    // eth_getTransactionByHash
    async fn get_transaction_by_hash(
        &self,
        tx_hash: &[u8; 32],
    ) -> Result<TransactionSigned, ArchiveError> {
        let tx_hash_key_suffix = hex::encode(tx_hash);
        let tx_hash_key = format!(
            "{}/{}",
            self.archive.tx_hash_table_prefix, tx_hash_key_suffix
        );

        let rlp_tx = self.archive.read(&tx_hash_key).await?;
        let mut rlp_tx_slice: &[u8] = &rlp_tx;
        let tx = TransactionSigned::decode(&mut rlp_tx_slice)
            .map_err(|_| ArchiveError::custom_error("Cannot decode transaction".into()))?;

        Ok(tx)
    }

    /*
        Receipt Methods
    */

    // eth_getBlockReceipts
    async fn get_block_receipts(
        &self,
        block_number: u64,
    ) -> Result<Vec<ReceiptWithBloom>, ArchiveError> {
        let receipts_key = format!(
            "{}/{:0width$}",
            self.archive.receipts_table_prefix,
            block_number,
            width = BLOCK_PADDING_WIDTH
        );

        let rlp_receipts = self.archive.read(&receipts_key).await?;
        let mut rlp_receipts_slice: &[u8] = &rlp_receipts;

        let receipts: Vec<ReceiptWithBloom> = Vec::decode(&mut rlp_receipts_slice)
            .map_err(|_| ArchiveError::custom_error("Cannot decode block".into()))?;

        Ok(receipts)
    }

    // eth_getTransactionReceipt
    async fn get_transaction_receipt(
        self,
        tx_hash: &[u8; 32],
    ) -> Result<ReceiptWithBloom, ArchiveError> {
        let receipt_hash_key_suffix = hex::encode(tx_hash);
        let receipt_hash_key = format!(
            "{}/{}",
            self.archive.receipt_hash_table_prefix, receipt_hash_key_suffix
        );

        let rlp_receipt = self.archive.read(&receipt_hash_key).await?;
        let mut rlp_receipt_slice: &[u8] = &rlp_receipt;
        let receipt = ReceiptWithBloom::decode(&mut rlp_receipt_slice)
            .map_err(|_| ArchiveError::custom_error("Cannot decode receipt".into()))?;

        Ok(receipt)
    }
}

pub fn make_block(block_header: BlockHeader, transactions: Vec<TransactionSigned>) -> Block {
    Block {
        header: block_header.header,
        body: transactions,
        ..Default::default()
    }
}
