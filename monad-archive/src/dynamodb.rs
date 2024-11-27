use std::{collections::HashMap, sync::Arc, time::Duration};

use alloy_rlp::{Decodable, Encodable, RlpDecodable, RlpEncodable};
use aws_config::SdkConfig;
use aws_sdk_dynamodb::{
    types::{AttributeValue, KeysAndAttributes, PutRequest, WriteRequest},
    Client,
};
use eyre::{bail, Context, Result};
use futures::future::join_all;
use reth_primitives::{Block, ReceiptWithBloom, TransactionSigned};
use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;
use tokio_retry::{
    strategy::{jitter, ExponentialBackoff},
    Retry,
};
use tracing::error;

use crate::metrics::Metrics;

const AWS_DYNAMODB_ERRORS: &'static str = "aws_dynamodb_errors";
const AWS_DYNAMODB_WRITES: &'static str = "aws_dynamodb_writes";
const AWS_DYNAMODB_READS: &'static str = "aws_dynamodb_reads";

pub trait TxIndexArchiver {
    async fn index_block(
        &self,
        block: Block,
        traces: Vec<Vec<u8>>,
        receipts: Vec<ReceiptWithBloom>,
    ) -> Result<()>;
}

#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, RlpEncodable, RlpDecodable,
)]
pub struct TxIndexedData {
    pub block_num: u64,
    pub tx: TransactionSigned,
    pub trace: Vec<u8>,
    pub receipt: ReceiptWithBloom,
}

#[derive(Clone)]
pub struct DynamoDBArchive {
    pub client: Client,
    pub table: String,
    pub semaphore: Arc<Semaphore>,
    pub metrics: Metrics,
}

impl DynamoDBArchive {
    pub async fn batch_get_txdata(&self, keys: &[String]) -> Result<Vec<Option<TxIndexedData>>> {
        let output = self
            .batch_get(keys)
            .await?
            .into_iter()
            .map(|map| {
                Some(TxIndexedData {
                    block_num: block_number_from_map(&map)?,
                    tx: decode_from_map(&map, "tx")?,
                    receipt: decode_from_map(&map, "receipt")?,
                    trace: decode_from_map(&map, "trace")?,
                })
            })
            .collect::<Vec<Option<TxIndexedData>>>();
        Ok(output)
    }

    pub async fn get_txdata(&self, key: impl Into<String>) -> Result<Option<TxIndexedData>> {
        self.batch_get_txdata(&[key.into()])
            .await
            .map(|mut v| v.remove(0))
    }
}

impl TxIndexArchiver for DynamoDBArchive {
    async fn index_block(
        &self,
        block: Block,
        traces: Vec<Vec<u8>>,
        receipts: Vec<ReceiptWithBloom>,
    ) -> Result<()> {
        let mut requests = Vec::new();
        let block_num = block.number.to_string();

        for ((tx, trace), receipt) in block
            .body
            .into_iter()
            .zip(traces.into_iter())
            .zip(receipts.into_iter())
        {
            let hash = tx.hash();
            let mut attribute_map = HashMap::new();
            attribute_map.insert("tx_hash".to_owned(), AttributeValue::S(hex::encode(hash)));

            // block_number
            attribute_map.insert(
                "block_number".to_owned(),
                AttributeValue::S(block_num.clone()),
            );

            // tx
            let mut rlp_tx = Vec::with_capacity(512);
            tx.encode(&mut rlp_tx);
            attribute_map.insert("tx".to_owned(), AttributeValue::B(rlp_tx.into()));

            // trace
            let mut rlp_trace = Vec::with_capacity(trace.capacity());
            trace.encode(&mut rlp_trace);
            attribute_map.insert("trace".to_owned(), AttributeValue::B(rlp_trace.into()));

            // receipt
            let mut rlp_receipt = Vec::with_capacity(512);
            receipt.encode(&mut rlp_receipt);
            attribute_map.insert("receipt".to_owned(), AttributeValue::B(rlp_receipt.into()));

            let put_request = PutRequest::builder()
                .set_item(Some(attribute_map))
                .build()?;

            let write_request = WriteRequest::builder().put_request(put_request).build();

            requests.push(write_request);
        }

        let batch_writes = split_into_batches(requests, Self::WRITE_BATCH_SIZE);
        // let batch_writes = split_into_batches(requests, 1);
        let mut batch_write_handles = Vec::new();
        for batch_write in batch_writes {
            let batch_write = batch_write.clone();

            let this = (*self).clone();
            let handle = tokio::spawn(async move { this.upload_to_db(batch_write).await });

            batch_write_handles.push(handle);
        }

        let results = join_all(batch_write_handles).await;

        for (idx, batch_write_result) in results.into_iter().enumerate() {
            batch_write_result.wrap_err_with(|| format!("Failed to upload index {idx}"))??;
        }
        Ok(())
    }
}

impl DynamoDBArchive {
    const READ_BATCH_SIZE: usize = 100;
    const WRITE_BATCH_SIZE: usize = 25;

    pub fn new(table: String, config: &SdkConfig, concurrency: usize, metrics: Metrics) -> Self {
        let client = Client::new(config);
        Self {
            client,
            table,
            semaphore: Arc::new(Semaphore::new(concurrency)),
            metrics,
        }
    }

    async fn batch_get(&self, keys: &[String]) -> Result<Vec<HashMap<String, AttributeValue>>> {
        let mut results = Vec::new();
        let batches = keys.chunks(Self::READ_BATCH_SIZE);

        for batch in batches {
            // Prepare the keys for this batch
            let mut key_maps = Vec::new();
            for key in batch {
                let key = key.trim_start_matches("0x");
                let mut key_map = HashMap::new();
                key_map.insert("tx_hash".to_string(), AttributeValue::S(key.to_string()));
                key_maps.push(key_map);
            }

            // Build the batch request
            let mut request_items = HashMap::new();
            request_items.insert(
                self.table.clone(),
                KeysAndAttributes::builder()
                    .set_keys(Some(key_maps))
                    .build()?,
            );

            let response = Retry::spawn(retry_strategy(), || async {
                self.client
                    .batch_get_item()
                    .set_request_items(Some(request_items.clone()))
                    .send()
                    .await
                    .wrap_err_with(|| {
                        inc_err(&self.metrics);
                        format!("Request keys (0x stripped in req): {:?}", &batch)
                    })
            })
            .await?;

            // Collect retrieved items
            if let Some(mut responses) = response.responses {
                if let Some(items) = responses.remove(&self.table) {
                    results.extend(items.into_iter());
                }
            }

            // Retry unprocessed keys
            let mut unprocessed_keys = response.unprocessed_keys;
            while let Some(unprocessed) = unprocessed_keys {
                if unprocessed.is_empty() {
                    break;
                }
                let response_retry = Retry::spawn(retry_strategy(), || async {
                    self.client
                        .batch_get_item()
                        .set_request_items(Some(unprocessed.clone()))
                        .send()
                        .await
                        .wrap_err_with(|| {
                            inc_err(&self.metrics);
                            "Failed to get unprocessed keys"
                        })
                })
                .await?;

                if let Some(mut responses_retry) = response_retry.responses {
                    if let Some(items_retry) = responses_retry.remove(&self.table) {
                        results.extend(items_retry.into_iter());
                    }
                }
                unprocessed_keys = response_retry.unprocessed_keys;
            }
        }

        self.metrics.counter(AWS_DYNAMODB_READS, keys.len() as u64);
        Ok(results)
    }

    async fn upload_to_db(&self, values: Vec<WriteRequest>) -> Result<()> {
        if values.len() > 25 {
            panic!("Batch size larger than limit = 25")
        }
        let num_writes = values.len();

        let retry_strategy = ExponentialBackoff::from_millis(10)
            .max_delay(Duration::from_secs(1))
            .map(jitter);

        // TODO: Only deal with unprocessed items, but it's pretty complicated
        Retry::spawn(retry_strategy, || {
            let values = values.clone();
            // let client = client;
            let client = &self.client;
            let table = self.table.clone();
            let semeaphore = Arc::clone(&self.semaphore);
            let metrics = &self.metrics;

            async move {
                let _permit = semeaphore.acquire().await.expect("semaphore dropped");
                let mut batch_write: HashMap<String, Vec<WriteRequest>> = HashMap::new();
                batch_write.insert(table.clone(), values.clone());

                let response = client
                    .batch_write_item()
                    .set_request_items(Some(batch_write.clone()))
                    .send()
                    .await
                    .wrap_err_with(|| {
                        inc_err(metrics);
                        format!("Failed to upload to table {}. Retrying...", table)
                    })?;

                // Check for unprocessed items
                if let Some(unprocessed) = response.unprocessed_items() {
                    if !unprocessed.is_empty() {
                        bail!(
                            "Unprocessed items detected for table {}: {:?}. Retrying...",
                            table,
                            unprocessed.get(&table).map(|v| v.len()).unwrap_or(0)
                        );
                    }
                }

                Ok(())
            }
        })
        .await
        .wrap_err_with(|| format!("Failed to upload to table {} after retries", self.table))?;

        self.metrics.counter(AWS_DYNAMODB_WRITES, num_writes as u64);
        Ok(())
    }
}

fn split_into_batches(values: Vec<WriteRequest>, batch_size: usize) -> Vec<Vec<WriteRequest>> {
    values
        .chunks(batch_size)
        .map(|chunk| chunk.to_vec())
        .collect()
}

fn decode_from_map<T: Decodable>(
    item: &HashMap<String, AttributeValue>,
    key: &'static str,
) -> Option<T> {
    if let Some(attr) = item.get(key) {
        if let AttributeValue::B(tx_blob) = attr {
            return T::decode(&mut tx_blob.as_ref()).ok();
        } else {
            error!("failed to get {key} from attr");
        }
    } else {
        error!("{key} not found");
    }
    None
}

fn block_number_from_map(item: &HashMap<String, AttributeValue>) -> Option<u64> {
    if let Some(block_number_attr) = item.get("block_number") {
        if let AttributeValue::S(block_number) = block_number_attr {
            return block_number.parse().ok();
        } else {
            dbg!("failed to get block_bumber from attr");
        }
    } else {
        dbg!("block number attr not found");
    }
    None
}

pub fn retry_strategy() -> std::iter::Map<ExponentialBackoff, fn(Duration) -> Duration> {
    ExponentialBackoff::from_millis(10)
        .max_delay(Duration::from_secs(1))
        .map(jitter)
}

fn inc_err(metrics: &Metrics) {
    metrics.counter(AWS_DYNAMODB_ERRORS, 1);
}
