use std::{collections::HashMap, sync::Arc, time::Duration};

use alloy_primitives::{hex::FromHex, TxHash};
use alloy_rlp::{Decodable, Encodable};
use aws_config::SdkConfig;
use aws_sdk_dynamodb::{
    types::{AttributeValue, KeysAndAttributes, PutRequest, WriteRequest},
    Client,
};
use eyre::{bail, Context, Result};
use futures::future::join_all;
use tokio::sync::Semaphore;
use tokio_retry::{
    strategy::{jitter, ExponentialBackoff},
    Retry,
};
use tracing::error;

use super::retry_strategy;
use crate::prelude::*;

const AWS_DYNAMODB_ERRORS: &str = "aws_dynamodb_errors";
const AWS_DYNAMODB_WRITES: &str = "aws_dynamodb_writes";
const AWS_DYNAMODB_READS: &str = "aws_dynamodb_reads";

#[derive(Clone)]
pub struct DynamoDBArchive {
    pub client: Client,
    pub table: String,
    pub semaphore: Arc<Semaphore>,
    pub metrics: Metrics,
}

impl IndexStoreReader for DynamoDBArchive {
    async fn bulk_get(&self, keys: &[TxHash]) -> Result<HashMap<TxHash, TxIndexedData>> {
        let str_keys = keys
            .iter()
            .map(|h| format!("{:x}", h))
            .collect::<Vec<String>>();
        let output = self
            .batch_get(&str_keys)
            .await?
            .into_iter()
            .filter_map(|(k, map)| {
                Some((
                    TxHash::from_hex(k).ok()?, // fmt
                    decode_from_map(&map, "data")?,
                ))
            })
            .collect::<HashMap<TxHash, TxIndexedData>>();
        Ok(output)
    }

    async fn get(&self, key: &TxHash) -> Result<Option<TxIndexedData>> {
        self.bulk_get(&[*key])
            .await // fmt
            .map(|mut v| v.remove(key))
    }
}

impl IndexStore for DynamoDBArchive {
    async fn bulk_put(&self, kvs: impl Iterator<Item = TxIndexedData>) -> Result<()> {
        let mut requests = Vec::with_capacity(kvs.size_hint().0);
        for data in kvs {
            let hash = format!("{:x}", data.tx.tx.tx_hash());
            let mut attribute_map = HashMap::new();
            attribute_map.insert("tx_hash".to_owned(), AttributeValue::S(hash));

            let mut rlp_data = Vec::with_capacity(1024);
            data.encode(&mut rlp_data);
            attribute_map.insert("data".to_owned(), AttributeValue::B(rlp_data.into()));

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

    async fn batch_get(
        &self,
        keys: &[String],
    ) -> Result<HashMap<String, HashMap<String, AttributeValue>>> {
        let mut results = HashMap::new();
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
                    results.extend(items.into_iter().filter_map(extract_txhash_from_map));
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
                    if let Some(items) = responses_retry.remove(&self.table) {
                        results.extend(items.into_iter().filter_map(extract_txhash_from_map));
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

fn extract_txhash_from_map(
    mut item: HashMap<String, AttributeValue>,
) -> Option<(String, HashMap<String, AttributeValue>)> {
    if let Some(attr) = item.remove("tx_hash") {
        if let AttributeValue::S(txhash) = attr {
            return Some((txhash, item));
        } else {
            dbg!("failed to get txhash from attr");
        }
    } else {
        dbg!("txhash attr not found");
    }
    None
}

fn inc_err(metrics: &Metrics) {
    metrics.counter(AWS_DYNAMODB_ERRORS, 1);
}
