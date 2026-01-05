// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::{collections::HashMap, sync::Arc};

use aws_config::SdkConfig;
use aws_sdk_dynamodb::{
    types::{AttributeValue, KeysAndAttributes, PutRequest, WriteRequest},
    Client,
};
use bytes::Bytes;
use eyre::{bail, Context, Result};
use futures::future::try_join_all;
use tokio::sync::Semaphore;
use tracing::error;

use super::{KVStoreType, MetricsResultExt};
use crate::prelude::*;

#[derive(Clone)]
pub struct DynamoDBArchive {
    pub s3: Bucket,
    pub client: Client,
    pub table: String,
    pub semaphore: Arc<Semaphore>,
    pub metrics: Metrics,
}

impl KVReader for DynamoDBArchive {
    async fn bulk_get(&self, keys: &[String]) -> Result<HashMap<String, Bytes>> {
        let start = Instant::now();
        self.batch_get(keys).await.write_get_metrics(
            start.elapsed(),
            KVStoreType::AwsDynamoDB,
            &self.metrics,
        )
    }

    async fn get(&self, key: &str) -> Result<Option<Bytes>> {
        self.bulk_get(&[key.to_owned()])
            .await
            .map(|mut v| v.remove(key))
    }

    async fn scan_prefix_with_max_keys(
        &self,
        _prefix: &str,
        _max_keys: usize,
    ) -> Result<Vec<String>> {
        unimplemented!()
    }
}

impl KVStore for DynamoDBArchive {
    fn bucket_name(&self) -> &str {
        &self.table
    }

    async fn bulk_put(&self, kvs: impl IntoIterator<Item = (String, Vec<u8>)>) -> Result<()> {
        let requests = kvs
            .into_iter()
            .filter_map(|(key, data)| {
                let attribute_map: HashMap<String, AttributeValue> = HashMap::from_iter([
                    ("tx_hash".to_owned(), AttributeValue::S(key)),
                    ("data".to_owned(), AttributeValue::B(data.into())),
                ]);
                match PutRequest::builder().set_item(Some(attribute_map)).build() {
                    Ok(put_request) => {
                        Some(WriteRequest::builder().put_request(put_request).build())
                    }
                    Err(e) => {
                        error!("Failed to build put request. Err: {e:?}");
                        None
                    }
                }
            })
            .collect::<Vec<_>>();

        let batch_writes = requests
            .chunks(Self::WRITE_BATCH_SIZE)
            .map(|chunk| chunk.to_vec())
            .map(|batch_writes| {
                let this = (*self).clone();
                tokio::spawn(async move {
                    let start = Instant::now();
                    this.upload_to_db(batch_writes).await.write_put_metrics(
                        start.elapsed(),
                        KVStoreType::AwsDynamoDB,
                        &this.metrics,
                    )
                })
            });

        try_join_all(batch_writes).await?;
        Ok(())
    }

    async fn put(&self, key: impl AsRef<str>, data: Vec<u8>) -> Result<()> {
        let put_request = PutRequest::builder()
            .item(key.as_ref(), AttributeValue::B(data.into()))
            .build()
            .wrap_err_with(|| format!("Failed to build put request, key: {}", key.as_ref()))?;
        let request = WriteRequest::builder().put_request(put_request).build();

        let start = Instant::now();
        self.upload_to_db(vec![request]).await.write_put_metrics(
            start.elapsed(),
            KVStoreType::AwsDynamoDB,
            &self.metrics,
        )
    }

    async fn delete(&self, _key: impl AsRef<str>) -> Result<()> {
        unimplemented!()
    }
}

impl DynamoDBArchive {
    const READ_BATCH_SIZE: usize = 100;
    const WRITE_BATCH_SIZE: usize = 25;

    pub fn new(
        s3: Bucket,
        table: String,
        config: &SdkConfig,
        concurrency: usize,
        metrics: Metrics,
    ) -> Self {
        let client = Client::new(config);
        Self {
            s3,
            client,
            table,
            semaphore: Arc::new(Semaphore::new(concurrency)),
            metrics,
        }
    }

    async fn batch_get(&self, keys: &[String]) -> Result<HashMap<String, Bytes>> {
        let mut results: HashMap<String, Bytes> = HashMap::new();
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

            let response = self
                .client
                .batch_get_item()
                .set_request_items(Some(request_items.clone()))
                .send()
                .await
                .wrap_err_with(|| format!("Request keys (0x stripped in req): {:?}", &batch))?;

            // Collect retrieved items
            if let Some(mut responses) = response.responses {
                if let Some(items) = responses.remove(&self.table) {
                    results.extend(items.into_iter().filter_map(extract_kv_from_map));
                }
            }

            // Retry unprocessed keys
            let mut unprocessed_keys = response.unprocessed_keys;
            while let Some(unprocessed) = unprocessed_keys {
                if unprocessed.is_empty() {
                    break;
                }
                let response_retry = self
                    .client
                    .batch_get_item()
                    .set_request_items(Some(unprocessed.clone()))
                    .send()
                    .await
                    .wrap_err_with(|| "Failed to get unprocessed keys")?;

                if let Some(mut responses_retry) = response_retry.responses {
                    if let Some(items) = responses_retry.remove(&self.table) {
                        results.extend(items.into_iter().filter_map(extract_kv_from_map));
                    }
                }
                unprocessed_keys = response_retry.unprocessed_keys;
            }
        }

        Ok(results)
    }

    async fn upload_to_db(&self, values: Vec<WriteRequest>) -> Result<()> {
        if values.len() > Self::WRITE_BATCH_SIZE {
            panic!("Batch size larger than limit = {}", Self::WRITE_BATCH_SIZE)
        }

        let _permit = self.semaphore.acquire().await.expect("semaphore dropped");
        let mut batch_write: HashMap<String, Vec<WriteRequest>> = HashMap::new();
        batch_write.insert(self.table.clone(), values.clone());

        let response = self
            .client
            .batch_write_item()
            .set_request_items(Some(batch_write.clone()))
            .send()
            .await
            .wrap_err_with(|| format!("Failed to upload to table {}", self.table))?;

        // Check for unprocessed items
        if let Some(unprocessed) = response.unprocessed_items() {
            if !unprocessed.is_empty() {
                bail!(
                    "Unprocessed items detected for table {}: {:?}. Retrying...",
                    self.table,
                    unprocessed.get(&self.table).map(|v| v.len()).unwrap_or(0)
                );
            }
        }

        Ok(())
    }
}

fn extract_kv_from_map(mut item: HashMap<String, AttributeValue>) -> Option<(String, Bytes)> {
    match (item.remove("key"), item.remove("data")) {
        (Some(AttributeValue::S(key)), Some(AttributeValue::B(data))) => {
            Some((key, Bytes::from(data.into_inner())))
        }
        (None, Some(AttributeValue::B(data))) => {
            // fallback to reading 1st schema
            let AttributeValue::S(key) = item.remove("tx_hash")? else {
                return None;
            };
            Some((key, Bytes::from(data.into_inner())))
        }
        _ => None,
    }
}
