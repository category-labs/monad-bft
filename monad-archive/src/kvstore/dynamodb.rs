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
    client::Waiters,
    types::{
        AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType,
        KeysAndAttributes, PutRequest, ScalarAttributeType, WriteRequest,
    },
    Client,
};
use bytes::Bytes;
use eyre::{bail, Context, Result};
use futures::future::try_join_all;
use tokio::sync::Semaphore;
use tracing::error;

use super::{KVStoreType, MetricsResultExt, PutResult, WritePolicy};
use crate::prelude::*;

/// Name of the partition-key attribute. Kept as `tx_hash` for backwards
/// compatibility with existing DynamoDB index tables; it is just the partition
/// key attribute name and is reused verbatim for block-data tables.
pub const PARTITION_KEY: &str = "tx_hash";

#[derive(Clone)]
pub struct DynamoDBArchive {
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

    async fn exists(&self, key: &str) -> Result<bool> {
        let start = Instant::now();
        let result = self
            .client
            .get_item()
            .table_name(&self.table)
            .key(PARTITION_KEY, AttributeValue::S(key.to_owned()))
            .projection_expression(PARTITION_KEY)
            .send()
            .await
            .wrap_err("DynamoDB exists check failed")
            .write_get_metrics_on_err(start.elapsed(), KVStoreType::AwsDynamoDB, &self.metrics)?;

        Ok(result.item.is_some()).write_get_metrics(
            start.elapsed(),
            KVStoreType::AwsDynamoDB,
            &self.metrics,
        )
    }
}

impl KVStore for DynamoDBArchive {
    async fn scan_prefix(&self, _prefix: &str) -> Result<Vec<String>> {
        unimplemented!()
    }

    fn bucket_name(&self) -> &str {
        &self.table
    }

    async fn bulk_put(
        &self,
        kvs: impl IntoIterator<Item = (String, Vec<u8>)>,
        _policy: WritePolicy,
    ) -> Result<PutResult> {
        // Note: WritePolicy is ignored for DynamoDB - always overwrites
        let requests = kvs
            .into_iter()
            .filter_map(|(key, data)| {
                let attribute_map: HashMap<String, AttributeValue> = HashMap::from_iter([
                    (PARTITION_KEY.to_owned(), AttributeValue::S(key)),
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
        Ok(PutResult::Written)
    }

    async fn put(
        &self,
        key: impl AsRef<str>,
        data: Vec<u8>,
        _policy: WritePolicy,
    ) -> Result<PutResult> {
        // Note: WritePolicy is ignored for DynamoDB - always overwrites
        let attribute_map: HashMap<String, AttributeValue> = HashMap::from_iter([
            (
                PARTITION_KEY.to_owned(),
                AttributeValue::S(key.as_ref().to_owned()),
            ),
            ("data".to_owned(), AttributeValue::B(data.into())),
        ]);
        let put_request = PutRequest::builder()
            .set_item(Some(attribute_map))
            .build()
            .wrap_err_with(|| format!("Failed to build put request, key: {}", key.as_ref()))?;
        let request = WriteRequest::builder().put_request(put_request).build();

        let start = Instant::now();
        self.upload_to_db(vec![request]).await.write_put_metrics(
            start.elapsed(),
            KVStoreType::AwsDynamoDB,
            &self.metrics,
        )?;
        Ok(PutResult::Written)
    }

    async fn delete(&self, _key: impl AsRef<str>) -> Result<()> {
        unimplemented!()
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

    /// Create the backing table (partition key [`PARTITION_KEY`], on-demand
    /// billing) if it does not already exist, then wait until it is active.
    ///
    /// DynamoDB proper expects tables to be provisioned out-of-band, but
    /// DynamoDB-compatible backends such as ScyllaDB Alternator let us create
    /// them on demand, matching the auto-create ergonomics of the Mongo backend.
    pub async fn ensure_table(&self) -> Result<()> {
        let existing = self
            .client
            .list_tables()
            .send()
            .await
            .wrap_err("Failed to list DynamoDB tables")?;
        if existing.table_names().contains(&self.table) {
            return Ok(());
        }

        info!("DynamoDB table '{}' not found, creating...", self.table);
        self.client
            .create_table()
            .table_name(&self.table)
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name(PARTITION_KEY)
                    .attribute_type(ScalarAttributeType::S)
                    .build()
                    .wrap_err("Failed to build attribute definition")?,
            )
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name(PARTITION_KEY)
                    .key_type(KeyType::Hash)
                    .build()
                    .wrap_err("Failed to build key schema")?,
            )
            .billing_mode(BillingMode::PayPerRequest)
            .send()
            .await
            .wrap_err_with(|| format!("Failed to create DynamoDB table '{}'", self.table))?;

        self.client
            .wait_until_table_exists()
            .table_name(&self.table)
            .wait(Duration::from_secs(30))
            .await
            .wrap_err_with(|| format!("Table '{}' did not become active", self.table))?;
        info!("DynamoDB table '{}' created", self.table);
        Ok(())
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
                key_map.insert(
                    PARTITION_KEY.to_string(),
                    AttributeValue::S(key.to_string()),
                );
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
            let AttributeValue::S(key) = item.remove(PARTITION_KEY)? else {
                return None;
            };
            Some((key, Bytes::from(data.into_inner())))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    //! Integration test verifying the DynamoDB backend works unmodified against
    //! a ScyllaDB Alternator endpoint (DynamoDB-compatible API).
    //!
    //! Run against a running Alternator with:
    //!   SCYLLA_ALTERNATOR_ENDPOINT=http://localhost:8000 \
    //!     cargo test -p monad-archive scylla_alternator -- --ignored --nocapture
    //!
    //! Spin one up first:
    //!   docker run --name scylla-test -p 8000:8000 -d scylladb/scylla \
    //!     --alternator-port=8000 --alternator-write-isolation=always --smp 1
    use std::str::FromStr;

    use super::*;
    use crate::cli::ScyllaCliArgs;

    fn alternator_endpoint() -> String {
        std::env::var("SCYLLA_ALTERNATOR_ENDPOINT")
            .unwrap_or_else(|_| "http://localhost:8000".to_string())
    }

    /// Builds a `DynamoDBArchive` whose dynamo client targets the Alternator
    /// endpoint, exactly as the `scylla` backend does, and auto-creates the
    /// backing table via the production `ensure_table` path.
    async fn archive(table: &str) -> Result<DynamoDBArchive> {
        // `ScyllaCliArgs::parse` takes the args *after* the `scylla` type token.
        let arg_string = format!("{ep} ns", ep = alternator_endpoint());
        let config = ScyllaCliArgs::parse(&arg_string)?.config().await?;
        let store = DynamoDBArchive::new(table.to_string(), &config, 50, Metrics::none());
        store.ensure_table().await?;
        Ok(store)
    }

    #[tokio::test]
    #[ignore = "requires a running ScyllaDB Alternator endpoint"]
    async fn scylla_alternator_put_get_roundtrip() {
        let store = archive("archive_index_roundtrip").await.unwrap();

        store
            .put("deadbeef", vec![1, 2, 3, 4], WritePolicy::AllowOverwrite)
            .await
            .unwrap();

        let got = store.get("deadbeef").await.unwrap();
        assert_eq!(got, Some(Bytes::from(vec![1, 2, 3, 4])));

        assert!(store.exists("deadbeef").await.unwrap());
        assert!(!store.exists("nonexistent").await.unwrap());

        let missing = store.get("nonexistent").await.unwrap();
        assert_eq!(missing, None);
    }

    #[tokio::test]
    #[ignore = "requires a running ScyllaDB Alternator endpoint"]
    async fn scylla_alternator_bulk_put_get() {
        let store = archive("archive_index_bulk").await.unwrap();

        let kvs: Vec<(String, Vec<u8>)> = (0..120)
            .map(|i| (format!("{i:064x}"), vec![i as u8; 8]))
            .collect();

        store
            .bulk_put(kvs.clone(), WritePolicy::AllowOverwrite)
            .await
            .unwrap();

        let keys: Vec<String> = kvs.iter().map(|(k, _)| k.clone()).collect();
        let got = store.bulk_get(&keys).await.unwrap();

        assert_eq!(got.len(), kvs.len());
        for (k, v) in &kvs {
            assert_eq!(got.get(k), Some(&Bytes::from(v.clone())));
        }
    }

    /// End-to-end exercise of the `scylla` backend used as a block-data store
    /// (analogous to the mongo backend): build via the public `ArchiveArgs`
    /// path, write block/receipts/traces + the latest marker, then read back.
    #[tokio::test]
    #[ignore = "requires a running ScyllaDB Alternator endpoint"]
    async fn scylla_backend_block_store_roundtrip() {
        use crate::{
            cli::ArchiveArgs,
            test_utils::{mock_block, mock_rx, mock_tx},
        };

        let locator = format!("scylla {} e2eblocks", alternator_endpoint());
        let sink = ArchiveArgs::from_str(&locator).unwrap();
        let archive = sink
            .build_block_data_archive(&Metrics::none())
            .await
            .unwrap();

        let tx = mock_tx(7);
        let block = mock_block(42, vec![tx]);
        let receipts = vec![mock_rx(10, 21000)];
        let traces = vec![vec![9, 9, 9]];

        archive
            .archive_block(block.clone(), WritePolicy::AllowOverwrite)
            .await
            .unwrap();
        archive
            .archive_receipts(receipts.clone(), 42, WritePolicy::AllowOverwrite)
            .await
            .unwrap();
        archive
            .archive_traces(traces.clone(), 42, WritePolicy::AllowOverwrite)
            .await
            .unwrap();
        archive
            .update_latest(42, LatestKind::Uploaded)
            .await
            .unwrap();

        let got_block = archive.get_block_by_number(42).await.unwrap();
        assert_eq!(got_block.header.number, 42);
        assert_eq!(got_block.header.hash_slow(), block.header.hash_slow());
        assert_eq!(archive.get_block_receipts(42).await.unwrap(), receipts);
        assert_eq!(archive.get_block_traces(42).await.unwrap(), traces);
        assert_eq!(
            archive.get_latest(LatestKind::Uploaded).await.unwrap(),
            Some(42)
        );

        // Block found by hash too (separate hash-index key in the same table).
        let by_hash = archive
            .get_block_by_hash(&block.header.hash_slow())
            .await
            .unwrap();
        assert_eq!(by_hash.header.number, 42);
    }

    /// End-to-end exercise of the `scylla` backend used as an archive sink:
    /// index a block (index table + block table on Scylla) and read the tx
    /// back through the index reader.
    #[tokio::test]
    #[ignore = "requires a running ScyllaDB Alternator endpoint"]
    async fn scylla_backend_index_roundtrip() {
        use crate::{
            cli::ArchiveArgs,
            test_utils::{mock_block, mock_rx, mock_tx},
        };

        let locator = format!("scylla {} e2eindex", alternator_endpoint());
        let sink = ArchiveArgs::from_str(&locator).unwrap();
        // Large inline threshold keeps everything InlineV1 in the index table.
        let indexer = sink
            .build_index_archive(&Metrics::none(), 1 << 20)
            .await
            .unwrap();

        let tx = mock_tx(3);
        let block = mock_block(100, vec![tx.clone()]);
        let receipts = vec![mock_rx(10, 21000)];
        let traces = vec![vec![1, 2, 3]];

        indexer
            .index_block(block, traces.clone(), receipts, None)
            .await
            .unwrap();

        let indexed = indexer
            .get_tx_indexed_data(tx.tx.tx_hash())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(indexed.tx.sender, tx.sender);
        assert_eq!(indexed.trace, traces[0]);
        assert_eq!(indexed.header_subset.block_number, 100);
        assert_eq!(indexed.header_subset.gas_used, 21000);
    }
}
