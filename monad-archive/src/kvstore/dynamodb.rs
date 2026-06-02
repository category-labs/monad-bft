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
    primitives::Blob,
    types::{AttributeValue, KeysAndAttributes, PutRequest, WriteRequest},
    Client,
};
use bytes::Bytes;
use eyre::{bail, Context, Result};
use futures::future::try_join_all;
use sha2::{Digest, Sha256};
use tokio::sync::Semaphore;
use tracing::error;

use super::{BulkPutResult, KVStoreType, MetricsResultExt, ObjectMeta, PutResult, WritePolicy};
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

    async fn exists(&self, key: &str) -> Result<bool> {
        let start = Instant::now();
        let result = self
            .client
            .get_item()
            .table_name(&self.table)
            .key("tx_hash", AttributeValue::S(key.to_owned()))
            .projection_expression("tx_hash")
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

    async fn metadata(&self, key: &str) -> Result<Option<ObjectMeta>> {
        let start = Instant::now();

        // Cheap path: project only `checksum`, never `data` -- fetching the body
        // would defeat the cheap path. Strip the `0x` prefix to match the key
        // form `batch_get` (and thus `get`) stores and queries under.
        let stored_key = key.trim_start_matches("0x");
        let result = self
            .client
            .get_item()
            .table_name(&self.table)
            .key("tx_hash", AttributeValue::S(stored_key.to_owned()))
            .projection_expression("tx_hash, checksum")
            .send()
            .await
            .wrap_err("DynamoDB metadata get_item failed")
            .write_get_metrics_on_err(start.elapsed(), KVStoreType::AwsDynamoDB, &self.metrics)?;

        let Some(item) = result.item else {
            return Ok(None).write_get_metrics(
                start.elapsed(),
                KVStoreType::AwsDynamoDB,
                &self.metrics,
            );
        };

        if let Some(AttributeValue::B(blob)) = item.get("checksum") {
            let checksum_sha256: [u8; 32] = blob.as_ref().try_into().wrap_err_with(|| {
                format!(
                    "DynamoDB checksum for key {key} had unexpected length {}",
                    blob.as_ref().len()
                )
            })?;
            return Ok(Some(ObjectMeta { checksum_sha256 })).write_get_metrics(
                start.elapsed(),
                KVStoreType::AwsDynamoDB,
                &self.metrics,
            );
        }

        // Legacy fallback: item exists but has no stored checksum. Fetch the
        // full payload and hash it.
        let Some(bytes) = self
            .get(key)
            .await
            .wrap_err("DynamoDB metadata legacy fallback fetch failed")?
        else {
            // Raced with a delete between the two reads.
            return Ok(None);
        };
        let checksum_sha256: [u8; 32] = Sha256::digest(&bytes).into();
        Ok(Some(ObjectMeta { checksum_sha256 }))
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
    ) -> Result<BulkPutResult> {
        // Note: WritePolicy is ignored for DynamoDB - always overwrites
        let requests = kvs
            .into_iter()
            .filter_map(|(key, data)| {
                let checksum: [u8; 32] = Sha256::digest(&data).into();
                let attribute_map: HashMap<String, AttributeValue> = HashMap::from_iter([
                    ("tx_hash".to_owned(), AttributeValue::S(key)),
                    ("data".to_owned(), AttributeValue::B(data.into())),
                    (
                        "checksum".to_owned(),
                        AttributeValue::B(Blob::new(checksum)),
                    ),
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
        // WritePolicy is ignored, so nothing is ever skipped.
        Ok(BulkPutResult::Written)
    }

    async fn put(
        &self,
        key: impl AsRef<str>,
        data: Vec<u8>,
        _policy: WritePolicy,
    ) -> Result<PutResult> {
        // Note: WritePolicy is ignored for DynamoDB - always overwrites
        let checksum_sha256: [u8; 32] = Sha256::digest(&data).into();
        let put_request = PutRequest::builder()
            .item(key.as_ref(), AttributeValue::B(data.into()))
            .item("checksum", AttributeValue::B(Blob::new(checksum_sha256)))
            .build()
            .wrap_err_with(|| format!("Failed to build put request, key: {}", key.as_ref()))?;
        let request = WriteRequest::builder().put_request(put_request).build();

        let start = Instant::now();
        self.upload_to_db(vec![request]).await.write_put_metrics(
            start.elapsed(),
            KVStoreType::AwsDynamoDB,
            &self.metrics,
        )?;
        Ok(PutResult::Written { checksum_sha256 })
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{cli::AwsCliArgs, test_utils::TestMinioContainer};

    #[tokio::test]
    #[ignore]
    async fn test_dynamodb_put_returns_sha256_checksum() {
        use sha2::{Digest, Sha256};

        let minio = TestMinioContainer::new().await.unwrap();
        let arg_string = format!(
            "aws test-bucket  --endpoint http://127.0.0.1:{port} --access-key-id minioadmin --secret-access-key minioadmin",
            port = minio.port
        );
        let sdk_config = AwsCliArgs::parse(&arg_string)
            .unwrap()
            .config()
            .await
            .unwrap();

        let bucket = Bucket::new("test-bucket".to_string(), &sdk_config, Metrics::none());
        let archive = DynamoDBArchive::new(
            bucket,
            "test-table".to_string(),
            &sdk_config,
            1,
            Metrics::none(),
        );

        let input = b"some payload bytes".to_vec();
        let expected: [u8; 32] = Sha256::digest(&input).into();

        let result = archive
            .put("checksum-key", input.clone(), WritePolicy::AllowOverwrite)
            .await
            .unwrap();

        let PutResult::Written { checksum_sha256 } = result else {
            panic!("expected PutResult::Written, got {result:?}");
        };
        assert_eq!(checksum_sha256, expected);
    }

    async fn minio_archive(minio: &TestMinioContainer) -> DynamoDBArchive {
        let arg_string = format!(
            "aws test-bucket  --endpoint http://127.0.0.1:{port} --access-key-id minioadmin --secret-access-key minioadmin",
            port = minio.port
        );
        let sdk_config = AwsCliArgs::parse(&arg_string)
            .unwrap()
            .config()
            .await
            .unwrap();

        let bucket = Bucket::new("test-bucket".to_string(), &sdk_config, Metrics::none());
        DynamoDBArchive::new(
            bucket,
            "test-table".to_string(),
            &sdk_config,
            1,
            Metrics::none(),
        )
    }

    #[tokio::test]
    #[ignore]
    async fn test_dynamodb_metadata_returns_some_with_checksum() {
        use sha2::{Digest, Sha256};

        let minio = TestMinioContainer::new().await.unwrap();
        let archive = minio_archive(&minio).await;

        let input = b"some payload bytes".to_vec();
        let expected: [u8; 32] = Sha256::digest(&input).into();

        archive
            .put("meta-key", input.clone(), WritePolicy::AllowOverwrite)
            .await
            .unwrap();

        let meta = archive
            .metadata("meta-key")
            .await
            .unwrap()
            .expect("expected Some metadata");
        assert_eq!(meta.checksum_sha256, expected);
    }

    #[tokio::test]
    #[ignore]
    async fn test_dynamodb_metadata_missing_key_returns_none() {
        let minio = TestMinioContainer::new().await.unwrap();
        let archive = minio_archive(&minio).await;

        let meta = archive.metadata("does-not-exist").await.unwrap();
        assert!(meta.is_none());
    }

    #[tokio::test]
    #[ignore]
    async fn test_dynamodb_metadata_legacy_fallback_hashes_body() {
        use sha2::{Digest, Sha256};

        let minio = TestMinioContainer::new().await.unwrap();
        let archive = minio_archive(&minio).await;

        let input = b"legacy payload without stored checksum".to_vec();
        let expected: [u8; 32] = Sha256::digest(&input).into();

        // Simulate a pre-checksum item: write tx_hash + data only, no checksum.
        let item: HashMap<String, AttributeValue> = HashMap::from_iter([
            (
                "tx_hash".to_owned(),
                AttributeValue::S("legacy-key".to_owned()),
            ),
            (
                "data".to_owned(),
                AttributeValue::B(Blob::new(input.clone())),
            ),
        ]);
        archive
            .client
            .put_item()
            .table_name(&archive.table)
            .set_item(Some(item))
            .send()
            .await
            .unwrap();

        // No stored checksum, so metadata falls back to reading + hashing the
        // body.
        let meta = archive
            .metadata("legacy-key")
            .await
            .unwrap()
            .expect("expected Some metadata");
        assert_eq!(meta.checksum_sha256, expected);
    }
}
