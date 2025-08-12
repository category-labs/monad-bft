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

use core::str;

use aws_config::SdkConfig;
use aws_sdk_s3::{error::SdkError, primitives::ByteStream, Client};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use bytes::Bytes;
use eyre::{Context, Result};
use sha2::Digest as Sha2Digest;
use tracing::trace;

use super::{metrics::MetricsResultExt, KVStoreType};
use crate::{kvstore::metrics::kvstore_get_metrics, metrics::Metrics, prelude::*};

pub trait S3KV: KVStore + KVReader {
    async fn head(&self, key: &str) -> Result<Option<HeadObj>>;
}

#[derive(Clone)]
pub struct S3Bucket {
    client: Client,
    pub bucket: String,
    metrics: Metrics,
}

#[derive(Debug, Clone)]

pub struct HeadObj {
    pub etag: String,
    pub checksum: Option<String>,
    pub content_length: u64,
    /// Seconds since epoch
    pub last_modified: Option<u64>,
    pub content_type: Option<String>,
    pub metadata: HashMap<String, String>,
}

impl S3Bucket {
    pub fn new(bucket: String, sdk_config: &SdkConfig, metrics: Metrics) -> Self {
        S3Bucket::from_client(bucket, Client::new(sdk_config), metrics)
    }

    pub fn from_client(bucket: String, client: Client, metrics: Metrics) -> Self {
        S3Bucket {
            bucket,
            client,
            metrics,
        }
    }

    pub async fn head(&self, key: &str) -> Result<Option<HeadObj>> {
        let req = self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(key)
            .checksum_mode(aws_sdk_s3::types::ChecksumMode::Enabled)
            .request_payer(aws_sdk_s3::types::RequestPayer::Requester);

        let start = Instant::now();
        let resp = req.send().await;
        let duration = start.elapsed();
        trace!(key, "S3 head, got response");

        let resp = match resp {
            Ok(resp) => resp,
            Err(SdkError::ServiceError(service_err)) => match service_err.err() {
                aws_sdk_s3::operation::head_object::HeadObjectError::NotFound(_) => {
                    kvstore_get_metrics(duration, true, KVStoreType::AwsS3, &self.metrics);
                    return Ok(None);
                }
                _ => Err(SdkError::ServiceError(service_err)).wrap_err_with(|| {
                    kvstore_get_metrics(duration, false, KVStoreType::AwsS3, &self.metrics);
                    format!("Failed to read key from s3 {key}")
                })?,
            },
            _ => resp.wrap_err_with(|| {
                kvstore_get_metrics(duration, false, KVStoreType::AwsS3, &self.metrics);
                format!("Failed to read key from s3 {key}")
            })?,
        };

        let data = HeadObj {
            etag: resp.e_tag().unwrap_or_default().to_string(),
            checksum: resp.checksum_sha256.clone(),
            content_length: resp.content_length.unwrap_or_default() as u64,
            last_modified: resp.last_modified.map(|dt| dt.secs() as u64),
            content_type: resp.content_type,
            metadata: resp.metadata.unwrap_or_default(),
        };

        Ok(Some(data))
    }
}

impl KVReader for S3Bucket {
    async fn get(&self, key: &str) -> Result<Option<Bytes>> {
        trace!(key, "S3 get");
        let req = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .request_payer(aws_sdk_s3::types::RequestPayer::Requester);

        let start = Instant::now();
        let resp = req.send().await;
        let duration = start.elapsed();
        trace!(key, "S3 get, got response");

        let resp = match resp {
            Ok(resp) => resp,
            Err(SdkError::ServiceError(service_err)) => match service_err.err() {
                aws_sdk_s3::operation::get_object::GetObjectError::NoSuchKey(_) => {
                    kvstore_get_metrics(duration, true, KVStoreType::AwsS3, &self.metrics);
                    return Ok(None);
                }
                _ => Err(SdkError::ServiceError(service_err)).wrap_err_with(|| {
                    kvstore_get_metrics(duration, false, KVStoreType::AwsS3, &self.metrics);
                    format!("Failed to read key from s3 {key}")
                })?,
            },
            _ => resp.wrap_err_with(|| {
                kvstore_get_metrics(duration, false, KVStoreType::AwsS3, &self.metrics);
                format!("Failed to read key from s3 {key}")
            })?,
        };

        let data = resp
            .body
            .collect()
            .await
            .write_get_metrics(duration, KVStoreType::AwsS3, &self.metrics)
            .wrap_err_with(|| "Unable to collect response data")?;

        let bytes = data.into_bytes();
        if bytes.is_empty() {
            Ok(None)
        } else {
            Ok(Some(bytes))
        }
    }
}

impl KVStore for S3Bucket {
    // Upload rlp-encoded bytes with retry
    async fn put(&self, key: impl AsRef<str>, data: Vec<u8>) -> Result<()> {
        let key = key.as_ref();

        let sha256 = sha2::Sha256::digest(&data);
        let sha256_b64 = STANDARD.encode(sha256);

        let req = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(ByteStream::from(data.clone()))
            .checksum_algorithm(aws_sdk_s3::types::ChecksumAlgorithm::Sha256)
            .checksum_sha256(sha256_b64.clone())
            .request_payer(aws_sdk_s3::types::RequestPayer::Requester);

        let start = Instant::now();
        let x = req
            .send()
            .await
            .write_put_metrics(start.elapsed(), KVStoreType::AwsS3, &self.metrics)
            .wrap_err_with(|| format!("Failed to upload, retries exhausted. Key: {}", key))?;

        let s3_checksum_sha256 = x
            .checksum_sha256()
            .wrap_err_with(|| format!("Failed to put object to s3 {key}"))?;
        if s3_checksum_sha256 != sha256_b64 {
            return Err(eyre::eyre!(
                "SHA256 mismatch: {s3_checksum_sha256} != {sha256_b64}"
            ));
        }

        Ok(())
    }

    fn bucket_name(&self) -> &str {
        &self.bucket
    }

    async fn scan_prefix(&self, prefix: &str) -> Result<Vec<String>> {
        let mut objects = Vec::new();
        let mut continuation_token = None;

        loop {
            let token = continuation_token.as_ref();
            let mut request = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(prefix)
                .request_payer(aws_sdk_s3::types::RequestPayer::Requester);

            if let Some(token) = token {
                request = request.continuation_token(token);
            }
            let response = request.send().await.wrap_err("Failed to list objects")?;

            // Process objects
            if let Some(contents) = response.contents {
                let keys = contents.into_iter().filter_map(|obj| obj.key);
                objects.extend(keys);
            }

            // Check if we need to continue
            if !response.is_truncated.unwrap_or(false) {
                break;
            }
            continuation_token = response.next_continuation_token;
        }

        Ok(objects)
    }

    async fn delete(&self, key: impl AsRef<str>) -> Result<()> {
        let key = key.as_ref();

        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(key)
            .request_payer(aws_sdk_s3::types::RequestPayer::Requester)
            .send()
            .await
            .wrap_err_with(|| format!("Failed to delete, retries exhausted. Key: {}", key))?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use std::sync::OnceLock;

    use aws_config::BehaviorVersion;
    use rand::Rng;

    use super::*;
    use crate::metrics::Metrics;

    fn test_bucket() -> String {
        std::env::var("TEST_S3_BUCKET").unwrap_or_else(|_| "jhow-local".to_owned())
    }

    // Helper: create a unique key under optional TEST_S3_PREFIX.
    fn unique_key() -> String {
        let prefix = std::env::var("TEST_S3_PREFIX").unwrap_or_else(|_| "checksum-tests/".into());
        format!("{}{}", prefix, rand::thread_rng().gen_range(0..1000000))
    }

    // store client in a global variable
    static CLIENT: OnceLock<S3Bucket> = OnceLock::new();

    async fn s3_client() -> S3Bucket {
        match CLIENT.get() {
            Some(client) => client,
            None => {
                let config = aws_config::defaults(BehaviorVersion::latest()).load().await;
                let bucket = test_bucket();
                CLIENT.get_or_init(|| {
                    S3Bucket::from_client(bucket, Client::new(&config), Metrics::none())
                })
            }
        }
        .clone()
    }

    #[tokio::test]
    #[ignore]
    async fn put_object_with_correct_checksum_sha256_succeeds() {
        let client = s3_client().await;

        let payload = b"hello sha256";
        let key = unique_key();

        client
            .put(&key, payload.to_vec())
            .await
            .expect("put should succeed");

        let head = client
            .head(&key)
            .await
            .expect("head should succeed")
            .expect("head should return some value");
        let checksum_sha256 = head.checksum.expect("checksum should be present");

        let hex_sha256 = STANDARD.encode(sha2::Sha256::digest(payload));
        assert_eq!(checksum_sha256, hex_sha256);
    }

    #[tokio::test]
    #[ignore]
    async fn put_object_with_wrong_checksum_sha256_fails_with_baddigest() {
        let client = s3_client().await;
        let bucket = &client.bucket;

        let payload = b"integrity check";
        let key = unique_key();

        // Intentionally wrong SHA256 - compute over different bytes.
        let digest = sha2::Sha256::digest(b"different-bytes");
        let wrong_sha256_b64 = STANDARD.encode(digest);

        // Build and send explicitly here so we can inspect the error.
        let res = client
            .client
            .put_object()
            .bucket(bucket)
            .key(&key)
            .body(ByteStream::from_static(payload))
            .checksum_sha256(wrong_sha256_b64)
            .send()
            .await;

        match res {
            Ok(_) => panic!("expected failure with BadDigest"),
            Err(SdkError::ServiceError(service_err)) => {
                // Error metadata contains service code like "BadDigest".
                let code = service_err
                    .err()
                    .meta()
                    .code()
                    .unwrap_or_default()
                    .to_string();
                assert_eq!(code, "BadDigest", "unexpected error code: {code}");
            }
            Err(other) => panic!("unexpected error variant: {other:?}"),
        }
    }
}
