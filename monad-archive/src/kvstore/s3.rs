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
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        RwLock,
    },
    time::SystemTime,
};

use aws_config::SdkConfig;
use aws_sdk_s3::{
    error::{ProvideErrorMetadata, SdkError},
    operation::create_bucket::CreateBucketError,
    primitives::ByteStream,
    Client,
};
use bytes::Bytes;
use eyre::{Context, Result};
use tracing::trace;

use super::{kvstore_get_metrics, kvstore_put_metrics, KVStoreType, PutResult, WritePolicy};
use crate::{metrics::Metrics, prelude::*};

const CLIENT_RECREATE_AFTER_SECS: u64 = 60;

#[derive(Clone)]
pub struct Bucket {
    inner: Arc<BucketInner>,
}

struct BucketInner {
    client: RwLock<Client>,
    bucket: String,
    metrics: Metrics,
    sdk_config: Option<SdkConfig>,
    last_success: AtomicU64,
}

fn epoch_secs_now() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

impl Bucket {
    pub fn new(bucket: String, sdk_config: &SdkConfig, metrics: Metrics) -> Self {
        Bucket {
            inner: Arc::new(BucketInner {
                client: RwLock::new(Client::new(sdk_config)),
                bucket,
                metrics,
                sdk_config: Some(sdk_config.clone()),
                last_success: AtomicU64::new(epoch_secs_now()),
            }),
        }
    }

    /// Build a `Bucket` from a pre-constructed `Client`.  Client recreation
    /// on sustained failures is disabled since no `SdkConfig` is available.
    pub fn from_client(bucket: String, client: Client, metrics: Metrics) -> Self {
        Bucket {
            inner: Arc::new(BucketInner {
                client: RwLock::new(client),
                bucket,
                metrics,
                sdk_config: None,
                last_success: AtomicU64::new(epoch_secs_now()),
            }),
        }
    }

    /// Clone the current S3 client (cheap -- all internal Arcs).
    /// Recovers from a poisoned lock rather than panicking.
    fn client(&self) -> Client {
        self.inner
            .client
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// Record the outcome of an S3 operation: update the recreation timer
    /// and, on failure, potentially recreate the client.
    fn on_result(&self, is_success: bool) {
        if is_success {
            self.inner
                .last_success
                .store(epoch_secs_now(), Ordering::Relaxed);
        } else {
            self.maybe_recreate_client();
        }
    }

    /// Combined: record OTel get metrics + update client health state.
    fn record_get_metrics(&self, duration: Duration, is_success: bool) {
        kvstore_get_metrics(
            duration,
            is_success,
            KVStoreType::AwsS3,
            &self.inner.metrics,
        );
        self.on_result(is_success);
    }

    /// Combined: record OTel put metrics + update client health state.
    fn record_put_metrics(&self, duration: Duration, is_success: bool) {
        kvstore_put_metrics(
            duration,
            is_success,
            KVStoreType::AwsS3,
            &self.inner.metrics,
        );
        self.on_result(is_success);
    }

    /// If no successful operation has occurred within the recreation window and
    /// we have an `SdkConfig`, rebuild the S3 client.  Uses `try_write` so
    /// only one thread performs the recreation at a time; others simply skip.
    fn maybe_recreate_client(&self) {
        let Some(sdk_config) = self.inner.sdk_config.as_ref() else {
            return;
        };
        let now = epoch_secs_now();
        let last = self.inner.last_success.load(Ordering::Relaxed);
        if now.saturating_sub(last) < CLIENT_RECREATE_AFTER_SECS {
            return;
        }
        // Non-blocking: if another thread already holds the write lock, skip.
        let Ok(mut guard) = self.inner.client.try_write() else {
            return;
        };
        // Double-check after acquiring lock.
        let last = self.inner.last_success.load(Ordering::Relaxed);
        if now.saturating_sub(last) < CLIENT_RECREATE_AFTER_SECS {
            return;
        }
        warn!(
            "Recreating S3 client after {} seconds without success",
            now.saturating_sub(last)
        );
        *guard = Client::new(sdk_config);
        // Reset timer so we don't immediately recreate again.
        self.inner.last_success.store(now, Ordering::Relaxed);
    }

    pub async fn create_bucket(&self) -> Result<()> {
        let client = self.client();
        match client
            .create_bucket()
            .bucket(&self.inner.bucket)
            .send()
            .await
        {
            Ok(_) => {
                self.on_result(true);
                Ok(())
            }
            Err(SdkError::ServiceError(service_err)) => match service_err.err() {
                CreateBucketError::BucketAlreadyExists(_)
                | CreateBucketError::BucketAlreadyOwnedByYou(_) => {
                    self.on_result(true);
                    Ok(())
                }
                _ => {
                    self.on_result(false);
                    Err(SdkError::ServiceError(service_err)).wrap_err_with(|| {
                        format!("Failed to create bucket {}", self.inner.bucket)
                    })?
                }
            },
            Err(e) => {
                self.on_result(false);
                Err(e).wrap_err_with(|| format!("Failed to create bucket {}", self.inner.bucket))
            }
        }
    }
}

impl KVReader for Bucket {
    async fn get(&self, key: &str) -> Result<Option<Bytes>> {
        trace!(key, "S3 get");
        let client = self.client();
        let req = client
            .get_object()
            .bucket(&self.inner.bucket)
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
                    self.record_get_metrics(duration, true);
                    return Ok(None);
                }
                _ => {
                    self.record_get_metrics(duration, false);
                    Err(SdkError::ServiceError(service_err))
                        .wrap_err_with(|| format!("Failed to read key from s3 {key}"))?
                }
            },
            Err(e) => {
                self.record_get_metrics(duration, false);
                return Err(e).wrap_err_with(|| format!("Failed to read key from s3 {key}"));
            }
        };

        let data = resp.body.collect().await;
        self.record_get_metrics(duration, data.is_ok());
        let data = data.wrap_err("Unable to collect response data")?;

        let bytes = data.into_bytes();
        if bytes.is_empty() {
            Ok(None)
        } else {
            Ok(Some(bytes))
        }
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        trace!(key, "S3 exists check");
        let client = self.client();
        let start = Instant::now();
        let resp = client
            .head_object()
            .bucket(&self.inner.bucket)
            .key(key)
            .request_payer(aws_sdk_s3::types::RequestPayer::Requester)
            .send()
            .await;
        let duration = start.elapsed();

        match resp {
            Ok(_) => {
                self.record_get_metrics(duration, true);
                Ok(true)
            }
            Err(SdkError::ServiceError(service_err)) if service_err.err().is_not_found() => {
                self.record_get_metrics(duration, true);
                Ok(false)
            }
            Err(e) => {
                self.record_get_metrics(duration, false);
                Err(e).wrap_err_with(|| format!("S3 exists check failed for key {key}"))
            }
        }
    }

    async fn scan_prefix(&self, prefix: &str) -> Result<Vec<String>> {
        self.scan_prefix_with_max_keys(prefix, usize::MAX).await
    }

    async fn scan_prefix_with_max_keys(
        &self,
        prefix: &str,
        max_keys: usize,
    ) -> Result<Vec<String>> {
        let mut objects = Vec::new();
        let mut continuation_token = None;

        loop {
            let client = self.client();
            let token = continuation_token.as_ref();
            let mut request = client
                .list_objects_v2()
                .bucket(&self.inner.bucket)
                .prefix(prefix)
                .request_payer(aws_sdk_s3::types::RequestPayer::Requester);

            if let Some(token) = token {
                request = request.continuation_token(token);
            }
            let response = match request.send().await {
                Ok(resp) => {
                    self.on_result(true);
                    resp
                }
                Err(e) => {
                    self.on_result(false);
                    return Err(e).wrap_err("Failed to list objects");
                }
            };

            if let Some(contents) = response.contents {
                let keys = contents.into_iter().filter_map(|obj| obj.key);
                objects.extend(keys);
            }

            if objects.len() >= max_keys {
                break;
            }

            if !response.is_truncated.unwrap_or(false) {
                break;
            }
            continuation_token = response.next_continuation_token;
        }

        objects.truncate(max_keys);

        Ok(objects)
    }
}

impl KVStore for Bucket {
    async fn put(
        &self,
        key: impl AsRef<str>,
        data: Vec<u8>,
        policy: WritePolicy,
    ) -> Result<PutResult> {
        let key = key.as_ref();
        let client = self.client();

        let mut req = client
            .put_object()
            .bucket(&self.inner.bucket)
            .key(key)
            .body(ByteStream::from(data.clone()))
            .request_payer(aws_sdk_s3::types::RequestPayer::Requester);

        if policy == WritePolicy::NoClobber {
            req = req.if_none_match("*");
        }

        let start = Instant::now();
        let result = req.send().await;

        match result {
            Ok(_) => {
                self.record_put_metrics(start.elapsed(), true);
                Ok(PutResult::Written)
            }
            Err(SdkError::ServiceError(service_err))
                if policy == WritePolicy::NoClobber
                    && service_err.err().code() == Some("PreconditionFailed") =>
            {
                self.record_put_metrics(start.elapsed(), true);
                warn!(key, "S3 put skipped: key already exists (NoClobber policy)");
                Ok(PutResult::Skipped)
            }
            Err(e) => {
                self.record_put_metrics(start.elapsed(), false);
                Err(e).wrap_err_with(|| format!("S3 upload failed. Key: {}", key))
            }
        }
    }

    fn bucket_name(&self) -> &str {
        &self.inner.bucket
    }

    async fn delete(&self, key: impl AsRef<str>) -> Result<()> {
        let key = key.as_ref();
        let client = self.client();

        match client
            .delete_object()
            .bucket(&self.inner.bucket)
            .key(key)
            .request_payer(aws_sdk_s3::types::RequestPayer::Requester)
            .send()
            .await
        {
            Ok(_) => {
                self.on_result(true);
                Ok(())
            }
            Err(e) => {
                self.on_result(false);
                Err(e).wrap_err_with(|| format!("S3 delete failed. Key: {}", key))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{cli::AwsCliArgs, test_utils::TestMinioContainer};

    #[tokio::test]
    #[ignore]
    async fn test_s3_bucket() {
        let minio = TestMinioContainer::new().await.unwrap();

        // connect to minio
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

        bucket.create_bucket().await.unwrap();

        bucket
            .put("test-key", vec![1, 2, 3], WritePolicy::AllowOverwrite)
            .await
            .unwrap();
        let value = bucket.get("test-key").await.unwrap().unwrap();
        assert_eq!(value, Bytes::from(vec![1, 2, 3]));
    }
}
