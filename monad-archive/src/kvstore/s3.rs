use core::str;

use aws_config::SdkConfig;
use aws_sdk_s3::{error::SdkError, primitives::ByteStream, Client};
use bytes::Bytes;
use eyre::{Context, Result};
use tracing::trace;

use super::{kvstore_get_metrics, KVStoreType, MetricsResultExt};
use crate::{metrics::Metrics, prelude::*};

#[derive(Clone)]
pub struct S3Bucket {
    client: Client,
    pub bucket: String,
    metrics: Metrics,
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

        let req = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(ByteStream::from(data.clone()))
            .request_payer(aws_sdk_s3::types::RequestPayer::Requester);

        let start = Instant::now();
        req.send()
            .await
            .write_put_metrics(start.elapsed(), KVStoreType::AwsS3, &self.metrics)
            .wrap_err_with(|| format!("Failed to upload, retries exhausted. Key: {}", key))?;

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
