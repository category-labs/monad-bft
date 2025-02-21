use core::str;

use aws_config::SdkConfig;
use aws_sdk_s3::{error::SdkError, primitives::ByteStream, Client};
use bytes::Bytes;
use eyre::{Context, Result};
use tracing::trace;

use super::retry;
use crate::{metrics::Metrics, prelude::*};

const AWS_S3_ERRORS: &str = "aws_s3_errors";
const AWS_S3_READS: &str = "aws_s3_reads";
const AWS_S3_WRITES: &str = "aws_s3_writes";

#[derive(Clone)]
pub struct S3Bucket {
    pub client: Client,
    pub bucket: String,
    pub metrics: Metrics,
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
        let resp = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await;
        trace!(key, "S3 get, got response");

        let resp = match resp {
            Ok(resp) => resp,
            Err(SdkError::ServiceError(service_err)) => match service_err.err() {
                aws_sdk_s3::operation::get_object::GetObjectError::NoSuchKey(_) => return Ok(None),
                _ => Err(SdkError::ServiceError(service_err)).wrap_err_with(|| {
                    self.metrics.inc_counter(AWS_S3_ERRORS);
                    format!("Failed to read key from s3 {key}")
                })?,
            },
            _ => resp.wrap_err_with(|| {
                self.metrics.inc_counter(AWS_S3_ERRORS);
                format!("Failed to read key from s3 {key}")
            })?,
        };

        let data = resp.body.collect().await.wrap_err_with(|| {
            self.metrics.inc_counter(AWS_S3_ERRORS);
            "Unable to collect response data"
        })?;

        self.metrics.counter(AWS_S3_READS, 1);

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
        retry(|| {
            let client = &self.client;
            let bucket = &self.bucket;
            let key = key.as_ref().to_string();
            let body = ByteStream::from(data.clone());
            let metrics = &self.metrics;

            async move {
                client
                    .put_object()
                    .bucket(bucket)
                    .key(&key)
                    .body(body)
                    .send()
                    .await
                    .wrap_err_with(|| {
                        metrics.inc_counter(AWS_S3_ERRORS);
                        format!("Failed to upload {}. Retrying...", key)
                    })
            }
        })
        .await
        .map(|_| ())
        .wrap_err_with(|| format!("Failed to upload {}. Retrying...", key.as_ref()))?;

        self.metrics.counter(AWS_S3_WRITES, 1);
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
            let response = retry(|| {
                let mut request = self
                    .client
                    .list_objects_v2()
                    .bucket(&self.bucket)
                    .prefix(prefix);

                if let Some(token) = token {
                    request = request.continuation_token(token);
                }
                request.send()
            })
            .await?;

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
        retry(|| {
            let client = &self.client;
            let bucket = &self.bucket;
            let key = key.as_ref().to_string();
            let metrics = &self.metrics;

            async move {
                client
                    .delete_object()
                    .bucket(bucket)
                    .key(&key)
                    .send()
                    .await
                    .wrap_err_with(|| {
                        metrics.inc_counter(AWS_S3_ERRORS);
                        format!("Failed to delete {}. Retrying...", key)
                    })
            }
        })
        .await
        .map(|_| ())
        .wrap_err_with(|| format!("Failed to delete {}. Retrying...", key.as_ref()))?;

        Ok(())
    }
}
