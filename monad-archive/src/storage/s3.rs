use core::str;
use std::{collections::HashMap, path::Path};

use aws_config::{meta::region::RegionProviderChain, SdkConfig};
use aws_sdk_s3::{
    config::{BehaviorVersion, Region},
    primitives::ByteStream,
    Client,
};
use bytes::Bytes;
use eyre::{Context, ContextCompat, Result};
use tokio::time::Duration;
use tokio_retry::{
    strategy::{jitter, ExponentialBackoff},
    Retry,
};
use tracing::info;

use super::retry_strategy;
use crate::{metrics::Metrics, BlobReader, BlobStore, TxIndexedData};

const AWS_S3_ERRORS: &str = "aws_s3_errors";
const AWS_S3_READS: &str = "aws_s3_reads";
const AWS_S3_WRITES: &str = "aws_s3_writes";

#[derive(Clone)]
pub struct S3Bucket {
    pub client: Client,
    pub bucket: String,
    pub metrics: Metrics,
}

pub async fn get_aws_config(region: Option<String>) -> SdkConfig {
    let region_provider = RegionProviderChain::default_provider().or_else(
        region
            .map(Region::new)
            .unwrap_or_else(|| Region::new("us-east-2")),
    );

    info!(
        "Running in region: {}",
        region_provider
            .region()
            .await
            .map(|r| r.to_string())
            .unwrap_or("No region found".into())
    );

    aws_config::defaults(BehaviorVersion::latest())
        .region(region_provider)
        .load()
        .await
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

impl BlobReader for S3Bucket {
    async fn read(&self, key: &str) -> Result<Bytes> {
        let resp = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .wrap_err_with(|| {
                self.metrics.inc_counter(AWS_S3_ERRORS);
                format!("Failed to read key from s3 {key}")
            })?;

        let data = resp.body.collect().await.wrap_err_with(|| {
            self.metrics.inc_counter(AWS_S3_ERRORS);
            "Unable to collect response data"
        })?;

        self.metrics.counter(AWS_S3_READS, 1);
        Ok(data.into_bytes())
    }
}

impl BlobStore for S3Bucket {
    // Upload rlp-encoded bytes with retry
    async fn upload(&self, key: &str, data: Vec<u8>) -> Result<()> {
        let retry_strategy = ExponentialBackoff::from_millis(10)
            .max_delay(Duration::from_secs(1))
            .map(jitter);

        Retry::spawn(retry_strategy, || {
            let client = &self.client;
            let bucket = &self.bucket;
            let key = key.to_string();
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
        .wrap_err_with(|| format!("Failed to upload {}. Retrying...", key))?;

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
            let response = Retry::spawn(retry_strategy(), || {
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
                objects.extend(contents.into_iter().filter_map(|obj| obj.key));
            }

            // Check if we need to continue
            if !response.is_truncated.unwrap_or(false) {
                break;
            }
            continuation_token = response.next_continuation_token;
        }

        Ok(objects)
    }
}
