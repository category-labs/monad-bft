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
use aws_sdk_s3::{
    error::{ProvideErrorMetadata, SdkError},
    operation::create_bucket::CreateBucketError,
    primitives::ByteStream,
    Client,
};
use bytes::Bytes;
use eyre::{Context, Result};
use tracing::trace;

use super::{
    kvstore_get_metrics, kvstore_put_metrics, KVStoreType, MetricsResultExt, PutResult, WritePolicy,
};
use crate::{metrics::Metrics, prelude::*};

#[derive(Clone)]
pub struct Bucket {
    pub(crate) client: Client,
    pub bucket: String,
    metrics: Metrics,
}

impl Bucket {
    pub fn new(bucket: String, sdk_config: &SdkConfig, metrics: Metrics) -> Self {
        Bucket::from_client(bucket, Client::new(sdk_config), metrics)
    }

    pub fn from_client(bucket: String, client: Client, metrics: Metrics) -> Self {
        Bucket {
            bucket,
            client,
            metrics,
        }
    }

    pub async fn create_bucket(&self) -> Result<()> {
        match self
            .client
            .create_bucket()
            .bucket(&self.bucket)
            .send()
            .await
        {
            Ok(_) => Ok(()),
            Err(SdkError::ServiceError(service_err)) => match service_err.err() {
                CreateBucketError::BucketAlreadyExists(_)
                | CreateBucketError::BucketAlreadyOwnedByYou(_) => Ok(()),
                _ => {
                    Err(SdkError::ServiceError(service_err)).wrap_err("Failed to create bucket")?
                }
            },
            Err(e) => Err(e.into()),
        }
    }
}

impl KVReader for Bucket {
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

impl KVStore for Bucket {
    // Upload rlp-encoded bytes with retry
    async fn put(
        &self,
        key: impl AsRef<str>,
        data: Vec<u8>,
        policy: WritePolicy,
    ) -> Result<PutResult> {
        let key = key.as_ref();

        let mut req = self
            .client
            .put_object()
            .bucket(&self.bucket)
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
                kvstore_put_metrics(start.elapsed(), true, KVStoreType::AwsS3, &self.metrics);
                Ok(PutResult::Written)
            }
            Err(SdkError::ServiceError(service_err))
                if policy == WritePolicy::NoClobber
                    && service_err.err().code() == Some("PreconditionFailed") =>
            {
                kvstore_put_metrics(start.elapsed(), true, KVStoreType::AwsS3, &self.metrics);
                warn!(key, "S3 put skipped: key already exists (NoClobber policy)");
                Ok(PutResult::Skipped)
            }
            Err(e) => {
                kvstore_put_metrics(start.elapsed(), false, KVStoreType::AwsS3, &self.metrics);
                Err(e)
                    .wrap_err_with(|| format!("Failed to upload, retries exhausted. Key: {}", key))
            }
        }
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
        let sdk_config = AwsCliArgs::parse(&arg_string).unwrap().config().await;

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
