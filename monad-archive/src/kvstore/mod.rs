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

pub mod cloud_proxy;
pub mod dynamodb;
pub mod fs;
pub mod memory;
pub mod mongo;
pub mod s3;
pub mod triedb_reader;

use std::collections::HashMap;

use bytes::Bytes;
use enum_dispatch::enum_dispatch;
use eyre::Result;
use futures::future::try_join_all;
use tokio_retry::{
    strategy::{jitter, ExponentialBackoff},
    RetryIf,
};

use self::{
    cloud_proxy::CloudProxyReader, fs::FsStorage, memory::MemoryStorage, mongo::MongoDbStorage,
};
use crate::prelude::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KVStoreType {
    AwsS3,
    AwsDynamoDB,
    Mongo,
    Memory,
    CloudProxy,
    TrieDb,
    FileSystem,
}

/// Policy controlling whether `put` can overwrite existing keys.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum WritePolicy {
    /// Allow overwriting existing keys (default behavior).
    #[default]
    AllowOverwrite,
    /// Prevent overwriting - skip write if key exists.
    NoClobber,
}

/// Result of a `put` operation indicating whether data was written.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PutResult {
    /// Data was successfully written to the store.
    Written,
    /// Write was skipped because the key already exists (NoClobber policy).
    Skipped,
}

impl KVStoreType {
    pub const fn as_str(&self) -> &'static str {
        match self {
            KVStoreType::AwsS3 => "aws_s3",
            KVStoreType::AwsDynamoDB => "aws_dynamodb",
            KVStoreType::Mongo => "mongo",
            KVStoreType::Memory => "memory",
            KVStoreType::CloudProxy => "cloud_proxy",
            KVStoreType::TrieDb => "triedb",
            KVStoreType::FileSystem => "fs",
        }
    }
}

#[enum_dispatch(KVStore, KVReader)]
#[derive(Clone)]
pub enum KVStoreErased {
    Bucket,
    DynamoDBArchive,
    MemoryStorage,
    MongoDbStorage,
    FsStorage,
}

#[enum_dispatch(KVReader)]
#[derive(Clone)]
pub enum KVReaderErased {
    Bucket,
    MemoryStorage,
    DynamoDBArchive,
    CloudProxyReader,
    MongoDbStorage,
    FsStorage,
}

impl From<KVStoreErased> for KVReaderErased {
    fn from(value: KVStoreErased) -> Self {
        match value {
            KVStoreErased::Bucket(x) => KVReaderErased::Bucket(x),
            KVStoreErased::MemoryStorage(x) => KVReaderErased::MemoryStorage(x),
            KVStoreErased::DynamoDBArchive(x) => KVReaderErased::DynamoDBArchive(x),
            KVStoreErased::MongoDbStorage(x) => KVReaderErased::MongoDbStorage(x),
            KVStoreErased::FsStorage(x) => KVReaderErased::FsStorage(x),
        }
    }
}

#[enum_dispatch]
pub trait KVStore: KVReader {
    async fn put(
        &self,
        key: impl AsRef<str>,
        data: Vec<u8>,
        policy: WritePolicy,
    ) -> Result<PutResult>;
    async fn bulk_put(
        &self,
        kvs: impl IntoIterator<Item = (String, Vec<u8>)>,
        policy: WritePolicy,
    ) -> Result<PutResult> {
        let results: Vec<PutResult> = futures::stream::iter(kvs)
            .map(|(k, v)| self.put(k, v, policy))
            .buffer_unordered(10)
            .try_collect()
            .await?;
        // Return Skipped if any put was skipped
        if results.iter().any(|r| matches!(r, PutResult::Skipped)) {
            Ok(PutResult::Skipped)
        } else {
            Ok(PutResult::Written)
        }
    }
    async fn scan_prefix(&self, prefix: &str) -> Result<Vec<String>>;
    async fn delete(&self, key: impl AsRef<str>) -> Result<()>;
    fn bucket_name(&self) -> &str;
}

#[enum_dispatch]
pub trait KVReader: Clone {
    async fn get(&self, key: &str) -> Result<Option<Bytes>>;

    /// Lightweight existence check; prefer this over `get` when you don't need payload bytes.
    async fn exists(&self, key: &str) -> Result<bool>;

    async fn bulk_get(&self, keys: &[String]) -> Result<HashMap<String, Bytes>> {
        // Note: a stream based approach runs into lifetime generality errors for some reason here.
        // After a lot of variations I could not get it to work, so fell back on this join_all approach even
        // though it involves an extra allocation.
        // Optimize at your own risk!
        let mut futs = Vec::with_capacity(keys.len());
        for key in keys {
            let reader = self.clone();
            futs.push(async move { reader.get(key).await });
        }
        let responses = try_join_all(futs).await?;

        let mut out = HashMap::with_capacity(responses.len());
        for (resp, key) in responses.into_iter().zip(keys) {
            // let resp = resp?;
            if let Some(bytes) = resp {
                out.insert(key.clone(), bytes);
            }
        }
        Ok(out)
    }
}

pub fn retry_strategy() -> std::iter::Map<ExponentialBackoff, fn(Duration) -> Duration> {
    ExponentialBackoff::from_millis(10)
        .max_delay(Duration::from_secs(1))
        .map(jitter)
}

pub fn retry<A: tokio_retry::Action>(
    a: A,
) -> RetryIf<std::iter::Map<ExponentialBackoff, fn(Duration) -> Duration>, A, RetryTimeout>
where
    A::Error: std::fmt::Debug,
{
    RetryIf::spawn(retry_strategy(), a, RetryTimeout::ms(5_000))
}

pub struct RetryTimeout {
    cutoff: Instant,
}

impl<E: std::fmt::Debug> tokio_retry::Condition<E> for RetryTimeout {
    fn should_retry(&mut self, e: &E) -> bool {
        #[cfg(test)]
        eprintln!("Encountered error: {e:?}");

        warn!("Encountered error: {e:?}, retrying...");
        Instant::now() < self.cutoff
    }
}

impl RetryTimeout {
    fn ms(ms: u64) -> Self {
        Self {
            cutoff: Instant::now() + Duration::from_millis(ms),
        }
    }
}

trait MetricsResultExt {
    fn write_put_metrics(
        self,
        duration: Duration,
        kvstore_type: KVStoreType,
        metrics: &Metrics,
    ) -> Self;
    fn write_get_metrics(
        self,
        duration: Duration,
        kvstore_type: KVStoreType,
        metrics: &Metrics,
    ) -> Self;
    fn write_get_metrics_on_err(
        self,
        duration: Duration,
        kvstore_type: KVStoreType,
        metrics: &Metrics,
    ) -> Self;
    fn write_put_metrics_on_err(
        self,
        duration: Duration,
        kvstore_type: KVStoreType,
        metrics: &Metrics,
    ) -> Self;
}

impl<T, E> MetricsResultExt for std::result::Result<T, E> {
    fn write_put_metrics(
        self,
        duration: Duration,
        kvstore_type: KVStoreType,
        metrics: &Metrics,
    ) -> Self {
        kvstore_put_metrics(duration, self.is_ok(), kvstore_type, metrics);
        self
    }

    fn write_get_metrics(
        self,
        duration: Duration,
        kvstore_type: KVStoreType,
        metrics: &Metrics,
    ) -> Self {
        kvstore_get_metrics(duration, self.is_ok(), kvstore_type, metrics);
        self
    }

    fn write_get_metrics_on_err(
        self,
        duration: Duration,
        kvstore_type: KVStoreType,
        metrics: &Metrics,
    ) -> Self {
        if self.is_err() {
            kvstore_get_metrics(duration, false, kvstore_type, metrics);
        }
        self
    }

    fn write_put_metrics_on_err(
        self,
        duration: Duration,
        kvstore_type: KVStoreType,
        metrics: &Metrics,
    ) -> Self {
        if self.is_err() {
            kvstore_put_metrics(duration, false, kvstore_type, metrics);
        }
        self
    }
}

pub(crate) fn kvstore_put_metrics(
    duration: Duration,
    is_success: bool,
    kvstore_type: KVStoreType,
    metrics: &Metrics,
) {
    let attrs = &[opentelemetry::KeyValue::new(
        "kvstore_type",
        kvstore_type.as_str(),
    )];
    metrics.histogram_with_attrs(
        MetricNames::KV_STORE_PUT_DURATION_MS,
        duration.as_millis() as f64,
        attrs,
    );
    if is_success {
        metrics.counter_with_attrs(MetricNames::KV_STORE_PUT_SUCCESS, 1, attrs);
    } else {
        metrics.counter_with_attrs(MetricNames::KV_STORE_PUT_FAILURE, 1, attrs);
    }

    // Legacy metrics for backwards compatibility
    match kvstore_type {
        KVStoreType::AwsS3 => {
            if is_success {
                metrics.inc_counter(MetricNames::AWS_S3_WRITES);
            } else {
                metrics.inc_counter(MetricNames::AWS_S3_ERRORS);
            }
        }
        KVStoreType::AwsDynamoDB => {
            if is_success {
                metrics.inc_counter(MetricNames::AWS_DYNAMODB_WRITES);
            } else {
                metrics.inc_counter(MetricNames::AWS_DYNAMODB_ERRORS);
            }
        }
        _ => {}
    }
}

fn kvstore_get_metrics(
    duration: Duration,
    is_success: bool,
    kvstore_type: KVStoreType,
    metrics: &Metrics,
) {
    let attrs = &[opentelemetry::KeyValue::new(
        "kvstore_type",
        kvstore_type.as_str(),
    )];
    metrics.histogram_with_attrs(
        MetricNames::KV_STORE_GET_DURATION_MS,
        duration.as_millis() as f64,
        attrs,
    );
    if is_success {
        metrics.counter_with_attrs(MetricNames::KV_STORE_GET_SUCCESS, 1, attrs);
    } else {
        metrics.counter_with_attrs(MetricNames::KV_STORE_GET_FAILURE, 1, attrs);
    }

    // Legacy metrics for backwards compatibility
    match kvstore_type {
        KVStoreType::AwsS3 => {
            if is_success {
                metrics.inc_counter(MetricNames::AWS_S3_READS);
            } else {
                metrics.inc_counter(MetricNames::AWS_S3_ERRORS);
            }
        }
        KVStoreType::AwsDynamoDB => {
            if is_success {
                metrics.inc_counter(MetricNames::AWS_DYNAMODB_READS);
            } else {
                metrics.inc_counter(MetricNames::AWS_DYNAMODB_ERRORS);
            }
        }
        _ => {}
    }
}
