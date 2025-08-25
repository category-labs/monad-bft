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
pub mod memory;
mod metrics;
pub mod mongo;
pub mod s3;
pub mod triedb_reader;

use std::collections::HashMap;

use bytes::Bytes;
use enum_dispatch::enum_dispatch;
use eyre::Result;
use tokio_retry::{
    strategy::{jitter, ExponentialBackoff},
    RetryIf,
};

use self::{cloud_proxy::CloudProxyReader, memory::MemoryStorage, mongo::MongoDbStorage};
use crate::prelude::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KVStoreType {
    AwsS3,
    AwsDynamoDB,
    Mongo,
    Memory,
    CloudProxy,
    TrieDb,
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
        }
    }
}

#[enum_dispatch(KVStore, KVReader)]
#[derive(Clone)]
pub enum KVStoreErased {
    S3Bucket,
    DynamoDBArchive,
    MemoryStorage,
    MongoDbStorage,
}

#[enum_dispatch(KVReader)]
#[derive(Clone)]
pub enum KVReaderErased {
    S3Bucket,
    MemoryStorage,
    DynamoDBArchive,
    CloudProxyReader,
    MongoDbStorage,
}

impl From<KVStoreErased> for KVReaderErased {
    fn from(value: KVStoreErased) -> Self {
        match value {
            KVStoreErased::S3Bucket(x) => KVReaderErased::S3Bucket(x),
            KVStoreErased::MemoryStorage(x) => KVReaderErased::MemoryStorage(x),
            KVStoreErased::DynamoDBArchive(x) => KVReaderErased::DynamoDBArchive(x),
            KVStoreErased::MongoDbStorage(x) => KVReaderErased::MongoDbStorage(x),
        }
    }
}

#[enum_dispatch]
pub trait KVStore: KVReader {
    async fn put(&self, key: impl AsRef<str>, data: Vec<u8>) -> Result<()>;
    async fn bulk_put(&self, kvs: impl IntoIterator<Item = (String, Vec<u8>)>) -> Result<()> {
        futures::stream::iter(kvs)
            .map(|(k, v)| self.put(k, v))
            .buffer_unordered(10)
            .count()
            .await;
        Ok(())
    }
    async fn scan_prefix(&self, prefix: &str) -> Result<Vec<String>>;
    async fn delete(&self, key: impl AsRef<str>) -> Result<()>;
    fn bucket_name(&self) -> &str;
}

#[enum_dispatch]
pub trait KVReader: Clone {
    async fn get(&self, key: &str) -> Result<Option<Bytes>>;
    async fn bulk_get(&self, keys: Vec<String>) -> Result<HashMap<String, Bytes>> {
        // TODO: use global concurrency throttle
        const CONCURRENCY: usize = 10;
        futures::stream::iter(keys)
            .map(|key| {
                async move {
                    self.get(&key)
                        .await // fmt
                        .map(|resp| (key, resp))
                }
            })
            .buffered(CONCURRENCY)
            .try_fold(HashMap::default(), async |mut map, (key, resp)| {
                if let Some(bytes) = resp {
                    map.insert(key, bytes);
                }
                Ok(map)
            })
            .await
    }
}

///////////
// Utils //
///////////

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
