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

#[cfg(feature = "dynamo")]
mod dynamo;
mod in_memory;
#[cfg(feature = "s3")]
mod s3;

use bytes::Bytes;
#[cfg(feature = "dynamo")]
pub(crate) use dynamo::MAX_CHUNK_SIZE;
#[cfg(feature = "dynamo")]
pub use dynamo::{DynamoBlobStore, DynamoBlobStoreConfig};
pub use in_memory::InMemoryBlobStore;
#[cfg(feature = "s3")]
pub use s3::{S3BlobStore, S3BlobStoreConfig, S3Credentials, S3ReadStatsSnapshot};

use crate::error::{MonadChainDataError, Result};

#[derive(Debug, Clone)]
pub struct BlobWriteOp {
    pub table: BlobTableId,
    pub key: Vec<u8>,
    pub value: Bytes,
}

/// Logical identifier for a blob table. Names are opaque; backends own any
/// prefixing/keyspacing needed to avoid collisions on shared physical
/// resources -- this type makes no namespacing guarantees on its own.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BlobTableId(&'static str);

impl BlobTableId {
    pub const fn new(name: &'static str) -> Self {
        Self(name)
    }

    pub const fn as_str(self) -> &'static str {
        self.0
    }
}

#[derive(Debug, Clone)]
pub struct BlobTable<B> {
    store: B,
    pub table: BlobTableId,
    /// Optional process-global cap on concurrent backend reads, shared across
    /// every query so collective fan-out cannot overwhelm the backend. `None`
    /// leaves reads unbounded; writes (the ingest path) are never gated.
    io_limit: Option<std::sync::Arc<tokio::sync::Semaphore>>,
}

impl<B> BlobTable<B> {
    pub fn new(store: B, table: BlobTableId) -> Self {
        Self {
            store,
            table,
            io_limit: None,
        }
    }

    /// Attaches a shared read-concurrency limiter; all clones share the same
    /// semaphore, so the cap is global across the tables that hold it.
    pub fn with_io_limit(mut self, io_limit: std::sync::Arc<tokio::sync::Semaphore>) -> Self {
        self.io_limit = Some(io_limit);
        self
    }
}

impl<B: BlobStore> BlobTable<B> {
    pub async fn put(&self, key: &[u8], value: Bytes) -> Result<()> {
        self.store.put_blob(self.table, key, value).await
    }

    pub async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let _permit = self.acquire_read().await;
        self.store.get_blob(self.table, key).await
    }

    /// Like writes, deletes (GC paths) are never gated by the read limiter.
    pub async fn delete(&self, key: &[u8]) -> Result<()> {
        self.store.delete_blob(self.table, key).await
    }

    pub async fn read_range(
        &self,
        key: &[u8],
        start: usize,
        end_exclusive: usize,
    ) -> Result<Option<Bytes>> {
        let _permit = self.acquire_read().await;
        self.store
            .read_range(self.table, key, start, end_exclusive)
            .await
    }

    /// Acquires one global read permit; `None` when no limiter is attached.
    async fn acquire_read(&self) -> Option<tokio::sync::SemaphorePermit<'_>> {
        match &self.io_limit {
            Some(sem) => Some(
                sem.acquire()
                    .await
                    .expect("blob io-limit semaphore is never closed"),
            ),
            None => None,
        }
    }
}

/// Unversioned object storage with range-read support.
///
/// Implementations must be cheaply cloneable (e.g. via internal `Arc`).
#[allow(async_fn_in_trait)]
pub trait BlobStore: Clone + Send + Sync + 'static {
    fn table(&self, table: BlobTableId) -> BlobTable<Self>
    where
        Self: Sized,
    {
        BlobTable::new(self.clone(), table)
    }

    // All methods return `Send` futures: callers drive them from spawned tasks,
    // and the cache layer stores `get_blob` in a cross-thread single-flight
    // `Shared`. Impls may use plain `async fn` as long as bodies are `Send`.
    fn put_blob(
        &self,
        table: BlobTableId,
        key: &[u8],
        value: Bytes,
    ) -> impl std::future::Future<Output = Result<()>> + Send;
    fn get_blob(
        &self,
        table: BlobTableId,
        key: &[u8],
    ) -> impl std::future::Future<Output = Result<Option<Bytes>>> + Send;
    /// Removes a blob; deleting a missing key is an idempotent no-op.
    fn delete_blob(
        &self,
        table: BlobTableId,
        key: &[u8],
    ) -> impl std::future::Future<Output = Result<()>> + Send;
    fn apply_writes(
        &self,
        writes: Vec<BlobWriteOp>,
    ) -> impl std::future::Future<Output = Result<()>> + Send;
    fn read_range(
        &self,
        table: BlobTableId,
        key: &[u8],
        start: usize,
        end_exclusive: usize,
    ) -> impl std::future::Future<Output = Result<Option<Bytes>>> + Send {
        async move {
            let Some(blob) = self.get_blob(table, key).await? else {
                return Ok(None);
            };
            if start > end_exclusive || start > blob.len() {
                return Err(MonadChainDataError::Decode("invalid blob range"));
            }
            Ok(Some(blob.slice(start..end_exclusive.min(blob.len()))))
        }
    }
}
