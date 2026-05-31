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

mod in_memory;
#[cfg(feature = "s3")]
mod s3;

use bytes::Bytes;
pub use in_memory::InMemoryBlobStore;
#[cfg(feature = "s3")]
pub use s3::{S3BlobStore, S3BlobStoreConfig, S3Credentials};

use crate::error::{MonadChainDataError, Result};

#[derive(Debug, Clone)]
pub struct BlobWriteOp {
    pub table: BlobTableId,
    pub key: Vec<u8>,
    pub value: Bytes,
}

/// Logical identifier for a blob table.
///
/// Identifiers are opaque names. Backends are responsible for any
/// prefixing or keyspacing required to map logical tables onto shared
/// physical resources without collisions (e.g. distinct S3 buckets,
/// per-deployment object-key prefixes). This type makes no namespacing
/// guarantees on its own.
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

#[derive(Debug)]
pub struct BlobTable<B> {
    store: B,
    pub table: BlobTableId,
}

impl<B> BlobTable<B> {
    pub fn new(store: B, table: BlobTableId) -> Self {
        Self { store, table }
    }
}

impl<B: Clone> Clone for BlobTable<B> {
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
            table: self.table,
        }
    }
}

impl<B: BlobStore> BlobTable<B> {
    pub async fn put(&self, key: &[u8], value: Bytes) -> Result<()> {
        self.store.put_blob(self.table, key, value).await
    }

    pub async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.store.get_blob(self.table, key).await
    }

    pub async fn read_range(
        &self,
        key: &[u8],
        start: usize,
        end_exclusive: usize,
    ) -> Result<Option<Bytes>> {
        self.store
            .read_range(self.table, key, start, end_exclusive)
            .await
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

    // These all return `Send` futures so callers can drive them across thread
    // boundaries (e.g. the ingest binary spawns the IO/plan workers that call
    // through here on a `tokio::spawn`ed task, which requires `Send`). Impls may
    // still use plain `async fn` as long as their bodies are `Send`.
    fn put_blob(
        &self,
        table: BlobTableId,
        key: &[u8],
        value: Bytes,
    ) -> impl std::future::Future<Output = Result<()>> + Send;
    // Point read returns a `Send` future so the cache layer can store it in a
    // cross-thread single-flight `Shared` (see `store/cache`).
    fn get_blob(
        &self,
        table: BlobTableId,
        key: &[u8],
    ) -> impl std::future::Future<Output = Result<Option<Bytes>>> + Send;
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
