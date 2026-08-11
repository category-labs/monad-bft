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
pub(crate) mod dynamo;
mod in_memory;
#[cfg(feature = "mongo")]
pub(crate) mod mongo;

use bytes::Bytes;
#[cfg(feature = "dynamo")]
pub use dynamo::{DynamoCredentials, DynamoMetaStore, DynamoMetaStoreConfig, DynamoTableLayout};
pub use in_memory::InMemoryMetaStore;
use monad_query_errors::Result;
#[cfg(feature = "mongo")]
pub use mongo::{MongoMetaStore, MongoMetaStoreConfig};

#[derive(Debug, Clone)]
pub enum MetaWriteOp {
    Put {
        table: TableId,
        row_key: Vec<u8>,
        row_data: Bytes,
    },
    ScanPut {
        table: ScannableTableId,
        partition: Vec<u8>,
        clustering_key: Vec<u8>,
        row_data: Bytes,
    },
}

/// Logical identifier for a key/value table. Names are opaque; backends own
/// any prefixing/keyspacing needed to avoid collisions on shared physical
/// resources.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TableId(&'static str);

impl TableId {
    pub const fn new(name: &'static str) -> Self {
        Self(name)
    }

    pub const fn as_str(self) -> &'static str {
        self.0
    }
}

/// Logical identifier for a scannable (partitioned + clustered) table.
/// Same namespacing model as [`TableId`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ScannableTableId(&'static str);

impl ScannableTableId {
    pub const fn new(name: &'static str) -> Self {
        Self(name)
    }

    pub const fn as_str(self) -> &'static str {
        self.0
    }
}

#[derive(Debug, Clone)]
pub struct KvTable<M> {
    meta_store: M,
    pub table: TableId,
}

impl<M: MetaStore> KvTable<M> {
    pub async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.meta_store.get(self.table, key).await
    }

    pub async fn put(&self, key: &[u8], value: Bytes) -> Result<()> {
        self.meta_store.put(self.table, key, value).await
    }
}

#[derive(Debug, Clone)]
pub struct ScannableKvTable<M> {
    meta_store: M,
    pub table: ScannableTableId,
}

impl<M: MetaStore> ScannableKvTable<M> {
    pub async fn get(&self, partition: &[u8], clustering: &[u8]) -> Result<Option<Bytes>> {
        self.meta_store
            .scan_get(self.table, partition, clustering)
            .await
    }

    pub async fn put(&self, partition: &[u8], clustering: &[u8], value: Bytes) -> Result<()> {
        self.meta_store
            .scan_put(self.table, partition, clustering, value)
            .await
    }

    /// Lists every clustering key in the partition, in clustering order.
    pub(crate) async fn scan_keys(&self, partition: &[u8]) -> Result<Vec<Vec<u8>>> {
        self.meta_store.scan_keys(self.table, partition).await
    }
}

/// Plain key-value and scannable metadata storage.
///
/// Writes are idempotent and content-deterministic at the chain-data layer,
/// so there are intentionally no versioning or compare-and-set semantics.
/// Implementations must be cheaply cloneable (e.g. via internal `Arc`).
#[allow(async_fn_in_trait)]
pub trait MetaStore: Clone + Send + Sync + 'static {
    fn table(&self, table: TableId) -> KvTable<Self> {
        KvTable {
            meta_store: self.clone(),
            table,
        }
    }

    fn scannable_table(&self, table: ScannableTableId) -> ScannableKvTable<Self> {
        ScannableKvTable {
            meta_store: self.clone(),
            table,
        }
    }

    // Every method returns a `Send` future (hence `impl Future + Send`, not
    // bare `async fn`): point reads feed the cache layer's cross-thread
    // single-flight `Shared`, and stores are driven from spawned tasks.
    // Implementations may still use `async fn` if the future is `Send`.
    fn get(
        &self,
        table: TableId,
        key: &[u8],
    ) -> impl std::future::Future<Output = Result<Option<Bytes>>> + Send;
    fn scan_get(
        &self,
        table: ScannableTableId,
        partition: &[u8],
        clustering: &[u8],
    ) -> impl std::future::Future<Output = Result<Option<Bytes>>> + Send;

    fn put(
        &self,
        table: TableId,
        key: &[u8],
        value: Bytes,
    ) -> impl std::future::Future<Output = Result<()>> + Send;
    fn scan_put(
        &self,
        table: ScannableTableId,
        partition: &[u8],
        clustering: &[u8],
        value: Bytes,
    ) -> impl std::future::Future<Output = Result<()>> + Send;

    /// Lists every clustering key in the partition, in clustering (unsigned
    /// byte) order.
    fn scan_keys(
        &self,
        table: ScannableTableId,
        partition: &[u8],
    ) -> impl std::future::Future<Output = Result<Vec<Vec<u8>>>> + Send;

    fn apply_writes(
        &self,
        writes: Vec<MetaWriteOp>,
    ) -> impl std::future::Future<Output = Result<()>> + Send;
}

/// Runtime-selected [`MetaStore`]: composition roots configure one service
/// instantiation over the backend product instead of one monomorphization per
/// meta × blob pairing.
#[derive(Clone)]
pub enum MetaBackend {
    InMemory(InMemoryMetaStore),
    #[cfg(feature = "dynamo")]
    Dynamo(DynamoMetaStore),
    #[cfg(feature = "mongo")]
    Mongo(MongoMetaStore),
}

macro_rules! with_meta_backend {
    ($backend:expr, $store:ident => $operation:expr) => {
        match $backend {
            MetaBackend::InMemory($store) => $operation,
            #[cfg(feature = "dynamo")]
            MetaBackend::Dynamo($store) => $operation,
            #[cfg(feature = "mongo")]
            MetaBackend::Mongo($store) => $operation,
        }
    };
}

impl MetaStore for MetaBackend {
    async fn get(&self, table: TableId, key: &[u8]) -> Result<Option<Bytes>> {
        with_meta_backend!(self, backend => backend.get(table, key).await)
    }

    async fn scan_get(
        &self,
        table: ScannableTableId,
        partition: &[u8],
        clustering: &[u8],
    ) -> Result<Option<Bytes>> {
        with_meta_backend!(self, backend => {
            backend.scan_get(table, partition, clustering).await
        })
    }

    async fn put(&self, table: TableId, key: &[u8], value: Bytes) -> Result<()> {
        with_meta_backend!(self, backend => backend.put(table, key, value).await)
    }

    async fn scan_put(
        &self,
        table: ScannableTableId,
        partition: &[u8],
        clustering: &[u8],
        value: Bytes,
    ) -> Result<()> {
        with_meta_backend!(self, backend => {
            backend.scan_put(table, partition, clustering, value).await
        })
    }

    async fn scan_keys(&self, table: ScannableTableId, partition: &[u8]) -> Result<Vec<Vec<u8>>> {
        with_meta_backend!(self, backend => backend.scan_keys(table, partition).await)
    }

    async fn apply_writes(&self, writes: Vec<MetaWriteOp>) -> Result<()> {
        with_meta_backend!(self, backend => backend.apply_writes(writes).await)
    }
}
