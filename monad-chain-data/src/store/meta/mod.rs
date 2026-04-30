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

use bytes::Bytes;
pub use in_memory::InMemoryMetaStore;

use crate::{error::Result, store::common::Page};

/// Logical identifier for a key/value table.
///
/// Identifiers are opaque names. Backends are responsible for any
/// prefixing or keyspacing required to map logical tables onto shared
/// physical resources without collisions (e.g. distinct Scylla
/// keyspaces, per-deployment table prefixes). This type makes no
/// namespacing guarantees on its own.
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
///
/// Same namespacing model as [`TableId`]: backends own collision
/// avoidance when these names map onto shared physical resources.
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
pub struct Record {
    pub version: u64,
    pub value: Bytes,
}

#[derive(Debug, Clone)]
pub struct PutResult {
    pub applied: bool,
    pub version: Option<u64>,
}

#[derive(Debug)]
pub struct KvTable<M> {
    store: M,
    pub table: TableId,
}

impl<M> KvTable<M> {
    pub fn new(store: M, table: TableId) -> Self {
        Self { store, table }
    }
}

impl<M: Clone> Clone for KvTable<M> {
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
            table: self.table,
        }
    }
}

impl<M: MetaStore> KvTable<M> {
    pub async fn get(&self, key: &[u8]) -> Result<Option<Record>> {
        self.store.get(self.table, key).await
    }

    pub async fn put(&self, key: &[u8], value: Bytes) -> Result<PutResult> {
        self.store.put(self.table, key, value).await
    }
}

#[derive(Debug)]
pub struct ScannableKvTable<M> {
    store: M,
    pub table: ScannableTableId,
}

impl<M> ScannableKvTable<M> {
    pub fn new(store: M, table: ScannableTableId) -> Self {
        Self { store, table }
    }
}

impl<M: Clone> Clone for ScannableKvTable<M> {
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
            table: self.table,
        }
    }
}

impl<M: MetaStore> ScannableKvTable<M> {
    pub async fn get(&self, partition: &[u8], clustering: &[u8]) -> Result<Option<Record>> {
        self.store.scan_get(self.table, partition, clustering).await
    }

    pub async fn put(
        &self,
        partition: &[u8],
        clustering: &[u8],
        value: Bytes,
    ) -> Result<PutResult> {
        self.store
            .scan_put(self.table, partition, clustering, value)
            .await
    }

    pub async fn list_prefix(
        &self,
        partition: &[u8],
        prefix: &[u8],
        cursor: Option<Vec<u8>>,
        limit: usize,
    ) -> Result<Page> {
        self.store
            .scan_list(self.table, partition, prefix, cursor, limit)
            .await
    }
}

/// Versioned key-value and scannable metadata storage.
///
/// Implementations must be cheaply cloneable (e.g. via internal `Arc`).
#[allow(async_fn_in_trait)]
#[auto_impl::auto_impl(Arc)]
pub trait MetaStore: Clone + Send + Sync {
    #[auto_impl(keep_default_for(Arc))]
    fn table(&self, table: TableId) -> KvTable<Self>
    where
        Self: Sized,
    {
        KvTable::new(self.clone(), table)
    }

    #[auto_impl(keep_default_for(Arc))]
    fn scannable_table(&self, table: ScannableTableId) -> ScannableKvTable<Self>
    where
        Self: Sized,
    {
        ScannableKvTable::new(self.clone(), table)
    }

    async fn get(&self, table: TableId, key: &[u8]) -> Result<Option<Record>>;
    async fn scan_get(
        &self,
        table: ScannableTableId,
        partition: &[u8],
        clustering: &[u8],
    ) -> Result<Option<Record>>;

    async fn put(&self, table: TableId, key: &[u8], value: Bytes) -> Result<PutResult>;
    async fn scan_put(
        &self,
        table: ScannableTableId,
        partition: &[u8],
        clustering: &[u8],
        value: Bytes,
    ) -> Result<PutResult>;

    async fn scan_list(
        &self,
        table: ScannableTableId,
        partition: &[u8],
        prefix: &[u8],
        cursor: Option<Vec<u8>>,
        limit: usize,
    ) -> Result<Page>;
}
