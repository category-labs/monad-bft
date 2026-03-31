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
pub use in_memory::InMemoryBlobStore;

use crate::{
    error::{MonadChainDataError, Result},
    store::common::Page,
};

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

    pub async fn list_prefix(
        &self,
        prefix: &[u8],
        cursor: Option<Vec<u8>>,
        limit: usize,
    ) -> Result<Page> {
        self.store
            .list_prefix(self.table, prefix, cursor, limit)
            .await
    }
}

/// Unversioned object storage with range-read support.
///
/// Implementations must be cheaply cloneable (e.g. via internal `Arc`).
#[allow(async_fn_in_trait)]
#[auto_impl::auto_impl(Arc)]
pub trait BlobStore: Clone + Send + Sync {
    #[auto_impl(keep_default_for(Arc))]
    fn table(&self, table: BlobTableId) -> BlobTable<Self>
    where
        Self: Sized,
    {
        BlobTable::new(self.clone(), table)
    }

    async fn put_blob(&self, table: BlobTableId, key: &[u8], value: Bytes) -> Result<()>;
    async fn get_blob(&self, table: BlobTableId, key: &[u8]) -> Result<Option<Bytes>>;
    async fn read_range(
        &self,
        table: BlobTableId,
        key: &[u8],
        start: usize,
        end_exclusive: usize,
    ) -> Result<Option<Bytes>> {
        let Some(blob) = self.get_blob(table, key).await? else {
            return Ok(None);
        };
        if start > end_exclusive || start > blob.len() {
            return Err(MonadChainDataError::Decode("invalid blob range"));
        }
        Ok(Some(blob.slice(start..end_exclusive.min(blob.len()))))
    }
    async fn list_prefix(
        &self,
        table: BlobTableId,
        prefix: &[u8],
        cursor: Option<Vec<u8>>,
        limit: usize,
    ) -> Result<Page>;
}
