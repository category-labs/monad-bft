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

use std::{future::Future, pin::Pin};

use bytes::Bytes;

use crate::{
    engine::tables::Tables,
    error::Result,
    store::{
        blob::{BlobStore, BlobTable, BlobWriteOp},
        cache::{CachedKvTable, CachedScannableKvTable},
        meta::{MetaStore, MetaWriteOp},
    },
};

/// Boxed future for the `for<'s>` HRTB closures the `with_writes*` entry
/// points take.
pub type SessionFuture<'s> = Pin<Box<dyn Future<Output = Result<()>> + Send + 's>>;

pub struct WriteSession<'a, M: MetaStore, B: BlobStore> {
    tables: &'a Tables<M, B>,
    meta_pending: Vec<MetaWriteOp>,
    blob_pending: Vec<BlobWriteOp>,
}

impl<'a, M: MetaStore, B: BlobStore> WriteSession<'a, M, B> {
    pub(crate) fn tables(&self) -> &'a Tables<M, B> {
        self.tables
    }

    pub(crate) fn new(tables: &'a Tables<M, B>) -> Self {
        Self {
            tables,
            meta_pending: Vec::new(),
            blob_pending: Vec::new(),
        }
    }

    /// Stages a durable metadata write. Read caches are read-populated only, so
    /// staging never touches them: an abandoned or failed session can never
    /// leave a phantom value resident, and there is nothing to evict on abort.
    pub fn put<V>(&mut self, table: &CachedKvTable<M, V>, key: &[u8], value: Bytes) {
        self.meta_pending.push(MetaWriteOp::Put {
            table: table.table_id(),
            key: key.to_vec(),
            value,
        });
    }

    pub fn scan_put<V>(
        &mut self,
        table: &CachedScannableKvTable<M, V>,
        partition: &[u8],
        clustering: &[u8],
        value: Bytes,
    ) {
        self.meta_pending.push(MetaWriteOp::ScanPut {
            table: table.table_id(),
            partition: partition.to_vec(),
            clustering: clustering.to_vec(),
            value,
        });
    }

    /// Stages a blob write. Blob bytes are never cached (the decoded-row
    /// caches sit above the store), so there is no cache to update here.
    pub fn put_blob(&mut self, table: &BlobTable<B>, key: &[u8], value: Bytes) {
        self.blob_pending.push(BlobWriteOp {
            table: table.table,
            key: key.to_vec(),
            value,
        });
    }

    pub(crate) fn take_meta(&mut self) -> Vec<MetaWriteOp> {
        std::mem::take(&mut self.meta_pending)
    }

    pub(crate) fn take_blob(&mut self) -> Vec<BlobWriteOp> {
        std::mem::take(&mut self.blob_pending)
    }
}
