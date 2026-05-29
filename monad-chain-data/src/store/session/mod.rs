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

use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
};

use bytes::Bytes;

use crate::{
    engine::tables::Tables,
    error::Result,
    store::{
        blob::{BlobStore, BlobWriteOp},
        cache::{CachedBlobTable, CachedInner, CachedKvTable, CachedScannableTable},
        meta::{MetaStore, MetaWriteOp},
    },
};

/// Convenience for the `for<'s>` HRTB closure shape every `with_writes*`
/// entry-point declares. Halves the line-length of those signatures.
pub type SessionFuture<'s> = Pin<Box<dyn Future<Output = Result<()>> + Send + 's>>;

pub struct WriteSession<'a, M: MetaStore, B: BlobStore> {
    tables: &'a Tables<M, B>,
    meta_pending: Mutex<Vec<MetaWriteOp>>,
    blob_pending: Mutex<Vec<BlobWriteOp>>,
    populated_kv: Mutex<Vec<(Arc<CachedInner<Vec<u8>>>, Vec<u8>)>>,
    populated_scan: Mutex<Vec<(Arc<CachedInner<(Vec<u8>, Vec<u8>)>>, Vec<u8>, Vec<u8>)>>,
    populated_blob: Mutex<Vec<(Arc<CachedInner<Vec<u8>>>, Vec<u8>)>>,
}

impl<'a, M: MetaStore, B: BlobStore> WriteSession<'a, M, B> {
    pub(crate) fn tables(&self) -> &'a Tables<M, B> {
        self.tables
    }

    pub(crate) fn new(tables: &'a Tables<M, B>) -> Self {
        Self {
            tables,
            meta_pending: Mutex::new(Vec::new()),
            blob_pending: Mutex::new(Vec::new()),
            populated_kv: Mutex::new(Vec::new()),
            populated_scan: Mutex::new(Vec::new()),
            populated_blob: Mutex::new(Vec::new()),
        }
    }

    pub fn put(&self, table: &CachedKvTable<M>, key: &[u8], value: Bytes) {
        let handle = table.cache_handle();
        handle.populate(key.to_vec(), value.clone());
        self.populated_kv
            .lock()
            .expect("write session populated_kv poisoned")
            .push((handle, key.to_vec()));
        self.meta_pending
            .lock()
            .expect("write session meta_pending poisoned")
            .push(MetaWriteOp::Put {
                table: table.table_id(),
                key: key.to_vec(),
                value,
            });
    }

    pub fn scan_put(
        &self,
        table: &CachedScannableTable<M>,
        partition: &[u8],
        clustering: &[u8],
        value: Bytes,
    ) {
        let handle = table.cache_handle();
        handle.populate((partition.to_vec(), clustering.to_vec()), value.clone());
        self.populated_scan
            .lock()
            .expect("write session populated_scan poisoned")
            .push((handle, partition.to_vec(), clustering.to_vec()));
        self.meta_pending
            .lock()
            .expect("write session meta_pending poisoned")
            .push(MetaWriteOp::ScanPut {
                table: table.table_id(),
                partition: partition.to_vec(),
                clustering: clustering.to_vec(),
                value,
            });
    }

    pub fn scan_put_uncached(
        &self,
        table: &CachedScannableTable<M>,
        partition: &[u8],
        clustering: &[u8],
        value: Bytes,
    ) {
        self.meta_pending
            .lock()
            .expect("write session meta_pending poisoned")
            .push(MetaWriteOp::ScanPut {
                table: table.table_id(),
                partition: partition.to_vec(),
                clustering: clustering.to_vec(),
                value,
            });
    }

    pub fn extend_meta_uncached(&self, ops: Vec<MetaWriteOp>) {
        self.meta_pending
            .lock()
            .expect("write session meta_pending poisoned")
            .extend(ops);
    }

    pub fn put_blob(&self, table: &CachedBlobTable<B>, key: &[u8], value: Bytes) {
        let handle = table.cache_handle();
        handle.populate(key.to_vec(), value.clone());
        self.populated_blob
            .lock()
            .expect("write session populated_blob poisoned")
            .push((handle, key.to_vec()));
        self.blob_pending
            .lock()
            .expect("write session blob_pending poisoned")
            .push(BlobWriteOp {
                table: table.table_id(),
                key: key.to_vec(),
                value,
            });
    }

    pub(crate) fn take_meta(&self) -> Vec<MetaWriteOp> {
        std::mem::take(
            &mut *self
                .meta_pending
                .lock()
                .expect("write session meta_pending poisoned"),
        )
    }

    pub(crate) fn take_blob(&self) -> Vec<BlobWriteOp> {
        std::mem::take(
            &mut *self
                .blob_pending
                .lock()
                .expect("write session blob_pending poisoned"),
        )
    }

    pub(crate) fn invalidate_populated(&self) {
        let kv = std::mem::take(
            &mut *self
                .populated_kv
                .lock()
                .expect("write session populated_kv poisoned"),
        );
        for (cache, key) in kv {
            cache.evict(&key);
        }
        let scan = std::mem::take(
            &mut *self
                .populated_scan
                .lock()
                .expect("write session populated_scan poisoned"),
        );
        for (cache, partition, clustering) in scan {
            cache.evict(&(partition, clustering));
        }
        let blob = std::mem::take(
            &mut *self
                .populated_blob
                .lock()
                .expect("write session populated_blob poisoned"),
        );
        for (cache, key) in blob {
            cache.evict(&key);
        }
    }
}

// Closures passed to `with_writes*` can panic mid-staging. The framework's
// happy / error paths both run `invalidate_populated` explicitly, but a panic
// unwinds past those branches and drops the session directly — without this
// Drop, the populated cache entries would survive while the corresponding
// pending ops are silently discarded, leaving phantom values that never
// reached the backend.
impl<'a, M: MetaStore, B: BlobStore> Drop for WriteSession<'a, M, B> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.invalidate_populated();
        }
    }
}
