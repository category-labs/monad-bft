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

use std::sync::Mutex;

use bytes::Bytes;

use crate::{
    engine::tables::Tables,
    store::{
        blob::{BlobStore, BlobTableId, BlobWriteOp},
        meta::{MetaStore, MetaWriteOp, ScannableTableId, TableId},
    },
};

pub struct WriteSession<'a, M: MetaStore, B: BlobStore> {
    tables: &'a Tables<M, B>,
    meta_pending: Mutex<Vec<MetaWriteOp>>,
    blob_pending: Mutex<Vec<BlobWriteOp>>,
    populated_kv: Mutex<Vec<(TableId, Vec<u8>)>>,
    populated_scan: Mutex<Vec<(ScannableTableId, Vec<u8>, Vec<u8>)>>,
    populated_blob: Mutex<Vec<(BlobTableId, Vec<u8>)>>,
}

impl<'a, M: MetaStore, B: BlobStore> WriteSession<'a, M, B> {
    pub fn tables(&self) -> &'a Tables<M, B> {
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

    pub fn put(&self, table: TableId, key: &[u8], value: Bytes) {
        self.tables.populate_kv_cache(table, key, value.clone());
        self.populated_kv
            .lock()
            .expect("write session populated_kv poisoned")
            .push((table, key.to_vec()));
        self.meta_pending
            .lock()
            .expect("write session meta_pending poisoned")
            .push(MetaWriteOp::Put {
                table,
                key: key.to_vec(),
                value,
            });
    }

    pub fn scan_put(
        &self,
        table: ScannableTableId,
        partition: &[u8],
        clustering: &[u8],
        value: Bytes,
    ) {
        self.tables
            .populate_scan_cache(table, partition, clustering, value.clone());
        self.populated_scan
            .lock()
            .expect("write session populated_scan poisoned")
            .push((table, partition.to_vec(), clustering.to_vec()));
        self.meta_pending
            .lock()
            .expect("write session meta_pending poisoned")
            .push(MetaWriteOp::ScanPut {
                table,
                partition: partition.to_vec(),
                clustering: clustering.to_vec(),
                value,
            });
    }

    pub fn put_blob(&self, table: BlobTableId, key: &[u8], value: Bytes) {
        self.tables.populate_blob_cache(table, key, value.clone());
        self.populated_blob
            .lock()
            .expect("write session populated_blob poisoned")
            .push((table, key.to_vec()));
        self.blob_pending
            .lock()
            .expect("write session blob_pending poisoned")
            .push(BlobWriteOp {
                table,
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
        for (table, key) in kv {
            self.tables.evict_kv_cache(table, &key);
        }
        let scan = std::mem::take(
            &mut *self
                .populated_scan
                .lock()
                .expect("write session populated_scan poisoned"),
        );
        for (table, partition, clustering) in scan {
            self.tables
                .evict_scan_cache(table, &partition, &clustering);
        }
        let blob = std::mem::take(
            &mut *self
                .populated_blob
                .lock()
                .expect("write session populated_blob poisoned"),
        );
        for (table, key) in blob {
            self.tables.evict_blob_cache(table, &key);
        }
    }
}
