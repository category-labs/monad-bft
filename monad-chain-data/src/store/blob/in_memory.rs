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
    collections::BTreeMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, RwLock,
    },
};

use bytes::Bytes;

use crate::{
    error::Result,
    store::blob::{BlobStore, BlobTableId, BlobWriteOp},
};

/// Test-only in-memory blob fixture; not a deployable backend.
#[derive(Debug, Clone, Default)]
pub struct InMemoryBlobStore {
    blobs: Arc<RwLock<BTreeMap<(BlobTableId, Vec<u8>), Bytes>>>,
    /// Backend object accesses (`get_blob`, which also backs the default
    /// `read_range`); lets cache tests assert fetch collapsing.
    get_blob_calls: Arc<AtomicU64>,
}

impl InMemoryBlobStore {
    /// Object-level reads served so far; a range read counts as one access.
    pub fn get_blob_calls(&self) -> u64 {
        self.get_blob_calls.load(Ordering::SeqCst)
    }

    /// Clones the entire blob map for cross-fixture equality assertions.
    pub fn blob_snapshot(&self) -> BTreeMap<(BlobTableId, Vec<u8>), Bytes> {
        self.blobs.read().expect("poisoned lock").clone()
    }
}

impl BlobStore for InMemoryBlobStore {
    async fn put_blob(&self, table: BlobTableId, key: &[u8], value: Bytes) -> Result<()> {
        let mut guard = self.blobs.write().expect("poisoned lock");
        guard.insert((table, key.to_vec()), value);
        Ok(())
    }

    async fn apply_writes(&self, writes: Vec<BlobWriteOp>) -> Result<()> {
        let mut guard = self.blobs.write().expect("poisoned lock");
        for BlobWriteOp { table, key, value } in writes {
            guard.insert((table, key), value);
        }
        Ok(())
    }

    async fn delete_blob(&self, table: BlobTableId, key: &[u8]) -> Result<()> {
        let mut guard = self.blobs.write().expect("poisoned lock");
        guard.remove(&(table, key.to_vec()));
        Ok(())
    }

    async fn get_blob(&self, table: BlobTableId, key: &[u8]) -> Result<Option<Bytes>> {
        self.get_blob_calls.fetch_add(1, Ordering::SeqCst);
        let guard = self.blobs.read().expect("poisoned lock");
        Ok(guard.get(&(table, key.to_vec())).cloned())
    }
}
