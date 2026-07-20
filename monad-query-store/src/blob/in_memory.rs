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
use monad_query_errors::Result;

use crate::blob::{BlobStore, BlobTableId, BlobWriteOp};

type BlobMap = BTreeMap<(BlobTableId, Vec<u8>), Bytes>;
type SharedBlobMap = Arc<RwLock<BlobMap>>;

/// Test-only in-memory blob fixture; not a deployable backend.
#[derive(Debug, Clone, Default)]
pub struct InMemoryBlobStore {
    blob_store_map: SharedBlobMap,
    /// Backend object accesses (`get_blob`, which also backs the default
    /// `read_range`); lets cache tests assert fetch collapsing.
    read_call_count: Arc<AtomicU64>,
}

impl InMemoryBlobStore {
    /// Object-level reads served so far; a range read counts as one access.
    pub fn get_blob_calls(&self) -> u64 {
        self.read_call_count.load(Ordering::SeqCst)
    }

    /// Clones the entire blob map for cross-fixture equality assertions.
    pub fn blob_snapshot(&self) -> BTreeMap<(BlobTableId, Vec<u8>), Bytes> {
        self.blob_store_map
            .read()
            .expect("in-memory blob store lock is not poisoned")
            .clone()
    }
}

impl BlobStore for InMemoryBlobStore {
    async fn put_blob(&self, table: BlobTableId, key: &[u8], value: Bytes) -> Result<()> {
        let mut blob_store = self
            .blob_store_map
            .write()
            .expect("in-memory blob store lock is not poisoned");
        blob_store.insert((table, key.to_vec()), value);
        Ok(())
    }

    async fn apply_writes(&self, writes: Vec<BlobWriteOp>) -> Result<()> {
        let mut blob_store = self
            .blob_store_map
            .write()
            .expect("in-memory blob store lock is not poisoned");
        for BlobWriteOp {
            table,
            blob_key,
            blob_data,
        } in writes
        {
            blob_store.insert((table, blob_key), blob_data);
        }
        Ok(())
    }

    async fn delete_blob(&self, table: BlobTableId, key: &[u8]) -> Result<()> {
        let mut blob_store = self
            .blob_store_map
            .write()
            .expect("in-memory blob store lock is not poisoned");
        blob_store.remove(&(table, key.to_vec()));
        Ok(())
    }

    async fn get_blob(&self, table: BlobTableId, key: &[u8]) -> Result<Option<Bytes>> {
        self.read_call_count.fetch_add(1, Ordering::SeqCst);
        let blob_store = self
            .blob_store_map
            .read()
            .expect("in-memory blob store lock is not poisoned");
        Ok(blob_store.get(&(table, key.to_vec())).cloned())
    }
}
