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

/// Test-only blob fixture. Holds blobs in memory behind a sync `RwLock`.
/// Not intended as a deployable backend.
#[derive(Debug, Clone, Default)]
pub struct InMemoryBlobStore {
    blobs: Arc<RwLock<BTreeMap<(BlobTableId, Vec<u8>), Bytes>>>,
    /// Count of backend object accesses (`get_blob`, which also backs the
    /// default `read_range`). Lets cache tests assert that single-flight +
    /// region caching collapse N row reads of one block onto one fetch.
    get_blob_calls: Arc<AtomicU64>,
}

impl InMemoryBlobStore {
    pub fn len(&self) -> usize {
        self.blobs
            .read()
            .map(|guard| guard.len())
            .unwrap_or_default()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Number of object-level reads served so far. One range read counts as one
    /// access (the default `read_range` fetches the object via `get_blob`).
    pub fn get_blob_calls(&self) -> u64 {
        self.get_blob_calls.load(Ordering::SeqCst)
    }

    /// Test-only: clones the entire blob map. Enables byte-equality assertions
    /// across two fixture instances without exposing the internal `RwLock`.
    pub fn blob_snapshot(&self) -> BTreeMap<(BlobTableId, Vec<u8>), Bytes> {
        self.blobs
            .read()
            .map(|guard| guard.clone())
            .unwrap_or_default()
    }
}

impl BlobStore for InMemoryBlobStore {
    async fn put_blob(&self, table: BlobTableId, key: &[u8], value: Bytes) -> Result<()> {
        let mut guard = self
            .blobs
            .write()
            .map_err(|_| crate::error::MonadChainDataError::Backend("poisoned lock".to_string()))?;
        guard.insert((table, key.to_vec()), value);
        Ok(())
    }

    async fn apply_writes(&self, writes: Vec<BlobWriteOp>) -> Result<()> {
        let mut guard = self
            .blobs
            .write()
            .map_err(|_| crate::error::MonadChainDataError::Backend("poisoned lock".to_string()))?;
        for BlobWriteOp { table, key, value } in writes {
            guard.insert((table, key), value);
        }
        Ok(())
    }

    async fn get_blob(&self, table: BlobTableId, key: &[u8]) -> Result<Option<Bytes>> {
        self.get_blob_calls.fetch_add(1, Ordering::SeqCst);
        let guard = self
            .blobs
            .read()
            .map_err(|_| crate::error::MonadChainDataError::Backend("poisoned lock".to_string()))?;
        Ok(guard.get(&(table, key.to_vec())).cloned())
    }
}
