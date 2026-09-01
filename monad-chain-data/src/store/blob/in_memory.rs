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
    sync::{Arc, RwLock},
};

use bytes::Bytes;

use crate::{
    error::Result,
    store::blob::{BlobStore, BlobTableId},
};

/// Test-only blob fixture. Holds blobs in memory behind a sync `RwLock`.
/// Not intended as a deployable backend.
#[derive(Debug, Clone, Default)]
pub struct InMemoryBlobStore {
    blobs: Arc<RwLock<BTreeMap<(BlobTableId, Vec<u8>), Bytes>>>,
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

    async fn get_blob(&self, table: BlobTableId, key: &[u8]) -> Result<Option<Bytes>> {
        let guard = self
            .blobs
            .read()
            .map_err(|_| crate::error::MonadChainDataError::Backend("poisoned lock".to_string()))?;
        Ok(guard.get(&(table, key.to_vec())).cloned())
    }
}
