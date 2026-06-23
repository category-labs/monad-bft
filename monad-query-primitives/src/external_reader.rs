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

use std::{collections::BTreeMap, sync::RwLock};

use futures::future::BoxFuture;
use monad_query_errors::{MonadChainDataError, Result};

/// Raw object bytes returned by an [`ExternalBlobReader`].
pub type RawBytes = bytes::Bytes;

/// Read-only byte-range access to the external archive's objects. Keys are
/// the RAW archive object keys (no chain-data prefix/table/hex mangling).
/// Object-safe (boxed futures) so the engine's `Tables` can hold it without
/// growing a third type parameter.
pub trait ExternalBlobReader: Send + Sync + 'static {
    /// Reads `[start, end_exclusive)` of `key`; `Ok(None)` when the object is
    /// absent. Semantics match the store's `BlobStore::read_range`: the end
    /// clamps to EOF, a start strictly past EOF is an error.
    fn read_range(
        &self,
        key: &[u8],
        start: usize,
        end_exclusive: usize,
    ) -> BoxFuture<'_, Result<Option<RawBytes>>>;
}

/// In-memory [`ExternalBlobReader`] for tests: a key -> object map.
#[derive(Debug, Default)]
pub struct InMemoryExternalBlobReader {
    objects: RwLock<BTreeMap<Vec<u8>, RawBytes>>,
}

impl InMemoryExternalBlobReader {
    pub fn insert(&self, key: impl Into<Vec<u8>>, object: impl Into<RawBytes>) {
        self.objects
            .write()
            .expect("poisoned lock")
            .insert(key.into(), object.into());
    }
}

impl ExternalBlobReader for InMemoryExternalBlobReader {
    fn read_range(
        &self,
        key: &[u8],
        start: usize,
        end_exclusive: usize,
    ) -> BoxFuture<'_, Result<Option<RawBytes>>> {
        let result = (|| {
            let objects = self.objects.read().expect("poisoned lock");
            let Some(object) = objects.get(key) else {
                return Ok(None);
            };
            if start > end_exclusive || start > object.len() {
                return Err(MonadChainDataError::Decode("invalid blob range"));
            }
            Ok(Some(object.slice(start..end_exclusive.min(object.len()))))
        })();
        Box::pin(async move { result })
    }
}
