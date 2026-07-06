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

use bytes::Bytes;
use monad_query_errors::{QueryError, Result};

use crate::blob::{BlobStore, BlobTableId, BlobWriteOp};

/// The blob backend of a store configured WITHOUT one (`[store.blob]`
/// omitted): an external-archive store whose row payloads live in the
/// monad-archive objects and whose checkpoints are disabled (snapshot payloads
/// are blob objects), so no blob traffic is ever legitimate.
///
/// Every access is a LOUD [`QueryError::Backend`] — never a silent
/// `None` — so a path that does reach the blob store (a native block-record
/// read, a checkpoint that slipped past the disable) surfaces as an error
/// instead of quietly missing data. The one exception is an empty
/// `apply_writes` batch, which the engine's write path issues unconditionally
/// and which touches nothing.
#[derive(Debug, Clone, Copy, Default)]
pub struct NullBlobStore;

fn no_blob_store(op: &str) -> QueryError {
    QueryError::Backend(format!(
        "no blob store configured ([store.blob] omitted), but {op} was called"
    ))
}

impl BlobStore for NullBlobStore {
    async fn put_blob(&self, _table: BlobTableId, _key: &[u8], _value: Bytes) -> Result<()> {
        Err(no_blob_store("put_blob"))
    }

    async fn get_blob(&self, _table: BlobTableId, _key: &[u8]) -> Result<Option<Bytes>> {
        Err(no_blob_store("get_blob"))
    }

    async fn delete_blob(&self, _table: BlobTableId, _key: &[u8]) -> Result<()> {
        Err(no_blob_store("delete_blob"))
    }

    async fn apply_writes(&self, writes: Vec<BlobWriteOp>) -> Result<()> {
        if writes.is_empty() {
            return Ok(());
        }
        Err(no_blob_store("apply_writes"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TABLE: BlobTableId = BlobTableId::new("test");

    fn assert_loud<T: std::fmt::Debug>(result: Result<T>) {
        match result {
            Err(QueryError::Backend(message)) => {
                assert!(message.contains("no blob store configured"), "{message}");
            }
            other => panic!("expected a loud Backend error, got {other:?}"),
        }
    }

    /// Every access — including reads, which must NOT degrade to a silent
    /// `Ok(None)` — surfaces the loud Backend error.
    #[tokio::test]
    async fn all_accesses_error_loudly() {
        let store = NullBlobStore;
        assert_loud(store.get_blob(TABLE, b"k").await);
        assert_loud(store.put_blob(TABLE, b"k", Bytes::from_static(b"v")).await);
        assert_loud(store.delete_blob(TABLE, b"k").await);
        // The default `read_range` goes through `get_blob`.
        assert_loud(store.read_range(TABLE, b"k", 0, 1).await);
        assert_loud(
            store
                .apply_writes(vec![BlobWriteOp {
                    table: TABLE,
                    key: b"k".to_vec(),
                    value: Bytes::from_static(b"v"),
                }])
                .await,
        );
    }

    /// The engine's write path applies the (possibly empty) blob batch
    /// unconditionally; an empty batch touches nothing and must succeed.
    #[tokio::test]
    async fn empty_apply_writes_is_ok() {
        assert!(NullBlobStore.apply_writes(Vec::new()).await.is_ok());
    }
}
