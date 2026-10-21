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
use monad_chain_data::{
    store::{BlobStore, BlobTableId},
    InMemoryBlobStore,
};

const TABLE: BlobTableId = BlobTableId::new("blob_store_test");
const KEY: &[u8] = b"blob";

#[tokio::test(flavor = "current_thread")]
async fn read_range_clamps_overshooting_end() {
    let store = InMemoryBlobStore::default();
    store
        .put_blob(TABLE, KEY, Bytes::from_static(b"abcdef"))
        .await
        .expect("put blob");

    let tail = store
        .read_range(TABLE, KEY, 2, 99)
        .await
        .expect("read range")
        .expect("blob present");
    assert_eq!(tail.as_ref(), b"cdef");

    let empty_tail = store
        .read_range(TABLE, KEY, 6, 99)
        .await
        .expect("read range")
        .expect("blob present");
    assert!(empty_tail.is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn read_range_rejects_invalid_start_bounds() {
    let store = InMemoryBlobStore::default();
    store
        .put_blob(TABLE, KEY, Bytes::from_static(b"abcdef"))
        .await
        .expect("put blob");

    let reversed = store
        .read_range(TABLE, KEY, 4, 3)
        .await
        .expect_err("reversed range should error");
    assert_eq!(reversed.to_string(), "decode error: invalid blob range");

    let past_end = store
        .read_range(TABLE, KEY, 7, 99)
        .await
        .expect_err("start past blob end should error");
    assert_eq!(past_end.to_string(), "decode error: invalid blob range");
}
