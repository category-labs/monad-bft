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

#![cfg(feature = "fjall")]

use bytes::Bytes;
use monad_chain_data::store::{
    BlobStore, BlobTableId, BlobWriteOp, FjallStore, FjallTuning, MetaStore, MetaWriteOp,
    ScannableTableId, TableId,
};

const T_KV: TableId = TableId::new("fjall_batch_atomicity_kv");
const T_SCAN: ScannableTableId = ScannableTableId::new("fjall_batch_atomicity_scan");
const T_BLOB: BlobTableId = BlobTableId::new("fjall_batch_atomicity_blob");

#[tokio::test(flavor = "current_thread")]
async fn meta_apply_writes_survive_reopen() {
    let dir = tempfile::tempdir().expect("tempdir");
    {
        let store = FjallStore::open(dir.path(), FjallTuning::default()).expect("open");
        MetaStore::apply_writes(
            &store,
            vec![
                MetaWriteOp::Put {
                    table: T_KV,
                    key: b"k".to_vec(),
                    value: Bytes::from_static(b"v"),
                },
                MetaWriteOp::ScanPut {
                    table: T_SCAN,
                    partition: b"p".to_vec(),
                    clustering: b"c".to_vec(),
                    value: Bytes::from_static(b"sv"),
                },
            ],
        )
        .await
        .expect("apply_writes");
    }
    let store = FjallStore::open(dir.path(), FjallTuning::default()).expect("reopen");
    assert_eq!(
        store.get(T_KV, b"k").await.unwrap().as_deref(),
        Some(&b"v"[..])
    );
    assert_eq!(
        store.scan_get(T_SCAN, b"p", b"c").await.unwrap().as_deref(),
        Some(&b"sv"[..])
    );
}

#[tokio::test(flavor = "current_thread")]
async fn blob_apply_writes_survive_reopen() {
    let dir = tempfile::tempdir().expect("tempdir");
    {
        let store = FjallStore::open(dir.path(), FjallTuning::default()).expect("open");
        BlobStore::apply_writes(
            &store,
            vec![BlobWriteOp {
                table: T_BLOB,
                key: b"k".to_vec(),
                value: Bytes::from_static(b"v"),
            }],
        )
        .await
        .expect("apply_writes");
    }
    let store = FjallStore::open(dir.path(), FjallTuning::default()).expect("reopen");
    assert_eq!(
        store.get_blob(T_BLOB, b"k").await.unwrap().as_deref(),
        Some(&b"v"[..])
    );
}
