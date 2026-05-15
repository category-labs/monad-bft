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
    FjallStore, FjallTuning, MetaStore, MetaWriteOp, TableId,
};

const T_KV: TableId = TableId::new("fjall_tuning_kv");

#[tokio::test(flavor = "current_thread")]
async fn tuning_knobs_round_trip_and_survive_reopen_with_defaults() {
    let tuning = FjallTuning {
        max_journaling_size_bytes: 128 * 1024 * 1024,
        max_memtable_size_bytes: 16 * 1024 * 1024,
        worker_threads: None,
    };
    let dir = tempfile::tempdir().expect("tempdir");
    {
        let store = FjallStore::open(dir.path(), tuning).expect("open tuned");
        store
            .apply_writes(vec![MetaWriteOp::Put {
                table: T_KV,
                key: b"k".to_vec(),
                value: Bytes::from_static(b"v"),
            }])
            .await
            .expect("apply_writes");
        assert_eq!(
            store.get(T_KV, b"k").await.unwrap().as_deref(),
            Some(&b"v"[..])
        );
    }

    let store = FjallStore::open(dir.path(), FjallTuning::default()).expect("reopen default");
    assert_eq!(
        store.get(T_KV, b"k").await.unwrap().as_deref(),
        Some(&b"v"[..])
    );
}
