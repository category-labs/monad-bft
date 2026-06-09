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
use monad_chain_data::{
    store::{BlobStore, BlobTableId, CacheConfig, FjallStore, FjallTuning},
    Family, FinalizedBlock, MonadChainDataService, QueryLimits, B256,
};
use tempfile::tempdir;

mod common;

use common::{chain_header, minimal_ingest_tx, test_header};

#[tokio::test(flavor = "current_thread")]
async fn fjall_round_trip_two_block_ingest() {
    let dir = tempdir().expect("tempdir");
    let store = FjallStore::open(dir.path(), FjallTuning::default()).expect("open fjall");

    let h1 = test_header(1, B256::ZERO);
    let h2 = chain_header(2, &h1);
    common::populate::populate_stores(
        store.clone(),
        store.clone(),
        vec![
            FinalizedBlock {
                header: h1,
                logs_by_tx: vec![vec![], vec![]],
                txs: vec![minimal_ingest_tx(), minimal_ingest_tx()],
                traces: vec![],
            },
            FinalizedBlock {
                header: h2,
                logs_by_tx: vec![vec![]],
                txs: vec![minimal_ingest_tx()],
                traces: vec![],
            },
        ],
    )
    .await
    .expect("populate");
    let service = MonadChainDataService::new_reader_only(
        store.clone(),
        store,
        QueryLimits::UNLIMITED,
        CacheConfig::default(),
    );

    // Round-trip read of the published state.
    let published_head = service
        .publication()
        .load_published_head()
        .await
        .expect("load head");
    assert_eq!(published_head, Some(2));

    // Round-trip read of a per-family block header from the blob path.
    let tx_family = service.tables().family(Family::Tx);
    let header = tx_family
        .load_block_header(1)
        .await
        .expect("load tx header")
        .expect("present");
    assert!(!header.offsets.is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn fjall_persists_across_reopen() {
    let dir = tempdir().expect("tempdir");

    {
        let store = FjallStore::open(dir.path(), FjallTuning::default()).expect("open fjall");
        common::populate::populate_stores(
            store.clone(),
            store.clone(),
            vec![FinalizedBlock {
                header: test_header(1, B256::ZERO),
                logs_by_tx: vec![vec![]],
                txs: vec![minimal_ingest_tx()],
                traces: vec![],
            }],
        )
        .await
        .expect("populate");
    }

    let store = FjallStore::open(dir.path(), FjallTuning::default()).expect("reopen fjall");
    let service = MonadChainDataService::new_reader_only(
        store.clone(),
        store,
        QueryLimits::UNLIMITED,
        CacheConfig::default(),
    );
    let head = service
        .publication()
        .load_published_head()
        .await
        .expect("load head");
    assert_eq!(head, Some(1));
}

#[tokio::test(flavor = "current_thread")]
async fn fjall_blob_roundtrips_value_above_kv_separation_threshold() {
    // KV separation kicks in at 1 KiB by default; this test puts a 64 KiB
    // value to exercise the blob-file path, then reads it back.
    const TEST_TABLE: BlobTableId = BlobTableId::new("kv_sep_smoke");

    let dir = tempdir().expect("tempdir");
    let store = FjallStore::open(dir.path(), FjallTuning::default()).expect("open fjall");

    let payload = Bytes::from(vec![0xAB; 64 * 1024]);
    store
        .put_blob(TEST_TABLE, b"big", payload.clone())
        .await
        .expect("put");
    let got = store
        .get_blob(TEST_TABLE, b"big")
        .await
        .expect("get")
        .expect("present");
    assert_eq!(got, payload);
}

// `keyspace_stats()` exposes fjall's runtime accounting for sampling
// from the ingest binary. Keyspaces are opened lazily, so a fresh store
// reports zero keyspaces until the first write — this test exercises
// that lifecycle and asserts the touched keyspace shows up with a
// non-zero approximate_len.
#[tokio::test(flavor = "current_thread")]
async fn fjall_keyspace_stats_reflects_writes() {
    const TEST_TABLE: BlobTableId = BlobTableId::new("ks_stats_smoke");

    let dir = tempdir().expect("tempdir");
    let store = FjallStore::open(dir.path(), FjallTuning::default()).expect("open fjall");

    assert!(
        store.keyspace_stats().expect("stats").is_empty(),
        "no keyspaces should be registered before first access"
    );

    for i in 0..4u8 {
        store
            .put_blob(TEST_TABLE, &[i], Bytes::from(vec![i; 32]))
            .await
            .expect("put_blob");
    }

    let stats = store.keyspace_stats().expect("stats");
    assert!(
        !stats.is_empty(),
        "writes should have opened at least one keyspace"
    );
    let touched = stats
        .iter()
        .find(|s| s.name.ends_with("ks_stats_smoke"))
        .expect("touched keyspace should appear in stats");
    assert!(
        touched.approximate_len > 0,
        "approximate_len should reflect the 4 inserted blobs"
    );
}
