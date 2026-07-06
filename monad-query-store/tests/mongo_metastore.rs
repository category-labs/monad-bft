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

//! Store-contract tests for the Mongo meta backend. (The full configured-path
//! round trip, whose external payload reader is built by monad-archive, lives
//! in monad-archive's `chain_data_mongo_e2e` test.)
//!
//! Run: `docker run -p 27017:27017 mongo`, then
//! `CHAIN_DATA_MONGO_TEST_URL=mongodb://127.0.0.1:27017 \
//!   cargo test -p monad-query-store --features mongo --test mongo_metastore -- --ignored`
#![cfg(feature = "mongo")]

use bytes::Bytes;
use monad_query_store::{
    MetaStore, MetaWriteOp, MongoMetaStore, MongoMetaStoreConfig, ScannableTableId, TableId,
};

const KV: TableId = TableId::new("it_kv");
const SCAN: ScannableTableId = ScannableTableId::new("it_scan");
const OTHER_SCAN: ScannableTableId = ScannableTableId::new("it_scan_other");

fn test_url() -> Option<String> {
    std::env::var("CHAIN_DATA_MONGO_TEST_URL").ok()
}

fn unique_database(test: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("chain_data_it_{test}_{nanos}")
}

/// `None` (silent skip) when `CHAIN_DATA_MONGO_TEST_URL` is unset.
async fn fresh_store(test: &str) -> Option<(MongoMetaStore, String)> {
    let url = test_url()?;
    let database = unique_database(test);
    let store = MongoMetaStore::new(MongoMetaStoreConfig::new(
        url,
        database.clone(),
        "chain_data_meta",
    ))
    .await
    .expect("build store");
    Some((store, database))
}

async fn drop_database(database: &str) {
    let client = mongodb::Client::with_uri_str(&test_url().unwrap())
        .await
        .expect("client");
    client.database(database).drop().await.expect("drop db");
}

#[tokio::test]
#[ignore = "requires CHAIN_DATA_MONGO_TEST_URL (a running MongoDB)"]
async fn get_put_round_trip() {
    let Some((store, database)) = fresh_store("getput").await else {
        return;
    };

    assert_eq!(store.get(KV, b"missing").await.unwrap(), None);
    store.put(KV, b"k", Bytes::from_static(b"v")).await.unwrap();
    assert_eq!(
        store.get(KV, b"k").await.unwrap(),
        Some(Bytes::from_static(b"v"))
    );
    store
        .put(KV, b"k", Bytes::from_static(b"v2"))
        .await
        .unwrap();
    assert_eq!(
        store.get(KV, b"k").await.unwrap(),
        Some(Bytes::from_static(b"v2"))
    );
    // Empty keys and empty values are both representable.
    store.put(KV, b"", Bytes::new()).await.unwrap();
    assert_eq!(store.get(KV, b"").await.unwrap(), Some(Bytes::new()));

    assert_eq!(store.scan_get(SCAN, b"p", b"c").await.unwrap(), None);
    store
        .scan_put(SCAN, b"p", b"c", Bytes::from_static(b"sv"))
        .await
        .unwrap();
    assert_eq!(
        store.scan_get(SCAN, b"p", b"c").await.unwrap(),
        Some(Bytes::from_static(b"sv"))
    );

    drop_database(&database).await;
}

#[tokio::test]
#[ignore = "requires CHAIN_DATA_MONGO_TEST_URL (a running MongoDB)"]
async fn scan_keys_drains_whole_partition_in_clustering_order() {
    let Some((store, database)) = fresh_store("scankeys").await else {
        return;
    };

    let part = 7u64.to_be_bytes();
    let value = Bytes::from(vec![0u8; 4096]);
    // Insert in shuffled order so the returned order is the index's, not
    // insertion's. Crosses the write-concurrency batch size (400 > 64).
    let mut ops: Vec<MetaWriteOp> = (0u16..400)
        .rev()
        .map(|i| MetaWriteOp::ScanPut {
            table: SCAN,
            partition: part.to_vec(),
            clustering: i.to_be_bytes().to_vec(),
            value: value.clone(),
        })
        .collect();
    // Neighboring partitions of the same table, and the same partition of
    // another table, must not leak into the scan.
    ops.push(MetaWriteOp::ScanPut {
        table: SCAN,
        partition: 6u64.to_be_bytes().to_vec(),
        clustering: vec![0xff, 0xff],
        value: Bytes::from_static(b"x"),
    });
    ops.push(MetaWriteOp::ScanPut {
        table: SCAN,
        partition: 8u64.to_be_bytes().to_vec(),
        clustering: vec![0, 0],
        value: Bytes::from_static(b"x"),
    });
    ops.push(MetaWriteOp::ScanPut {
        table: OTHER_SCAN,
        partition: part.to_vec(),
        clustering: vec![0, 1],
        value: Bytes::from_static(b"x"),
    });
    store.apply_writes(ops).await.unwrap();

    let keys = store.scan_keys(SCAN, &part).await.unwrap();
    let expected: Vec<Vec<u8>> = (0u16..400).map(|i| i.to_be_bytes().to_vec()).collect();
    assert_eq!(keys, expected, "whole partition in clustering byte order");

    drop_database(&database).await;
}
