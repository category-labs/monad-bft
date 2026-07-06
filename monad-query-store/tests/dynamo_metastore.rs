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

//! Run: `docker run -p 8000:8000 amazon/dynamodb-local`, then
//! `CHAIN_DATA_DYNAMO_TEST_ENDPOINT=http://localhost:8000 \
//!   cargo test -p monad-query-store --features dynamo --test dynamo_metastore -- --ignored`
#![cfg(feature = "dynamo")]

use bytes::Bytes;
use monad_query_store::{
    DynamoCredentials, DynamoMetaStore, DynamoMetaStoreConfig, DynamoTableLayout, MetaStore,
    MetaWriteOp, ScannableTableId, TableId,
};

const KV: TableId = TableId::new("it_kv");
const SCAN: ScannableTableId = ScannableTableId::new("it_scan");

/// `None` (silent skip) when `CHAIN_DATA_DYNAMO_TEST_ENDPOINT` is unset.
async fn fresh_store(test: &str) -> Option<DynamoMetaStore> {
    let endpoint = std::env::var("CHAIN_DATA_DYNAMO_TEST_ENDPOINT").ok()?;
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let table_name = format!("chain-data-it-{test}-{nanos}");
    let config = DynamoMetaStoreConfig {
        table_layout: DynamoTableLayout::single(table_name),
        endpoint_urls: vec![endpoint],
        region: Some("us-east-1".to_string()),
        profile: None,
        batch_max_concurrency: 8,
        batch_table_max_concurrency: 8,
        batch_write_max_items: 25,
        credentials: Some(DynamoCredentials {
            access_key_id: "test".to_string(),
            secret_access_key: "test".to_string(),
            session_token: None,
        }),
    };
    let store = DynamoMetaStore::new(config).await.expect("build store");
    store.create_table().await.expect("create table");
    Some(store)
}

#[tokio::test]
#[ignore = "requires CHAIN_DATA_DYNAMO_TEST_ENDPOINT (DynamoDB Local / Alternator)"]
async fn get_put_round_trip() {
    let Some(store) = fresh_store("getput").await else {
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

    assert_eq!(store.scan_get(SCAN, b"p", b"c").await.unwrap(), None);
    store
        .scan_put(SCAN, b"p", b"c", Bytes::from_static(b"sv"))
        .await
        .unwrap();
    assert_eq!(
        store.scan_get(SCAN, b"p", b"c").await.unwrap(),
        Some(Bytes::from_static(b"sv"))
    );
}

#[tokio::test]
#[ignore = "requires CHAIN_DATA_DYNAMO_TEST_ENDPOINT (DynamoDB Local / Alternator)"]
async fn scan_keys_drains_whole_partition() {
    let Some(store) = fresh_store("scankeys").await else {
        return;
    };

    let part = b"part";
    let value = Bytes::from(vec![0u8; 4096]);
    let mut ops: Vec<MetaWriteOp> = (0u16..400)
        .map(|i| MetaWriteOp::ScanPut {
            table: SCAN,
            partition: part.to_vec(),
            clustering: i.to_be_bytes().to_vec(),
            value: value.clone(),
        })
        .collect();
    ops.push(MetaWriteOp::ScanPut {
        table: SCAN,
        partition: b"other".to_vec(),
        clustering: vec![0, 0],
        value: Bytes::from_static(b"x"),
    });
    store.apply_writes(ops).await.unwrap();

    let keys = store.scan_keys(SCAN, part).await.unwrap();
    let expected: Vec<Vec<u8>> = (0u16..400).map(|i| i.to_be_bytes().to_vec()).collect();
    assert_eq!(
        keys, expected,
        "whole partition drained in clustering order"
    );
}

#[tokio::test]
#[ignore = "requires CHAIN_DATA_DYNAMO_TEST_ENDPOINT (DynamoDB Local / Alternator)"]
async fn apply_writes_crosses_batch_boundary() {
    let Some(store) = fresh_store("applywrites").await else {
        return;
    };

    // 60 ops > 25-item limit forces multiple BatchWriteItem chunks.
    let ops: Vec<MetaWriteOp> = (0u32..60)
        .map(|i| MetaWriteOp::Put {
            table: KV,
            key: i.to_be_bytes().to_vec(),
            value: Bytes::from(vec![i as u8]),
        })
        .collect();
    store.apply_writes(ops).await.unwrap();

    for i in 0u32..60 {
        assert_eq!(
            store.get(KV, &i.to_be_bytes()).await.unwrap(),
            Some(Bytes::from(vec![i as u8])),
            "row {i} survived the batched write"
        );
    }

    store.apply_writes(vec![]).await.unwrap();
}
