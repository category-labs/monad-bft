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

//! Integration tests for [`DynamoMetaStore`] against a real DynamoDB-API wire.
//!
//! Unlike the S3 blob test (which embeds an in-process `s3s` server), there is
//! no mature pure-Rust in-process DynamoDB server, so these tests target an
//! **external** DynamoDB-API endpoint: either Amazon's DynamoDB Local (the Java
//! jar / `amazon/dynamodb-local` container) or ScyllaDB's Alternator adapter.
//! We deliberately add no heavy/network crates; the tests are gated three ways
//! so the default and CI builds are unaffected:
//!   1. The whole file is `#![cfg(feature = "dynamo")]` -- without the `dynamo`
//!      feature it compiles to an empty test binary.
//!   2. Each test is `#[ignore]`d so it is skipped unless explicitly requested.
//!   3. Each test no-ops (returns early) unless `CHAIN_DATA_DYNAMO_TEST_ENDPOINT`
//!      names a reachable endpoint, so even `--ignored` runs are safe offline.
//!
//! Bring up a local endpoint, e.g.:
//!
//! ```text
//! docker run -p 8000:8000 amazon/dynamodb-local
//! # or a Scylla container with Alternator on :8000
//! ```
//!
//! then run:
//!
//! ```text
//! CHAIN_DATA_DYNAMO_TEST_ENDPOINT=http://localhost:8000 \
//!   cargo test -p monad-chain-data --features dynamo --test dynamo_metastore -- --ignored
//! ```
//!
//! Each test provisions its own uniquely-named table via the opt-in
//! `create_table` helper, so repeated runs against the same endpoint do not
//! interfere.
#![cfg(feature = "dynamo")]

use bytes::Bytes;
use monad_chain_data::store::{
    DynamoCredentials, DynamoMetaStore, DynamoMetaStoreConfig, DynamoTableLayout, MetaStore,
    MetaWriteOp, ScannableTableId, TableId,
};

const KV: TableId = TableId::new("it_kv");
const SCAN: ScannableTableId = ScannableTableId::new("it_scan");

/// Reads the endpoint from the env; `None` means "skip" (offline).
fn endpoint() -> Option<String> {
    std::env::var("CHAIN_DATA_DYNAMO_TEST_ENDPOINT").ok()
}

/// Builds a store against a freshly-created, uniquely-named table. DynamoDB
/// Local / Alternator accept any non-empty static credentials.
async fn fresh_store(test: &str) -> DynamoMetaStore {
    let endpoint = endpoint().expect("endpoint checked by caller");
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
    store
}

#[tokio::test]
#[ignore = "requires CHAIN_DATA_DYNAMO_TEST_ENDPOINT (DynamoDB Local / Alternator)"]
async fn get_put_round_trip() {
    if endpoint().is_none() {
        return;
    }
    let store = fresh_store("getput").await;

    assert_eq!(store.get(KV, b"missing").await.unwrap(), None);
    store.put(KV, b"k", Bytes::from_static(b"v")).await.unwrap();
    assert_eq!(
        store.get(KV, b"k").await.unwrap(),
        Some(Bytes::from_static(b"v"))
    );
    // Overwrite is a plain idempotent put.
    store
        .put(KV, b"k", Bytes::from_static(b"v2"))
        .await
        .unwrap();
    assert_eq!(
        store.get(KV, b"k").await.unwrap(),
        Some(Bytes::from_static(b"v2"))
    );

    // Scannable point get/put.
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
async fn scan_list_paginates_with_cursor() {
    if endpoint().is_none() {
        return;
    }
    let store = fresh_store("scanlist").await;

    // 5 clusterings under one partition, plus a decoy under a sibling prefix
    // and a decoy under a different partition that must never appear.
    let part = b"part";
    for i in 0u8..5 {
        store
            .scan_put(SCAN, part, &[b'a', i], Bytes::from_static(b"x"))
            .await
            .unwrap();
    }
    store
        .scan_put(SCAN, part, b"zz", Bytes::from_static(b"x"))
        .await
        .unwrap();
    store
        .scan_put(SCAN, b"other", &[b'a', 0], Bytes::from_static(b"x"))
        .await
        .unwrap();

    // Page through the `a*` prefix two at a time, following next_cursor.
    let mut seen: Vec<Vec<u8>> = Vec::new();
    let mut cursor: Option<Vec<u8>> = None;
    loop {
        let page = store
            .scan_list(SCAN, part, b"a", cursor.clone(), 2)
            .await
            .unwrap();
        seen.extend(page.keys.iter().cloned());
        match page.next_cursor {
            Some(c) => cursor = Some(c),
            None => break,
        }
    }

    let expected: Vec<Vec<u8>> = (0u8..5).map(|i| vec![b'a', i]).collect();
    assert_eq!(seen, expected, "prefix scan returns all `a*` keys in order");
}

#[tokio::test]
#[ignore = "requires CHAIN_DATA_DYNAMO_TEST_ENDPOINT (DynamoDB Local / Alternator)"]
async fn apply_writes_crosses_batch_boundary() {
    if endpoint().is_none() {
        return;
    }
    let store = fresh_store("applywrites").await;

    // 60 ops > 25 forces multiple BatchWriteItem chunks.
    let mut ops = Vec::new();
    for i in 0u32..60 {
        ops.push(MetaWriteOp::Put {
            table: KV,
            key: i.to_be_bytes().to_vec(),
            value: Bytes::from(vec![i as u8]),
        });
    }
    store.apply_writes(ops).await.unwrap();

    for i in 0u32..60 {
        assert_eq!(
            store.get(KV, &i.to_be_bytes()).await.unwrap(),
            Some(Bytes::from(vec![i as u8])),
            "row {i} survived the batched write"
        );
    }

    // Empty batch is a no-op fast path.
    store.apply_writes(vec![]).await.unwrap();
}
