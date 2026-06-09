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

//! Integration tests for the chunked [`DynamoBlobStore`] against a real
//! DynamoDB-API wire.
//!
//! Gated identically to the `dynamo_metastore` test (see its module docs):
//!   1. The whole file is `#![cfg(feature = "dynamo")]`.
//!   2. Each test is `#[ignore]`d.
//!   3. Each test no-ops unless `CHAIN_DATA_DYNAMO_TEST_ENDPOINT` names a
//!      reachable DynamoDB Local / ScyllaDB Alternator endpoint.
//!
//! ```text
//! CHAIN_DATA_DYNAMO_TEST_ENDPOINT=http://localhost:8000 \
//!   cargo test -p monad-chain-data --features dynamo --test dynamo_blobstore -- --ignored
//! ```
#![cfg(feature = "dynamo")]

use bytes::Bytes;
use monad_chain_data::store::{
    BlobStore, BlobTableId, BlobWriteOp, DynamoBlobStore, DynamoBlobStoreConfig, DynamoCredentials,
};

const TABLE: BlobTableId = BlobTableId::new("block_blob");

/// Reads the endpoint from the env; `None` means "skip" (offline).
fn endpoint() -> Option<String> {
    std::env::var("CHAIN_DATA_DYNAMO_TEST_ENDPOINT").ok()
}

/// Builds a store against a freshly-created, uniquely-named table with the given
/// chunk size. DynamoDB Local / Alternator accept any non-empty credentials.
async fn fresh_store(test: &str, chunk_size: usize) -> DynamoBlobStore {
    let endpoint = endpoint().expect("endpoint checked by caller");
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let table_name = format!("chain-data-blob-it-{test}-{nanos}");
    let config = DynamoBlobStoreConfig {
        table_name,
        endpoint_url: Some(endpoint),
        region: Some("us-east-1".to_string()),
        profile: None,
        batch_max_concurrency: 8,
        chunk_size,
        credentials: Some(DynamoCredentials {
            access_key_id: "test".to_string(),
            secret_access_key: "test".to_string(),
            session_token: None,
        }),
    };
    let store = DynamoBlobStore::new(config).await.expect("build store");
    store.create_table().await.expect("create table");
    store
}

#[tokio::test]
#[ignore = "requires CHAIN_DATA_DYNAMO_TEST_ENDPOINT (DynamoDB Local / Alternator)"]
async fn put_get_round_trip_single_chunk() {
    if endpoint().is_none() {
        return;
    }
    let store = fresh_store("getput", 64 * 1024).await;

    assert_eq!(store.get_blob(TABLE, b"missing").await.unwrap(), None);
    store
        .put_blob(TABLE, b"k", Bytes::from_static(b"hello"))
        .await
        .unwrap();
    assert_eq!(
        store.get_blob(TABLE, b"k").await.unwrap(),
        Some(Bytes::from_static(b"hello"))
    );
    // Overwrite with a shorter value: the stale tail must not survive (same
    // chunk index range is rewritten; here both fit in chunk 0).
    store
        .put_blob(TABLE, b"k", Bytes::from_static(b"hi"))
        .await
        .unwrap();
    assert_eq!(
        store.get_blob(TABLE, b"k").await.unwrap(),
        Some(Bytes::from_static(b"hi"))
    );
}

#[tokio::test]
#[ignore = "requires CHAIN_DATA_DYNAMO_TEST_ENDPOINT (DynamoDB Local / Alternator)"]
async fn put_get_round_trip_many_chunks() {
    if endpoint().is_none() {
        return;
    }
    // Tiny chunk size forces many chunks (and >25 chunks => multiple
    // BatchWriteItem calls) for a modest value.
    let store = fresh_store("manychunks", 16).await;
    let value: Vec<u8> = (0..1000u32).map(|i| i as u8).collect();
    store
        .put_blob(TABLE, b"big", Bytes::from(value.clone()))
        .await
        .unwrap();
    assert_eq!(
        store.get_blob(TABLE, b"big").await.unwrap(),
        Some(Bytes::from(value))
    );
}

#[tokio::test]
#[ignore = "requires CHAIN_DATA_DYNAMO_TEST_ENDPOINT (DynamoDB Local / Alternator)"]
async fn read_range_spans_chunks_and_matches_trait_contract() {
    if endpoint().is_none() {
        return;
    }
    // chunk_size 4 over "abcdefghij" (len 10): chunks "abcd","efgh","ij".
    let store = fresh_store("readrange", 4).await;
    store
        .put_blob(TABLE, b"k", Bytes::from_static(b"abcdefghij"))
        .await
        .unwrap();

    // A window that straddles the chunk-0/chunk-1 boundary.
    assert_eq!(
        store.read_range(TABLE, b"k", 2, 6).await.unwrap(),
        Some(Bytes::from_static(b"cdef"))
    );
    // End past EOF clamps to the blob length.
    assert_eq!(
        store.read_range(TABLE, b"k", 2, 99).await.unwrap(),
        Some(Bytes::from_static(b"cdefghij"))
    );
    // start == len is the empty tail, not an error.
    assert_eq!(
        store.read_range(TABLE, b"k", 10, 99).await.unwrap(),
        Some(Bytes::new())
    );
    // start > len errors.
    assert_eq!(
        store
            .read_range(TABLE, b"k", 11, 99)
            .await
            .unwrap_err()
            .to_string(),
        "decode error: invalid blob range"
    );
    // Reversed range errors.
    assert_eq!(
        store
            .read_range(TABLE, b"k", 4, 3)
            .await
            .unwrap_err()
            .to_string(),
        "decode error: invalid blob range"
    );
    // Missing object is None, not an error.
    assert_eq!(
        store.read_range(TABLE, b"absent", 0, 1).await.unwrap(),
        None
    );
}

#[tokio::test]
#[ignore = "requires CHAIN_DATA_DYNAMO_TEST_ENDPOINT (DynamoDB Local / Alternator)"]
async fn apply_writes_multiple_blobs() {
    if endpoint().is_none() {
        return;
    }
    // Small chunks so each blob is itself multi-chunk, and many blobs so the
    // flattened put set crosses the 25-item BatchWriteItem boundary.
    let store = fresh_store("applywrites", 8).await;
    let mut ops = Vec::new();
    let mut expected = Vec::new();
    for i in 0u32..20 {
        let value: Vec<u8> = (0..40u32).map(|b| (b + i) as u8).collect();
        ops.push(BlobWriteOp {
            table: TABLE,
            key: i.to_be_bytes().to_vec(),
            value: Bytes::from(value.clone()),
        });
        expected.push((i, value));
    }
    store.apply_writes(ops).await.unwrap();

    for (i, value) in expected {
        assert_eq!(
            store.get_blob(TABLE, &i.to_be_bytes()).await.unwrap(),
            Some(Bytes::from(value)),
            "blob {i} round-trips"
        );
    }
}
