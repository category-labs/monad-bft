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

//! Requires `CHAIN_DATA_DYNAMO_TEST_ENDPOINT` pointing at DynamoDB Local / Alternator; run with
//! `cargo test -p monad-query-store --features dynamo --test dynamo_blobstore -- --ignored`.
#![cfg(feature = "dynamo")]

use bytes::Bytes;
use monad_query_store::{
    BlobStore, BlobTableId, BlobWriteOp, DynamoBlobStore, DynamoBlobStoreConfig, DynamoCredentials,
};

const TABLE: BlobTableId = BlobTableId::new("block_blob");

/// `None` (silent skip) when `CHAIN_DATA_DYNAMO_TEST_ENDPOINT` is unset.
async fn connect_store(table_name: &str, chunk_size: usize) -> Option<DynamoBlobStore> {
    let endpoint = std::env::var("CHAIN_DATA_DYNAMO_TEST_ENDPOINT").ok()?;
    let config = DynamoBlobStoreConfig {
        table_name: table_name.to_string(),
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
    Some(DynamoBlobStore::new(config).await.expect("build store"))
}

fn unique_table_name(test: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock should be after the Unix epoch")
        .as_nanos();
    format!("chain-data-blob-it-{test}-{nanos}")
}

/// `None` (silent skip) when `CHAIN_DATA_DYNAMO_TEST_ENDPOINT` is unset.
async fn fresh_store(test: &str, chunk_size: usize) -> Option<DynamoBlobStore> {
    let store = connect_store(&unique_table_name(test), chunk_size).await?;
    store.create_table().await.expect("create table");
    Some(store)
}

#[tokio::test]
#[ignore = "requires CHAIN_DATA_DYNAMO_TEST_ENDPOINT (DynamoDB Local / Alternator)"]
async fn put_get_round_trip_single_chunk() {
    let Some(store) = fresh_store("getput", 64 * 1024).await else {
        return;
    };

    assert_eq!(store.get_blob(TABLE, b"missing").await.unwrap(), None);
    store
        .put_blob(TABLE, b"k", Bytes::from_static(b"hello"))
        .await
        .unwrap();
    assert_eq!(
        store.get_blob(TABLE, b"k").await.unwrap(),
        Some(Bytes::from_static(b"hello"))
    );
    // Overwriting with a shorter value must not leave a stale tail.
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
    // Tiny chunk size forces >25 chunks => multiple BatchWriteItem calls.
    let Some(store) = fresh_store("manychunks", 16).await else {
        return;
    };
    let blob_data: Vec<u8> = (0..1000u32).map(|i| i as u8).collect();
    store
        .put_blob(TABLE, b"big", Bytes::from(blob_data.clone()))
        .await
        .unwrap();
    assert_eq!(
        store.get_blob(TABLE, b"big").await.unwrap(),
        Some(Bytes::from(blob_data))
    );
}

#[tokio::test]
#[ignore = "requires CHAIN_DATA_DYNAMO_TEST_ENDPOINT (DynamoDB Local / Alternator)"]
async fn overwrite_with_shorter_multi_chunk_payload_has_no_stale_tail() {
    // Tiny chunks so each payload spans several items. `put_blob` only writes
    // the new payload's chunks, and `get_blob` concatenates EVERY item under
    // the pk, so a shorter overwrite that drops to fewer chunks would otherwise
    // read back `new ++ stale_tail`. This is the hazard `SnapshotStore::store`
    // guards with delete-before-put; assert the blob layer alone (no delete)
    // would leak a tail, then that delete_blob + put yields exactly the new
    // payload.
    let Some(store) = fresh_store("shorteroverwrite", 16).await else {
        return;
    };
    let long: Vec<u8> = (0..200u8).collect();
    // Offset by one so the short payload's bytes differ from the long prefix.
    let short: Vec<u8> = (1..=40u8).collect();

    store
        .put_blob(TABLE, b"k", Bytes::from(long.clone()))
        .await
        .unwrap();
    // Bare overwrite leaves stale tail chunks (documents the hazard).
    store
        .put_blob(TABLE, b"k", Bytes::from(short.clone()))
        .await
        .unwrap();
    assert_ne!(
        store.get_blob(TABLE, b"k").await.unwrap(),
        Some(Bytes::from(short.clone())),
        "bare overwrite is expected to leave a stale tail without an explicit delete"
    );

    // delete-before-put is the fix: exactly the short payload, no tail.
    store.delete_blob(TABLE, b"k").await.unwrap();
    store
        .put_blob(TABLE, b"k", Bytes::from(short.clone()))
        .await
        .unwrap();
    assert_eq!(
        store.get_blob(TABLE, b"k").await.unwrap(),
        Some(Bytes::from(short))
    );
}

#[tokio::test]
#[ignore = "requires CHAIN_DATA_DYNAMO_TEST_ENDPOINT (DynamoDB Local / Alternator)"]
async fn read_range_spans_chunks_and_matches_trait_contract() {
    // chunk_size 4 over "abcdefghij" (len 10): chunks "abcd","efgh","ij".
    let Some(store) = fresh_store("readrange", 4).await else {
        return;
    };
    store
        .put_blob(TABLE, b"k", Bytes::from_static(b"abcdefghij"))
        .await
        .unwrap();

    // A window that straddles the chunk-0/chunk-1 boundary.
    assert_eq!(
        store.read_range(TABLE, b"k", 2, 6).await.unwrap(),
        Some(Bytes::from_static(b"cdef"))
    );
    assert_eq!(
        store.read_range(TABLE, b"k", 2, 99).await.unwrap(),
        Some(Bytes::from_static(b"cdefghij"))
    );
    assert_eq!(
        store.read_range(TABLE, b"k", 10, 99).await.unwrap(),
        Some(Bytes::new())
    );
    assert_eq!(
        store
            .read_range(TABLE, b"k", 11, 99)
            .await
            .unwrap_err()
            .to_string(),
        "decode error: invalid blob range"
    );
    assert_eq!(
        store
            .read_range(TABLE, b"k", 4, 3)
            .await
            .unwrap_err()
            .to_string(),
        "decode error: invalid blob range"
    );
    assert_eq!(
        store.read_range(TABLE, b"absent", 0, 1).await.unwrap(),
        None
    );
}

#[tokio::test]
#[ignore = "requires CHAIN_DATA_DYNAMO_TEST_ENDPOINT (DynamoDB Local / Alternator)"]
async fn chunk_size_marker_rejects_mismatched_store() {
    // chunk_size sets the byte->chunk-index mapping, so a store opened with a
    // different size than the table's data silently mis-slices range reads.
    // create_table records the size in a marker item; validate_table (the
    // startup check) must reject a mismatched store, and an idempotent
    // re-provision must surface the mismatch rather than overwrite the marker.
    let table_name = unique_table_name("chunkmarker");
    let Some(writer) = connect_store(&table_name, 64 * 1024).await else {
        return;
    };
    writer.create_table().await.expect("create table");
    writer
        .validate_table()
        .await
        .expect("matching chunk size validates");
    // Re-provisioning with the same size stays idempotent.
    writer.create_table().await.expect("idempotent re-create");

    let mismatched = connect_store(&table_name, 32 * 1024)
        .await
        .expect("endpoint already known to be set");
    let err = mismatched
        .validate_table()
        .await
        .expect_err("mismatched chunk size must fail validation")
        .to_string();
    assert!(err.contains("chunk_size mismatch"), "{err}");
    let err = mismatched
        .create_table()
        .await
        .expect_err("re-provision must not mask the mismatch")
        .to_string();
    assert!(err.contains("chunk_size mismatch"), "{err}");
}

#[tokio::test]
#[ignore = "requires CHAIN_DATA_DYNAMO_TEST_ENDPOINT (DynamoDB Local / Alternator)"]
async fn apply_writes_multiple_blobs() {
    fn blob_value(i: u32) -> Vec<u8> {
        (0..40u32).map(|b| (b + i) as u8).collect()
    }

    // Small chunks + 20 blobs push the flattened put set past the 25-item BatchWriteItem limit.
    let Some(store) = fresh_store("applywrites", 8).await else {
        return;
    };
    let ops: Vec<BlobWriteOp> = (0u32..20)
        .map(|i| BlobWriteOp {
            table: TABLE,
            blob_key: i.to_be_bytes().to_vec(),
            blob_data: Bytes::from(blob_value(i)),
        })
        .collect();
    store.apply_writes(ops).await.unwrap();

    for i in 0u32..20 {
        assert_eq!(
            store.get_blob(TABLE, &i.to_be_bytes()).await.unwrap(),
            Some(Bytes::from(blob_value(i))),
            "blob {i} round-trips"
        );
    }
}

#[tokio::test]
#[ignore = "requires CHAIN_DATA_DYNAMO_TEST_ENDPOINT (DynamoDB Local / Alternator)"]
async fn delete_blob_removes_every_chunk_and_is_idempotent() {
    // Tiny chunk size forces a multi-chunk blob, so the delete must enumerate
    // and remove every chunk item, not just chunk 0.
    let Some(store) = fresh_store("delete", 16).await else {
        return;
    };
    let blob_data: Vec<u8> = (0..200u8).collect();
    store
        .put_blob(TABLE, b"k", Bytes::from(blob_data))
        .await
        .unwrap();

    store.delete_blob(TABLE, b"k").await.unwrap();
    assert_eq!(store.get_blob(TABLE, b"k").await.unwrap(), None);
    assert_eq!(store.read_range(TABLE, b"k", 0, 4).await.unwrap(), None);

    // Deleting a missing key is an idempotent no-op.
    store.delete_blob(TABLE, b"k").await.unwrap();
}
