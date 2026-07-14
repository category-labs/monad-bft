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

//! Byte-exactness guard for the Dynamo external read path against hand-written
//! `_chunk_{i}` fixtures: monad-archive's writer no longer chunks, but the
//! read path here must keep decoding the production wire format.

use std::{sync::Arc, time::Duration};

use aws_config::{BehaviorVersion, Region};
use aws_sdk_dynamodb::{
    client::Waiters,
    config::{Credentials, SharedCredentialsProvider},
    types::{
        AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType,
        ScalarAttributeType,
    },
    Client,
};
use monad_query_config::{ChainDataArchiveBackendConfig, ChainDataArchiveDynamoConfig, Redacted};
use monad_query_indexer::chain_data_external::build_archive_external_reader;
use monad_query_primitives::ExternalBlobReader;

/// Partition key attribute name of a monad-archive block-data table.
const PARTITION_KEY: &str = "tx_hash";

/// monad-archive's former Dynamo chunk threshold (`dynamodb::CHUNK_SIZE`,
/// removed along with the chunking writer itself).
const CHUNK_SIZE: usize = 350 * 1024;

fn alternator_endpoint() -> Option<String> {
    std::env::var("SCYLLA_ALTERNATOR_ENDPOINT").ok()
}

fn patterned(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

fn chunk_key(key: &str, index: usize) -> String {
    format!("{key}_chunk_{index}")
}

async fn alternator_client(endpoint: &str) -> Client {
    let sdk_config = aws_config::defaults(BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .endpoint_url(endpoint)
        .credentials_provider(SharedCredentialsProvider::new(Credentials::new(
            "archive",
            "archive",
            None,
            None,
            "chain-data-archive-test",
        )))
        .load()
        .await;
    Client::new(&sdk_config)
}

/// Creates the backing table (partition key [`PARTITION_KEY`], on-demand
/// billing) if it does not already exist, then waits until it is active.
async fn ensure_table(client: &Client, table: &str) {
    let existing = client.list_tables().send().await.expect("list tables");
    if existing.table_names().contains(&table.to_string()) {
        return;
    }
    client
        .create_table()
        .table_name(table)
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name(PARTITION_KEY)
                .attribute_type(ScalarAttributeType::S)
                .build()
                .expect("attribute definition"),
        )
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name(PARTITION_KEY)
                .key_type(KeyType::Hash)
                .build()
                .expect("key schema"),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await
        .expect("create table");
    client
        .wait_until_table_exists()
        .table_name(table)
        .wait(Duration::from_secs(30))
        .await
        .expect("table did not become active");
}

/// Writes an unchunked item: `{tx_hash: S(key), data: B(bytes)}`.
async fn put_item(client: &Client, table: &str, key: &str, data: &[u8]) {
    client
        .put_item()
        .table_name(table)
        .item(PARTITION_KEY, AttributeValue::S(key.to_string()))
        .item("data", AttributeValue::B(data.to_vec().into()))
        .send()
        .await
        .expect("put item");
}

/// Writes a chunked item: one `{key}_chunk_{i}` item per [`CHUNK_SIZE`]-sized
/// slice, plus a manifest item under `key` carrying a `chunks` count instead
/// of `data`. Mirrors the removed writer's wire format exactly.
async fn put_chunked_item(client: &Client, table: &str, key: &str, data: &[u8]) {
    for (index, chunk) in data.chunks(CHUNK_SIZE).enumerate() {
        put_item(client, table, &chunk_key(key, index), chunk).await;
    }
    client
        .put_item()
        .table_name(table)
        .item(PARTITION_KEY, AttributeValue::S(key.to_string()))
        .item(
            "chunks",
            AttributeValue::N(data.len().div_ceil(CHUNK_SIZE).to_string()),
        )
        .send()
        .await
        .expect("put chunk manifest");
}

async fn read(
    reader: &Arc<dyn ExternalBlobReader>,
    key: &str,
    start: usize,
    end: usize,
) -> monad_query_errors::Result<Option<bytes::Bytes>> {
    reader.read_range(key.as_bytes(), start, end).await
}

#[tokio::test]
#[ignore = "requires SCYLLA_ALTERNATOR_ENDPOINT (a running ScyllaDB Alternator)"]
async fn archive_written_items_range_read_through_chain_data() {
    let Some(endpoint) = alternator_endpoint() else {
        return;
    };
    let table = "fmtguard".to_string();
    let client = alternator_client(&endpoint).await;
    ensure_table(&client, &table).await;

    // One unchunked item and one that crosses the chunk threshold (three
    // chunks: 350 KB + 350 KB + tail), patterned so any offset slip shows.
    let small = patterned(100_000);
    let big = patterned(CHUNK_SIZE * 2 + 12_345);
    put_item(&client, &table, "block/000000000007", &small).await;
    put_chunked_item(&client, &table, "traces/000000000007", &big).await;

    // Read through the crate's configured entry point (what production calls)
    // so the test tracks whatever internal store shape backs it.
    let reader = build_archive_external_reader(&Some(ChainDataArchiveBackendConfig::Dynamo(
        ChainDataArchiveDynamoConfig {
            table: Some(table.clone()),
            endpoint_url: Some(endpoint.clone()),
            region: None,
            profile: None,
            access_key_id: Redacted(None),
            secret_access_key: Redacted(None),
            session_token: Redacted(None),
            max_concurrency: 16,
        },
    )))
    .await
    .expect("build external reader")
    .expect("dynamo external reader");

    // Unchunked item: interior slices, EOF clamp, past-EOF error, missing.
    for (start, end) in [(0, 64), (99_990, 100_000), (1234, 56_789)] {
        let got = read(&reader, "block/000000000007", start, end)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            got.as_ref(),
            &small[start..end],
            "small range {start}..{end}"
        );
    }
    let clamped = read(&reader, "block/000000000007", 99_000, 200_000)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(clamped.as_ref(), &small[99_000..]);
    assert!(read(&reader, "block/000000000007", 100_001, 100_002)
        .await
        .is_err());
    assert!(read(&reader, "block/000000000999", 0, 10)
        .await
        .unwrap()
        .is_none());

    // Chunked item: slices inside each chunk, straddling both chunk
    // boundaries, EOF clamp into the short last chunk, past-EOF error.
    let ranges = [
        (0, 4096),
        (CHUNK_SIZE - 100, CHUNK_SIZE + 100),
        (CHUNK_SIZE * 2 - 50, CHUNK_SIZE * 2 + 50),
        (CHUNK_SIZE + 1000, CHUNK_SIZE + 2000),
        (big.len() - 10, big.len()),
    ];
    for (start, end) in ranges {
        let got = read(&reader, "traces/000000000007", start, end)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(got.as_ref(), &big[start..end], "big range {start}..{end}");
    }
    let clamped = read(
        &reader,
        "traces/000000000007",
        big.len() - 5,
        big.len() + 100,
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(clamped.as_ref(), &big[big.len() - 5..]);
    assert!(
        read(&reader, "traces/000000000007", big.len() + 1, big.len() + 2)
            .await
            .is_err()
    );

    // Whole-object read agrees with the raw written bytes.
    let whole = read(&reader, "traces/000000000007", 0, big.len())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(whole.as_ref(), big.as_slice());
}
