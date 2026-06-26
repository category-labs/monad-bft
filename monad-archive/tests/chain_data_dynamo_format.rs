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

//! Guard for the Dynamo external read path: objects written by the
//! production `DynamoDBArchive` writer (including its 350 KB chunking into
//! `_chunk_{i}` items) must be byte-exactly range-readable through the
//! `ArchiveExternalReader` adapter chain-data consumes (covering-chunk
//! fetches only, `ExternalBlobReader` range semantics).
//!
//! Run against a running Alternator (see `kvstore::dynamodb` tests), then
//! `SCYLLA_ALTERNATOR_ENDPOINT=http://localhost:8000 \
//!   cargo test -p monad-archive --test chain_data_dynamo_format -- --ignored`
#![cfg(feature = "chain-data-ingest")]

use monad_archive::{
    chain_data_external::ArchiveExternalReader,
    cli::ScyllaCliArgs,
    kvstore::{dynamodb::CHUNK_SIZE, KVStore, WritePolicy},
    prelude::Metrics,
};
use monad_query_primitives::ExternalBlobReader;

fn alternator_endpoint() -> Option<String> {
    std::env::var("SCYLLA_ALTERNATOR_ENDPOINT").ok()
}

fn patterned(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

async fn read(
    reader: &ArchiveExternalReader,
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

    // The production archive writer over the scylla backend's block table.
    let scylla = ScyllaCliArgs::parse(&format!("{endpoint} fmtguard")).expect("scylla args");
    let store = scylla
        .block_store(&Metrics::none())
        .await
        .expect("block store");

    // One unchunked item and one that crosses the chunk threshold (three
    // chunks: 350 KB + 350 KB + tail), patterned so any offset slip shows.
    let small = patterned(100_000);
    let big = patterned(CHUNK_SIZE * 2 + 12_345);
    store
        .put("block/000000000007", small.clone(), WritePolicy::NoClobber)
        .await
        .expect("put small");
    store
        .put("traces/000000000007", big.clone(), WritePolicy::NoClobber)
        .await
        .expect("put big");

    let reader =
        ArchiveExternalReader::Dynamo(scylla.block_store(&Metrics::none()).await.expect("reader"));

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

    // Whole-object read agrees with the archive's own reader.
    let whole = read(&reader, "traces/000000000007", 0, big.len())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(whole.as_ref(), big.as_slice());
}
