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

//! Cross-crate guard for the Mongo document wire contract: objects written by
//! THIS crate's `MongoDbStorage` (the production archive writer, including
//! its 15 MiB chunking into `_chunk_{i}` side documents) must be byte-exactly
//! range-readable through chain-data's `MongoExternalBlobReader` mirror.
//!
//! Run: `docker run -p 27017:27017 mongo`, then
//! `CHAIN_DATA_MONGO_TEST_URL=mongodb://127.0.0.1:27017 \
//!   cargo test -p monad-archive --test chain_data_mongo_format -- --ignored`
#![cfg(feature = "chain-data-ingest")]

use monad_archive::{
    kvstore::{mongo::MongoDbStorage, KVStore, WritePolicy},
    prelude::Metrics,
};
use monad_chain_data::{
    store::{MongoExternalBlobReader, MongoExternalBlobReaderConfig},
    ExternalBlobReader,
};

/// monad-archive's Mongo chunk threshold (`mongo::CHUNK_SIZE`).
const CHUNK_SIZE: usize = 15 * 1024 * 1024;

fn test_url() -> Option<String> {
    std::env::var("CHAIN_DATA_MONGO_TEST_URL").ok()
}

fn patterned(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

async fn read(
    reader: &MongoExternalBlobReader,
    key: &str,
    start: usize,
    end: usize,
) -> monad_chain_data::error::Result<Option<bytes::Bytes>> {
    reader.read_range(key.as_bytes(), start, end).await
}

#[tokio::test]
#[ignore = "requires CHAIN_DATA_MONGO_TEST_URL (a running MongoDB)"]
async fn archive_written_objects_range_read_through_chain_data() {
    let Some(url) = test_url() else {
        return;
    };
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let database = format!("chain_data_fmt_{nanos}");

    // The production archive block-store writer (collection `block_level`).
    let store = MongoDbStorage::new_block_store(&url, &database, Metrics::none())
        .await
        .expect("block store");

    // One unchunked object and one that crosses the chunk threshold (two
    // chunks: 15 MiB + 1 MiB), patterned so any offset slip is visible.
    let small = patterned(100_000);
    let big = patterned(CHUNK_SIZE + (1 << 20));
    store
        .put("block/000000000007", small.clone(), WritePolicy::NoClobber)
        .await
        .expect("put small");
    store
        .put("traces/000000000007", big.clone(), WritePolicy::NoClobber)
        .await
        .expect("put big");

    let reader = MongoExternalBlobReader::new(MongoExternalBlobReaderConfig::new(
        url.clone(),
        database.clone(),
        "block_level",
    ))
    .await
    .expect("reader");

    // Unchunked object: interior slice, EOF clamp, past-EOF error, missing.
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

    // Chunked object: slices inside each chunk, one straddling the chunk
    // boundary, EOF clamp into the short last chunk, past-EOF error.
    let ranges = [
        (0, 4096),
        (CHUNK_SIZE - 100, CHUNK_SIZE + 100),
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

    // Whole-object reads agree with the archive's own reader.
    let whole = read(&reader, "traces/000000000007", 0, big.len())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(whole.as_ref(), big.as_slice());

    mongodb::Client::with_uri_str(&url)
        .await
        .expect("client")
        .database(&database)
        .drop()
        .await
        .expect("drop db");
}
