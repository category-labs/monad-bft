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

//! Byte-range reads over the archive's Mongo/Dynamo block-data formats:
//! monad-archive's storage structs expose no range-read API, so these free
//! functions mirror its `kvstore::mongo` / `kvstore::dynamodb` formats.

use std::{
    collections::{BTreeMap, HashMap},
    time::Duration,
};

use aws_sdk_dynamodb::{
    types::{AttributeValue, KeysAndAttributes},
    Client as DynamoClient,
};
use bytes::Bytes;
use eyre::{bail, eyre, Context, Result};
use futures::TryStreamExt;
use monad_archive::kvstore::mongo::KeyValueDocument;
use mongodb::{bson::doc, options::ClientOptions, Client as MongoClient, Collection};

use crate::chunked_range::{assemble_chunked_range, covering_chunks, slice_range};

/// Values above this size are split into `{_id}_chunk_{i}` side documents,
/// with the main document carrying a `chunks` count (15 MiB).
const MONGO_CHUNK_SIZE: usize = 1024 * 1024 * 15;
/// `maxTimeMS` for reader queries (archive default).
const MONGO_MAX_TIME_GET: Duration = Duration::from_secs(5);

/// Values above this size are split into `{key}_chunk_{i}` items, with the
/// main item carrying a `chunks` count instead of `data` (350 KB).
const DYNAMO_CHUNK_SIZE: usize = 350 * 1024;
/// Partition-key attribute name (`tx_hash`, reused verbatim for block data).
const DYNAMO_PARTITION_KEY: &str = "tx_hash";
/// Attribute holding a chunked value's chunk count.
const DYNAMO_CHUNKS_ATTR: &str = "chunks";
/// Max keys per `BatchGetItem` request.
const DYNAMO_READ_BATCH_SIZE: usize = 100;

/// Key of one chunk entry, shared by both backends (`{key}_chunk_{i}`).
fn chunk_key(key: &str, index: usize) -> String {
    format!("{key}_chunk_{index}")
}

/// Read-only Mongo collection handle for the archive block-data format: no
/// collection provisioning (safe for read-only credentials/replicas) and an
/// explicit connection-pool size.
pub async fn mongo_reader_collection(
    connection_string: &str,
    database: &str,
    collection_name: &str,
    max_pool_size: u32,
) -> Result<Collection<KeyValueDocument>> {
    let mut client_options = ClientOptions::parse(connection_string).await?;
    client_options.max_pool_size = Some(max_pool_size);
    client_options.connect_timeout = Some(Duration::from_secs(1));
    let client = MongoClient::with_options(client_options)?;
    Ok(client.database(database).collection(collection_name))
}

/// Reads `[start, end_exclusive)` of `key`, fetching only the covering
/// `_chunk_{i}` side documents for chunked values. `Ok(None)` when absent;
/// the end clamps to EOF; a start strictly past EOF is an error.
pub async fn mongo_read_range(
    collection: &Collection<KeyValueDocument>,
    key: &str,
    start: usize,
    end_exclusive: usize,
) -> Result<Option<Bytes>> {
    let Some(document) = collection
        .find_one(doc! { "_id": key })
        .max_time(MONGO_MAX_TIME_GET)
        .await
        .wrap_err("MongoDB read_range lookup failed")?
    else {
        return Ok(None);
    };
    match (document.value, document.chunks) {
        (Some(value), None) => Ok(Some(slice_range(
            &Bytes::from(value.bytes),
            start,
            end_exclusive,
        )?)),
        (None, Some(chunks)) if chunks > 0 => {
            let chunk_count = chunks as usize;
            let Some(fetch) = covering_chunks(start, end_exclusive, chunk_count, MONGO_CHUNK_SIZE)?
            else {
                return Ok(Some(Bytes::new()));
            };
            let ids: Vec<String> = fetch.clone().map(|i| chunk_key(key, i)).collect();
            let mut fetched: BTreeMap<usize, Bytes> = BTreeMap::new();
            let mut cursor = collection
                .find(doc! { "_id": { "$in": &ids } })
                .max_time(MONGO_MAX_TIME_GET)
                .await
                .wrap_err("MongoDB chunk fetch failed")?;
            while let Some(chunk_doc) = cursor.try_next().await? {
                let index: usize = chunk_doc
                    ._id
                    .rsplit('_')
                    .next()
                    .and_then(|s| s.parse().ok())
                    .ok_or_else(|| eyre!("malformed chunk id {}", chunk_doc._id))?;
                let value = chunk_doc
                    .value
                    .ok_or_else(|| eyre!("chunk {index} of {key} has no value"))?;
                fetched.insert(index, Bytes::from(value.bytes));
            }
            for index in fetch {
                if !fetched.contains_key(&index) {
                    bail!("chunk {index} of {key} is missing");
                }
            }
            Ok(Some(assemble_chunked_range(
                &fetched,
                chunk_count,
                MONGO_CHUNK_SIZE,
                start,
                end_exclusive,
            )?))
        }
        _ => bail!("document {key} has neither value nor chunks"),
    }
}

/// One stored Dynamo item, before chunk resolution.
enum RawItem {
    Value(Bytes),
    /// A chunked value's main item: the number of `{key}_chunk_{i}` items.
    Chunked(usize),
}

/// Reads `[start, end_exclusive)` of `key`, fetching only the covering
/// `{key}_chunk_{i}` items for chunked values. `Ok(None)` when absent;
/// the end clamps to EOF; a start strictly past EOF is an error.
pub async fn dynamo_read_range(
    client: &DynamoClient,
    table: &str,
    key: &str,
    start: usize,
    end_exclusive: usize,
) -> Result<Option<Bytes>> {
    let mut raw = dynamo_batch_get_raw(client, table, &[key.to_owned()]).await?;
    match raw.remove(key) {
        None => Ok(None),
        Some(RawItem::Value(bytes)) => Ok(Some(slice_range(&bytes, start, end_exclusive)?)),
        Some(RawItem::Chunked(chunk_count)) => {
            let Some(fetch) =
                covering_chunks(start, end_exclusive, chunk_count, DYNAMO_CHUNK_SIZE)?
            else {
                return Ok(Some(Bytes::new()));
            };
            let chunk_keys: Vec<String> = fetch.clone().map(|i| chunk_key(key, i)).collect();
            let mut raw_chunks = dynamo_batch_get_raw(client, table, &chunk_keys).await?;
            let mut fetched: BTreeMap<usize, Bytes> = Default::default();
            for index in fetch {
                match raw_chunks.remove(&chunk_key(key, index)) {
                    Some(RawItem::Value(bytes)) => {
                        fetched.insert(index, bytes);
                    }
                    _ => bail!("chunk {index} of {key} is missing in table {table}"),
                }
            }
            Ok(Some(assemble_chunked_range(
                &fetched,
                chunk_count,
                DYNAMO_CHUNK_SIZE,
                start,
                end_exclusive,
            )?))
        }
    }
}

async fn dynamo_batch_get_raw(
    client: &DynamoClient,
    table: &str,
    keys: &[String],
) -> Result<HashMap<String, RawItem>> {
    let mut results: HashMap<String, RawItem> = HashMap::new();
    let batches = keys.chunks(DYNAMO_READ_BATCH_SIZE);

    for batch in batches {
        let mut key_maps = Vec::new();
        for key in batch {
            let key = key.trim_start_matches("0x");
            let mut key_map = HashMap::new();
            key_map.insert(
                DYNAMO_PARTITION_KEY.to_string(),
                AttributeValue::S(key.to_string()),
            );
            key_maps.push(key_map);
        }

        let mut request_items = HashMap::new();
        request_items.insert(
            table.to_string(),
            KeysAndAttributes::builder()
                .set_keys(Some(key_maps))
                .build()?,
        );

        let response = client
            .batch_get_item()
            .set_request_items(Some(request_items.clone()))
            .send()
            .await
            .wrap_err_with(|| format!("Request keys (0x stripped in req): {:?}", &batch))?;

        if let Some(mut responses) = response.responses {
            if let Some(items) = responses.remove(table) {
                results.extend(items.into_iter().filter_map(extract_kv_from_map));
            }
        }

        let mut unprocessed_keys = response.unprocessed_keys;
        while let Some(unprocessed) = unprocessed_keys {
            if unprocessed.is_empty() {
                break;
            }
            let response_retry = client
                .batch_get_item()
                .set_request_items(Some(unprocessed.clone()))
                .send()
                .await
                .wrap_err_with(|| "Failed to get unprocessed keys")?;

            if let Some(mut responses_retry) = response_retry.responses {
                if let Some(items) = responses_retry.remove(table) {
                    results.extend(items.into_iter().filter_map(extract_kv_from_map));
                }
            }
            unprocessed_keys = response_retry.unprocessed_keys;
        }
    }

    Ok(results)
}

fn extract_kv_from_map(mut item: HashMap<String, AttributeValue>) -> Option<(String, RawItem)> {
    // Legacy items carried the key in a `key` attribute; fall back to the
    // partition-key attribute (the current schema).
    let key = match item.remove("key") {
        Some(AttributeValue::S(key)) => key,
        _ => match item.remove(DYNAMO_PARTITION_KEY)? {
            AttributeValue::S(key) => key,
            _ => return None,
        },
    };
    if let Some(AttributeValue::B(data)) = item.remove("data") {
        return Some((key, RawItem::Value(Bytes::from(data.into_inner()))));
    }
    if let Some(AttributeValue::N(count)) = item.remove(DYNAMO_CHUNKS_ATTR) {
        return Some((key, RawItem::Chunked(count.parse().ok()?)));
    }
    None
}
