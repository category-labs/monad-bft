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

//! Read-only [`ExternalBlobReader`] over a monad-archive MongoDB block store,
//! so external-archive chain-data stores can range-read row payloads straight
//! out of the operator's existing `block_level` collection.
//!
//! The document schema mirrors monad-archive's `KeyValueDocument` (a wire
//! contract; the cross-crate round-trip test in monad-archive is the guard):
//!
//! - `{_id: <archive key>, value: Binary}` for objects up to the 15 MiB
//!   chunk threshold, and
//! - `{_id: <archive key>, chunks: N}` plus side documents
//!   `{_id: "<key>_chunk_<i>", value: Binary}` (decimal `i`, 15 MiB chunks,
//!   only the last may be short) above it.
//!
//! MongoDB cannot serve byte sub-ranges of a `Binary` field, so a range read
//! fetches the covering document(s) whole and slices; chain-data's row cache
//! (which caches every decoded row of a fetched container) absorbs the
//! repeat-read cost.

use std::collections::BTreeMap;

use bytes::Bytes;
use futures::{future::BoxFuture, TryStreamExt};
use mongodb::{
    bson::{doc, Binary},
    options::ClientOptions,
    Client, Collection,
};
use serde::{Deserialize, Serialize};

use crate::{
    error::{MonadChainDataError, Result},
    external::ExternalBlobReader,
};

/// monad-archive's Mongo chunk threshold/size (its `CHUNK_SIZE`): values
/// above it are split into `_chunk_{i}` documents of exactly this many bytes
/// (last chunk excepted). Part of the document wire contract.
pub const ARCHIVE_CHUNK_SIZE: usize = 15 * 1024 * 1024;

const DEFAULT_MAX_POOL_SIZE: u32 = 64;

#[derive(Debug, Clone)]
pub struct MongoExternalBlobReaderConfig {
    pub connection_string: String,
    pub database: String,
    /// The archive's block-data collection.
    pub collection: String,
    pub max_pool_size: u32,
}

impl MongoExternalBlobReaderConfig {
    pub fn new(
        connection_string: impl Into<String>,
        database: impl Into<String>,
        collection: impl Into<String>,
    ) -> Self {
        Self {
            connection_string: connection_string.into(),
            database: database.into(),
            collection: collection.into(),
            max_pool_size: DEFAULT_MAX_POOL_SIZE,
        }
    }
}

/// Mirror of monad-archive's `KeyValueDocument`.
#[derive(Debug, Serialize, Deserialize)]
struct ArchiveDocument {
    #[serde(rename = "_id")]
    id: String,
    value: Option<Binary>,
    chunks: Option<u32>,
}

pub struct MongoExternalBlobReader {
    collection: Collection<ArchiveDocument>,
}

fn backend(context: &str, error: impl std::fmt::Display) -> MonadChainDataError {
    MonadChainDataError::Backend(format!("mongo archive {context}: {error}"))
}

fn chunk_doc_id(key: &str, index: usize) -> String {
    format!("{key}_chunk_{index}")
}

/// Slices `[start, end_exclusive)` out of one whole object, with the
/// [`crate::store::BlobStore::read_range`] contract: end clamps to EOF, a
/// start strictly past EOF (or past the end bound) is an error.
fn slice_range(object: &Bytes, start: usize, end_exclusive: usize) -> Result<Bytes> {
    if start > end_exclusive || start > object.len() {
        return Err(MonadChainDataError::Decode("invalid blob range"));
    }
    Ok(object.slice(start..end_exclusive.min(object.len())))
}

/// Chunk indices that must be fetched to serve `[start, end_exclusive)` of a
/// `chunk_count`-chunk object, or `None` when the range is satisfiable as
/// empty without any fetch. When `start` lands at/after the last chunk's
/// nominal offset the last chunk is always included, so the assembler can
/// learn the object's total length for its EOF check.
fn covering_chunks(
    start: usize,
    end_exclusive: usize,
    chunk_count: usize,
    chunk_size: usize,
) -> Result<Option<std::ops::RangeInclusive<usize>>> {
    if start > end_exclusive {
        return Err(MonadChainDataError::Decode("invalid blob range"));
    }
    let last = chunk_count - 1;
    let first_wanted = start / chunk_size;
    if start == end_exclusive {
        // Empty read: no bytes needed, but a start past the last chunk's
        // nominal offset still needs the EOF check below.
        if first_wanted < last || (first_wanted == last && start == last * chunk_size) {
            return Ok(None);
        }
        return Ok(Some(last..=last));
    }
    let last_wanted = (end_exclusive - 1) / chunk_size;
    Ok(Some(first_wanted.min(last)..=last_wanted.min(last)))
}

/// Concatenates `[start, end_exclusive)` from the fetched covering chunks.
/// Every non-last chunk must be exactly `chunk_size` bytes (the archive's
/// writer invariant); the last chunk's length defines the object's total
/// length for EOF clamping and the past-EOF start check.
fn assemble_chunked_range(
    fetched: &BTreeMap<usize, Bytes>,
    chunk_count: usize,
    chunk_size: usize,
    start: usize,
    end_exclusive: usize,
) -> Result<Bytes> {
    let last = chunk_count - 1;
    for (&index, chunk) in fetched {
        if index < last && chunk.len() != chunk_size {
            return Err(MonadChainDataError::Backend(format!(
                "mongo archive chunk {index} has {} bytes, expected {chunk_size}",
                chunk.len()
            )));
        }
    }
    if let Some(last_chunk) = fetched.get(&last) {
        let total = last * chunk_size + last_chunk.len();
        if start > total {
            return Err(MonadChainDataError::Decode("invalid blob range"));
        }
    }
    let mut out = Vec::new();
    for (&index, chunk) in fetched {
        let chunk_start = index * chunk_size;
        let lo = start.saturating_sub(chunk_start).min(chunk.len());
        let hi = end_exclusive.saturating_sub(chunk_start).min(chunk.len());
        if lo < hi {
            out.extend_from_slice(&chunk[lo..hi]);
        }
    }
    Ok(Bytes::from(out))
}

impl MongoExternalBlobReader {
    pub async fn new(config: MongoExternalBlobReaderConfig) -> Result<Self> {
        let mut options = ClientOptions::parse(&config.connection_string)
            .await
            .map_err(|e| backend("connection string", e))?;
        options.max_pool_size = Some(config.max_pool_size);
        let client = Client::with_options(options).map_err(|e| backend("client", e))?;
        let collection = client
            .database(&config.database)
            .collection(&config.collection);
        Ok(Self { collection })
    }

    async fn read_range_at(
        &self,
        key: &str,
        start: usize,
        end_exclusive: usize,
    ) -> Result<Option<Bytes>> {
        let Some(document) = self
            .collection
            .find_one(doc! { "_id": key })
            .await
            .map_err(|e| backend("get", e))?
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
                let Some(fetch) =
                    covering_chunks(start, end_exclusive, chunk_count, ARCHIVE_CHUNK_SIZE)?
                else {
                    return Ok(Some(Bytes::new()));
                };
                let ids: Vec<String> = fetch.clone().map(|i| chunk_doc_id(key, i)).collect();
                let mut fetched: BTreeMap<usize, Bytes> = BTreeMap::new();
                let mut cursor = self
                    .collection
                    .find(doc! { "_id": { "$in": &ids } })
                    .await
                    .map_err(|e| backend("chunk fetch", e))?;
                while let Some(chunk_doc) = cursor
                    .try_next()
                    .await
                    .map_err(|e| backend("chunk cursor", e))?
                {
                    let index: usize = chunk_doc
                        .id
                        .rsplit('_')
                        .next()
                        .and_then(|s| s.parse().ok())
                        .ok_or_else(|| {
                            MonadChainDataError::Backend(format!(
                                "mongo archive chunk id {} is malformed",
                                chunk_doc.id
                            ))
                        })?;
                    let value = chunk_doc.value.ok_or_else(|| {
                        MonadChainDataError::Backend(format!(
                            "mongo archive chunk {index} of {key} has no value"
                        ))
                    })?;
                    fetched.insert(index, Bytes::from(value.bytes));
                }
                for index in fetch {
                    if !fetched.contains_key(&index) {
                        return Err(MonadChainDataError::Backend(format!(
                            "mongo archive chunk {index} of {key} is missing"
                        )));
                    }
                }
                Ok(Some(assemble_chunked_range(
                    &fetched,
                    chunk_count,
                    ARCHIVE_CHUNK_SIZE,
                    start,
                    end_exclusive,
                )?))
            }
            _ => Err(MonadChainDataError::Backend(format!(
                "mongo archive document {key} has neither value nor chunks"
            ))),
        }
    }
}

impl ExternalBlobReader for MongoExternalBlobReader {
    fn read_range(
        &self,
        key: &[u8],
        start: usize,
        end_exclusive: usize,
    ) -> BoxFuture<'_, Result<Option<Bytes>>> {
        let key = std::str::from_utf8(key).map(str::to_owned);
        Box::pin(async move {
            let key = key.map_err(|_| {
                MonadChainDataError::Decode("external archive key is not valid utf-8")
            })?;
            self.read_range_at(&key, start, end_exclusive).await
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Reference implementation of the chunked range plan + assembly against
    /// a whole in-memory object, exercising the same code paths the live
    /// reader uses after its document fetches.
    fn read_via_chunks(
        object: &[u8],
        chunk_size: usize,
        start: usize,
        end_exclusive: usize,
    ) -> Result<Bytes> {
        let chunks: Vec<Bytes> = object
            .chunks(chunk_size)
            .map(|c| Bytes::copy_from_slice(c))
            .collect();
        assert!(chunks.len() > 1, "test object must actually chunk");
        let Some(fetch) = covering_chunks(start, end_exclusive, chunks.len(), chunk_size)? else {
            return Ok(Bytes::new());
        };
        let fetched: BTreeMap<usize, Bytes> = fetch
            .filter(|&i| i < chunks.len())
            .map(|i| (i, chunks[i].clone()))
            .collect();
        assemble_chunked_range(&fetched, chunks.len(), chunk_size, start, end_exclusive)
    }

    /// Every (start, end) pair over a 3.5-chunk object must agree with the
    /// trivial whole-object slice under the read_range contract.
    #[test]
    fn chunked_reads_match_whole_object_slices() {
        const CS: usize = 8;
        let object: Vec<u8> = (0..28u8).collect(); // chunks of 8,8,8,4
        for start in 0..=object.len() + 2 {
            for end in start..=object.len() + 4 {
                let direct = if start > object.len() {
                    None
                } else {
                    Some(&object[start..end.min(object.len())])
                };
                match (read_via_chunks(&object, CS, start, end), direct) {
                    (Ok(bytes), Some(expected)) => {
                        assert_eq!(bytes.as_ref(), expected, "range {start}..{end}");
                    }
                    (Err(_), None) => {}
                    (got, _) => panic!("range {start}..{end}: {got:?}"),
                }
            }
        }
        // start > end_exclusive is always an error.
        assert!(read_via_chunks(&object, CS, 5, 4).is_err());
    }

    /// The fetch plan never reads more chunks than the range needs: a range
    /// inside one chunk fetches exactly that chunk.
    #[test]
    fn covering_chunks_is_minimal() {
        assert_eq!(covering_chunks(0, 4, 4, 8).unwrap(), Some(0..=0));
        assert_eq!(covering_chunks(9, 15, 4, 8).unwrap(), Some(1..=1));
        assert_eq!(covering_chunks(7, 9, 4, 8).unwrap(), Some(0..=1));
        // Ranges past the last chunk clamp to it (EOF clamp + total check).
        assert_eq!(covering_chunks(25, 100, 4, 8).unwrap(), Some(3..=3));
        assert_eq!(covering_chunks(40, 50, 4, 8).unwrap(), Some(3..=3));
        // Empty in-bounds reads fetch nothing.
        assert_eq!(covering_chunks(8, 8, 4, 8).unwrap(), None);
        assert_eq!(covering_chunks(0, 0, 4, 8).unwrap(), None);
        // An empty read past the last chunk's nominal offset still needs the
        // last chunk to learn the total for the past-EOF check.
        assert_eq!(covering_chunks(30, 30, 4, 8).unwrap(), Some(3..=3));
    }

    /// A short non-last chunk is data corruption, not an EOF.
    #[test]
    fn short_interior_chunk_errors() {
        let fetched: BTreeMap<usize, Bytes> =
            [(0usize, Bytes::from(vec![0u8; 5]))].into_iter().collect();
        assert!(assemble_chunked_range(&fetched, 3, 8, 0, 4).is_err());
    }

    #[test]
    fn chunk_ids_match_archive_format() {
        assert_eq!(
            chunk_doc_id("block/000000000123", 0),
            "block/000000000123_chunk_0"
        );
        assert_eq!(
            chunk_doc_id("receipts/000000000123", 11),
            "receipts/000000000123_chunk_11"
        );
    }
}
