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
    store::blob::external_chunks::{
        archive_chunk_key, assemble_chunked_range, covering_chunks, slice_range,
    },
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
                let ids: Vec<String> = fetch.clone().map(|i| archive_chunk_key(key, i)).collect();
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
