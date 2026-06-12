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

//! [`MetaStore`] over MongoDB, so chain-data indexes can live in the same
//! database as an operator's existing monad-archive collections (which stay
//! untouched — this store owns one dedicated collection).
//!
//! Every row is one document `{_id: String, v: Binary}`. Keys are encoded as
//! strings because BSON compares `Binary` values length-first, which breaks
//! prefix-range scans over variable-length byte keys; lowercase hex preserves
//! exact unsigned-byte order under MongoDB's binary string comparison:
//!
//! - kv rows:   `k/{table}/{hex(key)}`
//! - scan rows: `s/{table}/{hex(partition)}/{hex(clustering)}`
//!
//! `scan_keys` is an `_id` range scan over `[prefix + "/", prefix + "0")`
//! (`'0'` is `'/' + 1`), served by the mandatory `_id` index — no secondary
//! indexes are needed.
//!
//! Values must fit one document: anything above [`MAX_VALUE_LEN`] errors
//! loudly. The engine already chunks its only unbounded rows (open-streams
//! deltas) far below that.
//!
//! Writes use journaled write concern; reads go to the primary (driver
//! default), so the single-writer ingest reads its own writes back.

use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use mongodb::{
    bson::{doc, spec::BinarySubtype, Binary},
    options::{ClientOptions, CollectionOptions, ReadConcern, WriteConcern},
    Client, Collection,
};
use serde::{Deserialize, Serialize};

use crate::{
    error::{MonadChainDataError, Result},
    store::meta::{MetaStore, MetaWriteOp, ScannableTableId, TableId},
};

/// Hard per-value ceiling: MongoDB caps documents at 16 MiB; leave margin for
/// the `_id` and BSON framing.
pub const MAX_VALUE_LEN: usize = 15 * 1024 * 1024;

/// Concurrent single-document writes per `apply_writes` batch (MongoDB's
/// client-level `bulkWrite` command requires server 8.0+, so batches are
/// fanned out as concurrent `replace_one` upserts instead).
const DEFAULT_WRITE_CONCURRENCY: usize = 64;

const DEFAULT_MAX_POOL_SIZE: u32 = 64;

#[derive(Debug, Clone)]
pub struct MongoMetaStoreConfig {
    /// MongoDB connection string (may embed credentials).
    pub connection_string: String,
    pub database: String,
    /// Collection holding every chain-data meta row.
    pub collection: String,
    pub max_pool_size: u32,
    pub write_concurrency: usize,
}

impl MongoMetaStoreConfig {
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
            write_concurrency: DEFAULT_WRITE_CONCURRENCY,
        }
    }
}

/// One meta row. `v` is optional only so keys-only projections (`scan_keys`)
/// deserialize; rows are always written with a value.
#[derive(Debug, Serialize, Deserialize)]
struct MetaDocument {
    #[serde(rename = "_id")]
    id: String,
    v: Option<Binary>,
}

#[derive(Clone)]
pub struct MongoMetaStore {
    collection: Collection<MetaDocument>,
    write_concurrency: usize,
}

fn backend(context: &str, error: impl std::fmt::Display) -> MonadChainDataError {
    MonadChainDataError::Backend(format!("mongo {context}: {error}"))
}

fn kv_id(table: TableId, key: &[u8]) -> String {
    format!(
        "k/{}/{}",
        table.as_str(),
        alloy_primitives::hex::encode(key)
    )
}

fn scan_id(table: ScannableTableId, partition: &[u8], clustering: &[u8]) -> String {
    format!(
        "s/{}/{}/{}",
        table.as_str(),
        alloy_primitives::hex::encode(partition),
        alloy_primitives::hex::encode(clustering)
    )
}

/// `_id` prefix shared by every row of one scan partition (trailing `/`
/// included).
fn scan_partition_prefix(table: ScannableTableId, partition: &[u8]) -> String {
    format!(
        "s/{}/{}/",
        table.as_str(),
        alloy_primitives::hex::encode(partition)
    )
}

/// Exclusive upper bound of a partition's `_id` range: the prefix with its
/// trailing `/` bumped to `'0'` (`'/' + 1`), so the bound sorts immediately
/// after every `prefix + hex` id under binary string comparison.
fn scan_partition_upper_bound(prefix: &str) -> String {
    let mut upper = prefix.to_string();
    upper.pop();
    upper.push('0');
    upper
}

/// Decodes the hex clustering suffix of a scan-row `_id` back to bytes.
fn clustering_from_id(id: &str, prefix_len: usize) -> Result<Vec<u8>> {
    alloy_primitives::hex::decode(&id[prefix_len..])
        .map_err(|_| MonadChainDataError::Decode("malformed mongo scan-row id"))
}

fn binary_value(value: &Bytes) -> Result<Binary> {
    if value.len() > MAX_VALUE_LEN {
        return Err(MonadChainDataError::Backend(format!(
            "mongo meta value of {} bytes exceeds the {MAX_VALUE_LEN}-byte document budget",
            value.len()
        )));
    }
    Ok(Binary {
        subtype: BinarySubtype::Generic,
        bytes: value.to_vec(),
    })
}

impl MongoMetaStore {
    pub async fn new(config: MongoMetaStoreConfig) -> Result<Self> {
        if config.write_concurrency == 0 {
            return Err(MonadChainDataError::Backend(
                "mongo write_concurrency must be >= 1".to_string(),
            ));
        }
        let mut options = ClientOptions::parse(&config.connection_string)
            .await
            .map_err(|e| backend("connection string", e))?;
        options.max_pool_size = Some(config.max_pool_size);
        let client = Client::with_options(options).map_err(|e| backend("client", e))?;
        let collection = client.database(&config.database).collection_with_options(
            &config.collection,
            CollectionOptions::builder()
                .write_concern(WriteConcern::builder().journal(true).build())
                .read_concern(ReadConcern::local())
                .build(),
        );
        Ok(Self {
            collection,
            write_concurrency: config.write_concurrency,
        })
    }

    async fn get_by_id(&self, id: String) -> Result<Option<Bytes>> {
        let doc = self
            .collection
            .find_one(doc! { "_id": &id })
            .await
            .map_err(|e| backend("get", e))?;
        match doc {
            None => Ok(None),
            Some(MetaDocument { v: Some(v), .. }) => Ok(Some(Bytes::from(v.bytes))),
            Some(MetaDocument { v: None, .. }) => Err(MonadChainDataError::Backend(format!(
                "mongo meta row {id} has no value"
            ))),
        }
    }

    async fn put_by_id(&self, id: String, value: Bytes) -> Result<()> {
        let document = MetaDocument {
            id: id.clone(),
            v: Some(binary_value(&value)?),
        };
        self.collection
            .replace_one(doc! { "_id": &id }, document)
            .upsert(true)
            .await
            .map_err(|e| backend("put", e))?;
        Ok(())
    }
}

impl MetaStore for MongoMetaStore {
    async fn get(&self, table: TableId, key: &[u8]) -> Result<Option<Bytes>> {
        self.get_by_id(kv_id(table, key)).await
    }

    async fn scan_get(
        &self,
        table: ScannableTableId,
        partition: &[u8],
        clustering: &[u8],
    ) -> Result<Option<Bytes>> {
        self.get_by_id(scan_id(table, partition, clustering)).await
    }

    async fn put(&self, table: TableId, key: &[u8], value: Bytes) -> Result<()> {
        self.put_by_id(kv_id(table, key), value).await
    }

    async fn scan_put(
        &self,
        table: ScannableTableId,
        partition: &[u8],
        clustering: &[u8],
        value: Bytes,
    ) -> Result<()> {
        self.put_by_id(scan_id(table, partition, clustering), value)
            .await
    }

    async fn scan_keys(&self, table: ScannableTableId, partition: &[u8]) -> Result<Vec<Vec<u8>>> {
        let prefix = scan_partition_prefix(table, partition);
        let upper = scan_partition_upper_bound(&prefix);
        let mut cursor = self
            .collection
            .find(doc! { "_id": { "$gte": &prefix, "$lt": &upper } })
            .projection(doc! { "_id": 1 })
            .sort(doc! { "_id": 1 })
            .await
            .map_err(|e| backend("scan_keys", e))?;
        let mut keys = Vec::new();
        while let Some(row) = cursor
            .try_next()
            .await
            .map_err(|e| backend("scan_keys cursor", e))?
        {
            keys.push(clustering_from_id(&row.id, prefix.len())?);
        }
        Ok(keys)
    }

    async fn apply_writes(&self, writes: Vec<MetaWriteOp>) -> Result<()> {
        let rows: Vec<(String, Bytes)> = writes
            .into_iter()
            .map(|op| match op {
                MetaWriteOp::Put { table, key, value } => (kv_id(table, &key), value),
                MetaWriteOp::ScanPut {
                    table,
                    partition,
                    clustering,
                    value,
                } => (scan_id(table, &partition, &clustering), value),
            })
            .collect();
        futures::stream::iter(rows)
            .map(|(id, value)| self.put_by_id(id, value))
            .buffer_unordered(self.write_concurrency)
            .try_collect::<()>()
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TABLE: TableId = TableId::new("block_metadata");
    const SCAN_TABLE: ScannableTableId = ScannableTableId::new("log_bitmap_by_block");

    #[test]
    fn ids_are_hex_string_encoded() {
        assert_eq!(kv_id(TABLE, &[0x00, 0xff]), "k/block_metadata/00ff");
        assert_eq!(
            scan_id(SCAN_TABLE, &[0xab], &[0x01, 0x02]),
            "s/log_bitmap_by_block/ab/0102"
        );
        // Empty keys stay well-formed (an empty hex segment).
        assert_eq!(kv_id(TABLE, &[]), "k/block_metadata/");
    }

    /// The `_id` range `[prefix, upper)` must admit exactly the partition's
    /// rows under binary string order: every `prefix + hex` id and nothing
    /// from a neighboring partition or table.
    #[test]
    fn partition_bounds_bracket_exactly_the_partition() {
        let prefix = scan_partition_prefix(SCAN_TABLE, &[0xab]);
        let upper = scan_partition_upper_bound(&prefix);
        assert_eq!(prefix, "s/log_bitmap_by_block/ab/");
        assert_eq!(upper, "s/log_bitmap_by_block/ab0");

        let inside = [
            scan_id(SCAN_TABLE, &[0xab], &[]),
            scan_id(SCAN_TABLE, &[0xab], &[0x00]),
            scan_id(SCAN_TABLE, &[0xab], &[0xff; 12]),
        ];
        for id in &inside {
            assert!(
                prefix.as_str() <= id.as_str() && id.as_str() < upper.as_str(),
                "{id}"
            );
        }
        let outside = [
            scan_id(SCAN_TABLE, &[0xaa], &[0xff]),
            scan_id(SCAN_TABLE, &[0xab, 0x00], &[0x00]),
            scan_id(SCAN_TABLE, &[0xac], &[0x00]),
            "s/other_table/ab/00".to_string(),
        ];
        for id in &outside {
            assert!(
                id.as_str() < prefix.as_str() || id.as_str() >= upper.as_str(),
                "{id}"
            );
        }
    }

    /// Hex preserves unsigned-byte order, so `_id` order == clustering order.
    #[test]
    fn hex_ids_sort_in_clustering_byte_order() {
        let mut clusterings: Vec<Vec<u8>> = vec![
            7u64.to_be_bytes().to_vec(),
            0u64.to_be_bytes().to_vec(),
            u64::MAX.to_be_bytes().to_vec(),
            255u64.to_be_bytes().to_vec(),
        ];
        let mut ids: Vec<String> = clusterings
            .iter()
            .map(|c| scan_id(SCAN_TABLE, &[0x01], c))
            .collect();
        clusterings.sort();
        ids.sort();
        let decoded: Vec<Vec<u8>> = ids
            .iter()
            .map(|id| {
                clustering_from_id(id, scan_partition_prefix(SCAN_TABLE, &[0x01]).len()).unwrap()
            })
            .collect();
        assert_eq!(decoded, clusterings);
    }

    #[test]
    fn oversized_values_error_loudly() {
        assert!(binary_value(&Bytes::from(vec![0u8; MAX_VALUE_LEN])).is_ok());
        assert!(binary_value(&Bytes::from(vec![0u8; MAX_VALUE_LEN + 1])).is_err());
    }
}
