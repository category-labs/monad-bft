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

use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
};

use bytes::Bytes;
use monad_query_errors::Result;

use crate::meta::{MetaStore, MetaWriteOp, ScannableTableId, TableId};

type KeyValueRecordMap = BTreeMap<(TableId, Vec<u8>), Bytes>;
type ScannableRecordMap = BTreeMap<(ScannableTableId, Vec<u8>, Vec<u8>), Bytes>;
type SharedKeyValueRecordMap = Arc<RwLock<KeyValueRecordMap>>;
type SharedScannableRecordMap = Arc<RwLock<ScannableRecordMap>>;

/// Test-only meta-store fixture. Holds records in memory behind sync
/// `RwLock`s. Not intended as a deployable backend.
#[derive(Debug, Clone, Default)]
pub struct InMemoryMetaStore {
    kv_records: SharedKeyValueRecordMap,
    scan_records: SharedScannableRecordMap,
}

impl InMemoryMetaStore {
    /// Test-only: remove a kv row from the fixture to simulate missing data.
    /// Not exposed on the [`MetaStore`] trait — real backends are append-only.
    pub fn clear_key(&self, table: TableId, key: &[u8]) {
        if let Ok(mut guard) = self.kv_records.write() {
            guard.remove(&(table, key.to_vec()));
        }
    }

    /// Test-only: clones the entire kv map for byte-equality assertions
    /// across fixture instances.
    pub fn kv_snapshot(&self) -> BTreeMap<(TableId, Vec<u8>), Bytes> {
        self.kv_records
            .read()
            .map(|guard| guard.clone())
            .unwrap_or_default()
    }

    /// Test-only: clones the entire scan map.
    pub fn scan_snapshot(&self) -> BTreeMap<(ScannableTableId, Vec<u8>, Vec<u8>), Bytes> {
        self.scan_records
            .read()
            .map(|guard| guard.clone())
            .unwrap_or_default()
    }
}

impl MetaStore for InMemoryMetaStore {
    async fn get(&self, table: TableId, key: &[u8]) -> Result<Option<Bytes>> {
        let records = self
            .kv_records
            .read()
            .map_err(|_| monad_query_errors::QueryError::Backend("poisoned lock".to_string()))?;
        Ok(records.get(&(table, key.to_vec())).cloned())
    }

    async fn scan_get(
        &self,
        table: ScannableTableId,
        partition: &[u8],
        clustering: &[u8],
    ) -> Result<Option<Bytes>> {
        let records = self
            .scan_records
            .read()
            .map_err(|_| monad_query_errors::QueryError::Backend("poisoned lock".to_string()))?;
        Ok(records
            .get(&(table, partition.to_vec(), clustering.to_vec()))
            .cloned())
    }

    async fn put(&self, table: TableId, key: &[u8], value: Bytes) -> Result<()> {
        let mut records = self
            .kv_records
            .write()
            .map_err(|_| monad_query_errors::QueryError::Backend("poisoned lock".to_string()))?;
        records.insert((table, key.to_vec()), value);
        Ok(())
    }

    async fn scan_put(
        &self,
        table: ScannableTableId,
        partition: &[u8],
        clustering: &[u8],
        value: Bytes,
    ) -> Result<()> {
        let mut records = self
            .scan_records
            .write()
            .map_err(|_| monad_query_errors::QueryError::Backend("poisoned lock".to_string()))?;
        records.insert((table, partition.to_vec(), clustering.to_vec()), value);
        Ok(())
    }

    async fn apply_writes(&self, writes: Vec<MetaWriteOp>) -> Result<()> {
        let mut kv_records = self
            .kv_records
            .write()
            .map_err(|_| monad_query_errors::QueryError::Backend("poisoned lock".to_string()))?;
        let mut scan_records = self
            .scan_records
            .write()
            .map_err(|_| monad_query_errors::QueryError::Backend("poisoned lock".to_string()))?;
        for op in writes {
            match op {
                MetaWriteOp::Put {
                    table,
                    row_key,
                    row_data,
                } => {
                    kv_records.insert((table, row_key), row_data);
                }
                MetaWriteOp::ScanPut {
                    table,
                    partition,
                    clustering_key,
                    row_data,
                } => {
                    scan_records.insert((table, partition, clustering_key), row_data);
                }
            }
        }
        Ok(())
    }

    async fn scan_keys(&self, table: ScannableTableId, partition: &[u8]) -> Result<Vec<Vec<u8>>> {
        let records = self
            .scan_records
            .read()
            .map_err(|_| monad_query_errors::QueryError::Backend("poisoned lock".to_string()))?;
        // Seek straight to the partition's first possible key and stop once
        // past it, instead of walking the whole multi-table map.
        Ok(records
            .range((table, partition.to_vec(), Vec::new())..)
            .take_while(|((record_table, record_partition, _), _)| {
                *record_table == table && record_partition.as_slice() == partition
            })
            .map(|((_, _, clustering), _)| clustering.clone())
            .collect())
    }
}
