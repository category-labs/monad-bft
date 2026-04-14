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

use crate::{
    error::Result,
    store::{
        common::Page,
        meta::{MetaStore, PutResult, Record, ScannableTableId, TableId},
    },
};

/// Test-only meta-store fixture. Holds records in memory behind sync
/// `RwLock`s. Not intended as a deployable backend.
#[derive(Debug, Clone, Default)]
pub struct InMemoryMetaStore {
    kv_records: Arc<RwLock<BTreeMap<(TableId, Vec<u8>), Record>>>,
    scan_records: Arc<RwLock<BTreeMap<(ScannableTableId, Vec<u8>, Vec<u8>), Record>>>,
}

impl InMemoryMetaStore {
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn len(&self) -> usize {
        self.kv_records
            .read()
            .map(|guard| guard.len())
            .unwrap_or_default()
            + self
                .scan_records
                .read()
                .map(|guard| guard.len())
                .unwrap_or_default()
    }

    /// Test-only: remove a kv row from the fixture to simulate missing data.
    /// Not exposed on the [`MetaStore`] trait — real backends are append-only.
    pub fn clear_key(&self, table: TableId, key: &[u8]) {
        if let Ok(mut guard) = self.kv_records.write() {
            guard.remove(&(table, key.to_vec()));
        }
    }
}

impl MetaStore for InMemoryMetaStore {
    async fn get(&self, table: TableId, key: &[u8]) -> Result<Option<Record>> {
        let guard = self
            .kv_records
            .read()
            .map_err(|_| crate::error::MonadChainDataError::Backend("poisoned lock".to_string()))?;
        Ok(guard.get(&(table, key.to_vec())).cloned())
    }

    async fn scan_get(
        &self,
        table: ScannableTableId,
        partition: &[u8],
        clustering: &[u8],
    ) -> Result<Option<Record>> {
        let guard = self
            .scan_records
            .read()
            .map_err(|_| crate::error::MonadChainDataError::Backend("poisoned lock".to_string()))?;
        Ok(guard
            .get(&(table, partition.to_vec(), clustering.to_vec()))
            .cloned())
    }

    async fn put(&self, table: TableId, key: &[u8], value: Bytes) -> Result<PutResult> {
        let mut guard = self
            .kv_records
            .write()
            .map_err(|_| crate::error::MonadChainDataError::Backend("poisoned lock".to_string()))?;

        let entry_key = (table, key.to_vec());
        let current = guard.get(&entry_key).cloned();
        let next_version = current.map_or(1, |record| record.version + 1);
        guard.insert(
            entry_key,
            Record {
                version: next_version,
                value,
            },
        );

        Ok(PutResult {
            applied: true,
            version: Some(next_version),
        })
    }

    async fn scan_put(
        &self,
        table: ScannableTableId,
        partition: &[u8],
        clustering: &[u8],
        value: Bytes,
    ) -> Result<PutResult> {
        let mut guard = self
            .scan_records
            .write()
            .map_err(|_| crate::error::MonadChainDataError::Backend("poisoned lock".to_string()))?;

        let entry_key = (table, partition.to_vec(), clustering.to_vec());
        let current = guard.get(&entry_key).cloned();
        let next_version = current.map_or(1, |record| record.version + 1);
        guard.insert(
            entry_key,
            Record {
                version: next_version,
                value,
            },
        );

        Ok(PutResult {
            applied: true,
            version: Some(next_version),
        })
    }

    async fn scan_list(
        &self,
        table: ScannableTableId,
        partition: &[u8],
        prefix: &[u8],
        cursor: Option<Vec<u8>>,
        limit: usize,
    ) -> Result<Page> {
        let guard = self
            .scan_records
            .read()
            .map_err(|_| crate::error::MonadChainDataError::Backend("poisoned lock".to_string()))?;
        let mut keys = Vec::new();
        let mut next_cursor = None;
        let cursor = cursor.as_deref();

        for ((record_table, record_partition, clustering), _) in guard.iter() {
            if *record_table != table || record_partition.as_slice() != partition {
                continue;
            }
            if !clustering.starts_with(prefix) {
                continue;
            }
            if let Some(cursor) = cursor {
                if clustering.as_slice() <= cursor {
                    continue;
                }
            }
            if keys.len() == limit {
                next_cursor = keys.last().cloned();
                break;
            }
            keys.push(clustering.clone());
        }

        Ok(Page { keys, next_cursor })
    }
}
