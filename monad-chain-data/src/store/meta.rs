use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
};

use bytes::Bytes;

use crate::{
    error::{Error, Result},
    store::traits::{
        DelCond, MetaStore, Page, PutCond, PutResult, Record, ScannableTableId, TableId,
    },
};

type MetaEntryKey = (TableId, Vec<u8>);
type MetaMap = BTreeMap<MetaEntryKey, Record>;
type ScanEntryKey = (ScannableTableId, Vec<u8>, Vec<u8>);
type ScanMap = BTreeMap<ScanEntryKey, Record>;

#[derive(Clone)]
pub struct InMemoryMetaStore {
    inner: Arc<RwLock<MetaMap>>,
    scan_inner: Arc<RwLock<ScanMap>>,
}

impl Default for InMemoryMetaStore {
    fn default() -> Self {
        Self {
            inner: Arc::new(RwLock::new(BTreeMap::new())),
            scan_inner: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }
}

impl MetaStore for InMemoryMetaStore {
    async fn get(&self, table: TableId, key: &[u8]) -> Result<Option<Record>> {
        let guard = self
            .inner
            .read()
            .map_err(|_| Error::Backend("poisoned lock".to_string()))?;
        Ok(guard.get(&(table, key.to_vec())).cloned())
    }

    async fn put(
        &self,
        table: TableId,
        key: &[u8],
        value: Bytes,
        cond: PutCond,
    ) -> Result<PutResult> {
        let mut guard = self
            .inner
            .write()
            .map_err(|_| Error::Backend("poisoned lock".to_string()))?;

        let entry_key = (table, key.to_vec());
        let current = guard.get(&entry_key).cloned();
        let allowed = match (cond, current.as_ref()) {
            (PutCond::Any, _) => true,
            (PutCond::IfAbsent, None) => true,
            (PutCond::IfAbsent, Some(_)) => false,
            (PutCond::IfVersion(version), Some(record)) => record.version == version,
            (PutCond::IfVersion(_), None) => false,
        };

        if !allowed {
            return Ok(PutResult {
                applied: false,
                version: current.map(|record| record.version),
            });
        }

        let next_version = current.map_or(1, |record| record.version + 1);
        guard.insert(
            entry_key,
            Record {
                value,
                version: next_version,
            },
        );
        Ok(PutResult {
            applied: true,
            version: Some(next_version),
        })
    }

    async fn delete(&self, table: TableId, key: &[u8], cond: DelCond) -> Result<()> {
        let mut guard = self
            .inner
            .write()
            .map_err(|_| Error::Backend("poisoned lock".to_string()))?;

        let entry_key = (table, key.to_vec());
        let should_delete = match (cond, guard.get(&entry_key)) {
            (DelCond::Any, Some(_)) => true,
            (DelCond::Any, None) => false,
            (DelCond::IfVersion(version), Some(record)) => record.version == version,
            (DelCond::IfVersion(_), None) => false,
        };

        if should_delete {
            guard.remove(&entry_key);
        }
        Ok(())
    }

    async fn scan_get(
        &self,
        table: ScannableTableId,
        partition: &[u8],
        clustering: &[u8],
    ) -> Result<Option<Record>> {
        let guard = self
            .scan_inner
            .read()
            .map_err(|_| Error::Backend("poisoned lock".to_string()))?;
        Ok(guard
            .get(&(table, partition.to_vec(), clustering.to_vec()))
            .cloned())
    }

    async fn scan_put(
        &self,
        table: ScannableTableId,
        partition: &[u8],
        clustering: &[u8],
        value: Bytes,
        cond: PutCond,
    ) -> Result<PutResult> {
        let mut guard = self
            .scan_inner
            .write()
            .map_err(|_| Error::Backend("poisoned lock".to_string()))?;

        let entry_key = (table, partition.to_vec(), clustering.to_vec());
        let current = guard.get(&entry_key).cloned();
        let allowed = match (cond, current.as_ref()) {
            (PutCond::Any, _) => true,
            (PutCond::IfAbsent, None) => true,
            (PutCond::IfAbsent, Some(_)) => false,
            (PutCond::IfVersion(version), Some(record)) => record.version == version,
            (PutCond::IfVersion(_), None) => false,
        };

        if !allowed {
            return Ok(PutResult {
                applied: false,
                version: current.map(|record| record.version),
            });
        }

        let next_version = current.map_or(1, |record| record.version + 1);
        guard.insert(
            entry_key,
            Record {
                value,
                version: next_version,
            },
        );
        Ok(PutResult {
            applied: true,
            version: Some(next_version),
        })
    }

    async fn scan_delete(
        &self,
        table: ScannableTableId,
        partition: &[u8],
        clustering: &[u8],
        cond: DelCond,
    ) -> Result<()> {
        let mut guard = self
            .scan_inner
            .write()
            .map_err(|_| Error::Backend("poisoned lock".to_string()))?;

        let entry_key = (table, partition.to_vec(), clustering.to_vec());
        let should_delete = match (cond, guard.get(&entry_key)) {
            (DelCond::Any, Some(_)) => true,
            (DelCond::Any, None) => false,
            (DelCond::IfVersion(version), Some(record)) => record.version == version,
            (DelCond::IfVersion(_), None) => false,
        };

        if should_delete {
            guard.remove(&entry_key);
        }
        Ok(())
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
            .scan_inner
            .read()
            .map_err(|_| Error::Backend("poisoned lock".to_string()))?;

        let mut keys = Vec::new();
        let has_cursor = cursor.is_some();
        let start = cursor.unwrap_or_default();
        let target_len = limit.saturating_add(1);

        for ((entry_table, entry_partition, key), _) in guard.iter() {
            if *entry_table != table {
                continue;
            }
            if entry_partition.as_slice() != partition {
                continue;
            }
            if !key.starts_with(prefix) {
                continue;
            }
            if has_cursor && key <= &start {
                continue;
            }
            if !has_cursor && key < &start {
                continue;
            }
            keys.push(key.clone());
            if keys.len() == target_len {
                break;
            }
        }

        let next_cursor = if keys.len() > limit {
            keys.pop();
            keys.last().cloned()
        } else {
            None
        };

        Ok(Page { keys, next_cursor })
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use futures::executor::block_on;

    use super::InMemoryMetaStore;
    use crate::store::traits::{MetaStore, PutCond, ScannableTableId};

    const TEST_TABLE: ScannableTableId = ScannableTableId::new("scan-test");

    #[test]
    fn list_pagination_does_not_repeat_cursor_entry() {
        block_on(async {
            let store = InMemoryMetaStore::default();
            for index in 0..5u64 {
                store
                    .scan_put(
                        TEST_TABLE,
                        b"partition",
                        format!("{index:04}").as_bytes(),
                        Bytes::from_static(b"v"),
                        PutCond::Any,
                    )
                    .await
                    .expect("seed key");
            }

            let mut cursor = None;
            let mut seen = Vec::new();
            loop {
                let page = store
                    .scan_list(TEST_TABLE, b"partition", b"", cursor.take(), 4)
                    .await
                    .expect("list");
                seen.extend(page.keys.iter().cloned());
                if page.next_cursor.is_none() {
                    break;
                }
                cursor = page.next_cursor;
            }

            let unique = seen.iter().collect::<std::collections::BTreeSet<_>>();
            assert_eq!(seen.len(), 5);
            assert_eq!(unique.len(), 5);
        });
    }
}
