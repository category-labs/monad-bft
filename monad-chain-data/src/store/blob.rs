use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use bytes::Bytes;

use crate::{
    error::{Error, Result},
    store::traits::{BlobStore, BlobTableId, Page},
};

type BlobEntryKey = (BlobTableId, Vec<u8>);
type BlobMap = HashMap<BlobEntryKey, Bytes>;

#[derive(Clone, Default)]
pub struct InMemoryBlobStore {
    inner: Arc<RwLock<BlobMap>>,
}

impl BlobStore for InMemoryBlobStore {
    async fn put_blob(&self, table: BlobTableId, key: &[u8], value: Bytes) -> Result<()> {
        let mut guard = self
            .inner
            .write()
            .map_err(|_| Error::Backend("poisoned lock".to_string()))?;
        guard.insert((table, key.to_vec()), value);
        Ok(())
    }

    async fn get_blob(&self, table: BlobTableId, key: &[u8]) -> Result<Option<Bytes>> {
        let guard = self
            .inner
            .read()
            .map_err(|_| Error::Backend("poisoned lock".to_string()))?;
        Ok(guard.get(&(table, key.to_vec())).cloned())
    }

    async fn delete_blob(&self, table: BlobTableId, key: &[u8]) -> Result<()> {
        let mut guard = self
            .inner
            .write()
            .map_err(|_| Error::Backend("poisoned lock".to_string()))?;
        guard.remove(&(table, key.to_vec()));
        Ok(())
    }

    async fn list_prefix(
        &self,
        table: BlobTableId,
        prefix: &[u8],
        cursor: Option<Vec<u8>>,
        limit: usize,
    ) -> Result<Page> {
        let guard = self
            .inner
            .read()
            .map_err(|_| Error::Backend("poisoned lock".to_string()))?;

        let has_cursor = cursor.is_some();
        let start = cursor.unwrap_or_default();
        let mut keys = Vec::new();
        let mut all_keys: Vec<Vec<u8>> = guard
            .keys()
            .filter_map(|(entry_table, key)| (*entry_table == table).then_some(key.clone()))
            .collect();
        all_keys.sort();

        let target_len = limit.saturating_add(1);
        for key in all_keys {
            if has_cursor && key <= start {
                continue;
            }
            if !has_cursor && key < start {
                continue;
            }
            if !key.starts_with(prefix) {
                continue;
            }
            keys.push(key);
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

    use super::InMemoryBlobStore;
    use crate::store::traits::{BlobStore, BlobTableId};

    const TEST_TABLE: BlobTableId = BlobTableId::new("test");

    #[test]
    fn list_prefix_pagination_does_not_repeat_cursor_entry() {
        block_on(async {
            let store = InMemoryBlobStore::default();
            for index in 0..5u64 {
                let key = format!("list-prefix/{index:04}").into_bytes();
                store
                    .put_blob(TEST_TABLE, &key, Bytes::from_static(b"v"))
                    .await
                    .expect("seed blob");
            }

            let mut cursor = None;
            let mut seen = Vec::new();
            loop {
                let page = store
                    .list_prefix(TEST_TABLE, b"list-prefix/", cursor.take(), 4)
                    .await
                    .expect("list prefix");
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
