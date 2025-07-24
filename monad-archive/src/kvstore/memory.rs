use std::{
    collections::HashMap,
    sync::{atomic::AtomicBool, Arc},
};

use bytes::Bytes;
use eyre::Result;
use tokio::sync::Mutex;

use crate::prelude::*;

#[derive(Clone)]
pub struct MemoryStorage {
    pub db: Arc<Mutex<HashMap<String, Bytes>>>,
    pub should_fail: Arc<AtomicBool>,
    pub name: String,
}

impl MemoryStorage {
    pub fn new(name: impl Into<String>) -> MemoryStorage {
        MemoryStorage {
            db: Arc::new(Mutex::new(HashMap::default())),
            should_fail: Arc::new(AtomicBool::new(false)),
            name: name.into(),
        }
    }
}

impl KVReader for MemoryStorage {
    async fn get(&self, key: &str) -> Result<Option<Bytes>> {
        use std::sync::atomic::Ordering;

        // Check if we should simulate a failure
        if self.should_fail.load(Ordering::SeqCst) {
            return Err(eyre::eyre!("MemoryStorage simulated failure"));
        }

        Ok(self.db.lock().await.get(key).map(ToOwned::to_owned))
    }
}

impl KVStore for MemoryStorage {
    fn bucket_name(&self) -> &str {
        &self.name
    }

    async fn put(&self, key: impl AsRef<str>, data: Vec<u8>) -> Result<()> {
        use std::sync::atomic::Ordering;

        // Check if we should simulate a failure
        if self.should_fail.load(Ordering::SeqCst) {
            return Err(eyre::eyre!("MemoryStorage simulated failure"));
        }

        self.db
            .lock()
            .await
            .insert(key.as_ref().to_owned(), data.into());
        Ok(())
    }

    async fn scan_prefix(&self, prefix: &str) -> Result<Vec<String>> {
        use std::sync::atomic::Ordering;

        // Check if we should simulate a failure
        if self.should_fail.load(Ordering::SeqCst) {
            return Err(eyre::eyre!("MemoryStorage simulated failure"));
        }

        Ok(self
            .db
            .lock()
            .await
            .keys()
            .filter_map(|k| {
                if k.starts_with(prefix) {
                    Some(k.to_owned())
                } else {
                    None
                }
            })
            .collect())
    }

    async fn delete(&self, key: impl AsRef<str>) -> Result<()> {
        self.db.lock().await.remove(key.as_ref());
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use bytes::Bytes;

    use super::*;

    #[tokio::test]
    async fn test_basic_blob_operations() -> Result<()> {
        let storage = MemoryStorage::new("test-bucket");

        // Test upload and read
        let key = "test-key";
        let data = b"hello world".to_vec();
        storage.put(key, data.clone()).await?;

        let result = storage.get(key).await?.unwrap();
        assert_eq!(result, Bytes::from(data));

        // Test non-existent key
        let option = storage.get("non-existent").await.unwrap();
        assert_eq!(option, None);

        Ok(())
    }

    #[tokio::test]
    async fn test_scan_prefix() -> Result<()> {
        let storage = MemoryStorage::new("test-bucket");

        // Upload test data
        storage.put("test1", b"data1".to_vec()).await?;
        storage.put("test2", b"data2".to_vec()).await?;
        storage.put("other", b"data3".to_vec()).await?;

        // Test scanning with prefix
        let results = storage.scan_prefix("test").await?;
        assert_eq!(results.len(), 2);
        assert!(results.contains(&"test1".to_string()));
        assert!(results.contains(&"test2".to_string()));

        // Test scanning with non-matching prefix
        let results = storage.scan_prefix("xyz").await?;
        assert!(results.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_bucket_name() {
        let name = "test-bucket";
        let storage = MemoryStorage::new(name);
        assert_eq!(storage.bucket_name(), name);
    }
}
