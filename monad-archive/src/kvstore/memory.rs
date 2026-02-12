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
    collections::HashMap,
    sync::{atomic::AtomicBool, Arc},
};

use bytes::Bytes;
use tokio::sync::Mutex;

use super::{PutResult, WritePolicy};
use crate::{error::Result, prelude::*};

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
            return Err(eyre::eyre!("MemoryStorage simulated failure").into());
        }

        Ok(self.db.lock().await.get(key).map(ToOwned::to_owned))
    }
}

impl KVStore for MemoryStorage {
    fn bucket_name(&self) -> &str {
        &self.name
    }

    async fn put(
        &self,
        key: impl AsRef<str>,
        data: Vec<u8>,
        policy: WritePolicy,
    ) -> Result<PutResult> {
        use std::sync::atomic::Ordering;

        // Check if we should simulate a failure
        if self.should_fail.load(Ordering::SeqCst) {
            return Err(eyre::eyre!("MemoryStorage simulated failure").into());
        }

        let key = key.as_ref();
        let mut db = self.db.lock().await;

        if policy == WritePolicy::NoClobber && db.contains_key(key) {
            warn!(
                key,
                "Memory put skipped: key already exists (NoClobber policy)"
            );
            return Ok(PutResult::Skipped);
        }

        db.insert(key.to_owned(), data.into());
        Ok(PutResult::Written)
    }

    async fn scan_prefix(&self, prefix: &str) -> Result<Vec<String>> {
        use std::sync::atomic::Ordering;

        // Check if we should simulate a failure
        if self.should_fail.load(Ordering::SeqCst) {
            return Err(eyre::eyre!("MemoryStorage simulated failure").into());
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
    use eyre::Result;

    use super::*;

    #[tokio::test]
    async fn test_basic_blob_operations() -> Result<()> {
        let storage = MemoryStorage::new("test-bucket");

        // Test upload and read
        let key = "test-key";
        let data = b"hello world".to_vec();
        storage
            .put(key, data.clone(), WritePolicy::AllowOverwrite)
            .await?;

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
        storage
            .put("test1", b"data1".to_vec(), WritePolicy::AllowOverwrite)
            .await?;
        storage
            .put("test2", b"data2".to_vec(), WritePolicy::AllowOverwrite)
            .await?;
        storage
            .put("other", b"data3".to_vec(), WritePolicy::AllowOverwrite)
            .await?;

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

    #[tokio::test]
    async fn test_noclobber_skips_existing_key() -> Result<()> {
        let storage = MemoryStorage::new("test-bucket");

        let key = "noclobber-test";
        let original_data = b"original".to_vec();
        let new_data = b"new".to_vec();

        // First write should succeed
        let result = storage
            .put(key, original_data.clone(), WritePolicy::NoClobber)
            .await?;
        assert_eq!(result, PutResult::Written);

        // Second write with NoClobber should be skipped
        let result = storage.put(key, new_data, WritePolicy::NoClobber).await?;
        assert_eq!(result, PutResult::Skipped);

        // Verify original data is preserved
        let stored = storage.get(key).await?.unwrap();
        assert_eq!(stored, Bytes::from(original_data));

        Ok(())
    }

    #[tokio::test]
    async fn test_allow_overwrite_overwrites_existing_key() -> Result<()> {
        let storage = MemoryStorage::new("test-bucket");

        let key = "overwrite-test";
        let original_data = b"original".to_vec();
        let new_data = b"new".to_vec();

        // First write
        storage
            .put(key, original_data, WritePolicy::AllowOverwrite)
            .await?;

        // Second write with AllowOverwrite should succeed
        let result = storage
            .put(key, new_data.clone(), WritePolicy::AllowOverwrite)
            .await?;
        assert_eq!(result, PutResult::Written);

        // Verify new data is stored
        let stored = storage.get(key).await?.unwrap();
        assert_eq!(stored, Bytes::from(new_data));

        Ok(())
    }
}
