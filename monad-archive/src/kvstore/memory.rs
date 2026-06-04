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
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
};

use bytes::Bytes;
use eyre::Result;
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;

use super::{ObjectMeta, PutResult, WritePolicy};
use crate::prelude::*;

#[derive(Clone)]
pub struct MemoryStorage {
    pub db: Arc<Mutex<HashMap<String, (Bytes, [u8; 32])>>>,
    pub should_fail: Arc<AtomicBool>,
    pub name: String,
    /// Counts body `get()` calls only (not `metadata`/`exists`/`put`/`scan_prefix`/
    /// `delete`). Test-support instrumentation used to prove the checker's checksum
    /// fast path avoids downloading bodies it doesn't need.
    get_count: Arc<AtomicUsize>,
}

impl MemoryStorage {
    pub fn new(name: impl Into<String>) -> MemoryStorage {
        MemoryStorage {
            db: Arc::new(Mutex::new(HashMap::default())),
            should_fail: Arc::new(AtomicBool::new(false)),
            name: name.into(),
            get_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Number of `get()` (body download) calls observed so far. Cloned handles share
    /// the same counter (the inner `Arc` is shared).
    pub fn get_count(&self) -> usize {
        self.get_count.load(Ordering::Relaxed)
    }

    /// Resets the body `get()` counter to zero.
    pub fn reset_get_count(&self) {
        self.get_count.store(0, Ordering::Relaxed);
    }
}

impl KVReader for MemoryStorage {
    async fn get(&self, key: &str) -> Result<Option<Bytes>> {
        // Count body downloads only (see `get_count`). Done before the failure check so
        // even a simulated-failure `get` is observed as an attempted body download.
        self.get_count.fetch_add(1, Ordering::Relaxed);

        // Check if we should simulate a failure
        if self.should_fail.load(Ordering::SeqCst) {
            return Err(eyre::eyre!("MemoryStorage simulated failure"));
        }

        Ok(self
            .db
            .lock()
            .await
            .get(key)
            .map(|(bytes, _checksum)| bytes.clone()))
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        if self.should_fail.load(Ordering::SeqCst) {
            return Err(eyre::eyre!("MemoryStorage simulated failure"));
        }

        Ok(self.db.lock().await.contains_key(key))
    }

    async fn metadata(&self, key: &str) -> Result<Option<ObjectMeta>> {
        if self.should_fail.load(Ordering::SeqCst) {
            return Err(eyre::eyre!("MemoryStorage simulated failure"));
        }

        // The map stores the checksum alongside the bytes; no legacy state exists.
        Ok(self
            .db
            .lock()
            .await
            .get(key)
            .map(|(_bytes, checksum_sha256)| ObjectMeta {
                checksum_sha256: *checksum_sha256,
            }))
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
        // Check if we should simulate a failure
        if self.should_fail.load(Ordering::SeqCst) {
            return Err(eyre::eyre!("MemoryStorage simulated failure"));
        }

        let key = key.as_ref();

        // Hash before locking so the CPU-bound digest doesn't block other ops.
        let checksum_sha256: [u8; 32] = Sha256::digest(&data).into();

        let mut db = self.db.lock().await;

        if policy == WritePolicy::NoClobber && db.contains_key(key) {
            warn!(
                key,
                "Memory put skipped: key already exists (NoClobber policy)"
            );
            return Ok(PutResult::Skipped);
        }

        db.insert(key.to_owned(), (data.into(), checksum_sha256));
        Ok(PutResult::Written { checksum_sha256 })
    }

    async fn scan_prefix(&self, prefix: &str) -> Result<Vec<String>> {
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
        assert!(matches!(result, PutResult::Written { .. }));

        // Second write with NoClobber should be skipped
        let result = storage.put(key, new_data, WritePolicy::NoClobber).await?;
        assert_eq!(result, PutResult::Skipped);

        // Verify original data is preserved
        let stored = storage.get(key).await?.unwrap();
        assert_eq!(stored, Bytes::from(original_data));

        Ok(())
    }

    #[tokio::test]
    async fn test_put_returns_sha256_checksum() -> Result<()> {
        use sha2::{Digest, Sha256};

        let storage = MemoryStorage::new("test-bucket");

        let key = "checksum-key";
        let input = b"some payload bytes".to_vec();
        let expected: [u8; 32] = Sha256::digest(&input).into();

        let result = storage
            .put(key, input.clone(), WritePolicy::AllowOverwrite)
            .await?;

        let PutResult::Written { checksum_sha256 } = result else {
            panic!("expected PutResult::Written, got {result:?}");
        };
        assert_eq!(checksum_sha256, expected);

        Ok(())
    }

    #[tokio::test]
    async fn test_metadata_returns_some_with_checksum() -> Result<()> {
        use sha2::{Digest, Sha256};

        let storage = MemoryStorage::new("test-bucket");

        let key = "metadata-key";
        let input = b"some payload bytes".to_vec();
        let expected: [u8; 32] = Sha256::digest(&input).into();

        storage
            .put(key, input.clone(), WritePolicy::AllowOverwrite)
            .await?;

        let meta = storage
            .metadata(key)
            .await?
            .expect("expected Some metadata");
        assert_eq!(meta.checksum_sha256, expected);

        Ok(())
    }

    #[tokio::test]
    async fn test_metadata_missing_key_returns_none() -> Result<()> {
        let storage = MemoryStorage::new("test-bucket");
        let meta = storage.metadata("does-not-exist").await?;
        assert!(meta.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn test_get_count_increments_on_get_only() -> Result<()> {
        let storage = MemoryStorage::new("test-bucket");

        let key = "count-key";
        let data = b"payload".to_vec();
        // `put` must not increment the body-get counter.
        storage
            .put(key, data.clone(), WritePolicy::AllowOverwrite)
            .await?;
        assert_eq!(storage.get_count(), 0, "put must not count as a body get");

        // Cheap metadata reads (used by the checker fast path) must not count.
        let _ = storage.metadata(key).await?;
        let _ = storage.metadata("missing").await?;
        assert_eq!(
            storage.get_count(),
            0,
            "metadata must not count as a body get"
        );

        // `exists` and `scan_prefix` must not count either.
        let _ = storage.exists(key).await?;
        let _ = storage.scan_prefix("count").await?;
        assert_eq!(
            storage.get_count(),
            0,
            "exists/scan_prefix must not count as a body get"
        );

        // Only `get` increments, once per call, including misses.
        let _ = storage.get(key).await?;
        assert_eq!(storage.get_count(), 1);
        let _ = storage.get("missing").await?;
        assert_eq!(storage.get_count(), 2, "a get miss still counts");

        // A cloned handle shares the same counter (shared inner Arc).
        let clone = storage.clone();
        assert_eq!(clone.get_count(), 2);
        let _ = clone.get(key).await?;
        assert_eq!(storage.get_count(), 3, "clone observes the same counter");

        // Reset zeroes it for both handles.
        storage.reset_get_count();
        assert_eq!(storage.get_count(), 0);
        assert_eq!(clone.get_count(), 0);

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
        assert!(matches!(result, PutResult::Written { .. }));

        // Verify new data is stored
        let stored = storage.get(key).await?.unwrap();
        assert_eq!(stored, Bytes::from(new_data));

        Ok(())
    }
}
