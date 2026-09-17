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
    path::{Component, Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use eyre::{Context, Result};
use tokio::{fs, io::AsyncWriteExt, task::spawn_blocking};

use super::{
    kvstore_get_metrics, kvstore_put_metrics, KVStoreType, MetricsResultExt, PutResult, WritePolicy,
};
use crate::{metrics::Metrics, prelude::*};

const TMP_DIR_NAME: &str = ".tmp";

static TMP_SEQ: AtomicU64 = AtomicU64::new(0);

#[derive(Clone)]
pub struct FsStorage {
    pub root: PathBuf,
    staging: PathBuf,
    metrics: Metrics,
    name: String,
}

fn tmp_name() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|since| since.as_nanos())
        .unwrap_or(0);
    format!(
        "{}-{}-{}.tmp",
        std::process::id(),
        nanos,
        TMP_SEQ.fetch_add(1, Ordering::Relaxed)
    )
}

impl FsStorage {
    pub fn new(root: impl Into<PathBuf>, metrics: Metrics) -> Result<Self> {
        let root = root.into();
        std::fs::create_dir_all(&root).wrap_err_with(|| format!("Failed to create {root:?}"))?;

        let staging = root.join(TMP_DIR_NAME);
        let name = root.to_string_lossy().into_owned();
        Ok(Self {
            root,
            staging,
            metrics,
            name,
        })
    }

    pub async fn with_prefix(self, prefix: impl AsRef<Path>) -> Result<Self> {
        let root = self.root.join(prefix.as_ref());
        fs::create_dir_all(&root)
            .await
            .wrap_err_with(|| format!("Failed to create {root:?}"))?;
        Ok(Self {
            root,
            staging: self.staging,
            metrics: self.metrics,
            name: self.name,
        })
    }

    async fn write_staged(&self, dir: &Path, data: &[u8]) -> Result<PathBuf> {
        let tmp_path = dir.join(tmp_name());

        let write = async {
            let mut file = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&tmp_path)
                .await?;
            file.write_all(data).await?;
            file.flush().await
        };

        match write.await {
            Ok(()) => Ok(tmp_path),
            Err(err) => {
                if err.kind() != std::io::ErrorKind::AlreadyExists {
                    let _ = fs::remove_file(&tmp_path).await;
                }
                Err(err).wrap_err_with(|| format!("Failed to write temp file {tmp_path:?}"))
            }
        }
    }

    async fn put_staged(&self, path: &Path, data: &[u8], policy: WritePolicy) -> Result<PutResult> {
        if policy == WritePolicy::NoClobber
            && fs::try_exists(path)
                .await
                .wrap_err_with(|| format!("Failed to check existence of {path:?}"))?
        {
            return Ok(PutResult::Skipped);
        }

        fs::create_dir_all(&self.staging)
            .await
            .wrap_err_with(|| format!("Failed to create {:?}", self.staging))?;
        let tmp_path = self.write_staged(&self.staging, data).await?;

        if let Err(err) = fs::rename(&tmp_path, path).await {
            let _ = fs::remove_file(&tmp_path).await;
            return Err(err).wrap_err_with(|| format!("Failed to rename {tmp_path:?} to {path:?}"));
        }
        Ok(PutResult::Written)
    }

    pub fn key_path(&self, key: &str) -> Result<PathBuf> {
        let relative = Path::new(key);
        if relative.is_absolute() {
            bail!("Absolute paths are not allowed for keys: {key}");
        }

        if relative
            .components()
            .any(|component| matches!(component, Component::ParentDir))
        {
            bail!("Parent directory segments are not allowed in keys: {key}");
        }

        Ok(self.root.join(relative))
    }

    pub fn path_to_key(root: &Path, name: &str, path: &Path) -> Result<String> {
        let relative = path
            .strip_prefix(root)
            .wrap_err_with(|| format!("Failed to strip prefix {name} from {path:?}"))?;

        Ok(relative
            .components()
            .map(|component| component.as_os_str().to_string_lossy())
            .collect::<Vec<_>>()
            .join("/"))
    }
}

impl KVReader for FsStorage {
    async fn get(&self, key: &str) -> Result<Option<Bytes>> {
        let path = self.key_path(key)?;
        let start = Instant::now();

        match fs::read(&path).await {
            Ok(bytes) => Ok(Some(Bytes::from(bytes))).write_get_metrics(
                start.elapsed(),
                KVStoreType::FileSystem,
                &self.metrics,
            ),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                kvstore_get_metrics(
                    start.elapsed(),
                    true,
                    KVStoreType::FileSystem,
                    &self.metrics,
                );
                Ok(None)
            }
            Err(err) => Err(err)
                .wrap_err_with(|| format!("Failed to read key {key} from path {path:?}"))
                .write_get_metrics_on_err(start.elapsed(), KVStoreType::FileSystem, &self.metrics),
        }
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        let path = self.key_path(key)?;
        let start = Instant::now();

        match fs::try_exists(&path).await {
            Ok(exists) => {
                kvstore_get_metrics(
                    start.elapsed(),
                    true,
                    KVStoreType::FileSystem,
                    &self.metrics,
                );
                Ok(exists)
            }
            Err(err) => Err(err)
                .wrap_err_with(|| {
                    format!("Failed to check existence of key {key} at path {path:?}")
                })
                .write_get_metrics_on_err(start.elapsed(), KVStoreType::FileSystem, &self.metrics),
        }
    }
}

impl KVStore for FsStorage {
    fn bucket_name(&self) -> &str {
        &self.name
    }

    async fn put(
        &self,
        key: impl AsRef<str>,
        data: Vec<u8>,
        policy: WritePolicy,
    ) -> Result<PutResult> {
        let key = key.as_ref();
        let path = self.key_path(key)?;

        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .await
                .wrap_err_with(|| format!("Failed to create directory {parent:?}"))?;
        }

        let start = Instant::now();

        match self.put_staged(&path, &data, policy).await {
            Ok(result) => {
                kvstore_put_metrics(
                    start.elapsed(),
                    true,
                    KVStoreType::FileSystem,
                    &self.metrics,
                );
                if result == PutResult::Skipped {
                    warn!(key, "FS put skipped: key already exists (NoClobber policy)");
                }
                Ok(result)
            }
            Err(err) => {
                kvstore_put_metrics(
                    start.elapsed(),
                    false,
                    KVStoreType::FileSystem,
                    &self.metrics,
                );
                Err(err).wrap_err_with(|| format!("Failed to write key {key} to path {path:?}"))
            }
        }
    }

    async fn scan_prefix(&self, prefix: &str) -> Result<Vec<String>> {
        let root = self.root.clone();
        let prefix = prefix.to_owned();
        let name = self.name.clone();
        let staging = self.staging.clone();

        spawn_blocking(move || -> Result<Vec<String>> {
            let mut matches = Vec::new();

            if !root.exists() {
                return Ok(matches);
            }

            let mut stack = vec![root.clone()];
            while let Some(dir) = stack.pop() {
                for entry in std::fs::read_dir(&dir)
                    .wrap_err_with(|| format!("Failed to read directory {dir:?}"))?
                {
                    let entry = entry?;
                    let path = entry.path();

                    if path.is_dir() {
                        if path != staging {
                            stack.push(path);
                        }
                        continue;
                    }

                    let key = Self::path_to_key(&root, &name, &path)?;

                    if key.starts_with(&prefix) {
                        matches.push(key);
                    }
                }
            }

            Ok(matches)
        })
        .await?
    }

    async fn delete(&self, key: impl AsRef<str>) -> Result<()> {
        let key = key.as_ref();
        let path = self.key_path(key)?;

        match fs::remove_file(&path).await {
            Ok(_) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => {
                Err(err).wrap_err_with(|| format!("Failed to delete key {key} at path {path:?}"))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    #[tokio::test]
    async fn test_basic_file_operations() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let storage = FsStorage::new(dir.path(), Metrics::none())?;

        let key = "nested/test-key";
        let data = b"hello world".to_vec();
        storage
            .put(key, data.clone(), WritePolicy::AllowOverwrite)
            .await?;

        let result = storage.get(key).await?.unwrap();
        assert_eq!(result, Bytes::from(data));

        let option = storage.get("missing").await?;
        assert!(option.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_scan_prefix() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let storage = FsStorage::new(dir.path(), Metrics::none())?;

        storage
            .put("test/a", b"a".to_vec(), WritePolicy::AllowOverwrite)
            .await?;
        storage
            .put("test/b", b"b".to_vec(), WritePolicy::AllowOverwrite)
            .await?;
        storage
            .put("other/c", b"c".to_vec(), WritePolicy::AllowOverwrite)
            .await?;

        let results = storage.scan_prefix("test").await?;
        assert_eq!(results.len(), 2);
        assert!(results.contains(&"test/a".to_string()));
        assert!(results.contains(&"test/b".to_string()));

        let results = storage.scan_prefix("missing").await?;
        assert!(results.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_delete() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let storage = FsStorage::new(dir.path(), Metrics::none())?;

        storage
            .put("delete/me", b"bye".to_vec(), WritePolicy::AllowOverwrite)
            .await?;
        storage.delete("delete/me").await?;

        let result = storage.get("delete/me").await?;
        assert!(result.is_none());

        // Deleting a missing key should be a no-op
        storage.delete("delete/me").await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_reject_parent_segments() {
        let dir = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(dir.path(), Metrics::none()).unwrap();
        let result = storage
            .put("../escape", b"oops".to_vec(), WritePolicy::AllowOverwrite)
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_nested_paths_create_directories() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let storage = FsStorage::new(dir.path(), Metrics::none())?;

        let key = "hi/bye/foo.txt";
        storage
            .put(key, b"data".to_vec(), WritePolicy::AllowOverwrite)
            .await?;

        let file_path = storage.root.join(key);
        assert!(file_path.is_file());
        assert!(storage.root.join("hi").is_dir());
        assert!(storage.root.join("hi/bye").is_dir());

        Ok(())
    }

    #[tokio::test]
    async fn test_noclobber_skips_existing_file() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let storage = FsStorage::new(dir.path(), Metrics::none())?;

        let key = "noclobber/test";
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
    async fn test_staging_hidden_from_scans_and_cleaned_up() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let storage = FsStorage::new(dir.path(), Metrics::none())?;
        let staging = storage.staging.clone();
        assert_eq!(staging, storage.root.join(TMP_DIR_NAME));

        storage
            .put("1", b"block".to_vec(), WritePolicy::AllowOverwrite)
            .await?;
        storage
            .put("2", b"block".to_vec(), WritePolicy::NoClobber)
            .await?;

        assert_eq!(std::fs::read_dir(&staging)?.count(), 0);
        let mut keys = storage.scan_prefix("").await?;
        keys.sort();
        assert_eq!(keys, vec!["1".to_string(), "2".to_string()]);

        let prefixed = storage.clone().with_prefix("1M/").await?;
        prefixed
            .put("3", b"block".to_vec(), WritePolicy::AllowOverwrite)
            .await?;
        assert_eq!(prefixed.staging, staging);
        assert_eq!(std::fs::read_dir(&staging)?.count(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_reads_do_not_create_tmp_dir() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let storage = FsStorage::new(dir.path(), Metrics::none())?;

        assert!(storage.get("missing").await?.is_none());
        assert!(!storage.staging.exists());

        Ok(())
    }

    #[tokio::test]
    async fn test_allow_overwrite_overwrites_existing_file() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let storage = FsStorage::new(dir.path(), Metrics::none())?;

        let key = "overwrite/test";
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
