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
    time::Instant,
};

use bytes::Bytes;
use eyre::{Context, Result};
use tokio::{fs, task::spawn_blocking};

use super::{kvstore_get_metrics, KVStoreType, MetricsResultExt};
use crate::{metrics::Metrics, prelude::*};

#[derive(Clone)]
pub struct FsStorage {
    pub root: PathBuf,
    metrics: Metrics,
    name: String,
}

impl FsStorage {
    pub fn new(root: impl Into<PathBuf>, metrics: Metrics) -> Result<Self> {
        let root = root.into();
        std::fs::create_dir_all(&root).wrap_err_with(|| format!("Failed to create {root:?}"))?;

        let name = root.to_string_lossy().into_owned();
        Ok(Self {
            root,
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
            metrics: self.metrics,
            name: self.name,
        })
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

    async fn scan_prefix_with_max_keys(
        &self,
        prefix: &str,
        max_keys: usize,
    ) -> Result<Vec<String>> {
        let root = self.root.clone();
        let prefix = prefix.to_owned();
        let name = self.name.clone();

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
                        stack.push(path);
                        continue;
                    }

                    let key = Self::path_to_key(&root, &name, &path)?;

                    if !key.starts_with(&prefix) {
                        continue;
                    }

                    matches.push(key);
                    if matches.len() >= max_keys {
                        return Ok(matches);
                    }
                }
            }

            Ok(matches)
        })
        .await?
    }
}

impl KVStore for FsStorage {
    fn bucket_name(&self) -> &str {
        &self.name
    }

    async fn put(&self, key: impl AsRef<str>, data: Vec<u8>) -> Result<()> {
        let key = key.as_ref();
        let path = self.key_path(key)?;

        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .await
                .wrap_err_with(|| format!("Failed to create directory {parent:?}"))?;
        }

        let start = Instant::now();
        fs::write(&path, &data)
            .await
            .write_put_metrics(start.elapsed(), KVStoreType::FileSystem, &self.metrics)
            .wrap_err_with(|| format!("Failed to write key {key} to path {path:?}"))?;

        Ok(())
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
        storage.put(key, data.clone()).await?;

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

        storage.put("test/a", b"a".to_vec()).await?;
        storage.put("test/b", b"b".to_vec()).await?;
        storage.put("other/c", b"c".to_vec()).await?;

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

        storage.put("delete/me", b"bye".to_vec()).await?;
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
        let result = storage.put("../escape", b"oops".to_vec()).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_nested_paths_create_directories() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let storage = FsStorage::new(dir.path(), Metrics::none())?;

        let key = "hi/bye/foo.txt";
        storage.put(key, b"data".to_vec()).await?;

        let file_path = dir.path().join(key);
        assert!(file_path.is_file());
        assert!(dir.path().join("hi").is_dir());
        assert!(dir.path().join("hi/bye").is_dir());

        Ok(())
    }
}
