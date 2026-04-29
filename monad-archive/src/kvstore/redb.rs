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

//! Local redb-backed `KVStore` for the BFT migration's legacy header cache and
//! local index. redb is single-writer / multi-reader: writes are serialized via
//! a tokio mutex; reads run concurrently. All txn bodies execute on the
//! blocking pool because redb's API is synchronous.

use std::{path::Path, sync::Arc};

use bytes::Bytes;
use eyre::{Context, Result};
use redb::{Database, ReadableTable, TableDefinition};
use tokio::sync::Mutex;

use super::{KVReader, KVStore, PutResult, WritePolicy};

const KV_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("kv");

#[derive(Clone)]
pub struct RedbStorage {
    db: Arc<Database>,
    name: String,
    write_lock: Arc<Mutex<()>>,
}

impl RedbStorage {
    /// Open or create a redb database at `path`, ensuring the kv table exists.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let name = path.to_string_lossy().into_owned();
        let db = Database::create(path)
            .wrap_err_with(|| format!("Failed to open redb at {name}"))?;
        // Force-create the table so empty-database reads don't fail.
        let txn = db.begin_write().wrap_err("Failed to begin redb write txn")?;
        {
            let _ = txn
                .open_table(KV_TABLE)
                .wrap_err("Failed to open redb kv table")?;
        }
        txn.commit().wrap_err("Failed to commit redb table init")?;
        Ok(Self {
            db: Arc::new(db),
            name,
            write_lock: Arc::new(Mutex::new(())),
        })
    }
}

impl KVReader for RedbStorage {
    async fn get(&self, key: &str) -> Result<Option<Bytes>> {
        let db = self.db.clone();
        let key = key.to_owned();
        tokio::task::spawn_blocking(move || -> Result<Option<Bytes>> {
            let txn = db.begin_read().wrap_err("redb begin_read")?;
            let table = txn.open_table(KV_TABLE).wrap_err("redb open_table")?;
            match table.get(key.as_str()).wrap_err("redb get")? {
                Some(guard) => Ok(Some(Bytes::copy_from_slice(guard.value()))),
                None => Ok(None),
            }
        })
        .await
        .wrap_err("redb get task panicked")?
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        let db = self.db.clone();
        let key = key.to_owned();
        tokio::task::spawn_blocking(move || -> Result<bool> {
            let txn = db.begin_read().wrap_err("redb begin_read")?;
            let table = txn.open_table(KV_TABLE).wrap_err("redb open_table")?;
            Ok(table.get(key.as_str()).wrap_err("redb get")?.is_some())
        })
        .await
        .wrap_err("redb exists task panicked")?
    }

    async fn scan_prefix_with_max_keys(
        &self,
        prefix: &str,
        max_keys: usize,
    ) -> Result<Vec<String>> {
        let db = self.db.clone();
        let prefix = prefix.to_owned();
        tokio::task::spawn_blocking(move || -> Result<Vec<String>> {
            let txn = db.begin_read().wrap_err("redb begin_read")?;
            let table = txn.open_table(KV_TABLE).wrap_err("redb open_table")?;
            let mut out = Vec::new();
            for entry in table
                .range::<&str>(prefix.as_str()..)
                .wrap_err("redb range")?
            {
                let (k, _) = entry.wrap_err("redb range entry")?;
                let k = k.value();
                if !k.starts_with(&prefix) {
                    break;
                }
                if out.len() >= max_keys {
                    break;
                }
                out.push(k.to_string());
            }
            Ok(out)
        })
        .await
        .wrap_err("redb scan_prefix task panicked")?
    }
}

impl KVStore for RedbStorage {
    fn bucket_name(&self) -> &str {
        &self.name
    }

    async fn put(
        &self,
        key: impl AsRef<str>,
        data: Vec<u8>,
        policy: WritePolicy,
    ) -> Result<PutResult> {
        let key = key.as_ref().to_owned();
        let db = self.db.clone();
        let guard = self.write_lock.clone().lock_owned().await;
        tokio::task::spawn_blocking(move || -> Result<PutResult> {
            let _guard = guard;
            let txn = db.begin_write().wrap_err("redb begin_write")?;
            let result = {
                let mut table = txn.open_table(KV_TABLE).wrap_err("redb open_table")?;
                if policy == WritePolicy::NoClobber
                    && table.get(key.as_str()).wrap_err("redb get")?.is_some()
                {
                    PutResult::Skipped
                } else {
                    table
                        .insert(key.as_str(), data.as_slice())
                        .wrap_err("redb insert")?;
                    PutResult::Written
                }
            };
            txn.commit().wrap_err("redb commit")?;
            Ok(result)
        })
        .await
        .wrap_err("redb put task panicked")?
    }

    async fn delete(&self, key: impl AsRef<str>) -> Result<()> {
        let key = key.as_ref().to_owned();
        let db = self.db.clone();
        let guard = self.write_lock.clone().lock_owned().await;
        tokio::task::spawn_blocking(move || -> Result<()> {
            let _guard = guard;
            let txn = db.begin_write().wrap_err("redb begin_write")?;
            {
                let mut table = txn.open_table(KV_TABLE).wrap_err("redb open_table")?;
                table.remove(key.as_str()).wrap_err("redb remove")?;
            }
            txn.commit().wrap_err("redb commit")?;
            Ok(())
        })
        .await
        .wrap_err("redb delete task panicked")?
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    fn fresh() -> (TempDir, RedbStorage) {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.redb");
        let storage = RedbStorage::open(&path).unwrap();
        (dir, storage)
    }

    #[tokio::test]
    async fn round_trip() {
        let (_dir, s) = fresh();
        s.put("a", b"hello".to_vec(), WritePolicy::AllowOverwrite)
            .await
            .unwrap();
        let got = s.get("a").await.unwrap().unwrap();
        assert_eq!(got.as_ref(), b"hello");
        assert!(s.exists("a").await.unwrap());
        assert!(!s.exists("missing").await.unwrap());
    }

    #[tokio::test]
    async fn no_clobber_skips_existing() {
        let (_dir, s) = fresh();
        let r1 = s
            .put("k", b"v1".to_vec(), WritePolicy::NoClobber)
            .await
            .unwrap();
        assert_eq!(r1, PutResult::Written);
        let r2 = s
            .put("k", b"v2".to_vec(), WritePolicy::NoClobber)
            .await
            .unwrap();
        assert_eq!(r2, PutResult::Skipped);
        assert_eq!(s.get("k").await.unwrap().unwrap().as_ref(), b"v1");
    }

    #[tokio::test]
    async fn allow_overwrite_replaces() {
        let (_dir, s) = fresh();
        s.put("k", b"v1".to_vec(), WritePolicy::AllowOverwrite)
            .await
            .unwrap();
        s.put("k", b"v2".to_vec(), WritePolicy::AllowOverwrite)
            .await
            .unwrap();
        assert_eq!(s.get("k").await.unwrap().unwrap().as_ref(), b"v2");
    }

    #[tokio::test]
    async fn delete_removes_key() {
        let (_dir, s) = fresh();
        s.put("k", b"v".to_vec(), WritePolicy::AllowOverwrite)
            .await
            .unwrap();
        s.delete("k").await.unwrap();
        assert!(s.get("k").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn scan_prefix_returns_matches_in_order() {
        let (_dir, s) = fresh();
        for k in ["a/1", "a/2", "a/3", "b/1", "ab/1"] {
            s.put(k, vec![0u8], WritePolicy::AllowOverwrite)
                .await
                .unwrap();
        }
        let got = s.scan_prefix("a/").await.unwrap();
        assert_eq!(got, vec!["a/1", "a/2", "a/3"]);
        let bounded = s.scan_prefix_with_max_keys("a/", 2).await.unwrap();
        assert_eq!(bounded, vec!["a/1", "a/2"]);
    }

    #[tokio::test]
    async fn persists_across_reopen() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("p.redb");
        {
            let s = RedbStorage::open(&path).unwrap();
            s.put("k", b"v".to_vec(), WritePolicy::AllowOverwrite)
                .await
                .unwrap();
        }
        let s = RedbStorage::open(&path).unwrap();
        assert_eq!(s.get("k").await.unwrap().unwrap().as_ref(), b"v");
    }
}
