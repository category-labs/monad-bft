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

//! Embedded fjall-backed `KVStore`. Stands in wherever `Bucket`, `FsStorage`,
//! or `MongoDbStorage` are used — same flat `&str`-keyed namespace as S3, so
//! the archive layout copies down byte-identical from a remote bucket.
//!
//! Each `FjallKvStore` opens a single fjall keyspace ("data") with KV
//! separation enabled. Small archive entries (latest/index keys) stay in the
//! LSM; block bodies/receipts/traces land in append-only blob files. This is
//! deliberately not a one-keyspace-per-prefix setup because the archive's
//! `&str` keys are opaque to us — we don't know which prefix bucket a key
//! belongs to without parsing, and KV separation makes the split irrelevant.

use std::{
    path::{Path, PathBuf},
    sync::Mutex,
    time::Instant,
};

use bytes::Bytes;
use eyre::{Context, Result};
use fjall::{
    config::CompressionPolicy, CompressionType, Config, Database, Keyspace,
    KeyspaceCreateOptions, KvSeparationOptions,
};
use tokio::task::spawn_blocking;

use super::{KVStoreType, MetricsResultExt, PutResult, WritePolicy};
use crate::{metrics::Metrics, prelude::*};

const KEYSPACE_NAME: &str = "data";

/// Embedded fjall-backed KV store. Cheaply cloneable — clones share the
/// underlying `Database` and `Keyspace` handles, which are themselves
/// `Arc`-wrapped internally.
#[derive(Clone)]
pub struct FjallKvStore {
    inner: Arc<Inner>,
}

struct Inner {
    // `Database` and `Keyspace` are `Arc`s under the hood, so cloning is
    // cheap; we still wrap in `Arc<Inner>` so `bucket_name()` returns a
    // borrow into stable, non-cloned storage.
    _db: Database,
    keyspace: Keyspace,
    name: String,
    metrics: Metrics,
    // Serializes NoClobber put paths so the check-then-insert is atomic
    // across blocking workers. Held only inside `spawn_blocking`, never
    // across an await.
    no_clobber_lock: Mutex<()>,
}

impl FjallKvStore {
    pub fn open(path: impl AsRef<Path>, metrics: Metrics) -> Result<Self> {
        let path: PathBuf = path.as_ref().to_path_buf();
        std::fs::create_dir_all(&path)
            .wrap_err_with(|| format!("Failed to create fjall directory {path:?}"))?;

        let db = Database::create_or_recover(Config::new(&path))
            .wrap_err_with(|| format!("Failed to open fjall database at {path:?}"))?;

        let keyspace = db
            .keyspace(KEYSPACE_NAME, || {
                // LZ4 everywhere: fjall's defaults skip L0/L1 data blocks and
                // all index blocks. Archive payloads compress well and we
                // care about on-disk size more than write latency, so apply
                // LZ4 to every level. (fjall 3.1 doesn't expose an LZ4 HC /
                // level knob — `Lz4` is the only setting available.)
                //
                // KV separation: defaults are 1 KiB threshold, 64 MiB blob
                // files. Small archive entries stay inline; block
                // bodies/receipts/traces land in blob files. Archive writes
                // are append-mostly so blob GC effectively never fires.
                KeyspaceCreateOptions::default()
                    .data_block_compression_policy(CompressionPolicy::all(
                        CompressionType::Lz4,
                    ))
                    .index_block_compression_policy(CompressionPolicy::all(
                        CompressionType::Lz4,
                    ))
                    .with_kv_separation(Some(
                        KvSeparationOptions::default().compression(CompressionType::Lz4),
                    ))
            })
            .wrap_err_with(|| format!("Failed to open fjall keyspace at {path:?}"))?;

        let name = path.to_string_lossy().into_owned();
        Ok(Self {
            inner: Arc::new(Inner {
                _db: db,
                keyspace,
                name,
                metrics,
                no_clobber_lock: Mutex::new(()),
            }),
        })
    }
}

impl KVReader for FjallKvStore {
    async fn get(&self, key: &str) -> Result<Option<Bytes>> {
        let inner = Arc::clone(&self.inner);
        let key = key.to_owned();
        let start = Instant::now();

        let res = spawn_blocking(move || -> Result<Option<Bytes>> {
            let value = inner
                .keyspace
                .get(key.as_bytes())
                .wrap_err_with(|| format!("fjall get {key}"))?;
            Ok(value.map(|v| Bytes::copy_from_slice(v.as_ref())))
        })
        .await
        .wrap_err("fjall get join")?;

        res.write_get_metrics(start.elapsed(), KVStoreType::Fjall, &self.inner.metrics)
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        let inner = Arc::clone(&self.inner);
        let key = key.to_owned();
        let start = Instant::now();

        let res = spawn_blocking(move || -> Result<bool> {
            inner
                .keyspace
                .contains_key(key.as_bytes())
                .wrap_err_with(|| format!("fjall exists {key}"))
        })
        .await
        .wrap_err("fjall exists join")?;

        res.write_get_metrics(start.elapsed(), KVStoreType::Fjall, &self.inner.metrics)
    }
}

impl KVStore for FjallKvStore {
    fn bucket_name(&self) -> &str {
        &self.inner.name
    }

    async fn put(
        &self,
        key: impl AsRef<str>,
        data: Vec<u8>,
        policy: WritePolicy,
    ) -> Result<PutResult> {
        let key = key.as_ref().to_owned();
        let inner = Arc::clone(&self.inner);
        let start = Instant::now();

        let res = spawn_blocking(move || -> Result<PutResult> {
            match policy {
                WritePolicy::AllowOverwrite => {
                    inner
                        .keyspace
                        .insert(key.as_bytes(), data)
                        .wrap_err_with(|| format!("fjall put {key}"))?;
                    Ok(PutResult::Written)
                }
                WritePolicy::NoClobber => {
                    let _guard = inner.no_clobber_lock.lock().map_err(|_| {
                        eyre::eyre!("fjall no_clobber_lock poisoned")
                    })?;
                    let exists = inner
                        .keyspace
                        .contains_key(key.as_bytes())
                        .wrap_err_with(|| format!("fjall put exists check {key}"))?;
                    if exists {
                        warn!(key, "Fjall put skipped: key already exists (NoClobber policy)");
                        return Ok(PutResult::Skipped);
                    }
                    inner
                        .keyspace
                        .insert(key.as_bytes(), data)
                        .wrap_err_with(|| format!("fjall put {key}"))?;
                    Ok(PutResult::Written)
                }
            }
        })
        .await
        .wrap_err("fjall put join")?;

        res.write_put_metrics(start.elapsed(), KVStoreType::Fjall, &self.inner.metrics)
    }

    async fn scan_prefix(&self, prefix: &str) -> Result<Vec<String>> {
        let inner = Arc::clone(&self.inner);
        let prefix = prefix.to_owned();
        let start = Instant::now();

        let res = spawn_blocking(move || -> Result<Vec<String>> {
            let mut out = Vec::new();
            for guard in inner.keyspace.prefix(prefix.as_bytes()) {
                let user_key = guard
                    .key()
                    .wrap_err_with(|| format!("fjall scan_prefix iter {prefix}"))?;
                let s = std::str::from_utf8(user_key.as_ref())
                    .wrap_err_with(|| format!("fjall scan_prefix: non-utf8 key under {prefix}"))?;
                out.push(s.to_owned());
            }
            Ok(out)
        })
        .await
        .wrap_err("fjall scan_prefix join")?;

        // scan_prefix is a read; reuse the get-metric channel.
        res.write_get_metrics(start.elapsed(), KVStoreType::Fjall, &self.inner.metrics)
    }

    async fn delete(&self, key: impl AsRef<str>) -> Result<()> {
        let key = key.as_ref().to_owned();
        let inner = Arc::clone(&self.inner);
        let start = Instant::now();

        let res = spawn_blocking(move || -> Result<()> {
            inner
                .keyspace
                .remove(key.as_bytes())
                .wrap_err_with(|| format!("fjall delete {key}"))
        })
        .await
        .wrap_err("fjall delete join")?;

        // Delete counts as a write for telemetry purposes.
        res.write_put_metrics(start.elapsed(), KVStoreType::Fjall, &self.inner.metrics)
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    fn open_store() -> (tempfile::TempDir, FjallKvStore) {
        let dir = tempfile::tempdir().unwrap();
        let store = FjallKvStore::open(dir.path(), Metrics::none()).unwrap();
        (dir, store)
    }

    #[tokio::test]
    async fn basic_put_get_roundtrip() -> Result<()> {
        let (_dir, store) = open_store();

        let key = "nested/test-key";
        let data = b"hello world".to_vec();
        store
            .put(key, data.clone(), WritePolicy::AllowOverwrite)
            .await?;

        let result = store.get(key).await?.unwrap();
        assert_eq!(result, Bytes::from(data));

        let missing = store.get("missing").await?;
        assert!(missing.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn exists_reports_membership() -> Result<()> {
        let (_dir, store) = open_store();
        assert!(!store.exists("k").await?);
        store
            .put("k", b"v".to_vec(), WritePolicy::AllowOverwrite)
            .await?;
        assert!(store.exists("k").await?);
        Ok(())
    }

    #[tokio::test]
    async fn scan_prefix_filters_by_prefix() -> Result<()> {
        let (_dir, store) = open_store();
        store
            .put("test/a", b"a".to_vec(), WritePolicy::AllowOverwrite)
            .await?;
        store
            .put("test/b", b"b".to_vec(), WritePolicy::AllowOverwrite)
            .await?;
        store
            .put("other/c", b"c".to_vec(), WritePolicy::AllowOverwrite)
            .await?;

        let mut results = store.scan_prefix("test").await?;
        results.sort();
        assert_eq!(results, vec!["test/a".to_string(), "test/b".to_string()]);

        let results = store.scan_prefix("missing").await?;
        assert!(results.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn delete_removes_key() -> Result<()> {
        let (_dir, store) = open_store();
        store
            .put("k", b"v".to_vec(), WritePolicy::AllowOverwrite)
            .await?;
        store.delete("k").await?;
        assert!(store.get("k").await?.is_none());
        // Deleting a missing key is a no-op.
        store.delete("k").await?;
        Ok(())
    }

    #[tokio::test]
    async fn noclobber_skips_existing_key() -> Result<()> {
        let (_dir, store) = open_store();
        let key = "noclobber/test";
        let original = b"original".to_vec();
        let replacement = b"new".to_vec();

        let r = store
            .put(key, original.clone(), WritePolicy::NoClobber)
            .await?;
        assert_eq!(r, PutResult::Written);

        let r = store.put(key, replacement, WritePolicy::NoClobber).await?;
        assert_eq!(r, PutResult::Skipped);

        let stored = store.get(key).await?.unwrap();
        assert_eq!(stored, Bytes::from(original));
        Ok(())
    }

    #[tokio::test]
    async fn allow_overwrite_replaces_existing() -> Result<()> {
        let (_dir, store) = open_store();
        let key = "overwrite/test";

        store
            .put(key, b"original".to_vec(), WritePolicy::AllowOverwrite)
            .await?;
        let r = store
            .put(key, b"new".to_vec(), WritePolicy::AllowOverwrite)
            .await?;
        assert_eq!(r, PutResult::Written);

        let stored = store.get(key).await?.unwrap();
        assert_eq!(stored, Bytes::from(b"new".to_vec()));
        Ok(())
    }

    #[tokio::test]
    async fn store_persists_across_reopen() -> Result<()> {
        let dir = tempfile::tempdir()?;
        {
            let store = FjallKvStore::open(dir.path(), Metrics::none())?;
            store
                .put("persist/k", b"v".to_vec(), WritePolicy::AllowOverwrite)
                .await?;
        }
        let store = FjallKvStore::open(dir.path(), Metrics::none())?;
        let got = store.get("persist/k").await?.unwrap();
        assert_eq!(got, Bytes::from(b"v".to_vec()));
        Ok(())
    }

    #[tokio::test]
    async fn bulk_get_returns_present_keys_only() -> Result<()> {
        let (_dir, store) = open_store();
        store
            .put("a", b"1".to_vec(), WritePolicy::AllowOverwrite)
            .await?;
        store
            .put("c", b"3".to_vec(), WritePolicy::AllowOverwrite)
            .await?;

        let got = store
            .bulk_get(&["a".to_string(), "b".to_string(), "c".to_string()])
            .await?;
        assert_eq!(got.len(), 2);
        assert_eq!(got.get("a").unwrap(), &Bytes::from(b"1".to_vec()));
        assert_eq!(got.get("c").unwrap(), &Bytes::from(b"3".to_vec()));
        assert!(!got.contains_key("b"));
        Ok(())
    }
}
