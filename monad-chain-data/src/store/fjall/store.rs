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
    path::Path,
    sync::{Arc, Mutex},
};

use bytes::Bytes;
use fjall::{Config, Database, Keyspace, KeyspaceCreateOptions, KvSeparationOptions};

use crate::{
    error::{MonadChainDataError, Result},
    store::{
        blob::{BlobStore, BlobTableId},
        common::Page,
        meta::{CasOutcome, CasVersion, MetaStore, MetaStoreCas, ScannableTableId, TableId},
    },
};

// Logical-table namespacing on top of fjall keyspaces. Each logical table
// gets its own physical fjall keyspace, prefixed by the kind so the four
// trait surfaces never share storage even if their logical names collide.
const KV_PREFIX: &str = "kv:";
const SCAN_PREFIX: &str = "scan:";
const CAS_PREFIX: &str = "cas:";
const BLOB_PREFIX: &str = "blob:";

/// Embedded fjall-backed implementation of [`MetaStore`], [`MetaStoreCas`],
/// and [`BlobStore`]. Single-process only: CAS rows are protected by a
/// process-local mutex around their read-modify-write critical section,
/// not by the storage engine itself.
///
/// Blob keyspaces are opened with key-value separation enabled, so the
/// LSM holds only `(key → blob_file_id, offset, length)` pointers and
/// the actual bytes live in append-only blob files. Chain-data blobs
/// are written once and never deleted, so blob-file GC effectively never
/// fires; we just append. Sized for ~280M+ objects in the 10s of KB to
/// 16 MiB range. Operators that want to colocate the meta store and
/// blob store on different disks should construct two `FjallStore`s
/// pointed at different paths and pass them as the meta and blob
/// arguments.
pub struct FjallStore {
    inner: Arc<Inner>,
}

struct Inner {
    db: Database,
    keyspaces: Mutex<HashMap<String, Keyspace>>,
    // Held only inside `spawn_blocking`, never across an await. The
    // publication-state row is the only chain-data row touching the CAS
    // surface today, so contention is bounded to a single hot key.
    cas_lock: Mutex<()>,
}

impl FjallStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let db = Database::create_or_recover(Config::new(path.as_ref()))
            .map_err(|e| MonadChainDataError::Backend(format!("fjall open: {e}")))?;
        Ok(Self {
            inner: Arc::new(Inner {
                db,
                keyspaces: Mutex::new(HashMap::new()),
                cas_lock: Mutex::new(()),
            }),
        })
    }

    fn keyspace(&self, name: &str) -> Result<Keyspace> {
        let mut guard =
            self.inner.keyspaces.lock().map_err(|_| {
                MonadChainDataError::Backend("fjall keyspace cache poisoned".into())
            })?;
        if let Some(ks) = guard.get(name) {
            return Ok(ks.clone());
        }
        let is_blob = name.starts_with(BLOB_PREFIX);
        let ks = self
            .inner
            .db
            .keyspace(name, || {
                let opts = KeyspaceCreateOptions::default();
                if is_blob {
                    // KV separation: defaults are 1 KiB separation threshold,
                    // 64 MiB blob files, LZ4. Suits 10s of KB - 16 MiB blobs;
                    // staleness/age GC thresholds never fire under our
                    // append-only workload.
                    opts.with_kv_separation(Some(KvSeparationOptions::default()))
                } else {
                    opts
                }
            })
            .map_err(|e| MonadChainDataError::Backend(format!("fjall keyspace {name}: {e}")))?;
        guard.insert(name.to_string(), ks.clone());
        Ok(ks)
    }
}

impl Clone for FjallStore {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

async fn blocking<F, R>(f: F) -> Result<R>
where
    F: FnOnce() -> Result<R> + Send + 'static,
    R: Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| MonadChainDataError::Backend(format!("fjall spawn_blocking join: {e}")))?
}

// `2 BE bytes len(partition) | partition | clustering`. Length-prefixing
// keeps clustering decoding unambiguous and lets us derive a fjall prefix
// from `(partition, scan_prefix)` directly.
fn scan_key(partition: &[u8], clustering: &[u8]) -> Vec<u8> {
    let len = u16::try_from(partition.len()).expect("scan partition length fits in u16");
    let mut key = Vec::with_capacity(2 + partition.len() + clustering.len());
    key.extend_from_slice(&len.to_be_bytes());
    key.extend_from_slice(partition);
    key.extend_from_slice(clustering);
    key
}

fn encode_cas_row(version: u64, value: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(8 + value.len());
    out.extend_from_slice(&version.to_be_bytes());
    out.extend_from_slice(value);
    out
}

fn decode_cas_row(bytes: &[u8]) -> Result<(u64, Bytes)> {
    if bytes.len() < 8 {
        return Err(MonadChainDataError::Decode(
            "cas row missing version prefix",
        ));
    }
    let mut version_bytes = [0u8; 8];
    version_bytes.copy_from_slice(&bytes[..8]);
    Ok((
        u64::from_be_bytes(version_bytes),
        Bytes::copy_from_slice(&bytes[8..]),
    ))
}

impl MetaStore for FjallStore {
    async fn get(&self, table: TableId, key: &[u8]) -> Result<Option<Bytes>> {
        let store = self.clone();
        let key = key.to_vec();
        let name = format!("{KV_PREFIX}{}", table.as_str());
        blocking(move || {
            let ks = store.keyspace(&name)?;
            let value = ks
                .get(&key)
                .map_err(|e| MonadChainDataError::Backend(format!("fjall get: {e}")))?;
            Ok(value.map(|v| Bytes::copy_from_slice(&v)))
        })
        .await
    }

    async fn scan_get(
        &self,
        table: ScannableTableId,
        partition: &[u8],
        clustering: &[u8],
    ) -> Result<Option<Bytes>> {
        let store = self.clone();
        let composite = scan_key(partition, clustering);
        let name = format!("{SCAN_PREFIX}{}", table.as_str());
        blocking(move || {
            let ks = store.keyspace(&name)?;
            let value = ks
                .get(&composite)
                .map_err(|e| MonadChainDataError::Backend(format!("fjall scan_get: {e}")))?;
            Ok(value.map(|v| Bytes::copy_from_slice(&v)))
        })
        .await
    }

    async fn put(&self, table: TableId, key: &[u8], value: Bytes) -> Result<()> {
        let store = self.clone();
        let key = key.to_vec();
        let name = format!("{KV_PREFIX}{}", table.as_str());
        let value = value.to_vec();
        blocking(move || {
            let ks = store.keyspace(&name)?;
            ks.insert(&key, value)
                .map_err(|e| MonadChainDataError::Backend(format!("fjall put: {e}")))?;
            Ok(())
        })
        .await
    }

    async fn scan_put(
        &self,
        table: ScannableTableId,
        partition: &[u8],
        clustering: &[u8],
        value: Bytes,
    ) -> Result<()> {
        let store = self.clone();
        let composite = scan_key(partition, clustering);
        let name = format!("{SCAN_PREFIX}{}", table.as_str());
        let value = value.to_vec();
        blocking(move || {
            let ks = store.keyspace(&name)?;
            ks.insert(&composite, value)
                .map_err(|e| MonadChainDataError::Backend(format!("fjall scan_put: {e}")))?;
            Ok(())
        })
        .await
    }

    async fn scan_list(
        &self,
        table: ScannableTableId,
        partition: &[u8],
        prefix: &[u8],
        cursor: Option<Vec<u8>>,
        limit: usize,
    ) -> Result<Page> {
        let store = self.clone();
        let partition = partition.to_vec();
        let prefix = prefix.to_vec();
        let name = format!("{SCAN_PREFIX}{}", table.as_str());
        blocking(move || {
            let ks = store.keyspace(&name)?;
            let search = scan_key(&partition, &prefix);
            // Skip past `len(2) + partition` to get the clustering portion.
            let header_len = 2 + partition.len();
            let mut keys: Vec<Vec<u8>> = Vec::new();
            let mut next_cursor: Option<Vec<u8>> = None;

            for guard in ks.prefix(&search) {
                let user_key = guard
                    .key()
                    .map_err(|e| MonadChainDataError::Backend(format!("fjall scan_list: {e}")))?;
                let key_bytes: &[u8] = user_key.as_ref();
                if key_bytes.len() < header_len {
                    return Err(MonadChainDataError::Decode(
                        "scan row missing partition prefix",
                    ));
                }
                let clustering = &key_bytes[header_len..];

                if let Some(ref cursor_bytes) = cursor {
                    if clustering <= cursor_bytes.as_slice() {
                        continue;
                    }
                }
                if keys.len() == limit {
                    next_cursor = keys.last().cloned();
                    break;
                }
                keys.push(clustering.to_vec());
            }

            Ok(Page { keys, next_cursor })
        })
        .await
    }
}

impl MetaStoreCas for FjallStore {
    async fn cas_get(&self, table: TableId, key: &[u8]) -> Result<Option<(CasVersion, Bytes)>> {
        let store = self.clone();
        let key = key.to_vec();
        let name = format!("{CAS_PREFIX}{}", table.as_str());
        blocking(move || {
            let ks = store.keyspace(&name)?;
            let raw = ks
                .get(&key)
                .map_err(|e| MonadChainDataError::Backend(format!("fjall cas_get: {e}")))?;
            let Some(raw) = raw else {
                return Ok(None);
            };
            let (version, value) = decode_cas_row(raw.as_ref())?;
            Ok(Some((CasVersion(version), value)))
        })
        .await
    }

    async fn cas_put(
        &self,
        table: TableId,
        key: &[u8],
        expected: Option<CasVersion>,
        value: Bytes,
    ) -> Result<CasOutcome> {
        let store = self.clone();
        let key = key.to_vec();
        let name = format!("{CAS_PREFIX}{}", table.as_str());
        let value = value.to_vec();
        blocking(move || {
            let ks = store.keyspace(&name)?;
            let _guard = store
                .inner
                .cas_lock
                .lock()
                .map_err(|_| MonadChainDataError::Backend("fjall cas lock poisoned".into()))?;

            let raw = ks
                .get(&key)
                .map_err(|e| MonadChainDataError::Backend(format!("fjall cas_put read: {e}")))?;
            let current_version = match raw {
                None => None,
                Some(raw) => {
                    let (version, _) = decode_cas_row(raw.as_ref())?;
                    Some(CasVersion(version))
                }
            };
            if current_version != expected {
                return Ok(CasOutcome::Conflict { current_version });
            }
            let new_version = current_version.map_or(1, |v| v.0 + 1);
            ks.insert(&key, encode_cas_row(new_version, &value))
                .map_err(|e| MonadChainDataError::Backend(format!("fjall cas_put write: {e}")))?;
            Ok(CasOutcome::Applied {
                new_version: CasVersion(new_version),
            })
        })
        .await
    }
}

impl BlobStore for FjallStore {
    async fn put_blob(&self, table: BlobTableId, key: &[u8], value: Bytes) -> Result<()> {
        let store = self.clone();
        let key = key.to_vec();
        let name = format!("{BLOB_PREFIX}{}", table.as_str());
        let value = value.to_vec();
        blocking(move || {
            let ks = store.keyspace(&name)?;
            ks.insert(&key, value)
                .map_err(|e| MonadChainDataError::Backend(format!("fjall put_blob: {e}")))?;
            Ok(())
        })
        .await
    }

    async fn get_blob(&self, table: BlobTableId, key: &[u8]) -> Result<Option<Bytes>> {
        let store = self.clone();
        let key = key.to_vec();
        let name = format!("{BLOB_PREFIX}{}", table.as_str());
        blocking(move || {
            let ks = store.keyspace(&name)?;
            let value = ks
                .get(&key)
                .map_err(|e| MonadChainDataError::Backend(format!("fjall get_blob: {e}")))?;
            Ok(value.map(|v| Bytes::copy_from_slice(&v)))
        })
        .await
    }
}
