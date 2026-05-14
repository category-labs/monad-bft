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

use std::{collections::HashMap, sync::Arc};

use bytes::Bytes;
use fjall::Keyspace;

use crate::{
    error::{MonadChainDataError, Result},
    store::{
        blob::{BlobTableId, BlobWriteBatch},
        fjall::store::{
            blocking, decode_cas_row, encode_cas_row, scan_key, Inner, BLOB_PREFIX, CAS_PREFIX,
            KV_PREFIX, SCAN_PREFIX,
        },
        meta::{CasOutcome, CasVersion, MetaWriteBatch, ScannableTableId, TableId},
    },
};

enum PendingMeta {
    Put(TableId, Vec<u8>, Vec<u8>),
    ScanPut(ScannableTableId, Vec<u8>, Vec<u8>, Vec<u8>),
}

pub struct FjallMetaBatch {
    inner: Arc<Inner>,
    pending: Vec<PendingMeta>,
}

impl FjallMetaBatch {
    pub(super) fn new(inner: Arc<Inner>) -> Self {
        Self {
            inner,
            pending: Vec::new(),
        }
    }
}

impl MetaWriteBatch for FjallMetaBatch {
    fn put(&mut self, table: TableId, key: &[u8], value: Bytes) {
        self.pending
            .push(PendingMeta::Put(table, key.to_vec(), value.to_vec()));
    }

    fn scan_put(
        &mut self,
        table: ScannableTableId,
        partition: &[u8],
        clustering: &[u8],
        value: Bytes,
    ) {
        self.pending.push(PendingMeta::ScanPut(
            table,
            partition.to_vec(),
            clustering.to_vec(),
            value.to_vec(),
        ));
    }

    async fn commit(self) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let inner = self.inner;
        let pending = self.pending;
        blocking(move || {
            let mut cache: HashMap<String, Keyspace> = HashMap::new();
            let mut batch = inner.db.batch();
            for op in pending {
                match op {
                    PendingMeta::Put(table, key, value) => {
                        let name = format!("{KV_PREFIX}{}", table.as_str());
                        let ks = keyspace_cached(&inner, &mut cache, name)?;
                        batch.insert(&ks, key, value);
                    }
                    PendingMeta::ScanPut(table, partition, clustering, value) => {
                        let name = format!("{SCAN_PREFIX}{}", table.as_str());
                        let ks = keyspace_cached(&inner, &mut cache, name)?;
                        let composite = scan_key(&partition, &clustering);
                        batch.insert(&ks, composite, value);
                    }
                }
            }
            batch
                .commit()
                .map_err(|e| MonadChainDataError::Backend(format!("fjall batch commit: {e}")))?;
            Ok(())
        })
        .await
    }

    /// On fjall this commit is strictly all-or-nothing: the implementation
    /// brackets the CAS read, the buffered-write staging, and the final
    /// `batch.commit()` under `inner.cas_lock`, then ships everything in a
    /// single `Batch::commit` so the WAL append is atomic. On `Conflict` no
    /// buffered writes are persisted (the batch is dropped without commit);
    /// on `Applied` every buffered kv/scan write lands together with the
    /// updated CAS row in one fjall transaction.
    async fn commit_with_cas(
        self,
        table: TableId,
        key: &[u8],
        expected: Option<CasVersion>,
        value: Bytes,
    ) -> Result<CasOutcome> {
        let inner = self.inner;
        let pending = self.pending;
        let cas_name = format!("{CAS_PREFIX}{}", table.as_str());
        let cas_key = key.to_vec();
        let cas_value = value.to_vec();
        blocking(move || {
            let mut cache: HashMap<String, Keyspace> = HashMap::new();
            let cas_ks = keyspace_cached(&inner, &mut cache, cas_name)?;
            let _guard = inner
                .cas_lock
                .lock()
                .map_err(|_| MonadChainDataError::Backend("fjall cas lock poisoned".into()))?;
            let raw = cas_ks
                .get(&cas_key)
                .map_err(|e| MonadChainDataError::Backend(format!("fjall cas read: {e}")))?;
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
            let mut batch = inner.db.batch();
            for op in pending {
                match op {
                    PendingMeta::Put(t, k, v) => {
                        let name = format!("{KV_PREFIX}{}", t.as_str());
                        let ks = keyspace_cached(&inner, &mut cache, name)?;
                        batch.insert(&ks, k, v);
                    }
                    PendingMeta::ScanPut(t, p, c, v) => {
                        let name = format!("{SCAN_PREFIX}{}", t.as_str());
                        let ks = keyspace_cached(&inner, &mut cache, name)?;
                        let composite = scan_key(&p, &c);
                        batch.insert(&ks, composite, v);
                    }
                }
            }
            batch.insert(&cas_ks, cas_key, encode_cas_row(new_version, &cas_value));
            batch
                .commit()
                .map_err(|e| MonadChainDataError::Backend(format!("fjall cas batch commit: {e}")))?;
            Ok(CasOutcome::Applied {
                new_version: CasVersion(new_version),
            })
        })
        .await
    }
}

pub struct FjallBlobBatch {
    inner: Arc<Inner>,
    pending: Vec<(BlobTableId, Vec<u8>, Vec<u8>)>,
}

impl FjallBlobBatch {
    pub(super) fn new(inner: Arc<Inner>) -> Self {
        Self {
            inner,
            pending: Vec::new(),
        }
    }
}

impl BlobWriteBatch for FjallBlobBatch {
    fn put_blob(&mut self, table: BlobTableId, key: &[u8], value: Bytes) {
        self.pending.push((table, key.to_vec(), value.to_vec()));
    }

    async fn commit(self) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let inner = self.inner;
        let pending = self.pending;
        blocking(move || {
            let mut cache: HashMap<String, Keyspace> = HashMap::new();
            let mut batch = inner.db.batch();
            for (table, key, value) in pending {
                let name = format!("{BLOB_PREFIX}{}", table.as_str());
                let ks = keyspace_cached(&inner, &mut cache, name)?;
                batch.insert(&ks, key, value);
            }
            batch
                .commit()
                .map_err(|e| MonadChainDataError::Backend(format!("fjall blob batch commit: {e}")))?;
            Ok(())
        })
        .await
    }
}

fn keyspace_cached(
    inner: &Arc<Inner>,
    cache: &mut HashMap<String, Keyspace>,
    name: String,
) -> Result<Keyspace> {
    if let Some(ks) = cache.get(&name) {
        return Ok(ks.clone());
    }
    let mut guard = inner
        .keyspaces
        .lock()
        .map_err(|_| MonadChainDataError::Backend("fjall keyspace cache poisoned".into()))?;
    if let Some(ks) = guard.get(&name) {
        let cloned = ks.clone();
        cache.insert(name, cloned.clone());
        return Ok(cloned);
    }
    let is_blob = name.starts_with(BLOB_PREFIX);
    let ks = inner
        .db
        .keyspace(&name, || {
            let opts = fjall::KeyspaceCreateOptions::default();
            if is_blob {
                opts.with_kv_separation(Some(fjall::KvSeparationOptions::default()))
            } else {
                opts
            }
        })
        .map_err(|e| MonadChainDataError::Backend(format!("fjall keyspace {name}: {e}")))?;
    guard.insert(name.clone(), ks.clone());
    cache.insert(name, ks.clone());
    Ok(ks)
}
