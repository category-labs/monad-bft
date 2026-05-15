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
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};

use bytes::Bytes;
use lru::LruCache;

use crate::{
    error::Result,
    store::{
        blob::{BlobStore, BlobTable, BlobTableId},
        common::Page,
        meta::{KvTable, MetaStore, ScannableKvTable, ScannableTableId, TableId},
    },
};

#[derive(Debug, Clone, Copy)]
pub struct CacheConfig {
    pub dir_by_block_entries: usize,
    pub dir_bucket_entries: usize,
    pub bitmap_by_block_entries: usize,
    pub bitmap_page_meta_entries: usize,
    pub bitmap_page_blob_entries: usize,
    pub open_bitmap_stream_entries: usize,
    pub block_record_entries: usize,
    pub block_header_entries: usize,
    pub block_hash_to_number_entries: usize,
    pub tx_hash_index_entries: usize,
    pub block_blob_entries: usize,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            dir_by_block_entries: 50_000,
            dir_bucket_entries: 4_096,
            bitmap_by_block_entries: 200_000,
            bitmap_page_meta_entries: 8_192,
            bitmap_page_blob_entries: 0,
            open_bitmap_stream_entries: 16_384,
            block_record_entries: 4_096,
            block_header_entries: 4_096,
            block_hash_to_number_entries: 4_096,
            tx_hash_index_entries: 0,
            block_blob_entries: 0,
        }
    }
}

impl CacheConfig {
    /// Total entry count across every table. Used as the proportional
    /// baseline for `--cache-mib` scaling.
    pub fn total_entries(&self) -> usize {
        self.dir_by_block_entries
            .saturating_add(self.dir_bucket_entries)
            .saturating_add(self.bitmap_by_block_entries)
            .saturating_add(self.bitmap_page_meta_entries)
            .saturating_add(self.bitmap_page_blob_entries)
            .saturating_add(self.open_bitmap_stream_entries)
            .saturating_add(self.block_record_entries)
            .saturating_add(self.block_header_entries)
            .saturating_add(self.block_hash_to_number_entries)
            .saturating_add(self.tx_hash_index_entries)
            .saturating_add(self.block_blob_entries)
    }

    /// Approximate average bytes per entry across all populated caches.
    /// Used by the binary to convert `--cache-mib` from MiB into entry
    /// counts. Held conservative — the LRU itself contributes ~96 bytes of
    /// metadata per entry on top of the value Bytes (which are Arc'd and
    /// inexpensive to clone).
    pub const APPROX_BYTES_PER_ENTRY: usize = 256;

    /// Sum of `total_entries` * `APPROX_BYTES_PER_ENTRY` rendered in MiB.
    /// Used as the denominator for `--cache-mib` linear scaling, so that
    /// `--cache-mib default_total_mib()` reproduces `CacheConfig::default()`.
    pub fn approx_total_mib(&self) -> usize {
        let bytes = self
            .total_entries()
            .saturating_mul(Self::APPROX_BYTES_PER_ENTRY);
        bytes / (1024 * 1024)
    }

    /// Scales every per-table entry count by `numer / denom`. Used by the
    /// CLI to convert a single `--cache-mib N` scalar into a proportional
    /// per-table budget. With `numer == 0` every entry count drops to 0,
    /// disabling caches (compile-time skip in the cached wrappers).
    pub fn scale(self, numer: usize, denom: usize) -> Self {
        if numer == 0 {
            return Self {
                dir_by_block_entries: 0,
                dir_bucket_entries: 0,
                bitmap_by_block_entries: 0,
                bitmap_page_meta_entries: 0,
                bitmap_page_blob_entries: 0,
                open_bitmap_stream_entries: 0,
                block_record_entries: 0,
                block_header_entries: 0,
                block_hash_to_number_entries: 0,
                tx_hash_index_entries: 0,
                block_blob_entries: 0,
            };
        }
        let denom = denom.max(1);
        let scale = |n: usize| n.saturating_mul(numer) / denom;
        Self {
            dir_by_block_entries: scale(self.dir_by_block_entries),
            dir_bucket_entries: scale(self.dir_bucket_entries),
            bitmap_by_block_entries: scale(self.bitmap_by_block_entries),
            bitmap_page_meta_entries: scale(self.bitmap_page_meta_entries),
            bitmap_page_blob_entries: scale(self.bitmap_page_blob_entries),
            open_bitmap_stream_entries: scale(self.open_bitmap_stream_entries),
            block_record_entries: scale(self.block_record_entries),
            block_header_entries: scale(self.block_header_entries),
            block_hash_to_number_entries: scale(self.block_hash_to_number_entries),
            tx_hash_index_entries: scale(self.tx_hash_index_entries),
            block_blob_entries: scale(self.block_blob_entries),
        }
    }
}

type KvLru = LruCache<Vec<u8>, Option<Bytes>>;
type ScanLru = LruCache<(Vec<u8>, Vec<u8>), Option<Bytes>>;
type BlobLru = LruCache<Vec<u8>, Option<Bytes>>;

fn make_lru<K, V>(entries: usize) -> Option<Arc<Mutex<LruCache<K, V>>>>
where
    K: std::hash::Hash + Eq,
{
    NonZeroUsize::new(entries).map(|cap| Arc::new(Mutex::new(LruCache::new(cap))))
}

pub struct CachedKvTable<M: MetaStore> {
    inner: KvTable<M>,
    cache: Option<Arc<Mutex<KvLru>>>,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl<M: MetaStore> CachedKvTable<M> {
    pub fn new(inner: KvTable<M>, entries: usize) -> Self {
        Self {
            inner,
            cache: make_lru(entries),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    pub fn inner(&self) -> &KvTable<M> {
        &self.inner
    }

    pub async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        if let Some(c) = &self.cache {
            if let Some(v) = c
                .lock()
                .expect("cache mutex poisoned")
                .get(key)
                .cloned()
            {
                self.hits.fetch_add(1, Ordering::Relaxed);
                return Ok(v);
            }
            self.misses.fetch_add(1, Ordering::Relaxed);
        }
        let v = self.inner.get(key).await?;
        if let Some(c) = &self.cache {
            c.lock()
                .expect("cache mutex poisoned")
                .put(key.to_vec(), v.clone());
        }
        Ok(v)
    }

    pub async fn put(&self, key: &[u8], value: Bytes) -> Result<()> {
        self.inner.put(key, value.clone()).await?;
        self.populate(key, value);
        Ok(())
    }

    pub fn table_id(&self) -> TableId {
        self.inner.table
    }

    /// Atomically reads and zeroes the (hits, misses) counters since the last call.
    /// Used by the ingest binary to emit per-window cache-hit-ratio metrics.
    pub fn take_window_stats(&self) -> (u64, u64) {
        (
            self.hits.swap(0, Ordering::Relaxed),
            self.misses.swap(0, Ordering::Relaxed),
        )
    }

    pub(crate) fn populate(&self, key: &[u8], value: Bytes) {
        if let Some(c) = &self.cache {
            c.lock()
                .expect("cache mutex poisoned")
                .put(key.to_vec(), Some(value));
        }
    }

    pub(crate) fn evict(&self, key: &[u8]) {
        if let Some(c) = &self.cache {
            c.lock().expect("cache mutex poisoned").pop(key);
        }
    }
}

pub struct CachedScannableTable<M: MetaStore> {
    inner: ScannableKvTable<M>,
    cache: Option<Arc<Mutex<ScanLru>>>,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl<M: MetaStore> CachedScannableTable<M> {
    pub fn new(inner: ScannableKvTable<M>, entries: usize) -> Self {
        Self {
            inner,
            cache: make_lru(entries),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    pub fn inner(&self) -> &ScannableKvTable<M> {
        &self.inner
    }

    pub async fn get(&self, partition: &[u8], clustering: &[u8]) -> Result<Option<Bytes>> {
        let key = (partition.to_vec(), clustering.to_vec());
        if let Some(c) = &self.cache {
            if let Some(v) = c
                .lock()
                .expect("cache mutex poisoned")
                .get(&key)
                .cloned()
            {
                self.hits.fetch_add(1, Ordering::Relaxed);
                return Ok(v);
            }
            self.misses.fetch_add(1, Ordering::Relaxed);
        }
        let v = self.inner.get(partition, clustering).await?;
        if let Some(c) = &self.cache {
            c.lock()
                .expect("cache mutex poisoned")
                .put(key, v.clone());
        }
        Ok(v)
    }

    pub async fn put(&self, partition: &[u8], clustering: &[u8], value: Bytes) -> Result<()> {
        self.inner.put(partition, clustering, value.clone()).await?;
        self.populate(partition, clustering, value);
        Ok(())
    }

    pub fn table_id(&self) -> ScannableTableId {
        self.inner.table
    }

    pub fn take_window_stats(&self) -> (u64, u64) {
        (
            self.hits.swap(0, Ordering::Relaxed),
            self.misses.swap(0, Ordering::Relaxed),
        )
    }

    pub async fn list_prefix(
        &self,
        partition: &[u8],
        prefix: &[u8],
        cursor: Option<Vec<u8>>,
        limit: usize,
    ) -> Result<Page> {
        // Prefix scans intentionally bypass the cache (see plan: caching
        // unbounded result sets requires invalidation on every adjacent
        // write). The per-clustering point gets that follow benefit from
        // populated entries instead.
        self.inner.list_prefix(partition, prefix, cursor, limit).await
    }

    pub(crate) fn populate(&self, partition: &[u8], clustering: &[u8], value: Bytes) {
        if let Some(c) = &self.cache {
            c.lock()
                .expect("cache mutex poisoned")
                .put((partition.to_vec(), clustering.to_vec()), Some(value));
        }
    }

    pub(crate) fn evict(&self, partition: &[u8], clustering: &[u8]) {
        if let Some(c) = &self.cache {
            c.lock()
                .expect("cache mutex poisoned")
                .pop(&(partition.to_vec(), clustering.to_vec()));
        }
    }
}

pub struct CachedBlobTable<B: BlobStore> {
    inner: BlobTable<B>,
    cache: Option<Arc<Mutex<BlobLru>>>,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl<B: BlobStore> CachedBlobTable<B> {
    pub fn new(inner: BlobTable<B>, entries: usize) -> Self {
        Self {
            inner,
            cache: make_lru(entries),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    pub fn inner(&self) -> &BlobTable<B> {
        &self.inner
    }

    pub async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        if let Some(c) = &self.cache {
            if let Some(v) = c
                .lock()
                .expect("cache mutex poisoned")
                .get(key)
                .cloned()
            {
                self.hits.fetch_add(1, Ordering::Relaxed);
                return Ok(v);
            }
            self.misses.fetch_add(1, Ordering::Relaxed);
        }
        let v = self.inner.get(key).await?;
        if let Some(c) = &self.cache {
            c.lock()
                .expect("cache mutex poisoned")
                .put(key.to_vec(), v.clone());
        }
        Ok(v)
    }

    pub async fn put(&self, key: &[u8], value: Bytes) -> Result<()> {
        self.inner.put(key, value.clone()).await?;
        self.populate(key, value);
        Ok(())
    }

    pub fn table_id(&self) -> BlobTableId {
        self.inner.table
    }

    pub fn take_window_stats(&self) -> (u64, u64) {
        (
            self.hits.swap(0, Ordering::Relaxed),
            self.misses.swap(0, Ordering::Relaxed),
        )
    }

    pub async fn read_range(
        &self,
        key: &[u8],
        start: usize,
        end_exclusive: usize,
    ) -> Result<Option<Bytes>> {
        // Range reads ignore the cache. Blob caches default to zero entries
        // and are not in the hot path; a partial-range hit would require
        // tracking full payload presence separately.
        self.inner.read_range(key, start, end_exclusive).await
    }

    pub(crate) fn populate(&self, key: &[u8], value: Bytes) {
        if let Some(c) = &self.cache {
            c.lock()
                .expect("cache mutex poisoned")
                .put(key.to_vec(), Some(value));
        }
    }

    pub(crate) fn evict(&self, key: &[u8]) {
        if let Some(c) = &self.cache {
            c.lock().expect("cache mutex poisoned").pop(key);
        }
    }
}
