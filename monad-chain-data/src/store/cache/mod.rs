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
    hash::Hash,
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
    /// Total entry count across every table. Retained for diagnostic logging;
    /// cache budgeting itself goes through per-table size weights via
    /// [`Self::approx_total_bytes`].
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

    /// Per-entry size estimate in bytes for each cached table. Roaring-bitmap
    /// payloads are capped at 8 KiB per fragment / page blob and dominate the
    /// budget; other tables are dominated by small metadata. Returned values
    /// are rounded up so `--cache-mib N` produces a conservative entry count.
    pub const fn approx_bytes_per_entry(field: CacheField) -> usize {
        match field {
            CacheField::BitmapByBlock | CacheField::BitmapPageBlob => 8 * 1024,
            CacheField::BlockHeader => 512,
            CacheField::DirBucket => 256,
            CacheField::BlockRecord => 128,
            CacheField::DirByBlock => 64,
            CacheField::TxHashIndex => 64,
            CacheField::BitmapPageMeta => 64,
            CacheField::BlockHashToNumber => 40,
            CacheField::OpenBitmapStream => 32,
            CacheField::BlockBlob => 4 * 1024 * 1024,
        }
    }

    /// Sum of per-table `entries * approx_bytes_per_entry`. Used by the CLI
    /// to scale `--cache-mib N` into proportional per-table entry counts.
    pub fn approx_total_bytes(&self) -> usize {
        let mut total: usize = 0;
        for (field, entries) in self.per_field() {
            total =
                total.saturating_add(entries.saturating_mul(Self::approx_bytes_per_entry(field)));
        }
        total
    }

    /// `approx_total_bytes` rendered in MiB. The denominator for `--cache-mib`
    /// linear scaling, so `--cache-mib default.approx_total_mib()` reproduces
    /// `CacheConfig::default()`.
    pub fn approx_total_mib(&self) -> usize {
        self.approx_total_bytes() / (1024 * 1024)
    }

    /// Scales every per-table entry count by `numer / denom`, weighted by the
    /// per-table size estimate so the result honors a byte budget rather than
    /// a uniform entry budget. With `numer == 0` every entry count drops to
    /// 0, disabling caches (compile-time skip in the cached wrappers).
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

    fn per_field(&self) -> [(CacheField, usize); 11] {
        [
            (CacheField::DirByBlock, self.dir_by_block_entries),
            (CacheField::DirBucket, self.dir_bucket_entries),
            (CacheField::BitmapByBlock, self.bitmap_by_block_entries),
            (CacheField::BitmapPageMeta, self.bitmap_page_meta_entries),
            (CacheField::BitmapPageBlob, self.bitmap_page_blob_entries),
            (
                CacheField::OpenBitmapStream,
                self.open_bitmap_stream_entries,
            ),
            (CacheField::BlockRecord, self.block_record_entries),
            (CacheField::BlockHeader, self.block_header_entries),
            (
                CacheField::BlockHashToNumber,
                self.block_hash_to_number_entries,
            ),
            (CacheField::TxHashIndex, self.tx_hash_index_entries),
            (CacheField::BlockBlob, self.block_blob_entries),
        ]
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheField {
    DirByBlock,
    DirBucket,
    BitmapByBlock,
    BitmapPageMeta,
    BitmapPageBlob,
    OpenBitmapStream,
    BlockRecord,
    BlockHeader,
    BlockHashToNumber,
    TxHashIndex,
    BlockBlob,
}

/// The per-key/per-counter machinery shared by every cached wrapper.
/// Generic over the key type so the three table flavors (KV / scannable /
/// blob) compose with the same LRU + hits/misses + populate/evict logic.
/// Wrapped in `Arc` by every owner so WriteSession can capture an eviction
/// handle without going back through the parent `Tables` to re-resolve.
pub(crate) struct CachedInner<K>
where
    K: Hash + Eq,
{
    cache: Option<Mutex<LruCache<K, Option<Bytes>>>>,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl<K> CachedInner<K>
where
    K: Hash + Eq,
{
    fn new(entries: usize) -> Arc<Self> {
        Arc::new(Self {
            cache: NonZeroUsize::new(entries).map(|cap| Mutex::new(LruCache::new(cap))),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        })
    }

    fn lookup(&self, key: &K) -> Option<Option<Bytes>> {
        let c = self.cache.as_ref()?;
        let hit = c.lock().expect("cache mutex poisoned").get(key).cloned();
        if hit.is_some() {
            self.hits.fetch_add(1, Ordering::Relaxed);
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
        }
        hit
    }

    fn store(&self, key: K, value: Option<Bytes>) {
        if let Some(c) = &self.cache {
            c.lock().expect("cache mutex poisoned").put(key, value);
        }
    }

    pub(crate) fn populate(&self, key: K, value: Bytes) {
        self.store(key, Some(value));
    }

    pub(crate) fn evict(&self, key: &K) {
        if let Some(c) = &self.cache {
            c.lock().expect("cache mutex poisoned").pop(key);
        }
    }

    /// Atomically reads and zeroes the (hits, misses) counters since the last
    /// call. Used by the ingest binary to emit per-window hit-ratio metrics.
    pub(crate) fn take_window_stats(&self) -> (u64, u64) {
        (
            self.hits.swap(0, Ordering::Relaxed),
            self.misses.swap(0, Ordering::Relaxed),
        )
    }
}

pub struct CachedKvTable<M: MetaStore> {
    inner: KvTable<M>,
    cache: Arc<CachedInner<Vec<u8>>>,
}

impl<M: MetaStore> CachedKvTable<M> {
    pub fn new(inner: KvTable<M>, entries: usize) -> Self {
        Self {
            inner,
            cache: CachedInner::new(entries),
        }
    }

    pub fn inner(&self) -> &KvTable<M> {
        &self.inner
    }

    pub async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        if let Some(v) = self.cache.lookup(&key.to_vec()) {
            return Ok(v);
        }
        let v = self.inner.get(key).await?;
        self.cache.store(key.to_vec(), v.clone());
        Ok(v)
    }

    pub async fn put(&self, key: &[u8], value: Bytes) -> Result<()> {
        self.inner.put(key, value.clone()).await?;
        self.cache.populate(key.to_vec(), value);
        Ok(())
    }

    pub fn table_id(&self) -> TableId {
        self.inner.table
    }

    pub fn take_window_stats(&self) -> (u64, u64) {
        self.cache.take_window_stats()
    }

    pub(crate) fn cache_handle(&self) -> Arc<CachedInner<Vec<u8>>> {
        self.cache.clone()
    }
}

pub struct CachedScannableTable<M: MetaStore> {
    inner: ScannableKvTable<M>,
    cache: Arc<CachedInner<(Vec<u8>, Vec<u8>)>>,
}

impl<M: MetaStore> CachedScannableTable<M> {
    pub fn new(inner: ScannableKvTable<M>, entries: usize) -> Self {
        Self {
            inner,
            cache: CachedInner::new(entries),
        }
    }

    pub fn inner(&self) -> &ScannableKvTable<M> {
        &self.inner
    }

    pub async fn get(&self, partition: &[u8], clustering: &[u8]) -> Result<Option<Bytes>> {
        let key = (partition.to_vec(), clustering.to_vec());
        if let Some(v) = self.cache.lookup(&key) {
            return Ok(v);
        }
        let v = self.inner.get(partition, clustering).await?;
        self.cache.store(key, v.clone());
        Ok(v)
    }

    pub async fn put(&self, partition: &[u8], clustering: &[u8], value: Bytes) -> Result<()> {
        self.inner.put(partition, clustering, value.clone()).await?;
        self.cache
            .populate((partition.to_vec(), clustering.to_vec()), value);
        Ok(())
    }

    pub fn table_id(&self) -> ScannableTableId {
        self.inner.table
    }

    pub fn take_window_stats(&self) -> (u64, u64) {
        self.cache.take_window_stats()
    }

    pub async fn list_prefix(
        &self,
        partition: &[u8],
        prefix: &[u8],
        cursor: Option<Vec<u8>>,
        limit: usize,
    ) -> Result<Page> {
        // Prefix scans intentionally bypass the cache: caching unbounded
        // result sets requires invalidation on every adjacent write. The
        // per-clustering point gets that follow benefit from populated
        // entries instead.
        self.inner
            .list_prefix(partition, prefix, cursor, limit)
            .await
    }

    pub(crate) fn cache_handle(&self) -> Arc<CachedInner<(Vec<u8>, Vec<u8>)>> {
        self.cache.clone()
    }
}

pub struct CachedBlobTable<B: BlobStore> {
    inner: BlobTable<B>,
    cache: Arc<CachedInner<Vec<u8>>>,
}

impl<B: BlobStore> CachedBlobTable<B> {
    pub fn new(inner: BlobTable<B>, entries: usize) -> Self {
        Self {
            inner,
            cache: CachedInner::new(entries),
        }
    }

    pub fn inner(&self) -> &BlobTable<B> {
        &self.inner
    }

    pub async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        if let Some(v) = self.cache.lookup(&key.to_vec()) {
            return Ok(v);
        }
        let v = self.inner.get(key).await?;
        self.cache.store(key.to_vec(), v.clone());
        Ok(v)
    }

    pub async fn put(&self, key: &[u8], value: Bytes) -> Result<()> {
        self.inner.put(key, value.clone()).await?;
        self.cache.populate(key.to_vec(), value);
        Ok(())
    }

    pub fn table_id(&self) -> BlobTableId {
        self.inner.table
    }

    pub fn take_window_stats(&self) -> (u64, u64) {
        self.cache.take_window_stats()
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

    pub(crate) fn cache_handle(&self) -> Arc<CachedInner<Vec<u8>>> {
        self.cache.clone()
    }
}
