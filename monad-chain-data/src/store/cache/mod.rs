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
    future::Future,
    hash::Hash,
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};

use bytes::Bytes;
use futures::future::{BoxFuture, FutureExt, Shared};
use lru::LruCache;

use crate::{
    error::{MonadChainDataError, Result},
    store::{
        blob::{BlobStore, BlobTable, BlobTableId},
        common::Page,
        meta::{KvTable, MetaStore, ScannableKvTable, ScannableTableId, TableId},
    },
};

/// Output stored in the single-flight map. [`Shared`] requires its output to
/// be `Clone`, but [`MonadChainDataError`] is not `Clone`, so the error is
/// wrapped in an `Arc`. Converted back to the crate `Result` at the boundary
/// by [`unshare`].
type SharedResult = std::result::Result<Option<Bytes>, Arc<MonadChainDataError>>;

/// A boxed, `'static` backend-fetch future shared by every coalesced caller.
type SharedFetch = Shared<BoxFuture<'static, SharedResult>>;

/// Maps a shared (Arc-wrapped error) fetch result back to the crate `Result`.
/// The leader usually owns the only `Arc` and hands the original error back
/// untouched; a follower that raced onto the same fetch re-wraps the message
/// in [`MonadChainDataError::Backend`] so callers still get a crate `Result`.
fn unshare(shared: SharedResult) -> Result<Option<Bytes>> {
    shared.map_err(|e| match Arc::try_unwrap(e) {
        Ok(err) => err,
        Err(shared) => MonadChainDataError::Backend(shared.to_string()),
    })
}

#[derive(Debug, Clone, Copy)]
pub struct CacheConfig {
    pub dir_by_block_entries: usize,
    pub dir_bucket_entries: usize,
    pub bitmap_by_block_entries: usize,
    pub bitmap_page_meta_entries: usize,
    pub bitmap_page_blob_entries: usize,
    pub bitmap_page_counts_entries: usize,
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
            bitmap_page_counts_entries: 8_192,
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
            .saturating_add(self.bitmap_page_counts_entries)
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
            // Sparse `(page, count)` rows: ≤256 pairs, typically far fewer.
            CacheField::BitmapPageCounts => 256,
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
                bitmap_page_counts_entries: 0,
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
            bitmap_page_counts_entries: scale(self.bitmap_page_counts_entries),
            open_bitmap_stream_entries: scale(self.open_bitmap_stream_entries),
            block_record_entries: scale(self.block_record_entries),
            block_header_entries: scale(self.block_header_entries),
            block_hash_to_number_entries: scale(self.block_hash_to_number_entries),
            tx_hash_index_entries: scale(self.tx_hash_index_entries),
            block_blob_entries: scale(self.block_blob_entries),
        }
    }

    fn per_field(&self) -> [(CacheField, usize); 12] {
        [
            (CacheField::DirByBlock, self.dir_by_block_entries),
            (CacheField::DirBucket, self.dir_bucket_entries),
            (CacheField::BitmapByBlock, self.bitmap_by_block_entries),
            (CacheField::BitmapPageMeta, self.bitmap_page_meta_entries),
            (CacheField::BitmapPageBlob, self.bitmap_page_blob_entries),
            (
                CacheField::BitmapPageCounts,
                self.bitmap_page_counts_entries,
            ),
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
    BitmapPageCounts,
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
    /// Single-flight map: concurrent misses on the same key share the one
    /// in-flight backend fetch held here instead of each issuing their own.
    /// Kept separate from the LRU mutex so neither is held across an `.await`.
    in_flight: Mutex<HashMap<K, SharedFetch>>,
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
            in_flight: Mutex::new(HashMap::new()),
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

impl<K> CachedInner<K>
where
    K: Hash + Eq + Clone + Send + 'static,
{
    /// Point-read with cache + single-flight (in-flight request coalescing).
    ///
    /// On a cache hit the value is returned immediately. On a miss, concurrent
    /// callers for the same key share ONE `fetch` future and a single cache
    /// populate; the first caller (the "leader") drives the fetch and populates
    /// the cache, every other caller (a "follower") awaits the same shared
    /// future and gets a clone of its result.
    ///
    /// `fetch` is the backend read. It lives on the wrapper, not here, so the
    /// caller passes it in already owned and `'static` (the wrappers build it
    /// by cloning their inner table handle and the key — both are `Clone` and
    /// `'static`), which lets it be stored in the `Shared` inside `in_flight`.
    ///
    /// Hit/miss accounting: the initial [`Self::lookup`] counts every call that
    /// reaches the backend as a miss, including followers that coalesce onto an
    /// existing in-flight fetch. We keep that behavior — a follower did observe
    /// a cache miss — so the miss counter reflects "requests not served from
    /// the LRU", and the win from single-flight shows up as fewer backend
    /// round-trips rather than in the ratio.
    async fn get_or_fetch<Fut>(&self, key: K, fetch: Fut) -> Result<Option<Bytes>>
    where
        Fut: Future<Output = Result<Option<Bytes>>> + Send + 'static,
    {
        // 1. Fast path: serve from the LRU (and count hit/miss).
        if let Some(v) = self.lookup(&key) {
            return Ok(v);
        }

        // 2. Join or start the single flight. Either clone an existing shared
        //    future (follower) or insert our own (leader). The mutex is never
        //    held across an `.await`.
        let (fut, is_leader) = {
            let mut map = self.in_flight.lock().expect("in_flight mutex poisoned");
            if let Some(existing) = map.get(&key) {
                (existing.clone(), false)
            } else {
                let shared = fetch.map(|r| r.map_err(Arc::new)).boxed().shared();
                map.insert(key.clone(), shared.clone());
                (shared, true)
            }
        };

        if !is_leader {
            // 3a. Follower: await the shared fetch and return a clone of its
            //     result. The leader owns cache population and map cleanup.
            return unshare(fut.await);
        }

        // 3b. Leader: drive the fetch to completion, then populate the cache
        //     and remove the in-flight entry. The guard removes the key even
        //     on error or panic so a failed fetch is not cached and the next
        //     call retries (errors are never cached).
        let _guard = InFlightGuard {
            inner: self,
            key: &key,
        };
        let result = fut.await;
        if let Ok(value) = &result {
            // Negative caching matches the existing behavior: `None` is stored
            // so repeat lookups for an absent key skip the backend.
            self.store(key.clone(), value.clone());
        }
        unshare(result)
    }
}

/// Removes a key from the single-flight map when the leader's drive scope
/// ends, including on early return, error, or panic. Guarantees a failed or
/// cancelled fetch never leaves a stale entry that would wedge later callers.
struct InFlightGuard<'a, K>
where
    K: Hash + Eq,
{
    inner: &'a CachedInner<K>,
    key: &'a K,
}

impl<K> Drop for InFlightGuard<'_, K>
where
    K: Hash + Eq,
{
    fn drop(&mut self) {
        self.inner
            .in_flight
            .lock()
            .expect("in_flight mutex poisoned")
            .remove(self.key);
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
        // Build an owned, `'static` fetch by cloning the inner handle and key
        // so it can live in the single-flight `Shared`.
        let inner = self.inner.clone();
        let k = key.to_vec();
        self.cache
            .get_or_fetch(key.to_vec(), async move { inner.get(&k).await })
            .await
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
        // Build an owned, `'static` fetch by cloning the inner handle and key
        // so it can live in the single-flight `Shared`.
        let inner = self.inner.clone();
        let (p, c) = key.clone();
        self.cache
            .get_or_fetch(key, async move { inner.get(&p, &c).await })
            .await
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
        // Build an owned, `'static` fetch by cloning the inner handle and key
        // so it can live in the single-flight `Shared`.
        let inner = self.inner.clone();
        let k = key.to_vec();
        self.cache
            .get_or_fetch(key.to_vec(), async move { inner.get(&k).await })
            .await
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

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, time::Duration};

    use super::*;
    use crate::store::{
        blob::BlobWriteOp,
        common::Page,
        meta::{
            CasOutcome, MetaWriteOp, PublicationCasParams, ScannableTableId as ScanId, TableId,
        },
    };

    /// Backend double that counts `get`/`get_blob`/`scan_get` calls and sleeps
    /// briefly inside each so concurrent callers overlap. Backs both the
    /// `MetaStore` and `BlobStore` traits so a single double exercises all
    /// three cached wrappers.
    #[derive(Clone, Default)]
    struct CountingStore {
        get_calls: Arc<AtomicU64>,
        data: Arc<Mutex<BTreeMap<Vec<u8>, Bytes>>>,
    }

    impl CountingStore {
        fn with_value(key: &[u8], value: Bytes) -> Self {
            let s = Self::default();
            s.data.lock().unwrap().insert(key.to_vec(), value);
            s
        }

        fn get_calls(&self) -> u64 {
            self.get_calls.load(Ordering::SeqCst)
        }

        async fn lookup(&self, key: &[u8]) -> Result<Option<Bytes>> {
            self.get_calls.fetch_add(1, Ordering::SeqCst);
            // Hold the fetch open long enough that concurrent callers reach the
            // single-flight join point before the leader completes.
            tokio::time::sleep(Duration::from_millis(50)).await;
            Ok(self.data.lock().unwrap().get(key).cloned())
        }
    }

    impl MetaStore for CountingStore {
        async fn get(&self, _table: TableId, key: &[u8]) -> Result<Option<Bytes>> {
            self.lookup(key).await
        }
        async fn scan_get(
            &self,
            _table: ScanId,
            partition: &[u8],
            clustering: &[u8],
        ) -> Result<Option<Bytes>> {
            let mut k = partition.to_vec();
            k.extend_from_slice(clustering);
            self.lookup(&k).await
        }
        async fn put(&self, _table: TableId, _key: &[u8], _value: Bytes) -> Result<()> {
            unimplemented!("not exercised by single-flight tests")
        }
        async fn scan_put(
            &self,
            _table: ScanId,
            _partition: &[u8],
            _clustering: &[u8],
            _value: Bytes,
        ) -> Result<()> {
            unimplemented!("not exercised by single-flight tests")
        }
        async fn scan_list(
            &self,
            _table: ScanId,
            _partition: &[u8],
            _prefix: &[u8],
            _cursor: Option<Vec<u8>>,
            _limit: usize,
        ) -> Result<Page> {
            unimplemented!("not exercised by single-flight tests")
        }
        async fn apply_writes(&self, _writes: Vec<MetaWriteOp>) -> Result<()> {
            unimplemented!("not exercised by single-flight tests")
        }
        async fn apply_writes_with_cas(
            &self,
            _writes: Vec<MetaWriteOp>,
            _cas: PublicationCasParams,
        ) -> Result<CasOutcome> {
            unimplemented!("not exercised by single-flight tests")
        }
    }

    impl BlobStore for CountingStore {
        async fn put_blob(&self, _table: BlobTableId, _key: &[u8], _value: Bytes) -> Result<()> {
            unimplemented!("not exercised by single-flight tests")
        }
        async fn get_blob(&self, _table: BlobTableId, key: &[u8]) -> Result<Option<Bytes>> {
            self.lookup(key).await
        }
        async fn apply_writes(&self, _writes: Vec<BlobWriteOp>) -> Result<()> {
            unimplemented!("not exercised by single-flight tests")
        }
    }

    const N: usize = 16;

    /// N concurrent misses on the same key collapse to one backend fetch, and
    /// every caller receives the value.
    #[tokio::test]
    async fn kv_get_coalesces_concurrent_misses() {
        let store = CountingStore::with_value(b"k", Bytes::from_static(b"v"));
        let table = KvTable::new(store.clone(), TableId::new("t"));
        let cached = Arc::new(CachedKvTable::new(table, 64));

        let mut handles = Vec::new();
        for _ in 0..N {
            let c = cached.clone();
            handles.push(tokio::spawn(async move { c.get(b"k").await }));
        }
        for h in handles {
            let got = h.await.unwrap().unwrap();
            assert_eq!(got, Some(Bytes::from_static(b"v")));
        }

        assert_eq!(store.get_calls(), 1, "single-flight should issue one fetch");
        // A subsequent get is served from the populated cache.
        assert_eq!(
            cached.get(b"k").await.unwrap(),
            Some(Bytes::from_static(b"v"))
        );
        assert_eq!(store.get_calls(), 1, "cache hit must not touch the backend");
    }

    /// Coalescing also covers the negative-cache case: a concurrent flood of
    /// misses on an absent key shares one fetch and all observe `None`.
    #[tokio::test]
    async fn kv_get_coalesces_absent_key() {
        let store = CountingStore::default();
        let table = KvTable::new(store.clone(), TableId::new("t"));
        let cached = Arc::new(CachedKvTable::new(table, 64));

        let mut handles = Vec::new();
        for _ in 0..N {
            let c = cached.clone();
            handles.push(tokio::spawn(async move { c.get(b"missing").await }));
        }
        for h in handles {
            assert_eq!(h.await.unwrap().unwrap(), None);
        }
        assert_eq!(store.get_calls(), 1);
        // Negative result is cached: no further backend calls.
        assert_eq!(cached.get(b"missing").await.unwrap(), None);
        assert_eq!(store.get_calls(), 1);
    }

    #[tokio::test]
    async fn scannable_get_coalesces_concurrent_misses() {
        let store = CountingStore::with_value(b"pc", Bytes::from_static(b"v"));
        let table = ScannableKvTable::new(store.clone(), ScanId::new("t"));
        let cached = Arc::new(CachedScannableTable::new(table, 64));

        let mut handles = Vec::new();
        for _ in 0..N {
            let c = cached.clone();
            handles.push(tokio::spawn(async move { c.get(b"p", b"c").await }));
        }
        for h in handles {
            assert_eq!(h.await.unwrap().unwrap(), Some(Bytes::from_static(b"v")));
        }
        assert_eq!(store.get_calls(), 1);
    }

    #[tokio::test]
    async fn blob_get_coalesces_concurrent_misses() {
        let store = CountingStore::with_value(b"k", Bytes::from_static(b"v"));
        let table = BlobTable::new(store.clone(), BlobTableId::new("t"));
        let cached = Arc::new(CachedBlobTable::new(table, 64));

        let mut handles = Vec::new();
        for _ in 0..N {
            let c = cached.clone();
            handles.push(tokio::spawn(async move { c.get(b"k").await }));
        }
        for h in handles {
            assert_eq!(h.await.unwrap().unwrap(), Some(Bytes::from_static(b"v")));
        }
        assert_eq!(store.get_calls(), 1);
    }

    /// After a fetch fails, the in-flight entry is removed so the next call
    /// retries the backend (errors are not cached).
    #[tokio::test]
    async fn failed_fetch_is_not_cached_and_retries() {
        // A store whose `get` always errors, counting attempts.
        #[derive(Clone, Default)]
        struct ErrStore {
            calls: Arc<AtomicU64>,
        }
        impl MetaStore for ErrStore {
            async fn get(&self, _t: TableId, _k: &[u8]) -> Result<Option<Bytes>> {
                self.calls.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(20)).await;
                Err(MonadChainDataError::Backend("boom".to_string()))
            }
            async fn scan_get(&self, _t: ScanId, _p: &[u8], _c: &[u8]) -> Result<Option<Bytes>> {
                unimplemented!()
            }
            async fn put(&self, _t: TableId, _k: &[u8], _v: Bytes) -> Result<()> {
                unimplemented!()
            }
            async fn scan_put(&self, _t: ScanId, _p: &[u8], _c: &[u8], _v: Bytes) -> Result<()> {
                unimplemented!()
            }
            async fn scan_list(
                &self,
                _t: ScanId,
                _p: &[u8],
                _pre: &[u8],
                _cur: Option<Vec<u8>>,
                _l: usize,
            ) -> Result<Page> {
                unimplemented!()
            }
            async fn apply_writes(&self, _w: Vec<MetaWriteOp>) -> Result<()> {
                unimplemented!()
            }
            async fn apply_writes_with_cas(
                &self,
                _w: Vec<MetaWriteOp>,
                _c: PublicationCasParams,
            ) -> Result<CasOutcome> {
                unimplemented!()
            }
        }

        let store = ErrStore::default();
        let calls = store.calls.clone();
        let table = KvTable::new(store, TableId::new("t"));
        let cached = Arc::new(CachedKvTable::new(table, 64));

        // First flight: all coalesce onto one failing fetch, all see an error.
        let mut handles = Vec::new();
        for _ in 0..N {
            let c = cached.clone();
            handles.push(tokio::spawn(async move { c.get(b"k").await }));
        }
        for h in handles {
            assert!(h.await.unwrap().is_err());
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        // The error was not cached and the in-flight entry was cleared, so a
        // later call hits the backend again.
        assert!(cached.get(b"k").await.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }
}
