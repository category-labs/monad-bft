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
    store::meta::{KvTable, MetaStore, ScannableKvTable, ScannableTableId, TableId},
};

const DEFAULT_CACHE_TOTAL_MIB: usize = 2048;

/// Fixed per-entry bookkeeping charge (key, LRU node, `Arc`) added to a
/// [`Weighted`] payload weight, so tiny decoded rows don't let a cache hold
/// far more entries than its byte budget affords.
pub const CACHE_ENTRY_OVERHEAD: usize = 64;

/// A decoded cache value plus the byte weight to charge it at, stamped at
/// decode time from the pre-decode byte length (unrecoverable after decoding).
#[derive(Debug, Clone)]
pub struct Weighted<T> {
    pub value: T,
    /// Payload weight in bytes (pre-decode length). [`CACHE_ENTRY_OVERHEAD`]
    /// is added by the weigher, not stored here.
    pub weight: usize,
}

/// Stamped payload weight plus per-entry overhead.
pub(crate) fn weigh_weighted<T>(value: &Weighted<T>) -> usize {
    value.weight.saturating_add(CACHE_ENTRY_OVERHEAD)
}

/// Decodes a fetched payload into a [`Weighted`] value, stamping the weight
/// from the pre-decode byte length — the one moment it is known.
fn decode_weighted<V>(
    bytes: Option<Bytes>,
    decode: fn(Bytes) -> Result<V>,
) -> Result<Option<Weighted<V>>> {
    match bytes {
        Some(bytes) => {
            let weight = bytes.len();
            Ok(Some(Weighted {
                value: decode(bytes)?,
                weight,
            }))
        }
        None => Ok(None),
    }
}

/// Output stored in the single-flight map. [`Shared`] requires `Clone` output
/// and [`MonadChainDataError`] is not `Clone`, so errors are `Arc`-wrapped;
/// [`unshare`] converts back at the boundary.
type SharedResult<V> = std::result::Result<Option<V>, Arc<MonadChainDataError>>;

/// A boxed, `'static` backend-fetch future shared by every coalesced caller.
type SharedFetch<V> = Shared<BoxFuture<'static, SharedResult<V>>>;

/// Maps a shared fetch result back to the crate `Result`: the sole `Arc` owner
/// gets the original error; under contention any awaiter gets a re-wrapped
/// `Backend` one (the other awaiters' shared handles keep the `Arc` alive).
fn unshare<V>(shared: SharedResult<V>) -> Result<Option<V>> {
    shared.map_err(|e| match Arc::try_unwrap(e) {
        Ok(err) => err,
        Err(shared) => MonadChainDataError::Backend(shared.to_string()),
    })
}

/// Per-cache byte budgets. Caches hold decoded values but weigh them by their
/// pre-decode byte length (plus [`CACHE_ENTRY_OVERHEAD`] per entry), so each
/// budget tracks stored size.
#[derive(Debug, Clone, Copy)]
pub struct CacheConfig {
    pub dir_by_block_cache_bytes: usize,
    pub dir_bucket_cache_bytes: usize,
    pub bitmap_by_block_cache_bytes: usize,
    pub bitmap_page_blob_cache_bytes: usize,
    pub bitmap_page_counts_cache_bytes: usize,
    pub open_bitmap_stream_cache_bytes: usize,
    pub block_header_cache_bytes: usize,
    pub block_hash_to_number_cache_bytes: usize,
    pub tx_hash_index_cache_bytes: usize,
    /// Budget for the per-(family, block, row) decoded-row caches, split
    /// evenly across the three families. Only rows queries actually
    /// materialize are cached, never a whole block's blob.
    pub row_cache_bytes: usize,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self::from_total_mib(DEFAULT_CACHE_TOTAL_MIB)
    }
}

impl CacheConfig {
    const RATIO_DENOMINATOR: usize = 1024;
    const RATIO_ROW_CACHE: usize = 464;
    const RATIO_BITMAP_BY_BLOCK: usize = 256;
    const RATIO_BITMAP_PAGE_BLOB: usize = 128;
    const RATIO_DIR_BY_BLOCK: usize = 32;
    const RATIO_DIR_BUCKET: usize = 16;
    const RATIO_BITMAP_PAGE_COUNTS: usize = 24;
    const RATIO_OPEN_BITMAP_STREAM: usize = 16;
    // Block metadata is on every materialization path (one row per touched
    // block), so it gets the share the row cache gives up: starving it to
    // 16/1024 made warm analytic queries re-fetch a block record per block
    // (mainnet-1 bench).
    const RATIO_BLOCK_HEADER: usize = 64;
    const RATIO_BLOCK_HASH_TO_NUMBER: usize = 16;
    const RATIO_TX_HASH_INDEX: usize = 8;

    /// Splits a total MiB budget into per-cache byte budgets via the default
    /// ratios. A zero budget disables all caches.
    pub fn from_total_mib(total_mib: usize) -> Self {
        let ratio_bytes =
            |ratio| Self::budget_mib_to_bytes(Self::ratio_budget_mib(total_mib, ratio));
        Self {
            dir_by_block_cache_bytes: ratio_bytes(Self::RATIO_DIR_BY_BLOCK),
            dir_bucket_cache_bytes: ratio_bytes(Self::RATIO_DIR_BUCKET),
            bitmap_by_block_cache_bytes: ratio_bytes(Self::RATIO_BITMAP_BY_BLOCK),
            bitmap_page_blob_cache_bytes: ratio_bytes(Self::RATIO_BITMAP_PAGE_BLOB),
            bitmap_page_counts_cache_bytes: ratio_bytes(Self::RATIO_BITMAP_PAGE_COUNTS),
            open_bitmap_stream_cache_bytes: ratio_bytes(Self::RATIO_OPEN_BITMAP_STREAM),
            block_header_cache_bytes: ratio_bytes(Self::RATIO_BLOCK_HEADER),
            block_hash_to_number_cache_bytes: ratio_bytes(Self::RATIO_BLOCK_HASH_TO_NUMBER),
            tx_hash_index_cache_bytes: ratio_bytes(Self::RATIO_TX_HASH_INDEX),
            row_cache_bytes: ratio_bytes(Self::RATIO_ROW_CACHE),
        }
    }

    pub fn budget_mib_to_bytes(mib: usize) -> usize {
        mib.saturating_mul(1024 * 1024)
    }

    fn ratio_budget_mib(total_mib: usize, ratio: usize) -> usize {
        total_mib.saturating_mul(ratio) / Self::RATIO_DENOMINATOR
    }

    /// Sum of every per-cache byte budget. Diagnostic only.
    pub fn total_bytes(&self) -> usize {
        [
            self.dir_by_block_cache_bytes,
            self.dir_bucket_cache_bytes,
            self.bitmap_by_block_cache_bytes,
            self.bitmap_page_blob_cache_bytes,
            self.bitmap_page_counts_cache_bytes,
            self.open_bitmap_stream_cache_bytes,
            self.block_header_cache_bytes,
            self.block_hash_to_number_cache_bytes,
            self.tx_hash_index_cache_bytes,
            self.row_cache_bytes,
        ]
        .into_iter()
        .fold(0usize, |acc, b| acc.saturating_add(b))
    }
}

/// LRU cache plus its running weighted size under one mutex, so the size
/// counter can never drift from the map. The lock is never held across an
/// `.await` (crate-wide invariant).
struct WeightedLru<K, V>
where
    K: Hash + Eq,
{
    lru: LruCache<K, V>,
    /// Sum of `weigher(value)` over resident entries, kept in lockstep with
    /// `lru` on insert/replace/evict.
    size: usize,
    /// Maximum total weight in bytes.
    budget: usize,
}

/// LRU + single-flight + hit/miss machinery shared by every cached wrapper,
/// generic over key and value so all table flavors reuse it. Owners wrap it
/// in `Arc`.
pub(crate) struct CachedInner<K, V>
where
    K: Hash + Eq,
{
    cache: Option<Mutex<WeightedLru<K, V>>>,
    /// Per-value weight in bytes; [`weigh_weighted`] adds
    /// [`CACHE_ENTRY_OVERHEAD`] to the stamped payload weight.
    weigher: fn(&V) -> usize,
    /// Single-flight map: concurrent misses on a key share one in-flight
    /// fetch. Separate from the LRU mutex; neither is held across an `.await`.
    in_flight: Mutex<HashMap<K, SharedFetch<V>>>,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl<K, V> CachedInner<K, V>
where
    K: Hash + Eq,
    V: Clone,
{
    /// Byte-budgeted cache: evicts LRU entries until the summed `weigher`
    /// weight is `<= budget` bytes. `budget == 0` disables the cache.
    pub(crate) fn with_weigher(budget: usize, weigher: fn(&V) -> usize) -> Arc<Self> {
        // Unbounded: eviction is driven by the byte budget, not entry count;
        // a count cap too would evict early.
        let cache = (budget > 0).then(|| {
            Mutex::new(WeightedLru {
                lru: LruCache::unbounded(),
                size: 0,
                budget,
            })
        });
        Arc::new(Self {
            cache,
            weigher,
            in_flight: Mutex::new(HashMap::new()),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        })
    }

    /// Cache-only lookup that PROMOTES the entry's LRU position and counts
    /// hit/miss (unlike the non-promoting `peek` of the underlying lru crate);
    /// never fetches. Batch readers probe with this, fetch misses themselves,
    /// then [`Self::insert`] the results.
    pub(crate) fn probe(&self, key: &K) -> Option<V> {
        let c = self.cache.as_ref()?;
        let hit = c
            .lock()
            .expect("cache mutex poisoned")
            .lru
            .get(key)
            .cloned();
        if hit.is_some() {
            self.hits.fetch_add(1, Ordering::Relaxed);
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
        }
        hit
    }

    /// Seeds the cache with a caller-fetched value; no-op when disabled.
    pub(crate) fn insert(&self, key: K, value: V) {
        let Some(c) = &self.cache else {
            return;
        };
        let added = (self.weigher)(&value);
        let mut guard = c.lock().expect("cache mutex poisoned");
        // Refund a replaced value's weight so `size` tracks residency exactly.
        if let Some(old) = guard.lru.put(key, value) {
            let refund = (self.weigher)(&old);
            guard.size = guard.size.saturating_sub(refund);
        }
        guard.size = guard.size.saturating_add(added);
        // Evict LRU entries until within budget; terminates because
        // `pop_lru` eventually empties even for one oversized value.
        while guard.size > guard.budget {
            let Some((_, evicted)) = guard.lru.pop_lru() else {
                break;
            };
            let freed = (self.weigher)(&evicted);
            guard.size = guard.size.saturating_sub(freed);
        }
    }
}

/// Stats reader needing no `V: Clone`, so the unbounded table accessors
/// ([`CachedKvTable::take_window_stats`] and its scannable twin) resolve —
/// keep it split out so a future merge into the `V: Clone` block does not
/// silently re-tighten [`WriteSession::put`](crate::store::session::WriteSession::put)'s bounds.
impl<K, V> CachedInner<K, V>
where
    K: Hash + Eq,
{
    /// Atomically reads and zeroes the (hits, misses) counters since the last
    /// call, for per-window hit-ratio metrics.
    pub(crate) fn take_window_stats(&self) -> (u64, u64) {
        (
            self.hits.swap(0, Ordering::Relaxed),
            self.misses.swap(0, Ordering::Relaxed),
        )
    }
}

impl<K, V> CachedInner<K, V>
where
    K: Hash + Eq + Clone + Send + 'static,
    V: Clone + Send + 'static,
{
    /// Point-read with cache + single-flight: concurrent misses on a key share
    /// ONE fetch. The first caller (leader) inserts the shared future and
    /// owns the in-flight cleanup; followers clone and await it.
    ///
    /// `make_fetch` builds the owned `'static` fetch future and runs only on
    /// the MISS path, so a hit pays no fetch-capture allocations.
    ///
    /// EVERY awaiter populates the cache on a present value: the leader can be
    /// cancelled mid-await, leaving a follower to drive the `Shared` to
    /// completion — if only the leader populated, that value would be silently
    /// uncached. Redundant populates are idempotent. No mutex is held across
    /// an `.await`. Coalesced followers still count as misses, so
    /// single-flight wins show up as fewer backend reads, not in the hit ratio.
    pub(crate) async fn get_or_fetch<F, Fut>(&self, key: K, make_fetch: F) -> Result<Option<V>>
    where
        F: FnOnce(&K) -> Fut,
        Fut: Future<Output = Result<Option<V>>> + Send + 'static,
    {
        if let Some(v) = self.probe(&key) {
            return Ok(Some(v));
        }

        // Join an existing flight (follower) or start one (leader).
        let (fut, is_leader) = {
            let mut map = self.in_flight.lock().expect("in_flight mutex poisoned");
            if let Some(existing) = map.get(&key) {
                (existing.clone(), false)
            } else {
                let shared = make_fetch(&key)
                    .map(|r| r.map_err(Arc::new))
                    .boxed()
                    .shared();
                map.insert(key.clone(), shared.clone());
                (shared, true)
            }
        };

        // The leader's guard removes the in-flight entry when this scope ends
        // (completion, error, or panic), so a failed or cancelled fetch never
        // leaves a stale entry.
        let guard = is_leader.then(|| InFlightGuard {
            inner: self,
            key: &key,
        });

        let result = fut.await;

        if let Ok(Some(value)) = &result {
            // Only present values are cached. Absence is never cacheable:
            // writes never seed or invalidate read caches, and a key absent
            // now (an unmined tx hash, an open page's artifact) can become
            // present later — a stored `None` would mask it until eviction.
            // Errors are never cached either.
            self.insert(key.clone(), value.clone());
        }
        // Release the leader's in-flight entry BEFORE unwrapping: the map's
        // `Shared` clone keeps the slot's copy of an `Err(Arc)` alive, and
        // `unshare` recovers the original error only for the sole `Arc`
        // owner. An uncontended caller is then guaranteed the original
        // variant; under contention any awaiter (leader included) may see
        // the `Backend` re-wrap, since the other awaiters' shared handles
        // keep the error `Arc` alive.
        drop(guard);
        unshare(result)
    }
}

/// Drop guard that removes the leader's single-flight entry — including on
/// early return, error, or panic — so a failed or cancelled fetch never
/// leaves a stale entry that would wedge later callers.
struct InFlightGuard<'a, K, V>
where
    K: Hash + Eq,
{
    inner: &'a CachedInner<K, V>,
    key: &'a K,
}

impl<K, V> Drop for InFlightGuard<'_, K, V>
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

/// A point-read KV table fronted by a read-populated, byte-budgeted cache.
/// `decode` runs once per miss inside the single-flight fetch; the value is
/// cached as [`Weighted`] (stamped with the payload's byte length), so hits
/// skip both the backend read and the decode. Read-populated only — writes
/// never seed it, so nothing needs invalidation on a failed commit. Only
/// present values are cached: misses always re-probe the backend, so a key
/// written later is visible immediately.
#[derive(Clone)]
pub struct CachedKvTable<M: MetaStore, V = Bytes> {
    inner: KvTable<M>,
    cache: Arc<CachedInner<Vec<u8>, Weighted<V>>>,
    decode: fn(Bytes) -> Result<V>,
}

impl<M: MetaStore, V> CachedKvTable<M, V>
where
    V: Clone + Send + Sync + 'static,
{
    pub fn new(inner: KvTable<M>, budget_bytes: usize, decode: fn(Bytes) -> Result<V>) -> Self {
        Self {
            inner,
            cache: CachedInner::with_weigher(budget_bytes, weigh_weighted),
            decode,
        }
    }

    pub async fn get(&self, key: &[u8]) -> Result<Option<V>> {
        // The fetch is built only on a miss; decode runs inside it so missers
        // coalesce on the decode too.
        let decode = self.decode;
        Ok(self
            .cache
            .get_or_fetch(key.to_vec(), |key| {
                let inner = self.inner.clone();
                let key = key.clone();
                async move { decode_weighted(inner.get(&key).await?, decode) }
            })
            .await?
            .map(|w| w.value))
    }
}

/// Accessors needing no `V` bounds, so callers that only read identity/stats
/// (e.g. [`WriteSession::put`](crate::store::session::WriteSession::put))
/// need not carry them.
impl<M: MetaStore, V> CachedKvTable<M, V> {
    pub fn table_id(&self) -> TableId {
        self.inner.table
    }

    pub fn take_window_stats(&self) -> (u64, u64) {
        self.cache.take_window_stats()
    }
}

/// A scannable (partition, clustering) KV table with a read-populated
/// per-clustering point cache; see [`CachedKvTable`] for decode-on-miss
/// semantics. Prefix scans always bypass the cache.
#[derive(Clone)]
pub struct CachedScannableKvTable<M: MetaStore, V> {
    inner: ScannableKvTable<M>,
    cache: Arc<CachedInner<(Vec<u8>, Vec<u8>), Weighted<V>>>,
    decode: fn(Bytes) -> Result<V>,
}

impl<M: MetaStore, V> CachedScannableKvTable<M, V>
where
    V: Clone + Send + Sync + 'static,
{
    pub fn new(
        inner: ScannableKvTable<M>,
        budget_bytes: usize,
        decode: fn(Bytes) -> Result<V>,
    ) -> Self {
        Self {
            inner,
            cache: CachedInner::with_weigher(budget_bytes, weigh_weighted),
            decode,
        }
    }

    /// Durable put; never seeds the read cache.
    pub async fn put(&self, partition: &[u8], clustering: &[u8], value: Bytes) -> Result<()> {
        self.inner.put(partition, clustering, value).await
    }

    pub async fn get(&self, partition: &[u8], clustering: &[u8]) -> Result<Option<V>> {
        // The fetch is built only on a miss; decode runs inside it so missers
        // coalesce on the decode too.
        let decode = self.decode;
        Ok(self
            .cache
            .get_or_fetch((partition.to_vec(), clustering.to_vec()), |key| {
                let inner = self.inner.clone();
                let (p, c) = key.clone();
                async move { decode_weighted(inner.get(&p, &c).await?, decode) }
            })
            .await?
            .map(|w| w.value))
    }

    /// Lists every clustering key in the partition, in clustering order.
    /// Bypasses the cache:
    /// caching unbounded result sets would require invalidation on every
    /// adjacent write.
    pub async fn scan_keys(&self, partition: &[u8]) -> Result<Vec<Vec<u8>>> {
        self.inner.scan_keys(partition).await
    }
}

/// Accessors needing no `V` bounds; see [`CachedKvTable`]'s twin block.
impl<M: MetaStore, V> CachedScannableKvTable<M, V> {
    pub fn table_id(&self) -> ScannableTableId {
        self.inner.table
    }

    pub fn take_window_stats(&self) -> (u64, u64) {
        self.cache.take_window_stats()
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, time::Duration};

    use super::*;
    use crate::store::meta::{MetaWriteOp, ScannableTableId as ScanId, TableId};

    /// Backend double counting `get`/`scan_get` calls; each sleeps so concurrent callers overlap.
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
        async fn scan_keys(&self, _table: ScanId, _partition: &[u8]) -> Result<Vec<Vec<u8>>> {
            unimplemented!("not exercised by single-flight tests")
        }
        async fn apply_writes(&self, _writes: Vec<MetaWriteOp>) -> Result<()> {
            unimplemented!("not exercised by single-flight tests")
        }
    }

    const N: usize = 16;

    #[tokio::test]
    async fn kv_get_coalesces_concurrent_misses() {
        let store = CountingStore::with_value(b"k", Bytes::from_static(b"v"));
        let table = KvTable::new(store.clone(), TableId::new("t"));
        let cached = Arc::new(CachedKvTable::new(table, 4096, Ok));

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
        assert_eq!(
            cached.get(b"k").await.unwrap(),
            Some(Bytes::from_static(b"v"))
        );
        assert_eq!(store.get_calls(), 1, "cache hit must not touch the backend");
    }

    #[tokio::test]
    async fn kv_get_coalesces_absent_key_without_caching_the_miss() {
        let store = CountingStore::default();
        let table = KvTable::new(store.clone(), TableId::new("t"));
        let cached = Arc::new(CachedKvTable::new(table, 4096, Ok));

        let mut handles = Vec::new();
        for _ in 0..N {
            let c = cached.clone();
            handles.push(tokio::spawn(async move { c.get(b"missing").await }));
        }
        for h in handles {
            assert_eq!(h.await.unwrap().unwrap(), None);
        }
        assert_eq!(store.get_calls(), 1, "concurrent misses share one fetch");

        // Absence is never cached: writes never seed read caches, so a cached
        // miss would mask a later-written key. Every repeat lookup re-probes.
        assert_eq!(cached.get(b"missing").await.unwrap(), None);
        assert_eq!(store.get_calls(), 2, "a miss must not be served from cache");

        // A value written after the misses is visible on the next read...
        store
            .data
            .lock()
            .unwrap()
            .insert(b"missing".to_vec(), Bytes::from_static(b"v"));
        assert_eq!(
            cached.get(b"missing").await.unwrap(),
            Some(Bytes::from_static(b"v"))
        );
        assert_eq!(store.get_calls(), 3);
        // ...and is cached from then on.
        assert_eq!(
            cached.get(b"missing").await.unwrap(),
            Some(Bytes::from_static(b"v"))
        );
        assert_eq!(store.get_calls(), 3);
    }

    #[tokio::test]
    async fn scannable_get_coalesces_concurrent_misses() {
        let store = CountingStore::with_value(b"pc", Bytes::from_static(b"v"));
        let table = ScannableKvTable::new(store.clone(), ScanId::new("t"));
        let cached = Arc::new(CachedScannableKvTable::new(table, 4096, Ok));

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
    async fn failed_fetch_is_not_cached_and_retries() {
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
            async fn scan_keys(&self, _t: ScanId, _p: &[u8]) -> Result<Vec<Vec<u8>>> {
                unimplemented!()
            }
            async fn apply_writes(&self, _w: Vec<MetaWriteOp>) -> Result<()> {
                unimplemented!()
            }
        }

        let store = ErrStore::default();
        let calls = store.calls.clone();
        let table = KvTable::new(store, TableId::new("t"));
        let cached = Arc::new(CachedKvTable::new(table, 4096, Ok));

        let mut handles = Vec::new();
        for _ in 0..N {
            let c = cached.clone();
            handles.push(tokio::spawn(async move { c.get(b"k").await }));
        }
        for h in handles {
            assert!(h.await.unwrap().is_err());
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        assert!(cached.get(b"k").await.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    /// A sole uncontended caller must get the ORIGINAL error variant back,
    /// not a `Backend` re-wrap: the leader has to release its in-flight
    /// entry before `unshare` tries to unwrap the error `Arc`.
    #[tokio::test]
    async fn sole_caller_gets_original_error_variant() {
        #[derive(Clone, Default)]
        struct CorruptStore;
        impl MetaStore for CorruptStore {
            async fn get(&self, _t: TableId, _k: &[u8]) -> Result<Option<Bytes>> {
                Err(MonadChainDataError::Decode("invalid block metadata rlp"))
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
            async fn scan_keys(&self, _t: ScanId, _p: &[u8]) -> Result<Vec<Vec<u8>>> {
                unimplemented!()
            }
            async fn apply_writes(&self, _w: Vec<MetaWriteOp>) -> Result<()> {
                unimplemented!()
            }
        }

        let table = KvTable::new(CorruptStore, TableId::new("t"));
        let cached = CachedKvTable::new(table, 4096, Ok);
        let err = cached.get(b"k").await.expect_err("fetch fails");
        assert!(
            matches!(err, MonadChainDataError::Decode(_)),
            "sole caller must see the original variant, got {err:?}"
        );
    }

    /// A cancelled leader must not leave the result uncached: every awaiter populates.
    #[tokio::test]
    async fn follower_populates_cache_when_leader_is_cancelled() {
        let store = CountingStore::with_value(b"k", Bytes::from_static(b"v"));
        let table = KvTable::new(store.clone(), TableId::new("t"));
        let cached = Arc::new(CachedKvTable::new(table, 4096, Ok));

        let leader = {
            let c = cached.clone();
            tokio::spawn(async move { c.get(b"k").await })
        };
        let follower = {
            let c = cached.clone();
            tokio::spawn(async move { c.get(b"k").await })
        };

        // Let both reach the single-flight join point, then cancel the leader mid-fetch.
        tokio::time::sleep(Duration::from_millis(10)).await;
        leader.abort();
        let _ = leader.await;

        let got = follower.await.unwrap().unwrap();
        assert_eq!(got, Some(Bytes::from_static(b"v")));
        assert_eq!(store.get_calls(), 1, "single backend fetch despite cancel");

        assert_eq!(
            cached.get(b"k").await.unwrap(),
            Some(Bytes::from_static(b"v"))
        );
        assert_eq!(
            store.get_calls(),
            1,
            "follower must have populated the cache"
        );
    }

    /// Exact byte weigher for region-cache values.
    fn byte_weigher(v: &Bytes) -> usize {
        v.len()
    }

    #[test]
    fn byte_budget_evicts_lru_until_within_budget() {
        let cache = CachedInner::<u64, Bytes>::with_weigher(10, byte_weigher);
        cache.insert(1, Bytes::from_static(b"aaaa")); // size 4
        cache.insert(2, Bytes::from_static(b"bbbb")); // size 8
                                                      // Touch key 1 so key 2 becomes the LRU victim.
        assert_eq!(cache.probe(&1), Some(Bytes::from_static(b"aaaa")));
        cache.insert(3, Bytes::from_static(b"cccc")); // size 12 -> evict LRU (key 2) -> size 8

        assert_eq!(cache.probe(&1), Some(Bytes::from_static(b"aaaa")));
        assert_eq!(cache.probe(&3), Some(Bytes::from_static(b"cccc")));
        assert_eq!(cache.probe(&2), None, "LRU entry evicted to honor budget");
    }

    #[test]
    fn byte_budget_refunds_replaced_weight() {
        let cache = CachedInner::<u64, Bytes>::with_weigher(10, byte_weigher);
        cache.insert(1, Bytes::from_static(b"aaaa")); // size 4
        cache.insert(2, Bytes::from_static(b"bbbb")); // size 8
                                                      // Overwrite key 1 with an equal-size value: size stays 8, no eviction.
        cache.insert(1, Bytes::from_static(b"AAAA"));
        assert_eq!(cache.probe(&1), Some(Bytes::from_static(b"AAAA")));
        assert_eq!(cache.probe(&2), Some(Bytes::from_static(b"bbbb")));
    }

    #[test]
    fn byte_budget_handles_empty_value() {
        let cache = CachedInner::<u64, Bytes>::with_weigher(8, byte_weigher);
        cache.insert(1, Bytes::new()); // weighs 0
        assert_eq!(cache.probe(&1), Some(Bytes::new()));
        cache.insert(2, Bytes::from_static(b"bbbbbbbb"));
        assert_eq!(cache.probe(&1), Some(Bytes::new()));
        assert_eq!(cache.probe(&2), Some(Bytes::from_static(b"bbbbbbbb")));
    }

    #[test]
    fn zero_budget_disables_cache() {
        let cache = CachedInner::<u64, Bytes>::with_weigher(0, byte_weigher);
        cache.insert(1, Bytes::from_static(b"x"));
        assert_eq!(cache.probe(&1), None);
    }

    #[test]
    fn weighted_values_evict_by_stamped_weight() {
        // Budget fits exactly two entries of payload weight 100.
        let budget = 2 * (100 + CACHE_ENTRY_OVERHEAD);
        let cache =
            CachedInner::<u64, Weighted<&'static str>>::with_weigher(budget, weigh_weighted);
        cache.insert(
            1,
            Weighted {
                value: "a",
                weight: 100,
            },
        );
        cache.insert(
            2,
            Weighted {
                value: "b",
                weight: 100,
            },
        );
        assert_eq!(cache.probe(&1).map(|w| w.value), Some("a"));
        // Key 1 was just touched, so inserting a third evicts key 2 (LRU).
        cache.insert(
            3,
            Weighted {
                value: "c",
                weight: 100,
            },
        );
        assert_eq!(cache.probe(&1).map(|w| w.value), Some("a"));
        assert!(cache.probe(&2).is_none());
        assert_eq!(cache.probe(&3).map(|w| w.value), Some("c"));
    }
}
