use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use bytes::Bytes;
use quick_cache::{sync::Cache, DefaultHashBuilder, Lifecycle, OptionsBuilder, Weighter};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TableCacheConfig {
    pub max_bytes: u64,
}

impl TableCacheConfig {
    pub const fn disabled() -> Self {
        Self { max_bytes: 0 }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BytesCacheConfig {
    pub block_records: TableCacheConfig,
    pub log_block_headers: TableCacheConfig,
    pub log_dir_buckets: TableCacheConfig,
    pub log_dir_sub_buckets: TableCacheConfig,
    pub log_block_blobs: TableCacheConfig,
    pub block_tx_blobs: TableCacheConfig,
    pub block_trace_blobs: TableCacheConfig,
    pub log_bitmap_page_meta: TableCacheConfig,
    pub log_bitmap_page_blobs: TableCacheConfig,
}

impl BytesCacheConfig {
    pub const fn disabled() -> Self {
        Self {
            block_records: TableCacheConfig::disabled(),
            log_block_headers: TableCacheConfig::disabled(),
            log_dir_buckets: TableCacheConfig::disabled(),
            log_dir_sub_buckets: TableCacheConfig::disabled(),
            log_block_blobs: TableCacheConfig::disabled(),
            block_tx_blobs: TableCacheConfig::disabled(),
            block_trace_blobs: TableCacheConfig::disabled(),
            log_bitmap_page_meta: TableCacheConfig::disabled(),
            log_bitmap_page_blobs: TableCacheConfig::disabled(),
        }
    }
}

impl Default for BytesCacheConfig {
    fn default() -> Self {
        Self::disabled()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TableCacheMetrics {
    pub hits: u64,
    pub misses: u64,
    pub inserts: u64,
    pub evictions: u64,
    pub bytes_used: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct BytesCacheMetrics {
    pub block_records: TableCacheMetrics,
    pub log_block_headers: TableCacheMetrics,
    pub log_dir_buckets: TableCacheMetrics,
    pub log_dir_sub_buckets: TableCacheMetrics,
    pub log_block_blobs: TableCacheMetrics,
    pub block_tx_blobs: TableCacheMetrics,
    pub block_trace_blobs: TableCacheMetrics,
    pub log_bitmap_page_meta: TableCacheMetrics,
    pub log_bitmap_page_blobs: TableCacheMetrics,
}

#[derive(Clone, Debug)]
struct WeightedBytes {
    bytes: Bytes,
    weight: u64,
}

#[derive(Debug, Default)]
struct Metrics {
    inserts: AtomicU64,
    evictions: AtomicU64,
}

#[derive(Clone, Copy, Debug, Default)]
struct BytesWeighter;

impl Weighter<Vec<u8>, WeightedBytes> for BytesWeighter {
    fn weight(&self, _key: &Vec<u8>, value: &WeightedBytes) -> u64 {
        value.weight
    }
}

#[derive(Clone, Debug, Default)]
struct MetricsLifecycle {
    metrics: Arc<Metrics>,
}

impl Lifecycle<Vec<u8>, WeightedBytes> for MetricsLifecycle {
    type RequestState = ();

    fn begin_request(&self) -> Self::RequestState {}

    fn on_evict(&self, _state: &mut Self::RequestState, _key: Vec<u8>, _val: WeightedBytes) {
        self.metrics.evictions.fetch_add(1, Ordering::Relaxed);
    }
}

type QuickBytesCache =
    Cache<Vec<u8>, WeightedBytes, BytesWeighter, DefaultHashBuilder, MetricsLifecycle>;

#[derive(Clone, Debug)]
pub struct HashMapTableBytesCache {
    max_bytes: u64,
    inner: Option<Arc<QuickBytesCache>>,
    metrics: Arc<Metrics>,
}

impl HashMapTableBytesCache {
    pub fn new(max_bytes: u64) -> Self {
        let metrics = Arc::new(Metrics::default());
        let inner = (max_bytes > 0).then(|| {
            let mut options = OptionsBuilder::new();
            options
                .estimated_items_capacity(estimated_items_capacity(max_bytes))
                .weight_capacity(max_bytes);
            Arc::new(QuickBytesCache::with_options(
                options.build().expect("valid quick_cache options"),
                BytesWeighter,
                DefaultHashBuilder::default(),
                MetricsLifecycle {
                    metrics: Arc::clone(&metrics),
                },
            ))
        });
        Self {
            max_bytes,
            inner,
            metrics,
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<Bytes> {
        let inner = self.inner.as_ref()?;
        inner.get(key).map(|value| value.bytes)
    }

    pub fn put(&self, key: &[u8], value: Bytes, weight: usize) {
        let Some(inner) = self.inner.as_ref() else {
            return;
        };
        let weight = u64::try_from(weight).unwrap_or(u64::MAX);
        if weight > self.max_bytes {
            return;
        }
        inner.insert(
            key.to_vec(),
            WeightedBytes {
                bytes: value,
                weight,
            },
        );
        self.metrics.inserts.fetch_add(1, Ordering::Relaxed);
    }

    pub fn metrics_snapshot(&self) -> TableCacheMetrics {
        let (hits, misses, bytes_used) = match self.inner.as_ref() {
            Some(inner) => (inner.hits(), inner.misses(), inner.weight()),
            None => (0, 0, 0),
        };
        TableCacheMetrics {
            hits,
            misses,
            inserts: self.metrics.inserts.load(Ordering::Relaxed),
            evictions: self.metrics.evictions.load(Ordering::Relaxed),
            bytes_used,
        }
    }
}

impl Default for HashMapTableBytesCache {
    fn default() -> Self {
        Self::new(0)
    }
}

pub fn cache_for(max_bytes: u64) -> HashMapTableBytesCache {
    HashMapTableBytesCache::new(max_bytes)
}

pub fn no_cache() -> HashMapTableBytesCache {
    HashMapTableBytesCache::default()
}

fn estimated_items_capacity(max_bytes: u64) -> usize {
    const DEFAULT_ESTIMATED_ENTRY_BYTES: u64 = 256;
    let estimate = (max_bytes / DEFAULT_ESTIMATED_ENTRY_BYTES).max(1);
    usize::try_from(estimate).unwrap_or(usize::MAX)
}
