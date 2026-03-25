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
