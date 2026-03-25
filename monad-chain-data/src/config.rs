use std::{fmt, sync::Arc};

use crate::kernel::cache::BytesCacheConfig;

#[derive(Clone)]
pub struct Config {
    pub observe_upstream_finalized_block: Arc<dyn Fn() -> Option<u64> + Send + Sync>,
    pub publication_lease_blocks: u64,
    pub publication_lease_renew_threshold_blocks: u64,
    pub planner_max_or_terms: usize,
    pub assume_empty_streams: bool,
    pub stream_append_concurrency: usize,
    pub bytes_cache: BytesCacheConfig,
}

impl fmt::Debug for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Config")
            .field("observe_upstream_finalized_block", &"<callback>")
            .field("publication_lease_blocks", &self.publication_lease_blocks)
            .field(
                "publication_lease_renew_threshold_blocks",
                &self.publication_lease_renew_threshold_blocks,
            )
            .field("planner_max_or_terms", &self.planner_max_or_terms)
            .field("assume_empty_streams", &self.assume_empty_streams)
            .field("stream_append_concurrency", &self.stream_append_concurrency)
            .field("bytes_cache", &self.bytes_cache)
            .finish()
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            observe_upstream_finalized_block: Arc::new(|| None),
            publication_lease_blocks: 10,
            publication_lease_renew_threshold_blocks: 2,
            planner_max_or_terms: 128,
            assume_empty_streams: false,
            stream_append_concurrency: 96,
            bytes_cache: BytesCacheConfig::default(),
        }
    }
}
