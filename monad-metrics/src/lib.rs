use std::sync::RwLock;

use lazy_static::lazy_static;
use monad_consensus_types::metrics::GlobalMetrics;

pub struct StaticMetrics {
    metrics: GlobalMetrics,
}

lazy_static! {
    pub static ref METRICS: RwLock<StaticMetrics> = RwLock::new(StaticMetrics::new());
}

impl StaticMetrics {
    fn new() -> Self {
        StaticMetrics {
            metrics: GlobalMetrics::default(),
        }
    }

    pub fn read_metrics(&self) -> &GlobalMetrics {
        &self.metrics
    }

    pub fn metrics(&mut self) -> &mut GlobalMetrics {
        &mut self.metrics
    }
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn test_metric_scoped_access() {
        for i in 0..10 {
            let mut global_metrics = METRICS.write().unwrap();
            let metrics = global_metrics.metrics();

            assert_eq!(metrics.backendcache_events.db_account_num_reads, i);
            metrics.backendcache_events.db_account_num_reads += 1;
            assert_eq!(metrics.backendcache_events.db_account_num_reads, i + 1);
        }
    }

    #[test]
    fn test_metric_use_global() {
        // This test will fail if it runs standalone.
        let mut global_metrics = METRICS.write().unwrap();
        let metrics = global_metrics.metrics();
        // Expect value set in the previous test
        assert_eq!(metrics.backendcache_events.db_account_num_reads, 10);
        metrics.backendcache_events.db_account_num_reads += 1;
        assert_eq!(metrics.backendcache_events.db_account_num_reads, 11);
    }
}
