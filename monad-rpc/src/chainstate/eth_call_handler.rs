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
    future::Future,
    path::Path,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use dashmap::DashMap;
use monad_ethcall::{EthCallExecutor, PoolConfig};
use tokio::sync::{Semaphore, SemaphorePermit, TryAcquireError};
use tracing::error;

use crate::{middleware::TimingRequestId, types::jsonrpc::JsonRpcError};

#[derive(Clone, Debug)]
pub struct EthCallHandlerConfig {
    pub enable_stats: bool,
    pub pool_low: PoolConfig,
    pub pool_high: PoolConfig,
    pub pool_block: PoolConfig,
    pub tx_exec_num_fibers: u32,
    pub node_cache_max_mem: u64,
    pub max_concurrent_permits: usize,
}

#[derive(Clone)]
pub struct EthCallHandler {
    config: EthCallHandlerConfig,

    executor: Arc<EthCallExecutor>,
    rate_limiter: Arc<Semaphore>,
    stats_tracker: Option<Arc<EthCallStatsTracker>>,
}

impl EthCallHandler {
    pub fn new(config: EthCallHandlerConfig, triedb_path: &Path) -> Self {
        let executor = Arc::new(EthCallExecutor::new(
            config.pool_low,
            config.pool_high,
            config.pool_block,
            config.tx_exec_num_fibers,
            config.node_cache_max_mem,
            triedb_path,
        ));

        let rate_limiter = Arc::new(Semaphore::new(config.max_concurrent_permits));

        let stats_tracker = config
            .enable_stats
            .then(|| Arc::new(EthCallStatsTracker::default()));

        Self {
            config,

            executor,
            rate_limiter,
            stats_tracker,
        }
    }

    pub async fn acquire(
        &self,
        request_id: TimingRequestId,
    ) -> Result<EthCallPermit<'_>, JsonRpcError> {
        let permit = match self.rate_limiter.try_acquire() {
            Ok(permit) => permit,
            Err(err) => match err {
                TryAcquireError::Closed => {
                    error!("EthCallHandler acquire rate_limiter closed");
                    return Err(JsonRpcError::internal_error("method unavailable".into()));
                }
                TryAcquireError::NoPermits => {
                    if let Some(tracker) = &self.stats_tracker {
                        tracker.record_queue_rejection();
                    }
                    return Err(JsonRpcError::internal_error(
                        "concurrent requests limit".into(),
                    ));
                }
            },
        };

        Ok(EthCallPermit {
            permit,
            request_id,

            executor: &self.executor,
            stats_tracker: self.stats_tracker.as_deref(),
        })
    }

    pub fn config(&self) -> &EthCallHandlerConfig {
        &self.config
    }

    pub fn available_permits(&self) -> usize {
        self.rate_limiter.available_permits()
    }

    pub fn stats_tracker(&self) -> Option<&EthCallStatsTracker> {
        self.stats_tracker.as_deref()
    }
}

pub struct EthCallPermit<'a> {
    #[allow(dead_code)]
    permit: SemaphorePermit<'a>,
    request_id: TimingRequestId,

    executor: &'a EthCallExecutor,
    stats_tracker: Option<&'a EthCallStatsTracker>,
}

impl<'a> EthCallPermit<'a> {
    pub async fn execute<T, E, F>(self, f: impl FnOnce(&'a EthCallExecutor) -> F) -> Result<T, E>
    where
        F: Future<Output = Result<T, E>>,
    {
        if let Some(tracker) = self.stats_tracker {
            tracker.record_request_start(self.request_id);
        }

        let result = f(self.executor).await;

        if let Some(tracker) = self.stats_tracker {
            tracker.record_request_complete(&self.request_id, result.is_err());
        }

        result
    }
}

#[derive(Debug)]
struct EthCallRequestStats {
    entry_time: Instant,
}

#[derive(Debug, Default)]
struct CumulativeStats {
    total_requests: AtomicU64,
    total_errors: AtomicU64,
    queue_rejections: AtomicU64,
}

impl CumulativeStats {
    fn create_view(&self) -> CumulativeStatsView {
        CumulativeStatsView {
            total_requests: self.total_requests.load(Ordering::Relaxed),
            total_errors: self.total_errors.load(Ordering::Relaxed),
            queue_rejections: self.queue_rejections.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug)]
pub struct CumulativeStatsView {
    pub total_requests: u64,
    pub total_errors: u64,
    pub queue_rejections: u64,
}

#[derive(Debug, Default)]
pub struct EthCallStatsTracker {
    active_requests: DashMap<TimingRequestId, EthCallRequestStats>,
    stats: CumulativeStats,
}

impl EthCallStatsTracker {
    fn record_request_start(&self, request_id: TimingRequestId) {
        self.active_requests.insert(
            request_id,
            EthCallRequestStats {
                entry_time: Instant::now(),
            },
        );

        self.stats.total_requests.fetch_add(1, Ordering::Relaxed);
    }

    fn record_request_complete(&self, request_id: &TimingRequestId, is_error: bool) {
        self.active_requests.remove(request_id);

        if is_error {
            self.stats.total_errors.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn record_queue_rejection(&self) {
        self.stats.queue_rejections.fetch_add(1, Ordering::Relaxed);
        self.stats.total_requests.fetch_add(1, Ordering::Relaxed);
        self.stats.total_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn get_stats(&self) -> (Option<Duration>, Option<Duration>, CumulativeStatsView) {
        let mut requests = 0usize;

        let now = Instant::now();
        let mut max_age = Duration::ZERO;
        let mut total_age = Duration::ZERO;

        for stats in self.active_requests.iter() {
            requests += 1;

            let age = now.saturating_duration_since(stats.value().entry_time);
            max_age = max_age.max(age);
            total_age += age;
        }

        if requests == 0 {
            return (None, None, self.stats.create_view());
        }

        let avg_age = total_age / requests as u32;

        (Some(max_age), Some(avg_age), self.stats.create_view())
    }
}
