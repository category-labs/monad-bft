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

use std::{future::Future, path::Path, sync::Arc};

use monad_ethcall::{EthCallExecutor, PoolConfig};
use tokio::sync::{Semaphore, SemaphorePermit, TryAcquireError};
use tracing::error;

use crate::{
    handlers::eth::call::EthCallStatsTracker, middleware::TimingRequestId,
    types::jsonrpc::JsonRpcError,
};

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
                    error!("EthCallHandler acquire ratelimiter closed");
                    return Err(JsonRpcError::internal_error(
                        "eth_call method unavailable".into(),
                    ));
                }
                TryAcquireError::NoPermits => {
                    if let Some(tracker) = &self.stats_tracker {
                        tracker.record_queue_rejection().await;
                    }
                    return Err(JsonRpcError::internal_error(
                        "eth_call concurrent requests limit".into(),
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
            tracker.record_request_start(self.request_id).await;
        }

        let result = f(self.executor).await;

        if let Some(tracker) = self.stats_tracker {
            tracker
                .record_request_complete(&self.request_id, result.is_err())
                .await;
        }

        result
    }
}
