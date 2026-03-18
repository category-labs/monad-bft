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

use monad_eth_txpool::EthTxPoolMetrics;
use monad_executor::{ExecutorMetricHandle, ExecutorMetrics};

monad_executor::metric_consts! {
    REJECT_FORWARDED_INVALID_BYTES {
        name: "monad.bft.txpool.reject_forwarded_invalid_bytes",
        help: "Forwarded txs rejected due to invalid bytes",
    }
    CREATE_PROPOSAL {
        name: "monad.bft.txpool.create_proposal",
        help: "Proposals created from txpool",
    }
    CREATE_PROPOSAL_ELAPSED_NS {
        name: "monad.bft.txpool.create_proposal_elapsed_ns",
        help: "Time spent creating proposals in nanoseconds",
    }
    PRELOAD_BACKEND_LOOKUPS {
        name: "monad.bft.txpool.preload_backend_lookups",
        help: "Preload backend lookups",
    }
    PRELOAD_BACKEND_REQUESTS {
        name: "monad.bft.txpool.preload_backend_requests",
        help: "Preload backend requests",
    }
}

#[derive(Debug)]
pub struct EthTxPoolExecutorMetrics {
    pub reject_forwarded_invalid_bytes: ExecutorMetricHandle,

    pub create_proposal: ExecutorMetricHandle,
    pub create_proposal_elapsed_ns: ExecutorMetricHandle,

    pub preload_backend_lookups: ExecutorMetricHandle,
    pub preload_backend_requests: ExecutorMetricHandle,

    pub pool: EthTxPoolMetrics,
}

impl EthTxPoolExecutorMetrics {
    pub fn register(executor_metrics: &mut ExecutorMetrics) -> Self {
        Self {
            reject_forwarded_invalid_bytes: executor_metrics
                .register(REJECT_FORWARDED_INVALID_BYTES),
            create_proposal: executor_metrics.register(CREATE_PROPOSAL),
            create_proposal_elapsed_ns: executor_metrics.register(CREATE_PROPOSAL_ELAPSED_NS),
            preload_backend_lookups: executor_metrics.register(PRELOAD_BACKEND_LOOKUPS),
            preload_backend_requests: executor_metrics.register(PRELOAD_BACKEND_REQUESTS),
            pool: EthTxPoolMetrics::register(executor_metrics),
        }
    }
}

impl Default for EthTxPoolExecutorMetrics {
    fn default() -> Self {
        let mut executor_metrics = ExecutorMetrics::default();
        Self::register(&mut executor_metrics)
    }
}
