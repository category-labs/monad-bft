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

use std::sync::atomic::{AtomicU64, Ordering};

use monad_eth_txpool::EthTxPoolMetrics;
use monad_executor::ExecutorMetrics;
use serde::{Deserialize, Serialize};

monad_executor::define_metric!(REJECT_FORWARDED_INVALID_BYTES, "monad.bft.txpool.reject_forwarded_invalid_bytes", "Forwarded txs rejected due to invalid bytes");
monad_executor::define_metric!(CREATE_PROPOSAL, "monad.bft.txpool.create_proposal", "Proposals created from txpool");
monad_executor::define_metric!(CREATE_PROPOSAL_ELAPSED_NS, "monad.bft.txpool.create_proposal_elapsed_ns", "Time spent creating proposals in nanoseconds");
monad_executor::define_metric!(PRELOAD_BACKEND_LOOKUPS, "monad.bft.txpool.preload_backend_lookups", "Preload backend lookups");
monad_executor::define_metric!(PRELOAD_BACKEND_REQUESTS, "monad.bft.txpool.preload_backend_requests", "Preload backend requests");

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct EthTxPoolExecutorMetrics {
    pub reject_forwarded_invalid_bytes: AtomicU64,

    pub create_proposal: AtomicU64,
    pub create_proposal_elapsed_ns: AtomicU64,

    pub preload_backend_lookups: AtomicU64,
    pub preload_backend_requests: AtomicU64,

    pub pool: EthTxPoolMetrics,
}

impl EthTxPoolExecutorMetrics {
    pub fn update(&self, metrics: &mut ExecutorMetrics) {
        metrics[&REJECT_FORWARDED_INVALID_BYTES] =
            self.reject_forwarded_invalid_bytes.load(Ordering::SeqCst);

        metrics[&CREATE_PROPOSAL] = self.create_proposal.load(Ordering::SeqCst);
        metrics[&CREATE_PROPOSAL_ELAPSED_NS] =
            self.create_proposal_elapsed_ns.load(Ordering::SeqCst);

        metrics[&PRELOAD_BACKEND_LOOKUPS] = self.preload_backend_lookups.load(Ordering::SeqCst);
        metrics[&PRELOAD_BACKEND_REQUESTS] = self.preload_backend_requests.load(Ordering::SeqCst);

        self.pool.update(metrics);
    }
}
