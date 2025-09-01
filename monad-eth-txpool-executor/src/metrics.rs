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
        metrics["monad.bft.txpool.reject_forwarded_invalid_bytes"] =
            self.reject_forwarded_invalid_bytes.load(Ordering::SeqCst);

        metrics["monad.bft.txpool.create_proposal"] = self.create_proposal.load(Ordering::SeqCst);
        metrics["monad.bft.txpool.create_proposal_elapsed_ns"] =
            self.create_proposal_elapsed_ns.load(Ordering::SeqCst);

        metrics["monad.bft.txpool.preload_backend_lookups"] =
            self.preload_backend_lookups.load(Ordering::SeqCst);
        metrics["monad.bft.txpool.preload_backend_requests"] =
            self.preload_backend_requests.load(Ordering::SeqCst);

        self.pool.update(metrics);
    }
}
