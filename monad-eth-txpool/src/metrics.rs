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

use monad_executor::ExecutorMetrics;
use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct EthTxPoolMetrics {
    pub insert_owned_txs: AtomicU64,
    pub insert_forwarded_txs: AtomicU64,

    pub drop_not_well_formed: AtomicU64,
    pub drop_invalid_signature: AtomicU64,
    pub drop_nonce_too_low: AtomicU64,
    pub drop_fee_too_low: AtomicU64,
    pub drop_insufficient_balance: AtomicU64,
    pub drop_existing_higher_priority: AtomicU64,
    pub drop_replaced_by_higher_priority: AtomicU64,
    pub drop_pool_full: AtomicU64,
    pub drop_pool_not_ready: AtomicU64,
    pub drop_internal_state_backend_error: AtomicU64,
    pub drop_internal_not_ready: AtomicU64,

    pub create_proposal: AtomicU64,
    pub create_proposal_txs: AtomicU64,
    pub create_proposal_tracked_addresses: AtomicU64,
    pub create_proposal_available_addresses: AtomicU64,
    pub create_proposal_backend_lookups: AtomicU64,

    pub tracked: EthTxPoolTrackedMetrics,
}

impl EthTxPoolMetrics {
    pub fn update(&self, metrics: &mut ExecutorMetrics) {
        metrics["monad.bft.txpool.pool.insert_owned_txs"] =
            self.insert_owned_txs.load(Ordering::SeqCst);
        metrics["monad.bft.txpool.pool.insert_forwarded_txs"] =
            self.insert_forwarded_txs.load(Ordering::SeqCst);

        metrics["monad.bft.txpool.pool.drop_not_well_formed"] =
            self.drop_not_well_formed.load(Ordering::SeqCst);
        metrics["monad.bft.txpool.pool.drop_invalid_signature"] =
            self.drop_invalid_signature.load(Ordering::SeqCst);
        metrics["monad.bft.txpool.pool.drop_nonce_too_low"] =
            self.drop_nonce_too_low.load(Ordering::SeqCst);
        metrics["monad.bft.txpool.pool.drop_fee_too_low"] =
            self.drop_fee_too_low.load(Ordering::SeqCst);
        metrics["monad.bft.txpool.pool.drop_insufficient_balance"] =
            self.drop_insufficient_balance.load(Ordering::SeqCst);
        metrics["monad.bft.txpool.pool.drop_existing_higher_priority"] =
            self.drop_existing_higher_priority.load(Ordering::SeqCst);
        metrics["monad.bft.txpool.pool.drop_replaced_by_higher_priority"] =
            self.drop_replaced_by_higher_priority.load(Ordering::SeqCst);
        metrics["monad.bft.txpool.pool.drop_pool_full"] =
            self.drop_pool_full.load(Ordering::SeqCst);
        metrics["monad.bft.txpool.pool.drop_pool_not_ready"] =
            self.drop_pool_not_ready.load(Ordering::SeqCst);
        metrics["monad.bft.txpool.pool.drop_internal_state_backend_error"] = self
            .drop_internal_state_backend_error
            .load(Ordering::SeqCst);
        metrics["monad.bft.txpool.pool.drop_internal_not_ready"] =
            self.drop_internal_not_ready.load(Ordering::SeqCst);

        metrics["monad.bft.txpool.pool.create_proposal"] =
            self.create_proposal.load(Ordering::SeqCst);
        metrics["monad.bft.txpool.pool.create_proposal_txs"] =
            self.create_proposal_txs.load(Ordering::SeqCst);
        metrics["monad.bft.txpool.pool.create_proposal_tracked_addresses"] = self
            .create_proposal_tracked_addresses
            .load(Ordering::SeqCst);
        metrics["monad.bft.txpool.pool.create_proposal_available_addresses"] = self
            .create_proposal_available_addresses
            .load(Ordering::SeqCst);
        metrics["monad.bft.txpool.pool.create_proposal_backend_lookups"] =
            self.create_proposal_backend_lookups.load(Ordering::SeqCst);

        self.tracked.update(metrics);
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct EthTxPoolTrackedMetrics {
    pub addresses: AtomicU64,
    pub txs: AtomicU64,
    pub evict_expired_addresses: AtomicU64,
    pub evict_expired_txs: AtomicU64,
    pub remove_committed_addresses: AtomicU64,
    pub remove_committed_txs: AtomicU64,
}

impl EthTxPoolTrackedMetrics {
    pub fn update(&self, metrics: &mut ExecutorMetrics) {
        metrics["monad.bft.txpool.pool.tracked.addresses"] = self.addresses.load(Ordering::SeqCst);
        metrics["monad.bft.txpool.pool.tracked.txs"] = self.txs.load(Ordering::SeqCst);
        metrics["monad.bft.txpool.pool.tracked.evict_expired_addresses"] =
            self.evict_expired_addresses.load(Ordering::SeqCst);
        metrics["monad.bft.txpool.pool.tracked.evict_expired_txs"] =
            self.evict_expired_txs.load(Ordering::SeqCst);
        metrics["monad.bft.txpool.pool.tracked.remove_committed_addresses"] =
            self.remove_committed_addresses.load(Ordering::SeqCst);
        metrics["monad.bft.txpool.pool.tracked.remove_committed_txs"] =
            self.remove_committed_txs.load(Ordering::SeqCst);
    }
}
