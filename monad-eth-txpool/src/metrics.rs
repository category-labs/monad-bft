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

monad_executor::define_metric!(POOL_INSERT_OWNED_TXS, "monad.bft.txpool.pool.insert_owned_txs", "Owned transactions inserted into the pool");
monad_executor::define_metric!(POOL_INSERT_FORWARDED_TXS, "monad.bft.txpool.pool.insert_forwarded_txs", "Forwarded transactions inserted into the pool");
monad_executor::define_metric!(POOL_DROP_NOT_WELL_FORMED, "monad.bft.txpool.pool.drop_not_well_formed", "Transactions dropped due to malformed data");
monad_executor::define_metric!(POOL_DROP_INVALID_SIGNATURE, "monad.bft.txpool.pool.drop_invalid_signature", "Transactions dropped due to invalid signature");
monad_executor::define_metric!(POOL_DROP_NONCE_TOO_LOW, "monad.bft.txpool.pool.drop_nonce_too_low", "Transactions dropped due to nonce too low");
monad_executor::define_metric!(POOL_DROP_FEE_TOO_LOW, "monad.bft.txpool.pool.drop_fee_too_low", "Transactions dropped due to fee too low");
monad_executor::define_metric!(POOL_DROP_INSUFFICIENT_BALANCE, "monad.bft.txpool.pool.drop_insufficient_balance", "Transactions dropped due to insufficient balance");
monad_executor::define_metric!(POOL_DROP_EXISTING_HIGHER_PRIORITY, "monad.bft.txpool.pool.drop_existing_higher_priority", "Transactions dropped - existing tx has higher priority");
monad_executor::define_metric!(POOL_DROP_REPLACED_BY_HIGHER_PRIORITY, "monad.bft.txpool.pool.drop_replaced_by_higher_priority", "Transactions replaced by higher priority");
monad_executor::define_metric!(POOL_DROP_POOL_FULL, "monad.bft.txpool.pool.drop_pool_full", "Transactions dropped because pool is full");
monad_executor::define_metric!(POOL_DROP_POOL_NOT_READY, "monad.bft.txpool.pool.drop_pool_not_ready", "Transactions dropped because pool is not ready");
monad_executor::define_metric!(POOL_DROP_INTERNAL_STATE_BACKEND_ERROR, "monad.bft.txpool.pool.drop_internal_state_backend_error", "Transactions dropped due to backend error");
monad_executor::define_metric!(POOL_DROP_INTERNAL_NOT_READY, "monad.bft.txpool.pool.drop_internal_not_ready", "Transactions dropped due to internal not ready");
monad_executor::define_metric!(POOL_CREATE_PROPOSAL, "monad.bft.txpool.pool.create_proposal", "Proposals created from txpool");
monad_executor::define_metric!(POOL_CREATE_PROPOSAL_TXS, "monad.bft.txpool.pool.create_proposal_txs", "Transactions included in proposals");
monad_executor::define_metric!(POOL_CREATE_PROPOSAL_TRACKED_ADDRESSES, "monad.bft.txpool.pool.create_proposal_tracked_addresses", "Tracked addresses during proposal creation");
monad_executor::define_metric!(POOL_CREATE_PROPOSAL_AVAILABLE_ADDRESSES, "monad.bft.txpool.pool.create_proposal_available_addresses", "Available addresses during proposal creation");
monad_executor::define_metric!(POOL_CREATE_PROPOSAL_BACKEND_LOOKUPS, "monad.bft.txpool.pool.create_proposal_backend_lookups", "Backend lookups during proposal creation");
monad_executor::define_metric!(TRACKED_ADDRESSES, "monad.bft.txpool.pool.tracked.addresses", "Addresses being tracked in the pool");
monad_executor::define_metric!(TRACKED_TXS, "monad.bft.txpool.pool.tracked.txs", "Transactions being tracked in the pool");
monad_executor::define_metric!(TRACKED_EVICT_EXPIRED_ADDRESSES, "monad.bft.txpool.pool.tracked.evict_expired_addresses", "Addresses evicted due to expiration");
monad_executor::define_metric!(TRACKED_EVICT_EXPIRED_TXS, "monad.bft.txpool.pool.tracked.evict_expired_txs", "Transactions evicted due to expiration");
monad_executor::define_metric!(TRACKED_REMOVE_COMMITTED_ADDRESSES, "monad.bft.txpool.pool.tracked.remove_committed_addresses", "Addresses removed after commitment");
monad_executor::define_metric!(TRACKED_REMOVE_COMMITTED_TXS, "monad.bft.txpool.pool.tracked.remove_committed_txs", "Transactions removed after commitment");

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
        metrics[&POOL_INSERT_OWNED_TXS] = self.insert_owned_txs.load(Ordering::SeqCst);
        metrics[&POOL_INSERT_FORWARDED_TXS] = self.insert_forwarded_txs.load(Ordering::SeqCst);

        metrics[&POOL_DROP_NOT_WELL_FORMED] = self.drop_not_well_formed.load(Ordering::SeqCst);
        metrics[&POOL_DROP_INVALID_SIGNATURE] = self.drop_invalid_signature.load(Ordering::SeqCst);
        metrics[&POOL_DROP_NONCE_TOO_LOW] = self.drop_nonce_too_low.load(Ordering::SeqCst);
        metrics[&POOL_DROP_FEE_TOO_LOW] = self.drop_fee_too_low.load(Ordering::SeqCst);
        metrics[&POOL_DROP_INSUFFICIENT_BALANCE] =
            self.drop_insufficient_balance.load(Ordering::SeqCst);
        metrics[&POOL_DROP_EXISTING_HIGHER_PRIORITY] =
            self.drop_existing_higher_priority.load(Ordering::SeqCst);
        metrics[&POOL_DROP_REPLACED_BY_HIGHER_PRIORITY] = self
            .drop_replaced_by_higher_priority
            .load(Ordering::SeqCst);
        metrics[&POOL_DROP_POOL_FULL] = self.drop_pool_full.load(Ordering::SeqCst);
        metrics[&POOL_DROP_POOL_NOT_READY] = self.drop_pool_not_ready.load(Ordering::SeqCst);
        metrics[&POOL_DROP_INTERNAL_STATE_BACKEND_ERROR] = self
            .drop_internal_state_backend_error
            .load(Ordering::SeqCst);
        metrics[&POOL_DROP_INTERNAL_NOT_READY] =
            self.drop_internal_not_ready.load(Ordering::SeqCst);

        metrics[&POOL_CREATE_PROPOSAL] = self.create_proposal.load(Ordering::SeqCst);
        metrics[&POOL_CREATE_PROPOSAL_TXS] = self.create_proposal_txs.load(Ordering::SeqCst);
        metrics[&POOL_CREATE_PROPOSAL_TRACKED_ADDRESSES] = self
            .create_proposal_tracked_addresses
            .load(Ordering::SeqCst);
        metrics[&POOL_CREATE_PROPOSAL_AVAILABLE_ADDRESSES] = self
            .create_proposal_available_addresses
            .load(Ordering::SeqCst);
        metrics[&POOL_CREATE_PROPOSAL_BACKEND_LOOKUPS] =
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
        metrics[&TRACKED_ADDRESSES] = self.addresses.load(Ordering::SeqCst);
        metrics[&TRACKED_TXS] = self.txs.load(Ordering::SeqCst);
        metrics[&TRACKED_EVICT_EXPIRED_ADDRESSES] =
            self.evict_expired_addresses.load(Ordering::SeqCst);
        metrics[&TRACKED_EVICT_EXPIRED_TXS] = self.evict_expired_txs.load(Ordering::SeqCst);
        metrics[&TRACKED_REMOVE_COMMITTED_ADDRESSES] =
            self.remove_committed_addresses.load(Ordering::SeqCst);
        metrics[&TRACKED_REMOVE_COMMITTED_TXS] = self.remove_committed_txs.load(Ordering::SeqCst);
    }
}
