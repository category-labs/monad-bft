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

monad_executor::metric_consts! {
    POOL_INSERT_OWNED_TXS {
        name: "monad.bft.txpool.pool.insert_owned_txs",
        help: "Owned transactions inserted into the pool",
    }
    POOL_INSERT_FORWARDED_TXS {
        name: "monad.bft.txpool.pool.insert_forwarded_txs",
        help: "Forwarded transactions inserted into the pool",
    }
    POOL_DROP_NOT_WELL_FORMED {
        name: "monad.bft.txpool.pool.drop_not_well_formed",
        help: "Transactions dropped due to malformed data",
    }
    POOL_DROP_INVALID_SIGNATURE {
        name: "monad.bft.txpool.pool.drop_invalid_signature",
        help: "Transactions dropped due to invalid signature",
    }
    POOL_DROP_NONCE_TOO_LOW {
        name: "monad.bft.txpool.pool.drop_nonce_too_low",
        help: "Transactions dropped due to nonce too low",
    }
    POOL_DROP_FEE_TOO_LOW {
        name: "monad.bft.txpool.pool.drop_fee_too_low",
        help: "Transactions dropped due to fee too low",
    }
    POOL_DROP_INSUFFICIENT_BALANCE {
        name: "monad.bft.txpool.pool.drop_insufficient_balance",
        help: "Transactions dropped due to insufficient balance",
    }
    POOL_DROP_EXISTING_HIGHER_PRIORITY {
        name: "monad.bft.txpool.pool.drop_existing_higher_priority",
        help: "Transactions dropped - existing tx has higher priority",
    }
    POOL_DROP_REPLACED_BY_HIGHER_PRIORITY {
        name: "monad.bft.txpool.pool.drop_replaced_by_higher_priority",
        help: "Transactions replaced by higher priority",
    }
    POOL_DROP_POOL_FULL {
        name: "monad.bft.txpool.pool.drop_pool_full",
        help: "Transactions dropped because pool is full",
    }
    POOL_DROP_POOL_NOT_READY {
        name: "monad.bft.txpool.pool.drop_pool_not_ready",
        help: "Transactions dropped because pool is not ready",
    }
    POOL_DROP_INTERNAL_STATE_BACKEND_ERROR {
        name: "monad.bft.txpool.pool.drop_internal_state_backend_error",
        help: "Transactions dropped due to backend error",
    }
    POOL_DROP_INTERNAL_NOT_READY {
        name: "monad.bft.txpool.pool.drop_internal_not_ready",
        help: "Transactions dropped due to internal not ready",
    }
    POOL_CREATE_PROPOSAL {
        name: "monad.bft.txpool.pool.create_proposal",
        help: "Proposals created from txpool",
    }
    POOL_CREATE_PROPOSAL_TXS {
        name: "monad.bft.txpool.pool.create_proposal_txs",
        help: "Transactions included in proposals",
    }
    POOL_CREATE_PROPOSAL_TRACKED_ADDRESSES {
        name: "monad.bft.txpool.pool.create_proposal_tracked_addresses",
        help: "Tracked addresses during proposal creation",
    }
    POOL_CREATE_PROPOSAL_AVAILABLE_ADDRESSES {
        name: "monad.bft.txpool.pool.create_proposal_available_addresses",
        help: "Available addresses during proposal creation",
    }
    POOL_CREATE_PROPOSAL_BACKEND_LOOKUPS {
        name: "monad.bft.txpool.pool.create_proposal_backend_lookups",
        help: "Backend lookups during proposal creation",
    }
    TRACKED_ADDRESSES {
        name: "monad.bft.txpool.pool.tracked.addresses",
        help: "Addresses being tracked in the pool",
    }
    TRACKED_TXS {
        name: "monad.bft.txpool.pool.tracked.txs",
        help: "Transactions being tracked in the pool",
    }
    TRACKED_EVICT_EXPIRED_ADDRESSES {
        name: "monad.bft.txpool.pool.tracked.evict_expired_addresses",
        help: "Addresses evicted due to expiration",
    }
    TRACKED_EVICT_EXPIRED_TXS {
        name: "monad.bft.txpool.pool.tracked.evict_expired_txs",
        help: "Transactions evicted due to expiration",
    }
    TRACKED_REMOVE_COMMITTED_ADDRESSES {
        name: "monad.bft.txpool.pool.tracked.remove_committed_addresses",
        help: "Addresses removed after commitment",
    }
    TRACKED_REMOVE_COMMITTED_TXS {
        name: "monad.bft.txpool.pool.tracked.remove_committed_txs",
        help: "Transactions removed after commitment",
    }
}

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
        metrics[POOL_INSERT_OWNED_TXS] = self.insert_owned_txs.load(Ordering::SeqCst);
        metrics[POOL_INSERT_FORWARDED_TXS] = self.insert_forwarded_txs.load(Ordering::SeqCst);

        metrics[POOL_DROP_NOT_WELL_FORMED] = self.drop_not_well_formed.load(Ordering::SeqCst);
        metrics[POOL_DROP_INVALID_SIGNATURE] = self.drop_invalid_signature.load(Ordering::SeqCst);
        metrics[POOL_DROP_NONCE_TOO_LOW] = self.drop_nonce_too_low.load(Ordering::SeqCst);
        metrics[POOL_DROP_FEE_TOO_LOW] = self.drop_fee_too_low.load(Ordering::SeqCst);
        metrics[POOL_DROP_INSUFFICIENT_BALANCE] =
            self.drop_insufficient_balance.load(Ordering::SeqCst);
        metrics[POOL_DROP_EXISTING_HIGHER_PRIORITY] =
            self.drop_existing_higher_priority.load(Ordering::SeqCst);
        metrics[POOL_DROP_REPLACED_BY_HIGHER_PRIORITY] =
            self.drop_replaced_by_higher_priority.load(Ordering::SeqCst);
        metrics[POOL_DROP_POOL_FULL] = self.drop_pool_full.load(Ordering::SeqCst);
        metrics[POOL_DROP_POOL_NOT_READY] = self.drop_pool_not_ready.load(Ordering::SeqCst);
        metrics[POOL_DROP_INTERNAL_STATE_BACKEND_ERROR] = self
            .drop_internal_state_backend_error
            .load(Ordering::SeqCst);
        metrics[POOL_DROP_INTERNAL_NOT_READY] = self.drop_internal_not_ready.load(Ordering::SeqCst);

        metrics[POOL_CREATE_PROPOSAL] = self.create_proposal.load(Ordering::SeqCst);
        metrics[POOL_CREATE_PROPOSAL_TXS] = self.create_proposal_txs.load(Ordering::SeqCst);
        metrics[POOL_CREATE_PROPOSAL_TRACKED_ADDRESSES] = self
            .create_proposal_tracked_addresses
            .load(Ordering::SeqCst);
        metrics[POOL_CREATE_PROPOSAL_AVAILABLE_ADDRESSES] = self
            .create_proposal_available_addresses
            .load(Ordering::SeqCst);
        metrics[POOL_CREATE_PROPOSAL_BACKEND_LOOKUPS] =
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
        metrics[TRACKED_ADDRESSES] = self.addresses.load(Ordering::SeqCst);
        metrics[TRACKED_TXS] = self.txs.load(Ordering::SeqCst);
        metrics[TRACKED_EVICT_EXPIRED_ADDRESSES] =
            self.evict_expired_addresses.load(Ordering::SeqCst);
        metrics[TRACKED_EVICT_EXPIRED_TXS] = self.evict_expired_txs.load(Ordering::SeqCst);
        metrics[TRACKED_REMOVE_COMMITTED_ADDRESSES] =
            self.remove_committed_addresses.load(Ordering::SeqCst);
        metrics[TRACKED_REMOVE_COMMITTED_TXS] = self.remove_committed_txs.load(Ordering::SeqCst);
    }
}
