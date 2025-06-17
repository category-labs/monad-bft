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
    pub drop_pool_full: AtomicU64,
    pub drop_pool_not_ready: AtomicU64,
    pub drop_internal_state_backend_error: AtomicU64,
    pub drop_internal_not_ready: AtomicU64,

    pub create_proposal: AtomicU64,
    pub create_proposal_txs: AtomicU64,
    pub create_proposal_tracked_addresses: AtomicU64,
    pub create_proposal_available_addresses: AtomicU64,
    pub create_proposal_backend_lookups: AtomicU64,

    pub pending: EthTxpoolPendingMetrics,
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
        metrics["monad.bft.txpool.pool.drop_nonce_too_low"] =
            self.drop_nonce_too_low.load(Ordering::SeqCst);
        metrics["monad.bft.txpool.pool.drop_fee_too_low"] =
            self.drop_fee_too_low.load(Ordering::SeqCst);
        metrics["monad.bft.txpool.pool.drop_insufficient_balance"] =
            self.drop_insufficient_balance.load(Ordering::SeqCst);
        metrics["monad.bft.txpool.pool.drop_existing_higher_priority"] =
            self.drop_existing_higher_priority.load(Ordering::SeqCst);
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

        self.pending.update(metrics);
        self.tracked.update(metrics);
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct EthTxpoolPendingMetrics {
    pub addresses: AtomicU64,
    pub txs: AtomicU64,
    pub promote_addresses: AtomicU64,
    pub promote_txs: AtomicU64,
    pub drop_unknown_addresses: AtomicU64,
    pub drop_unknown_txs: AtomicU64,
    pub drop_low_nonce_addresses: AtomicU64,
    pub drop_low_nonce_txs: AtomicU64,
}

impl EthTxpoolPendingMetrics {
    pub fn update(&self, metrics: &mut ExecutorMetrics) {
        metrics["monad.bft.txpool.pool.pending.addresses"] = self.addresses.load(Ordering::SeqCst);
        metrics["monad.bft.txpool.pool.pending.txs"] = self.txs.load(Ordering::SeqCst);
        metrics["monad.bft.txpool.pool.pending.promote_addresses"] =
            self.promote_addresses.load(Ordering::SeqCst);
        metrics["monad.bft.txpool.pool.pending.promote_txs"] =
            self.promote_txs.load(Ordering::SeqCst);
        metrics["monad.bft.txpool.pool.pending.drop_unknown_addresses"] =
            self.drop_unknown_addresses.load(Ordering::SeqCst);
        metrics["monad.bft.txpool.pool.pending.drop_unknown_txs"] =
            self.drop_unknown_txs.load(Ordering::SeqCst);
        metrics["monad.bft.txpool.pool.pending.drop_low_nonce_addresses"] =
            self.drop_low_nonce_addresses.load(Ordering::SeqCst);
        metrics["monad.bft.txpool.pool.pending.drop_low_nonce_txs"] =
            self.drop_low_nonce_txs.load(Ordering::SeqCst);
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
