use monad_executor::ExecutorMetrics;
use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct EthTxPoolMetrics {
    pub insert_owned_txs: u64,
    pub insert_forwarded_txs: u64,

    pub drop_not_well_formed: u64,
    pub drop_invalid_signature: u64,
    pub drop_nonce_too_low: u64,
    pub drop_fee_too_low: u64,
    pub drop_insufficient_balance: u64,
    pub drop_existing_higher_priority: u64,
    pub drop_pool_full: u64,
    pub drop_pool_not_ready: u64,
    pub drop_internal_state_backend_error: u64,

    pub create_proposal: u64,
    pub create_proposal_txs: u64,
    pub create_proposal_tracked_addresses: u64,
    pub create_proposal_available_addresses: u64,
    pub create_proposal_backend_lookups: u64,

    pub pending: EthTxpoolPendingMetrics,
    pub tracked: EthTxPoolTrackedMetrics,
}

impl EthTxPoolMetrics {
    pub fn update(&self, metrics: &mut ExecutorMetrics) {
        metrics["monad.bft.txpool.pool.insert_owned_txs"] = self.insert_owned_txs;
        metrics["monad.bft.txpool.pool.insert_forwarded_txs"] = self.insert_forwarded_txs;

        metrics["monad.bft.txpool.pool.drop_not_well_formed"] = self.drop_not_well_formed;
        metrics["monad.bft.txpool.pool.drop_nonce_too_low"] = self.drop_nonce_too_low;
        metrics["monad.bft.txpool.pool.drop_fee_too_low"] = self.drop_fee_too_low;
        metrics["monad.bft.txpool.pool.drop_insufficient_balance"] = self.drop_insufficient_balance;
        metrics["monad.bft.txpool.pool.drop_existing_higher_priority"] =
            self.drop_existing_higher_priority;
        metrics["monad.bft.txpool.pool.drop_pool_full"] = self.drop_pool_full;
        metrics["monad.bft.txpool.pool.drop_pool_not_ready"] = self.drop_pool_not_ready;
        metrics["monad.bft.txpool.pool.drop_internal_state_backend_error"] =
            self.drop_internal_state_backend_error;

        metrics["monad.bft.txpool.pool.create_proposal"] = self.create_proposal;
        metrics["monad.bft.txpool.pool.create_proposal_txs"] = self.create_proposal_txs;
        metrics["monad.bft.txpool.pool.create_proposal_tracked_addresses"] =
            self.create_proposal_tracked_addresses;
        metrics["monad.bft.txpool.pool.create_proposal_available_addresses"] =
            self.create_proposal_available_addresses;
        metrics["monad.bft.txpool.pool.create_proposal_backend_lookups"] =
            self.create_proposal_backend_lookups;

        self.pending.update(metrics);
        self.tracked.update(metrics);
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct EthTxpoolPendingMetrics {
    pub addresses: u64,
    pub txs: u64,
    pub promote_addresses: u64,
    pub promote_txs: u64,
    pub drop_unknown_addresses: u64,
    pub drop_unknown_txs: u64,
    pub drop_low_nonce_addresses: u64,
    pub drop_low_nonce_txs: u64,
}

impl EthTxpoolPendingMetrics {
    pub fn update(&self, metrics: &mut ExecutorMetrics) {
        metrics["monad.bft.txpool.pool.pending.addresses"] = self.addresses;
        metrics["monad.bft.txpool.pool.pending.txs"] = self.txs;
        metrics["monad.bft.txpool.pool.pending.promote_addresses"] = self.promote_addresses;
        metrics["monad.bft.txpool.pool.pending.promote_txs"] = self.promote_txs;
        metrics["monad.bft.txpool.pool.pending.drop_unknown_addresses"] =
            self.drop_unknown_addresses;
        metrics["monad.bft.txpool.pool.pending.drop_unknown_txs"] = self.drop_unknown_txs;
        metrics["monad.bft.txpool.pool.pending.drop_low_nonce_addresses"] =
            self.drop_low_nonce_addresses;
        metrics["monad.bft.txpool.pool.pending.drop_low_nonce_txs"] = self.drop_low_nonce_txs;
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct EthTxPoolTrackedMetrics {
    pub addresses: u64,
    pub txs: u64,
    pub evict_expired_addresses: u64,
    pub evict_expired_txs: u64,
    pub remove_committed_addresses: u64,
    pub remove_committed_txs: u64,
}

impl EthTxPoolTrackedMetrics {
    pub fn update(&self, metrics: &mut ExecutorMetrics) {
        metrics["monad.bft.txpool.pool.tracked.addresses"] = self.addresses;
        metrics["monad.bft.txpool.pool.tracked.txs"] = self.txs;
        metrics["monad.bft.txpool.pool.tracked.evict_expired_addresses"] =
            self.evict_expired_addresses;
        metrics["monad.bft.txpool.pool.tracked.evict_expired_txs"] = self.evict_expired_txs;
        metrics["monad.bft.txpool.pool.tracked.remove_committed_addresses"] =
            self.remove_committed_addresses;
        metrics["monad.bft.txpool.pool.tracked.remove_committed_txs"] = self.remove_committed_txs;
    }
}
