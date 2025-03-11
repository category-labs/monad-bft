use std::sync::atomic::{AtomicU64, Ordering};

use monad_eth_txpool::EthTxPoolMetrics;
use monad_executor::ExecutorMetrics;
use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct EthTxPoolExecutorMetrics {
    pub reject_forwarded_invalid_bytes: AtomicU64,
    pub reject_forwarded_invalid_signer: AtomicU64,

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
        metrics["monad.bft.txpool.reject_forwarded_invalid_signer"] =
            self.reject_forwarded_invalid_signer.load(Ordering::SeqCst);

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
