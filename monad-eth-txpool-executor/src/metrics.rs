use monad_eth_txpool::EthTxPoolMetrics;
use monad_executor::ExecutorMetrics;
use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct EthTxPoolExecutorMetrics {
    pub reject_forwarded_invalid_bytes: u64,
    pub reject_forwarded_invalid_signer: u64,

    pub create_proposal: u64,
    pub create_proposal_elapsed_ns: u64,

    pub pool: EthTxPoolMetrics,
}

impl EthTxPoolExecutorMetrics {
    pub fn update(&self, metrics: &mut ExecutorMetrics) {
        metrics["monad.bft.txpool.reject_forwarded_invalid_bytes"] =
            self.reject_forwarded_invalid_bytes;
        metrics["monad.bft.txpool.reject_forwarded_invalid_signer"] =
            self.reject_forwarded_invalid_signer;

        metrics["monad.bft.txpool.create_proposal"] = self.create_proposal;
        metrics["monad.bft.txpool.create_proposal_elapsed_ns"] = self.create_proposal_elapsed_ns;

        self.pool.update(metrics);
    }
}
