use std::{path::PathBuf, time::Duration};

#[derive(Clone)]
pub struct EthTxPoolIpcConfig {
    pub bind_path: PathBuf,
    /// Number of txs per batch
    pub tx_batch_size: usize,
    /// Max number of batches to queue
    pub max_queued_batches: usize,
    /// Warn if number of queued batches exceeds this
    pub queued_batches_watermark: usize,
}

pub struct EthTxPoolConfig {
    pub do_local_insert: bool,
    pub soft_tx_expiry: Duration,
    pub hard_tx_expiry: Duration,
    pub proposal_gas_limit: u64,
}
