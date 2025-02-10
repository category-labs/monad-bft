use std::path::PathBuf;

pub struct EthTxPoolIpcConfig {
    pub bind_path: PathBuf,
    /// Number of txs per batch
    pub tx_batch_size: usize,
    /// Max number of batches to queue
    pub max_queued_batches: usize,
    /// Warn if number of queued batches exceeds this
    pub queued_batches_watermark: usize,
}
