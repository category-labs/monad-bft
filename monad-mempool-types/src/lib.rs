use std::time::SystemTime;

use reth_primitives::TransactionSignedEcRecovered;

pub mod convert;

pub struct EthTxBatch {
    pub txs: Vec<TransactionSignedEcRecovered>,
    pub time: SystemTime,
}

impl EthTxBatch {
    pub fn new(txs: Vec<TransactionSignedEcRecovered>) -> Self {
        Self {
            txs,
            time: SystemTime::now(),
        }
    }
}

// #[derive(RefCast)]
#[repr(transparent)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MonadMempoolMessage();
