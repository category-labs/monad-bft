use std::collections::HashSet;

use alloy_primitives::TxHash;
use monad_eth_txpool_types::EthTxPoolSnapshot;
use tracing::warn;

#[derive(Default)]
pub struct EthTxPoolSnapshotManager {
    pending: HashSet<TxHash>,
    tracked: HashSet<TxHash>,
}

impl EthTxPoolSnapshotManager {
    pub fn add_pending(&mut self, tx_hash: &TxHash) {
        self.pending.insert(tx_hash.to_owned());
    }

    pub fn remove_pending(&mut self, tx_hash: &TxHash) {
        self.pending.remove(tx_hash);
    }

    pub fn add_tracked(&mut self, tx_hash: &TxHash) {
        self.tracked.insert(tx_hash.to_owned());
    }

    pub fn remove_tracked(&mut self, tx_hash: &TxHash) {
        self.tracked.remove(tx_hash);
    }

    pub fn promote(&mut self, tx_hash: &TxHash) {
        if !self.pending.remove(tx_hash) {
            warn!(?tx_hash, "detected promotion of non-existent tx_hash");
        }
        self.tracked.insert(tx_hash.to_owned());
    }

    pub fn generate_snapshot(&self) -> EthTxPoolSnapshot {
        EthTxPoolSnapshot {
            pending: self.pending.clone(),
            tracked: self.tracked.clone(),
        }
    }
}
