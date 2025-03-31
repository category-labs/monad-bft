use std::{collections::HashMap, sync::Arc};

use alloy_consensus::TxEnvelope;
use alloy_primitives::{Address, TxHash};
use flume::{Sender, TrySendError};

use super::{state::EthTxPoolBridgeStateView, TxStatus};

#[derive(Clone)]
pub struct EthTxPoolBridgeClient {
    tx_sender: Sender<TxEnvelope>,
    tx_inflight: Arc<()>,

    state: EthTxPoolBridgeStateView,
}

impl EthTxPoolBridgeClient {
    pub(super) fn new(tx_sender: Sender<TxEnvelope>, state: EthTxPoolBridgeStateView) -> Self {
        Self {
            tx_sender,
            tx_inflight: Arc::new(()),

            state,
        }
    }

    pub fn acquire_inflight_tx_guard(&self) -> Arc<()> {
        self.tx_inflight.clone()
    }

    pub fn try_send(&self, tx: TxEnvelope) -> Result<(), TrySendError<TxEnvelope>> {
        self.tx_sender.try_send(tx)
    }

    pub fn get_status_by_hash(&self, hash: &TxHash) -> Option<TxStatus> {
        self.state.get_status_by_hash(hash)
    }

    pub fn get_status_by_address(&self, address: &Address) -> Option<HashMap<TxHash, TxStatus>> {
        self.state.get_status_by_address(address)
    }
}

#[cfg(test)]
impl EthTxPoolBridgeClient {
    pub fn for_testing() -> Self {
        let (tx_sender, _) = flume::bounded(0);

        Self {
            tx_sender,
            tx_inflight: Arc::new(()),

            state: EthTxPoolBridgeStateView::for_testing(),
        }
    }
}
