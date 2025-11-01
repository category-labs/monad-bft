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

use std::{collections::HashMap, sync::Arc};

use alloy_consensus::TxEnvelope;
use alloy_primitives::{Address, TxHash};
use flume::{Sender, TrySendError};

use super::{
    state::{EthTxPoolBridgeStateView, TxStatusSender},
    TxStatus,
};

#[derive(Clone)]
pub struct EthTxPoolBridgeClient {
    tx_sender: Sender<(TxEnvelope, TxStatusSender)>,
    tx_sender_capacity: usize,

    tx_inflight: Arc<()>,

    state: EthTxPoolBridgeStateView,
}

impl EthTxPoolBridgeClient {
    pub(super) fn new(
        tx_sender: Sender<(TxEnvelope, TxStatusSender)>,
        state: EthTxPoolBridgeStateView,
    ) -> Self {
        let tx_sender_capacity = tx_sender
            .capacity()
            .expect("EthTxPoolBridgeClient uses bounded channel");

        Self {
            tx_sender,
            tx_sender_capacity,

            tx_inflight: Arc::new(()),

            state,
        }
    }

    pub fn acquire_tx_inflight_guard(&self) -> Option<Arc<()>> {
        let tx_inflight_guard = self.tx_inflight.clone();

        if Arc::strong_count(&tx_inflight_guard) > self.tx_sender_capacity {
            return None;
        }

        Some(tx_inflight_guard)
    }

    pub fn try_send(
        &self,
        tx: TxEnvelope,
        tx_status_send: TxStatusSender,
    ) -> Result<(), TrySendError<(TxEnvelope, TxStatusSender)>> {
        self.tx_sender.try_send((tx, tx_status_send))
    }

    pub fn get_status_by_hash(&self, hash: &TxHash) -> Option<TxStatus> {
        self.state.get_status_by_hash(hash)
    }

    pub fn get_status_by_address(&self, address: &Address) -> Option<HashMap<TxHash, TxStatus>> {
        self.state.get_status_by_address(address)
    }

    pub fn for_testing() -> Self {
        let (tx_sender, _) = flume::bounded(0);

        Self {
            tx_sender,
            tx_sender_capacity: 0,

            tx_inflight: Arc::new(()),

            state: EthTxPoolBridgeStateView::for_testing(),
        }
    }
}
