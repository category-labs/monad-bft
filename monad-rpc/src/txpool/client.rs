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

use alloy_primitives::TxHash;
use flume::{Sender, TrySendError};
use monad_eth_types::{AccountKey, EthTxEnvelope, NamespaceTransactionBatch};

use super::{
    state::{EthTxPoolBridgeStateView, TxStatusReceiverSender},
    TxStatus,
};

pub(crate) enum EthTxPoolBridgeSubmission {
    Transaction {
        tx: EthTxEnvelope,
        tx_status_recv_send: TxStatusReceiverSender,
    },
    NamespaceBatch {
        batch: NamespaceTransactionBatch,
        tx_status_recv_sends: Vec<TxStatusReceiverSender>,
    },
}

#[derive(Clone)]
pub struct EthTxPoolBridgeClient {
    tx_sender: Sender<EthTxPoolBridgeSubmission>,
    tx_sender_capacity: usize,

    tx_inflight: Arc<()>,

    state: EthTxPoolBridgeStateView,
}

impl EthTxPoolBridgeClient {
    pub(super) fn new(
        tx_sender: Sender<EthTxPoolBridgeSubmission>,
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

    pub(crate) fn try_send(
        &self,
        tx: EthTxEnvelope,
        tx_status_recv_send: TxStatusReceiverSender,
    ) -> Result<(), TrySendError<EthTxPoolBridgeSubmission>> {
        self.tx_sender
            .try_send(EthTxPoolBridgeSubmission::Transaction {
                tx,
                tx_status_recv_send,
            })
    }

    pub(crate) fn try_send_batch(
        &self,
        batch: NamespaceTransactionBatch,
        tx_status_recv_sends: Vec<TxStatusReceiverSender>,
    ) -> Result<(), TrySendError<EthTxPoolBridgeSubmission>> {
        self.tx_sender
            .try_send(EthTxPoolBridgeSubmission::NamespaceBatch {
                batch,
                tx_status_recv_sends,
            })
    }

    pub fn get_status_by_hash(&self, hash: &TxHash) -> Option<TxStatus> {
        self.state.get_status_by_hash(hash)
    }

    pub fn get_status_by_address(
        &self,
        account_key: &AccountKey,
    ) -> Option<HashMap<TxHash, TxStatus>> {
        self.state.get_status_by_address(account_key)
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
