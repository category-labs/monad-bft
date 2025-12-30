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

use alloy_consensus::TxEnvelope;
use alloy_primitives::U256;
use alloy_rlp::{RlpDecodable, RlpEncodable};
use monad_eth_txpool_types::DEFAULT_TX_PRIORITY;

#[derive(RlpEncodable, RlpDecodable)]
pub struct EthTxPoolIpcTx {
    pub tx: TxEnvelope,
    pub priority: U256,

    // TODO(andr-dev): Pass extra_data to custom sequencers
    pub extra_data: Vec<u8>,
}

impl EthTxPoolIpcTx {
    pub fn new_with_default_priority(tx: TxEnvelope, extra_data: Vec<u8>) -> Self {
        Self {
            tx,
            priority: DEFAULT_TX_PRIORITY,
            extra_data,
        }
    }
}
