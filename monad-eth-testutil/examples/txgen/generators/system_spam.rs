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

use alloy_consensus::{SignableTransaction, TxEip1559, TxEnvelope};
use alloy_primitives::{hex, Address, Bytes, TxKind, B256, U256};
use alloy_signer::SignerSync;
use alloy_signer_local::LocalSigner;

use crate::{config::SystemCallType, prelude::*};

const SYSTEM_SENDER_PRIV_KEY: B256 = B256::new(hex!(
    "b0358e6d701a955d9926676f227e40172763296b317ff554e49cdf2c2c35f8a7"
));

// Staking contract address and function selectors
const STAKING_CONTRACT_ADDRESS: Address =
    Address::new(hex!("0x0000000000000000000000000000000000001000"));
const REWARD_FUNCTION_SELECTOR: [u8; 4] = hex!("00000064");
const SNAPSHOT_FUNCTION_SELECTOR: [u8; 4] = hex!("00000065");
const EPOCH_CHANGE_FUNCTION_SELECTOR: [u8; 4] = hex!("00000066");

pub struct SystemTransactionSpamGenerator {
    pub(crate) recipient_keys: KeyPool,
    pub(crate) tx_per_sender: usize,
    pub(crate) system_nonce: u64,
    pub(crate) call_type: SystemCallType,
}

impl Generator for SystemTransactionSpamGenerator {
    fn handle_acct_group(
        &mut self,
        _accts: &mut [SimpleAccount],
        ctx: &GenCtx,
    ) -> Vec<(TxEnvelope, Address)> {
        let mut txs = Vec::with_capacity(self.tx_per_sender);
        let system_signer = LocalSigner::from_bytes(&SYSTEM_SENDER_PRIV_KEY).unwrap();

        for i in 0..self.tx_per_sender {
            let recipient = self.recipient_keys.next_addr();

            let (input, value) = match self.call_type {
                SystemCallType::Reward => {
                    let mut input_data = [0_u8; 56];
                    let reward_amount = U256::from(1_000_000_000_000_000_000_i64);
                    input_data[0..4].copy_from_slice(&REWARD_FUNCTION_SELECTOR);
                    input_data[4..24].copy_from_slice(recipient.as_slice());
                    input_data[24..56].copy_from_slice(&reward_amount.to_be_bytes::<32>());
                    (Bytes::from(input_data), reward_amount)
                }
                SystemCallType::Snapshot => (Bytes::from(SNAPSHOT_FUNCTION_SELECTOR), U256::ZERO),
                SystemCallType::EpochChange => {
                    let mut input_data = [0_u8; 12];
                    input_data[0..4].copy_from_slice(&EPOCH_CHANGE_FUNCTION_SELECTOR);
                    let epoch = (i as u64) + 1;
                    input_data[4..12].copy_from_slice(&epoch.to_be_bytes());
                    (Bytes::from(input_data), U256::ZERO)
                }
            };

            let tx = TxEip1559 {
                chain_id: ctx.chain_id,
                nonce: self.system_nonce + u64::try_from(i).unwrap_or(0),
                gas_limit: ctx.set_tx_gas_limit.unwrap_or(0), // 0 default for system txs, override with --set-tx-gas-limit
                max_fee_per_gas: 0,
                max_priority_fee_per_gas: ctx.priority_fee.unwrap_or(0) as u128, // 0 default for system txs, override with --priority-fee
                to: TxKind::Call(STAKING_CONTRACT_ADDRESS),
                value,
                access_list: Default::default(),
                input,
            };

            let signature_hash = tx.signature_hash();
            let signature = system_signer.sign_hash_sync(&signature_hash).unwrap();
            let signed_tx = TxEnvelope::Eip1559(tx.into_signed(signature));

            txs.push((signed_tx, recipient));
        }

        self.system_nonce += u64::try_from(self.tx_per_sender).unwrap_or(0);

        txs
    }
}
