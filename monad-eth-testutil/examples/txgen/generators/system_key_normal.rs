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
use alloy_primitives::{hex, Address, TxKind, B256};
use alloy_signer::SignerSync;
use alloy_signer_local::LocalSigner;

use crate::prelude::*;

const SYSTEM_SENDER_PRIV_KEY: B256 = B256::new(hex!(
    "b0358e6d701a955d9926676f227e40172763296b317ff554e49cdf2c2c35f8a7"
));

pub struct SystemKeyNormalTxGenerator {
    pub(crate) recipient_keys: KeyPool,
    pub(crate) tx_per_sender: usize,
    pub(crate) system_nonce: u64,
    pub(crate) random_priority_fee: bool,
}

impl Generator for SystemKeyNormalTxGenerator {
    fn handle_acct_group(
        &mut self,
        _accts: &mut [SimpleAccount],
        ctx: &GenCtx,
    ) -> Vec<(TxEnvelope, Address)> {
        let mut rng = SmallRng::from_entropy();
        let mut txs = Vec::with_capacity(self.tx_per_sender);
        let system_signer = LocalSigner::from_bytes(&SYSTEM_SENDER_PRIV_KEY).unwrap();

        for i in 0..self.tx_per_sender {
            let to = self.recipient_keys.next_addr();
            let priority_fee = if self.random_priority_fee {
                let (min, max) = ctx.random_priority_fee_range.unwrap_or((0, 1000));
                rng.gen_range(min..=max)
            } else {
                ctx.priority_fee.unwrap_or(0)
            };

            let tx = TxEip1559 {
                chain_id: ctx.chain_id,
                nonce: self.system_nonce + u64::try_from(i).unwrap_or(0),
                gas_limit: ctx.set_tx_gas_limit.unwrap_or(0), // 0 default for system txs, override with --set-tx-gas-limit
                max_fee_per_gas: 0,
                max_priority_fee_per_gas: priority_fee as u128,
                to: TxKind::Call(to),
                value: U256::from(10),
                access_list: Default::default(),
                input: Default::default(),
            };

            let signature_hash = tx.signature_hash();
            let signature = system_signer.sign_hash_sync(&signature_hash).unwrap();
            let signed_tx = TxEnvelope::Eip1559(tx.into_signed(signature));

            txs.push((signed_tx, to));
        }

        self.system_nonce += u64::try_from(self.tx_per_sender).unwrap_or(0);

        txs
    }
}
