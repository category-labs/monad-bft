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

use itertools::Itertools;

use super::*;

pub struct ReserveBalanceGenerator {
    pub recipient_keys: KeyPool,
    pub num_drain_txs: usize,
}

impl Generator for ReserveBalanceGenerator {
    fn handle_acct_group(
        &mut self,
        accts: &mut [SimpleAccount],
        ctx: &GenCtx,
    ) -> Vec<(TxEnvelope, Address, crate::shared::private_key::PrivateKey)> {
        accts
            .iter_mut()
            .flat_map(|sender| {
                let num_txs = if sender.native_bal < U256::from(1_000_000_000_000_000_000u64) {
                    0
                } else {
                    self.num_drain_txs
                };

                let max_fee_per_gas = ctx.base_fee * 2;
                let gas_limit = ctx.set_tx_gas_limit.unwrap_or(21_000);

                let total_gas_cost = (num_txs.checked_mul(gas_limit as usize).unwrap() as u128)
                    .checked_mul(max_fee_per_gas)
                    .unwrap();

                let drain_per_tx = sender
                    .native_bal
                    .checked_sub(U256::from(total_gas_cost))
                    .unwrap_or_default()
                    .checked_div(U256::from(self.num_drain_txs as u64))
                    .unwrap();

                (0..num_txs)
                    .map(|_| {
                        let to = self.recipient_keys.next_addr();

                        let tx = TxEip1559 {
                            chain_id: ctx.chain_id,
                            nonce: sender.nonce,
                            gas_limit,
                            max_fee_per_gas,
                            max_priority_fee_per_gas: ctx.priority_fee.unwrap_or(0),
                            to: TxKind::Call(to),
                            value: drain_per_tx,
                            access_list: Default::default(),
                            input: Default::default(),
                        };

                        let new_native_bal = sender
                            .native_bal
                            .checked_sub(
                                drain_per_tx + U256::from(gas_limit as u128 * max_fee_per_gas),
                            )
                            .unwrap();

                        sender.nonce += 1;
                        sender.native_bal = new_native_bal;

                        let sig = sender.key.sign_transaction(&tx);
                        let tx = TxEnvelope::Eip1559(tx.into_signed(sig));

                        (tx, to, sender.key.clone())
                    })
                    .collect_vec()
                    .into_iter()
            })
            .collect()
    }
}
