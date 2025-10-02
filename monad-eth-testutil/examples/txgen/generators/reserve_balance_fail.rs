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

use super::*;

pub struct ReserveBalanceFailGenerator {
    pub recipient_keys: KeyPool,
    pub num_fail_txs: usize,
}

impl Generator for ReserveBalanceFailGenerator {
    fn handle_acct_group(
        &mut self,
        accts: &mut [SimpleAccount],
        ctx: &GenCtx,
    ) -> Vec<(TxEnvelope, Address)> {
        accts
            .iter_mut()
            .flat_map(|sender| {
                let max_fee_per_gas = ctx.base_fee * 2;
                let gas_limit = 50_000;

                let max_allowed_balance = U256::from(5_000_000_000_000_000_u64);
                let mut transactions = Vec::new();

                if sender.native_bal > max_allowed_balance {
                    let single_tx_cost =
                        U256::from(gas_limit) * U256::from(max_fee_per_gas) + U256::from(1000);
                    let target_balance = single_tx_cost;
                    let drain_amount = if sender.native_bal > target_balance {
                        sender.native_bal - target_balance
                    } else {
                        U256::from(0)
                    };

                    if drain_amount > U256::from(0) {
                        let drain_to = self.recipient_keys.next_addr();

                        let drain_tx = TxEip1559 {
                            chain_id: ctx.chain_id,
                            nonce: sender.nonce,
                            gas_limit,
                            max_fee_per_gas,
                            max_priority_fee_per_gas: 0,
                            to: TxKind::Call(drain_to),
                            value: drain_amount,
                            access_list: Default::default(),
                            input: Default::default(),
                        };

                        sender.nonce += 1;
                        sender.native_bal -= drain_amount;

                        let sig = sender.key.sign_transaction(&drain_tx);
                        let drain_tx = TxEnvelope::Eip1559(drain_tx.into_signed(sig));

                        transactions.push((drain_tx, drain_to));
                    }
                }

                for _ in 0..self.num_fail_txs + 1 {
                    let to = self.recipient_keys.next_addr();

                    let tx = TxEip1559 {
                        chain_id: ctx.chain_id,
                        nonce: sender.nonce,
                        gas_limit,
                        max_fee_per_gas,
                        max_priority_fee_per_gas: 0,
                        to: TxKind::Call(to),
                        value: U256::from(1000),
                        access_list: Default::default(),
                        input: Default::default(),
                    };

                    sender.nonce += 1;

                    let sig = sender.key.sign_transaction(&tx);
                    let tx = TxEnvelope::Eip1559(tx.into_signed(sig));

                    transactions.push((tx, to));
                }

                transactions.into_iter()
            })
            .collect()
    }
}
