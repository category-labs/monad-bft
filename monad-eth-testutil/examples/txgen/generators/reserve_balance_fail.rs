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
                let gas_limit = ctx.set_tx_gas_limit.unwrap_or(50_000); // 50k default, override with --set-tx-gas-limit

                if sender.native_bal > U256::from(5_000_000_000_000_000_u64) {
                    warn!(
                        "Sender balance too high for reserve_balance_fail: {}\nEnsure min_native_amount and seed_native_amount are set less than 5*10^15",
                        sender.native_bal);
                }

                (0..self.num_fail_txs + 1)
                    .map(|_| {
                        let to = self.recipient_keys.next_addr();

                        let tx = TxEip1559 {
                            chain_id: ctx.chain_id,
                            nonce: sender.nonce,
                            gas_limit,
                            max_fee_per_gas,
                            max_priority_fee_per_gas: ctx.priority_fee.unwrap_or(0), // 0 default, override with --priority-fee
                            to: TxKind::Call(to),
                            value: U256::from(1000),
                            access_list: Default::default(),
                            input: Default::default(),
                        };

                        sender.nonce += 1;

                        let sig = sender.key.sign_transaction(&tx);
                        let tx = TxEnvelope::Eip1559(tx.into_signed(sig));

                        (tx, to)
                    })
                    .collect_vec()
                    .into_iter()
            })
            .collect()
    }
}
