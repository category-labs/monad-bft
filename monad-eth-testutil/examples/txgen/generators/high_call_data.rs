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

use crate::{prelude::*, shared::erc20::ERC20};

pub struct HighCallDataTxGenerator {
    pub(crate) recipient_keys: KeyPool,
    pub(crate) tx_per_sender: usize,
    pub(crate) gas_limit: u64,
}

impl Generator for HighCallDataTxGenerator {
    fn handle_acct_group(
        &mut self,
        accts: &mut [SimpleAccount],
        ctx: &GenCtx,
    ) -> Vec<(TxEnvelope, Address)> {
        let mut txs = Vec::with_capacity(accts.len());

        for sender in accts {
            for _ in 0..self.tx_per_sender {
                let to = self.recipient_keys.next_addr();

                let tx = ERC20::deploy_tx_with_gas_limit_and_priority(
                    sender.nonce,
                    &sender.key,
                    ctx.base_fee * 2,
                    ctx.chain_id,
                    ctx.set_tx_gas_limit.unwrap_or(self.gas_limit), // use CLI override or generator default
                    ctx.priority_fee.unwrap_or(10), // 10 default, override with --priority-fee
                );
                sender.nonce += 1;

                txs.push((tx, to));
            }
        }

        txs
    }
}
