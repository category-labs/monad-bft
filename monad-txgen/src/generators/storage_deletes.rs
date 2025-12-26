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

use crate::{generators::ERC20Pool, prelude::*, shared::erc20::IERC20};

pub struct StorageDeletesTxGenerator {
    pub recipient_keys: KeyPool,
    pub erc20_pool: ERC20Pool,
    pub tx_per_sender: usize,
}

// note: we need to mint first
impl Generator for StorageDeletesTxGenerator {
    fn handle_acct_group(
        &mut self,
        accts: &mut [SimpleAccount],
        ctx: &GenCtx,
    ) -> Vec<(TxEnvelope, Address, crate::shared::private_key::PrivateKey)> {
        let mut idxs: Vec<usize> = (0..accts.len()).collect();
        let mut rng = SmallRng::from_entropy();
        let mut txs = Vec::with_capacity(self.tx_per_sender * accts.len());

        for _ in 0..(self.tx_per_sender) {
            idxs.shuffle(&mut rng);

            for &idx in &idxs {
                let from = &mut accts[idx];
                let to = self.recipient_keys.next_addr();
                let erc20 = self.erc20_pool.next_contract();

                let tx = if rng.gen_bool(0.3) {
                    erc20.construct_tx(
                        from,
                        IERC20::resetCall { addr: to },
                        ctx.base_fee,
                        ctx.chain_id,
                        ctx.set_tx_gas_limit,
                        ctx.priority_fee,
                    )
                } else {
                    let amount = U256::from(10);
                    let tx = erc20.construct_tx(
                        from,
                        IERC20::transferCall {
                            recipient: to,
                            amount,
                        },
                        ctx.base_fee,
                        ctx.chain_id,
                        ctx.set_tx_gas_limit,
                        ctx.priority_fee,
                    );
                    let balance = from.erc20_balances.entry(erc20.addr).or_insert(U256::ZERO);
                    *balance = balance.checked_sub(amount).unwrap_or(U256::ZERO);
                    tx
                };

                txs.push((tx, to, from.key.clone()));
            }
        }

        txs
    }
}
