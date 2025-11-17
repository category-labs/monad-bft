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

use crate::{
    prelude::*,
    shared::erc20::{ERC20, IERC20},
};

pub struct NonDeterministicStorageTxGenerator {
    pub(crate) recipient_keys: KeyPool,
    pub(crate) erc20_pool: Option<Vec<ERC20>>,
    pub(crate) erc20_index: usize,
    pub(crate) tx_per_sender: usize,
}

// note: we need to mint first
impl Generator for NonDeterministicStorageTxGenerator {
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

                let erc20_pool = self
                    .erc20_pool
                    .as_ref()
                    .expect("No ERC20 contract found, but tx_type is erc20");
                let erc20 = erc20_pool[self.erc20_index % erc20_pool.len()];
                self.erc20_index = (self.erc20_index + 1) % erc20_pool.len();

                if rng.gen_bool(0.3) {
                    txs.push((
                        erc20.construct_tx(
                            from,
                            IERC20::transferToFriendsCall {
                                amount: U256::from(10),
                            },
                            ctx.base_fee,
                            ctx.chain_id,
                            ctx.set_tx_gas_limit,
                            ctx.priority_fee,
                        ),
                        from.addr,
                        from.key.clone(),
                    ));
                } else {
                    let to = self.recipient_keys.next_addr(); // change sampling strategy?
                    txs.push((
                        erc20.construct_tx(
                            from,
                            IERC20::addFriendCall { friend: to },
                            ctx.base_fee,
                            ctx.chain_id,
                            ctx.set_tx_gas_limit,
                            ctx.priority_fee,
                        ),
                        to,
                        from.key.clone(),
                    ));
                };
            }
        }

        txs
    }
}
