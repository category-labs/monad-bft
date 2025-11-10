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
    pub(crate) erc20: ERC20,
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

                if rng.gen_bool(0.3) {
                    txs.push((
                        self.erc20.construct_tx(
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
                        self.erc20.construct_tx(
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
