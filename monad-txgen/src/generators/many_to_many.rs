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

use super::{erc20_transfer, native_transfer};
use crate::{generators::GenTxType, prelude::*};

pub struct ManyToManyGenerator {
    pub recipient_keys: KeyPool,
    pub tx_per_sender: usize,
    pub tx_type: GenTxType,
}

impl Generator for ManyToManyGenerator {
    fn handle_acct_group(
        &mut self,
        accts: &mut [SimpleAccount],
        ctx: &GenCtx,
    ) -> Vec<(TxEnvelope, Address, crate::shared::private_key::PrivateKey)> {
        let mut idxs: Vec<usize> = (0..accts.len()).collect();
        let mut rng = SmallRng::from_entropy();
        let mut txs = Vec::with_capacity(self.tx_per_sender * accts.len());

        for _ in 0..self.tx_per_sender {
            idxs.shuffle(&mut rng);

            for &idx in &idxs {
                let sender = &mut accts[idx];
                let to = self.recipient_keys.next_addr(); // change sampling strategy?

                let tx = match &self.tx_type {
                    GenTxType::ERC20(pool) => {
                        erc20_transfer(sender, to, U256::from(10), pool.next_contract(), ctx)
                    }
                    GenTxType::Native => native_transfer(sender, to, U256::from(10), ctx),
                };

                txs.push((tx, to, sender.key.clone()));
            }
        }

        txs
    }
}
