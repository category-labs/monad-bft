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

use super::{erc20_transfer, native_transfer_priority_fee};
use crate::{generators::GenTxType, prelude::*};

pub struct DuplicateTxGenerator {
    pub(crate) recipient_keys: KeyPool,
    pub(crate) tx_per_sender: usize,
    pub random_priority_fee: bool,
    pub tx_type: GenTxType,
}

impl Generator for DuplicateTxGenerator {
    fn handle_acct_group(
        &mut self,
        accts: &mut [SimpleAccount],
        ctx: &GenCtx,
    ) -> Vec<(TxEnvelope, Address, crate::shared::private_key::PrivateKey)> {
        let mut rng = SmallRng::from_entropy();
        let mut txs = Vec::with_capacity(self.tx_per_sender * accts.len());

        for sender in accts {
            let to = self.recipient_keys.next_addr(); // change sampling strategy?
            for _ in 0..self.tx_per_sender {
                let priority_fee = if self.random_priority_fee {
                    let (min, max) = ctx.random_priority_fee_range.unwrap_or((0, 1000));
                    rng.gen_range(min..=max)
                } else {
                    0
                };
                let tx = match &self.tx_type {
                    GenTxType::ERC20(pool) => {
                        erc20_transfer(sender, to, U256::from(10), pool.next_contract(), ctx)
                    }
                    GenTxType::Native => {
                        native_transfer_priority_fee(sender, to, U256::from(10), priority_fee, ctx)
                    }
                };
                txs.push((tx, to, sender.key.clone()));
            }
        }

        txs
    }
}
