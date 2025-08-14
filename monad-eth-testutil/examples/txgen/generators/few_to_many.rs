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
use crate::{config::TxType, prelude::*, shared::erc20::ERC20};

pub struct CreateAccountsGenerator {
    pub recipient_keys: KeyPool,
    pub tx_per_sender: usize,
    pub tx_type: TxType,
    pub erc20: Option<ERC20>,
}

impl Generator for CreateAccountsGenerator {
    fn handle_acct_group(
        &mut self,
        accts: &mut [SimpleAccount],
        ctx: &GenCtx,
    ) -> Vec<(TxEnvelope, Address)> {
        let mut txs = Vec::with_capacity(accts.len());

        for sender in accts {
            for _ in 0..self.tx_per_sender {
                let to = self.recipient_keys.next_addr();

                let tx = match self.tx_type {
                    TxType::ERC20 => erc20_transfer(
                        sender,
                        to,
                        U256::from(10),
                        self.erc20
                            .as_ref()
                            .expect("No ERC20 contract found, but tx_type is erc20"),
                        ctx,
                    ),
                    TxType::Native => native_transfer(sender, to, U256::from(10), ctx),
                };

                txs.push((tx, to));
            }
        }

        txs
    }
}
