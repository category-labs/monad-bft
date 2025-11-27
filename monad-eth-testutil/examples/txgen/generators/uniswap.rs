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
use crate::shared::uniswap::Uniswap;

pub struct UniswapGenerator {
    pub uniswap: Uniswap,
    pub tx_per_sender: usize,
}

impl Generator for UniswapGenerator {
    fn handle_acct_group(
        &mut self,
        accts: &mut [SimpleAccount],
        ctx: &GenCtx,
    ) -> Vec<(TxEnvelope, Address, crate::shared::private_key::PrivateKey)> {
        let mut txs = Vec::with_capacity(self.tx_per_sender * accts.len());

        // for each sender:
        // - mint tokens A&B for liquidity
        // - approve tokens A&B for non-fungible position manager
        // - provide liquidity in uniswap pools
        for sender in accts {
            for _ in 0..self.tx_per_sender {
                let tx = self.uniswap.construct_token_mint_tx(
                    sender,
                    self.uniswap.token_a_addr,
                    ctx.base_fee,
                    ctx.chain_id,
                    ctx.set_tx_gas_limit,
                );
                txs.push((
                    tx,
                    self.uniswap.nonfungible_position_manager_addr,
                    sender.key.clone(),
                ));
                sender.nonce += 1;

                let tx = self.uniswap.construct_token_mint_tx(
                    sender,
                    self.uniswap.token_b_addr,
                    ctx.base_fee,
                    ctx.chain_id,
                    ctx.set_tx_gas_limit,
                );
                txs.push((
                    tx,
                    self.uniswap.nonfungible_position_manager_addr,
                    sender.key.clone(),
                ));
                sender.nonce += 1;

                // approval txs
                let tx = self.uniswap.construct_token_approve_tx(
                    sender,
                    self.uniswap.token_a_addr,
                    self.uniswap.nonfungible_position_manager_addr,
                    ctx.base_fee,
                    ctx.chain_id,
                    ctx.set_tx_gas_limit,
                );
                txs.push((
                    tx,
                    self.uniswap.nonfungible_position_manager_addr,
                    sender.key.clone(),
                ));
                sender.nonce += 1;

                let tx = self.uniswap.construct_token_approve_tx(
                    sender,
                    self.uniswap.token_b_addr,
                    self.uniswap.nonfungible_position_manager_addr,
                    ctx.base_fee,
                    ctx.chain_id,
                    ctx.set_tx_gas_limit,
                );
                txs.push((
                    tx,
                    self.uniswap.nonfungible_position_manager_addr,
                    sender.key.clone(),
                ));
                sender.nonce += 1;

                // provide liquidity tx
                let tx = self.uniswap.construct_add_liquidity_tx(
                    sender,
                    self.uniswap.nonfungible_position_manager_addr,
                    self.uniswap.token_a_addr,
                    self.uniswap.token_b_addr,
                    ctx.base_fee,
                    ctx.chain_id,
                    ctx.set_tx_gas_limit,
                );
                txs.push((
                    tx,
                    self.uniswap.nonfungible_position_manager_addr,
                    sender.key.clone(),
                ));
                sender.nonce += 1;
            }
        }

        txs
    }
}
