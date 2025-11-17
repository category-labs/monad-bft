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
    pub erc20_pool: Option<Vec<ERC20>>,
    pub erc20_index: usize,
}

impl Generator for HighCallDataTxGenerator {
    fn handle_acct_group(
        &mut self,
        accts: &mut [SimpleAccount],
        ctx: &GenCtx,
    ) -> Vec<(TxEnvelope, Address, crate::shared::private_key::PrivateKey)> {
        let mut txs = Vec::with_capacity(accts.len());

        for sender in accts {
            for _ in 0..self.tx_per_sender {
                let to = self.recipient_keys.next_addr();

                let tx = high_calldata_erc20_call(
                    sender,
                    self.erc20_pool
                        .as_ref()
                        .expect("No ERC20 contract found, but tx_type is erc20"),
                    &mut self.erc20_index,
                    ctx,
                );

                txs.push((tx, to, sender.key.clone()));
            }
        }

        txs
    }
}

pub fn high_calldata_erc20_call(
    from: &mut SimpleAccount,
    erc20_pool: &[ERC20],
    erc20_index: &mut usize,
    ctx: &GenCtx,
) -> TxEnvelope {
    let erc20 = erc20_pool[*erc20_index % erc20_pool.len()];
    *erc20_index = (*erc20_index + 1) % erc20_pool.len();
    let max_fee_per_gas = ctx.base_fee * 2;
    let input = vec![0u8; 1 << 15];
    let tx = crate::shared::erc20::make_tx(
        from.nonce,
        &from.key,
        erc20.addr,
        U256::ZERO,
        input,
        max_fee_per_gas,
        ctx.chain_id,
        ctx.set_tx_gas_limit,
        ctx.priority_fee,
    );

    // update from
    from.nonce += 1;
    from.native_bal = from
        .native_bal
        .checked_sub(U256::from(400_000 * max_fee_per_gas))
        .unwrap_or(U256::ZERO); // todo: wire gas correctly, see above comment
    tx
}
