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

use alloy_primitives::Bytes;

use super::native_transfer_with_params;
use crate::{prelude::*, shared::erc20::ERC20};

pub struct ExtremeValuesGenerator {
    pub recipient_keys: KeyPool,
    pub tx_per_sender: usize,
    pub erc20_pool: Option<Vec<ERC20>>,
    pub erc20_index: usize,
    pub current_combination: usize,
    pub combinations: Vec<ExtremeValueCombination>,
}

#[derive(Debug, Clone)]
pub struct ExtremeValueCombination {
    pub nonce: Option<u64>,
    pub gas_limit: u64,
    pub max_fee_per_gas: u128,
    pub max_priority_fee_per_gas: u128,
    pub value: U256,
    pub input_data: Bytes,
}

impl ExtremeValuesGenerator {
    pub fn new(
        recipient_keys: KeyPool,
        tx_per_sender: usize,
        erc20_pool: Option<Vec<ERC20>>,
    ) -> Self {
        let combinations = Self::generate_extreme_combinations();
        Self {
            recipient_keys,
            tx_per_sender,
            erc20_pool,
            erc20_index: 0,
            current_combination: 0,
            combinations,
        }
    }

    fn generate_extreme_combinations() -> Vec<ExtremeValueCombination> {
        let nonces = vec![
            None,           // Valid nonce
            Some(0),        // Minimum nonce
            Some(u64::MAX), // Maximum nonce
        ];

        let gas_limits = vec![0, 21_000, 30_000_000, 150_000_000, 200_000_000, 300_000_000];

        let max_fee_per_gas_values = vec![
            0,
            1,
            1_000_000_000,
            10_000_000_000,
            100_000_000_000,
            u128::MAX,
        ];

        let max_priority_fee_per_gas_values = vec![0, 1, 1_000_000_000, 10_000_000_000, u128::MAX];

        let values = vec![
            U256::ZERO,
            U256::from(1),
            U256::from(10_u128.pow(18)),
            U256::from(10_u128.pow(30)),
            U256::MAX,
        ];

        let input_data_options = vec![
            Bytes::new(),
            Bytes::from(vec![0x00; 32]),
            Bytes::from(vec![0xFF; 100]),
            Bytes::from(vec![0xAA; 1000]),
            Bytes::from(vec![0x55; 10000]),
            Bytes::from(vec![0xCC; 128 * 1024]),
            Bytes::from(vec![0xBB; 256 * 1024]),
        ];

        let mut combinations = Vec::new();

        for nonce in &nonces {
            for &gas_limit in &gas_limits {
                for &max_fee_per_gas in &max_fee_per_gas_values {
                    for &max_priority_fee_per_gas in &max_priority_fee_per_gas_values {
                        for &value in &values {
                            for input_data in &input_data_options {
                                combinations.push(ExtremeValueCombination {
                                    nonce: *nonce,
                                    gas_limit,
                                    max_fee_per_gas,
                                    max_priority_fee_per_gas,
                                    value,
                                    input_data: input_data.clone(),
                                });
                            }
                        }
                    }
                }
            }
        }

        combinations
    }
}

impl Generator for ExtremeValuesGenerator {
    fn handle_acct_group(
        &mut self,
        accts: &mut [SimpleAccount],
        ctx: &GenCtx,
    ) -> Vec<(TxEnvelope, Address, crate::shared::private_key::PrivateKey)> {
        let mut txs = Vec::with_capacity(self.tx_per_sender * accts.len());

        for sender in accts {
            for _ in 0..(self.tx_per_sender / 2) {
                let combination = &self.combinations[self.current_combination];
                self.current_combination = (self.current_combination + 1) % self.combinations.len();

                let to = self.recipient_keys.next_addr();

                let native_tx = self.create_native_transaction(sender, to, combination, ctx);
                let erc20_pool = self
                    .erc20_pool
                    .as_ref()
                    .expect("No ERC20 contract found, but tx_type is erc20");
                let erc20 = erc20_pool[self.erc20_index % erc20_pool.len()];
                self.erc20_index = (self.erc20_index + 1) % erc20_pool.len();
                let erc20_tx = self.create_erc20_transaction(sender, to, combination, &erc20, ctx);

                txs.push((native_tx, to, sender.key.clone()));
                txs.push((erc20_tx, to, sender.key.clone()));
            }
        }

        txs
    }
}

impl ExtremeValuesGenerator {
    fn create_native_transaction(
        &self,
        sender: &mut SimpleAccount,
        to: Address,
        combination: &ExtremeValueCombination,
        ctx: &GenCtx,
    ) -> TxEnvelope {
        native_transfer_with_params(
            sender,
            to,
            combination.value,
            combination.nonce,
            Some(combination.gas_limit),
            Some(combination.max_fee_per_gas),
            Some(combination.max_priority_fee_per_gas),
            Some(combination.input_data.clone()),
            ctx,
        )
    }

    fn create_erc20_transaction(
        &self,
        sender: &mut SimpleAccount,
        to: Address,
        combination: &ExtremeValueCombination,
        erc20: &ERC20,
        ctx: &GenCtx,
    ) -> TxEnvelope {
        let nonce = combination.nonce.unwrap_or(sender.nonce);

        let tx = erc20.construct_transfer(
            &sender.key,
            to,
            nonce,
            combination.value,
            combination.max_fee_per_gas,
            ctx.chain_id,
            Some(combination.gas_limit),
            Some(combination.max_priority_fee_per_gas),
        );

        if combination.nonce.is_none() {
            sender.nonce += 1;
        } else {
            sender.nonce = nonce;
        }
        let gas_cost = U256::from(combination.gas_limit as u128 * combination.max_fee_per_gas);
        sender.native_bal = sender
            .native_bal
            .checked_sub(gas_cost)
            .unwrap_or(U256::ZERO);

        let balance = sender
            .erc20_balances
            .entry(erc20.addr)
            .or_insert(U256::ZERO);
        *balance = balance.checked_sub(combination.value).unwrap_or(U256::ZERO);

        tx
    }
}
