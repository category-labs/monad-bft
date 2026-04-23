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

mod eth_swarm_common;

#[cfg(test)]
mod test {
    use std::collections::BTreeMap;

    use alloy_eips::eip2718::Encodable2718;
    use alloy_primitives::B256;
    use itertools::Itertools;
    use monad_eth_testutil::{
        make_eip1559_tx_with_value, make_eip7702_tx_with_value, make_legacy_tx_with_value,
        make_signed_authorization, secret_to_eth_address,
    };
    use monad_mock_swarm::{
        terminator::UntilTerminator,
        verifier::{happy_path_tick_by_block, MockSwarmVerifier},
    };
    use monad_state_backend::AccountState;
    use monad_testutil::swarm::swarm_ledger_verification;
    use monad_types::Balance;
    use monad_updaters::txpool::ByzantineConfig;
    use rand::Rng;
    use rand_chacha::{rand_core::SeedableRng, ChaChaRng};
    use seq_macro::seq;

    use crate::eth_swarm_common::{
        generate_eth_swarm_with_accounts, BASE_FEE, CONSENSUS_DELTA, GAS_LIMIT,
    };

    const GAS_COST: u128 = BASE_FEE * GAS_LIMIT as u128;
    const GAS_LIMIT_7702: u64 = 70_000;

    seq!(N in 0..512 {
        #[test]
        fn test_rand_reserve_balance_~N() {
            rand_reserve_balance(N);
        }
    });

    fn rand_reserve_balance(seed: u64) {
        let mut rng = ChaChaRng::seed_from_u64(seed);

        // 2-4 senders, each with a random balance between 0 and 10 * GAS_COSTs
        // balance_multiplier=0 tests insufficient balance path
        let num_senders = rng.gen_range(2..=4u8);
        let sender_keys: Vec<B256> = (0..num_senders)
            .map(|i| B256::repeat_byte(0x40 + i))
            .collect();
        let sender_addrs: Vec<_> = sender_keys
            .iter()
            .map(|k| secret_to_eth_address(*k))
            .collect();

        let existing_accounts: BTreeMap<_, _> = sender_addrs
            .iter()
            .map(|addr| {
                let balance_multiplier = rng.gen_range(0..=10u128);
                (
                    *addr,
                    AccountState::new_with_balance(Balance::from(balance_multiplier * GAS_COST)),
                )
            })
            .collect();

        let mut swarm = generate_eth_swarm_with_accounts(
            2,
            existing_accounts,
            |_| ByzantineConfig::default(),
            true,
        );
        let node_ids = swarm.states().keys().copied().collect_vec();
        let node_1_id = node_ids[0];

        while swarm
            .step_until(&mut UntilTerminator::new().until_block(1))
            .is_some()
        {}

        let mut sender_nonces: Vec<u64> = vec![0; num_senders as usize];
        let mut current_block: usize = 2;

        for (sender_idx, sender_key) in sender_keys.iter().enumerate() {
            let num_txns = rng.gen_range(1..=8u64);

            for _ in 0..num_txns {
                let nonce = sender_nonces[sender_idx];
                // random gas_price: 1x to 3x BASE_FEE
                let gas_price_multiplier = rng.gen_range(1..=3u128);
                let gas_price = BASE_FEE * gas_price_multiplier;
                // random transfer value: 0 to 2 GAS_COSTs
                let value = rng.gen_range(0..=2u128) * GAS_COST;
                // random tx type: legacy, eip1559, or eip7702
                let tx_type = rng.gen_range(0..=2u8);

                let txn = match tx_type {
                    0 => make_legacy_tx_with_value(
                        *sender_key,
                        value,
                        gas_price,
                        GAS_LIMIT,
                        nonce,
                        10,
                    ),
                    1 => make_eip1559_tx_with_value(
                        *sender_key,
                        value,
                        gas_price,
                        0,
                        GAS_LIMIT,
                        nonce,
                        10,
                    ),
                    _ => {
                        let auth_target_idx = (sender_idx + 1) % num_senders as usize;
                        let auth_key = sender_keys[auth_target_idx];
                        let auth_nonce = sender_nonces[auth_target_idx];
                        let auth_list = vec![make_signed_authorization(
                            auth_key,
                            alloy_primitives::Address::repeat_byte(0xDD),
                            auth_nonce,
                        )];
                        sender_nonces[auth_target_idx] += 1;
                        make_eip7702_tx_with_value(
                            *sender_key,
                            value,
                            gas_price,
                            0,
                            GAS_LIMIT_7702,
                            nonce,
                            auth_list,
                            10,
                        )
                    }
                };

                sender_nonces[sender_idx] += 1;
                swarm.send_transaction(node_1_id, txn.encoded_2718().into());

                // randomly advance a block with 10% probability
                if rng.gen_bool(0.1) {
                    current_block += 1;
                    while swarm
                        .step_until(&mut UntilTerminator::new().until_block(current_block))
                        .is_some()
                    {}
                }
            }
        }

        // run consensus block policy to filter invalid transactions
        // and ledger_propose asserts reserve balance invariants on execution
        let total_blocks = current_block + 10;
        while swarm
            .step_until(&mut UntilTerminator::new().until_block(total_blocks))
            .is_some()
        {}

        let mut verifier = MockSwarmVerifier::default().tick_range(
            happy_path_tick_by_block(total_blocks, CONSENSUS_DELTA),
            CONSENSUS_DELTA,
        );
        verifier.metrics_happy_path(&node_ids, &swarm);
        assert!(verifier.verify(&swarm));

        swarm_ledger_verification(&swarm, 2);
    }
}
