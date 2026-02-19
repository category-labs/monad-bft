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
    use std::collections::HashSet;

    use alloy_eips::eip2718::Encodable2718;
    use alloy_primitives::B256;
    use itertools::Itertools;
    use monad_consensus_types::{timeout::HighExtend, RoundCertificate};
    use monad_eth_testutil::{
        make_eip7702_tx, make_legacy_tx, make_signed_authorization, secret_to_eth_address,
    };
    use monad_mock_swarm::{
        terminator::UntilTerminator,
        verifier::{happy_path_tick_by_block, MockSwarmVerifier},
    };
    use monad_testutil::swarm::swarm_ledger_verification;
    use monad_transformer::{
        DropTransformer, GenericTransformer, LatencyTransformer, PartitionTransformer,
    };
    use monad_updaters::{ledger::MockableLedger, txpool::ByzantineConfig};
    use rand::Rng;
    use rand_chacha::{rand_core::SeedableRng, ChaChaRng};
    use seq_macro::seq;
    use tracing::info;

    use crate::eth_swarm_common::{
        generate_eth_swarm, verify_transactions_in_ledger, BASE_FEE, CONSENSUS_DELTA, GAS_LIMIT,
    };

    #[test]
    fn non_sequential_nonces() {
        let sender_1_key = B256::repeat_byte(15);
        let mut swarm = generate_eth_swarm(2, vec![secret_to_eth_address(sender_1_key)], |_| {
            ByzantineConfig::default()
        });
        let node_ids = swarm.states().keys().copied().collect_vec();
        let node_1_id = node_ids[0];

        // step until nodes are ready to receive txs (post statesync)
        while swarm
            .step_until(&mut UntilTerminator::new().until_block(1))
            .is_some()
        {}

        let mut expected_txns = Vec::new();
        for nonce in 0..10 {
            let eth_txn = make_legacy_tx(sender_1_key, BASE_FEE, GAS_LIMIT, nonce, 10);

            swarm.send_transaction(node_1_id, eth_txn.encoded_2718().into());

            expected_txns.push(eth_txn);
        }

        for nonce in 20..30 {
            let eth_txn = make_legacy_tx(sender_1_key, BASE_FEE, GAS_LIMIT, nonce, 10);

            swarm.send_transaction(node_1_id, eth_txn.encoded_2718().into());
        }

        while swarm
            .step_until(&mut UntilTerminator::new().until_block(5))
            .is_some()
        {}

        let mut verifier = MockSwarmVerifier::default().tick_range(
            happy_path_tick_by_block(5, CONSENSUS_DELTA),
            CONSENSUS_DELTA,
        );
        verifier.metrics_happy_path(&node_ids, &swarm);
        assert!(verifier.verify(&swarm));

        assert!(verify_transactions_in_ledger(
            &swarm,
            swarm.states().keys().cloned().collect_vec(),
            expected_txns
        ));

        swarm_ledger_verification(&swarm, 2);
    }

    #[test]
    fn sanity_7702() {
        let sender_1_key = B256::repeat_byte(0xA);
        let sender_2_key = B256::repeat_byte(0xBu8);
        let mut swarm = generate_eth_swarm(
            2,
            vec![
                secret_to_eth_address(sender_1_key),
                secret_to_eth_address(sender_2_key),
            ],
            |_| ByzantineConfig::default(),
        );
        let node_ids = swarm.states().keys().copied().collect_vec();
        let node_1_id = node_ids[0];

        // step until nodes are ready to receive txs (post statesync)
        while swarm
            .step_until(&mut UntilTerminator::new().until_block(1))
            .is_some()
        {}

        let mut expected_txns = Vec::new();
        let txn1 = make_legacy_tx(sender_1_key, BASE_FEE, GAS_LIMIT, 0, 10);
        swarm.send_transaction(node_1_id, txn1.encoded_2718().into());
        expected_txns.push(txn1);

        let auth_list = vec![
            make_signed_authorization(
                sender_2_key,
                secret_to_eth_address(B256::repeat_byte(0x1u8)),
                0,
            ),
            make_signed_authorization(
                sender_2_key,
                secret_to_eth_address(B256::repeat_byte(0x3u8)),
                5,
            ),
        ];
        let txn2 = make_eip7702_tx(sender_1_key, BASE_FEE, 1, 1_000_000, 1, auth_list, 0);
        swarm.send_transaction(node_1_id, txn2.encoded_2718().into());
        expected_txns.push(txn2);

        let txn3 = make_legacy_tx(sender_2_key, BASE_FEE, GAS_LIMIT, 1, 10);
        swarm.send_transaction(node_1_id, txn3.encoded_2718().into());
        expected_txns.push(txn3);

        while swarm
            .step_until(&mut UntilTerminator::new().until_block(10))
            .is_some()
        {}

        let mut verifier = MockSwarmVerifier::default().tick_range(
            happy_path_tick_by_block(10, CONSENSUS_DELTA),
            CONSENSUS_DELTA,
        );
        verifier.metrics_happy_path(&node_ids, &swarm);
        assert!(verifier.verify(&swarm));

        assert!(verify_transactions_in_ledger(
            &swarm,
            swarm.states().keys().cloned().collect_vec(),
            expected_txns
        ));

        swarm_ledger_verification(&swarm, 2);
    }

    seq!(N in 0..512 {
        #[test]
        fn test_rand_nonces_7702_~N() {
            rand_nonces_7702(N);
        }

    });

    fn rand_nonces_7702(seed: u64) {
        let test_sender = B256::repeat_byte(0xA);
        let sender_7702 = B256::repeat_byte(0xBu8);
        let mut swarm = generate_eth_swarm(
            2,
            vec![
                secret_to_eth_address(test_sender),
                secret_to_eth_address(sender_7702),
            ],
            |_| ByzantineConfig::default(),
        );
        let node_ids = swarm.states().keys().copied().collect_vec();
        let node_1_id = node_ids[0];

        // step until nodes are ready to receive txs (post statesync)
        while swarm
            .step_until(&mut UntilTerminator::new().until_block(1))
            .is_some()
        {}

        // make a list of nonces, create txns or auths from those nonces, send
        let mut rng = ChaChaRng::seed_from_u64(seed);

        let nonces: Vec<u64> = (0..10).map(|_| rng.gen_range(1..=10)).collect();
        let mut txns = Vec::new();
        let mut auths = Vec::new();

        for nonce in nonces {
            if rng.gen_bool(0.5) {
                txns.push(make_legacy_tx(test_sender, BASE_FEE, GAS_LIMIT, nonce, 10));
            } else {
                auths.push(make_signed_authorization(
                    test_sender,
                    secret_to_eth_address(B256::repeat_byte(0x1u8)),
                    nonce,
                ));
            }
        }

        let txn1 = make_legacy_tx(test_sender, BASE_FEE, GAS_LIMIT, 0, 10);
        swarm.send_transaction(node_1_id, txn1.encoded_2718().into());

        let txn2 = make_eip7702_tx(sender_7702, BASE_FEE, 1, 1_000_000, 1, auths, 0);
        swarm.send_transaction(node_1_id, txn2.encoded_2718().into());

        while swarm
            .step_until(&mut UntilTerminator::new().until_block(10))
            .is_some()
        {}

        let mut verifier = MockSwarmVerifier::default().tick_range(
            happy_path_tick_by_block(10, CONSENSUS_DELTA),
            CONSENSUS_DELTA,
        );
        verifier.metrics_happy_path(&node_ids, &swarm);
        assert!(verifier.verify(&swarm));

        swarm_ledger_verification(&swarm, 2);
    }

    #[test]
    fn duplicate_nonces_multi_nodes() {
        let sender_1_key = B256::repeat_byte(15);
        let mut swarm = generate_eth_swarm(2, vec![secret_to_eth_address(sender_1_key)], |_| {
            ByzantineConfig::default()
        });

        let node_ids = swarm.states().keys().copied().collect_vec();
        let node_1_id = node_ids[0];
        let node_2_id = node_ids[1];

        // step until nodes are ready to receive txs (post statesync)
        while swarm
            .step_until(&mut UntilTerminator::new().until_block(1))
            .is_some()
        {}

        let mut expected_txns = Vec::new();
        // Send 10 transactions with nonces 0..10 to Node 1. Leader for round 1
        for nonce in 0..10 {
            let eth_txn = make_legacy_tx(sender_1_key, BASE_FEE, GAS_LIMIT, nonce, 10);

            swarm.send_transaction(node_1_id, eth_txn.encoded_2718().into());

            expected_txns.push(eth_txn);
        }

        while swarm
            .step_until(&mut UntilTerminator::new().until_block(5))
            .is_some()
        {}

        // The first 10 transactions should be in the ledger
        assert!(verify_transactions_in_ledger(
            &swarm,
            swarm.states().keys().cloned().collect_vec(),
            expected_txns.clone()
        ));

        // Send 10 different transactions with nonces 0..10 to Node 2
        for nonce in 0..10 {
            let eth_txn = make_legacy_tx(sender_1_key, BASE_FEE, GAS_LIMIT, nonce, 1000);

            swarm.send_transaction(node_2_id, eth_txn.encoded_2718().into());
        }

        while swarm
            .step_until(&mut UntilTerminator::new().until_block(8))
            .is_some()
        {}

        let mut verifier = MockSwarmVerifier::default().tick_range(
            happy_path_tick_by_block(8, CONSENSUS_DELTA),
            CONSENSUS_DELTA,
        );
        verifier.metrics_happy_path(&node_ids, &swarm);
        assert!(verifier.verify(&swarm));

        // Only the first 10 transactions should be in the ledger
        assert!(verify_transactions_in_ledger(
            &swarm,
            swarm.states().keys().cloned().collect_vec(),
            expected_txns
        ));

        swarm_ledger_verification(&swarm, 8);
    }

    #[test]
    fn test_forkpoint_serde_roundtrip() {
        let sender_1_key = B256::repeat_byte(15);
        let mut swarm = generate_eth_swarm(4, vec![secret_to_eth_address(sender_1_key)], |_| {
            ByzantineConfig::default()
        });

        // pick the second node because it proposes in the first `delay` blocks
        let bad_node_idx = 1;

        {
            let (_id, node) = swarm.states().iter().nth(bad_node_idx).unwrap();
            let sbt = node.state.state_backend();
            sbt.lock().unwrap().extra_data = 1;
        }

        let mut seen_tc_with_tip = false;
        while let Some((_, id, _)) = swarm.step_until(&mut UntilTerminator::new().until_block(10)) {
            let node = swarm.states().get(&id).unwrap();
            if let Some(state) = node.state.consensus() {
                let high_certificate = state.get_high_certificate();
                if let RoundCertificate::Tc(tc) = &high_certificate {
                    if let HighExtend::Tip(_) = &tc.high_extend {
                        seen_tc_with_tip = true;
                    }
                }
                let high_certificate_json_ser = serde_json::to_string(high_certificate)
                    .expect("failed to json serialize high_certificate");
                let high_certificate_json_roundtrip =
                    serde_json::from_str(&high_certificate_json_ser)
                        .expect("failed to json deserialize high_certificate");
                assert_eq!(
                    high_certificate, &high_certificate_json_roundtrip,
                    "failed to json roundtrip high_certificate"
                );

                let high_certificate_toml_ser = toml::to_string(high_certificate)
                    .expect("failed to toml serialize high_certificate");
                let high_certificate_toml_roundtrip = toml::from_str(&high_certificate_toml_ser)
                    .expect("failed to toml deserialize high_certificate");
                assert_eq!(
                    high_certificate, &high_certificate_toml_roundtrip,
                    "failed to toml roundtrip high_certificate"
                );
            }
        }
        assert!(
            seen_tc_with_tip,
            "never tested TC roundtrip with HighExtend::Tip"
        );
    }

    #[test]
    fn test_nec() {
        let sender_1_key = B256::repeat_byte(15);
        let mut swarm = generate_eth_swarm(4, vec![secret_to_eth_address(sender_1_key)], |_| {
            ByzantineConfig::default()
        });

        // pick the second node because it proposes in the first `delay` blocks
        let bad_node_idx = 1;

        {
            let (_id, node) = swarm.states().iter().nth(bad_node_idx).unwrap();
            let sbt = node.state.state_backend();
            sbt.lock().unwrap().extra_data = 1;
        }

        while swarm
            .step_until(&mut UntilTerminator::new().until_block(10))
            .is_some()
        {}

        // only 1 NEC is constructed, for the first block the bad node produces
        // after that, the bad node will have fallen behind
        assert_eq!(
            swarm
                .states()
                .values()
                .map(|state| state.state.metrics().consensus_events.created_nec)
                .max()
                .unwrap(),
            1
        );

        let ledger_lens = swarm
            .states()
            .values()
            .map(|x| x.executor.ledger().get_finalized_blocks().len())
            .collect::<Vec<_>>();
        for (i, ledger_len) in ledger_lens.iter().enumerate() {
            if i == bad_node_idx {
                // bad node can't validate block 4, so block 3 is never committed
                assert_eq!(*ledger_len, 2);
            } else {
                assert!(*ledger_len >= 10);
            }
        }
    }

    #[test]
    fn committed_nonces() {
        let sender_1_key = B256::repeat_byte(15);
        let sender_2_key = B256::repeat_byte(16);
        let mut swarm = generate_eth_swarm(
            2,
            vec![
                secret_to_eth_address(sender_1_key),
                secret_to_eth_address(sender_2_key),
            ],
            |_| ByzantineConfig::default(),
        );

        let node_ids = swarm.states().keys().copied().collect_vec();
        let node_1_id = node_ids[0];
        let node_2_id = node_ids[1];

        // step until nodes are ready to receive txs (post statesync)
        while swarm
            .step_until(&mut UntilTerminator::new().until_block(1))
            .is_some()
        {}

        let mut expected_txns = Vec::new();
        // Send transactions with nonces 0..10 to Node 1. Leader for round 1
        for nonce in 0..10 {
            let eth_txn_sender_1 = make_legacy_tx(sender_1_key, BASE_FEE, GAS_LIMIT, nonce, 10);
            let eth_txn_sender_2 = make_legacy_tx(sender_2_key, BASE_FEE, GAS_LIMIT, nonce, 10);

            swarm.send_transaction(node_1_id, eth_txn_sender_1.encoded_2718().into());
            swarm.send_transaction(node_1_id, eth_txn_sender_2.encoded_2718().into());

            expected_txns.push(eth_txn_sender_1);
            expected_txns.push(eth_txn_sender_2);
        }

        while swarm
            .step_until(&mut UntilTerminator::new().until_block(10))
            .is_some()
        {}

        let mut verifier = MockSwarmVerifier::default().tick_range(
            happy_path_tick_by_block(10, CONSENSUS_DELTA),
            CONSENSUS_DELTA,
        );
        verifier.metrics_happy_path(&node_ids, &swarm);
        assert!(verifier.verify(&swarm));

        assert!(verify_transactions_in_ledger(
            &swarm,
            swarm.states().keys().cloned().collect_vec(),
            expected_txns.clone()
        ));

        swarm_ledger_verification(&swarm, 8);

        // After the transactions have been committed, send the next 10 transactions to Node 2

        // Send transactions with nonces 5..10 to Node 2 that shouldn't be in the blocks
        for nonce in 5..10 {
            let eth_txn_sender_1 = make_legacy_tx(sender_1_key, BASE_FEE, GAS_LIMIT, nonce, 10);
            let eth_txn_sender_2 = make_legacy_tx(sender_2_key, BASE_FEE, GAS_LIMIT, nonce, 10);

            swarm.send_transaction(node_2_id, eth_txn_sender_1.encoded_2718().into());
            swarm.send_transaction(node_2_id, eth_txn_sender_2.encoded_2718().into());
        }

        // Send transactions with nonces 10..20 to Node 2
        for nonce in 10..20 {
            let eth_txn_sender_1 = make_legacy_tx(sender_1_key, BASE_FEE, GAS_LIMIT, nonce, 10);
            let eth_txn_sender_2 = make_legacy_tx(sender_2_key, BASE_FEE, GAS_LIMIT, nonce, 10);

            swarm.send_transaction(node_2_id, eth_txn_sender_1.encoded_2718().into());
            swarm.send_transaction(node_2_id, eth_txn_sender_2.encoded_2718().into());

            expected_txns.push(eth_txn_sender_1);
            expected_txns.push(eth_txn_sender_2);
        }

        while swarm
            .step_until(&mut UntilTerminator::new().until_block(20))
            .is_some()
        {}

        let mut verifier = MockSwarmVerifier::default().tick_range(
            happy_path_tick_by_block(20, CONSENSUS_DELTA),
            CONSENSUS_DELTA,
        );
        verifier.metrics_happy_path(&node_ids, &swarm);
        assert!(verifier.verify(&swarm));

        assert!(verify_transactions_in_ledger(
            &swarm,
            swarm.states().keys().cloned().collect_vec(),
            expected_txns.clone()
        ));

        swarm_ledger_verification(&swarm, 18);
    }

    #[test]
    fn blocksync_missing_nonces() {
        let sender_1_key = B256::repeat_byte(15);

        let mut swarm = generate_eth_swarm(4, vec![secret_to_eth_address(sender_1_key)], |_| {
            ByzantineConfig::default()
        });
        let node_ids = swarm.states().keys().copied().collect_vec();
        let (node_1_id, other_nodes) = node_ids.split_first().unwrap();
        let node_1_id = *node_1_id;
        let other_nodes = other_nodes.to_owned().to_vec();

        // step until nodes are ready to receive txs (post statesync)
        while swarm
            .step_until(&mut UntilTerminator::new().until_block(1))
            .is_some()
        {}

        // blackout node 1 and let other nodes continue
        println!("blackout node: {}", node_1_id);

        let filter_one_node = HashSet::from([node_1_id]);
        let blackout_pipeline = vec![
            GenericTransformer::Latency(LatencyTransformer::new(CONSENSUS_DELTA)),
            GenericTransformer::Partition(PartitionTransformer(filter_one_node)),
            GenericTransformer::Drop(DropTransformer::new()),
        ];
        swarm.update_outbound_pipeline_for_all(blackout_pipeline);

        let mut expected_txns = Vec::new();
        // Send transactions with nonces 0..10 to node 2 so that nodes 2, 3 and 4 can make progress
        for nonce in 0..10 {
            let eth_txn = make_legacy_tx(sender_1_key, BASE_FEE, GAS_LIMIT, nonce, 10);

            swarm.send_transaction(other_nodes[0], eth_txn.encoded_2718().into());

            expected_txns.push(eth_txn);
        }
        info!("node starting with blackout {}", node_1_id);

        while swarm
            .step_until(&mut UntilTerminator::new().until_block(10))
            .is_some()
        {}

        assert!(verify_transactions_in_ledger(
            &swarm,
            other_nodes,
            expected_txns.clone()
        ));
        assert!(verify_transactions_in_ledger(
            &swarm,
            vec![node_1_id],
            vec![]
        ));

        info!(
            id = format!("{}", node_1_id),
            "node restarting metrics {:?}",
            swarm.states().get(&node_1_id).unwrap().state.metrics()
        );

        // remove blackout from node 1
        let regular_pipeline = vec![GenericTransformer::Latency(LatencyTransformer::new(
            CONSENSUS_DELTA,
        ))];
        swarm.update_outbound_pipeline_for_all(regular_pipeline);

        println!("restoring pipeline");

        // Send transactions with nonces 10..20 to node 1 so that it can propose them after it catches up with blocksync
        for nonce in 10..20 {
            let eth_txn = make_legacy_tx(sender_1_key, BASE_FEE, GAS_LIMIT, nonce, 10);

            swarm.send_transaction(node_1_id, eth_txn.encoded_2718().into());

            expected_txns.push(eth_txn);
        }

        // run sufficiently long so that node 1 can catch and propose the transactions in has in its tx pool
        while swarm
            .step_until(&mut UntilTerminator::new().until_block(30))
            .is_some()
        {}

        println!(
            "node {} metrics {:#?}",
            node_1_id,
            swarm.states().get(&node_1_id).unwrap().state.metrics()
        );

        assert!(verify_transactions_in_ledger(
            &swarm,
            swarm.states().keys().cloned().collect_vec(),
            expected_txns
        ));
    }

    #[test]
    fn non_sequential_seqnum() {
        for byz_idx in 0..4 {
            let mut swarm = generate_eth_swarm(4, Vec::new(), |idx| {
                if idx == byz_idx {
                    ByzantineConfig {
                        no_increment_seq_num: true,
                        ..Default::default()
                    }
                } else {
                    ByzantineConfig::default()
                }
            });

            while swarm
                .step_until(&mut UntilTerminator::new().until_block(20))
                .is_some()
            {}

            swarm_ledger_verification(&swarm, 18);
        }
    }
}
