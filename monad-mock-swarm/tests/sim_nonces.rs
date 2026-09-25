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

//! Nonce / transaction-commitment coverage, ported onto the `monad-sim`
//! harness (mirrors the legacy `nonces.rs`, which it does not replace).
//!
//! NOT ported here (tracked for follow-up):
//! - `test_forkpoint_serde_roundtrip`: per-step `high_certificate` RLP/JSON/TOML
//!   round-trip — serialization coverage, independent of the engine; extract to
//!   a standalone serialization test before the legacy engine is deleted.

mod eth_swarm_common;

#[cfg(test)]
mod test {
    use alloy_eips::eip2718::Encodable2718;
    use alloy_primitives::B256;
    use monad_eth_testutil::{
        make_eip7702_tx, make_legacy_tx, make_signed_authorization, secret_to_eth_address,
    };
    use monad_mock_swarm::{
        sim::{Network, SimNodeBuilder, SimSwarm},
        sim_verify::SimVerifier,
    };
    use monad_sim::{time::secs, Time};
    use monad_updaters::txpool::ByzantineConfig;
    use rand::Rng;
    use rand_chacha::{rand_core::SeedableRng, ChaChaRng};
    use seq_macro::seq;

    use crate::eth_swarm_common::{
        eth_swarm_config, verify_transactions_in_sim, EthSwarm, BASE_FEE, CONSENSUS_DELTA,
        GAS_LIMIT,
    };

    fn deadline() -> Time {
        Time(0) + secs(120)
    }

    /// Build an Eth `SimSwarm` over a reliable `CONSENSUS_DELTA` link and run it
    /// past initial sync (block 1) so nodes are ready to receive transactions.
    fn eth_sim(builders: Vec<SimNodeBuilder<EthSwarm>>) -> SimSwarm<EthSwarm> {
        let mut swarm =
            SimSwarm::from_builders(0, builders, |_| Network::reliable(CONSENSUS_DELTA));
        assert!(swarm.run_until_blocks(1, deadline()), "no initial progress");
        swarm
    }

    #[test]
    fn non_sequential_nonces() {
        let sender = B256::repeat_byte(15);
        let builders = eth_swarm_config(2, vec![secret_to_eth_address(sender)], |_| {
            ByzantineConfig::default()
        });
        let mut swarm = eth_sim(builders);
        let node_1 = swarm.node_ids()[0];

        let mut expected = Vec::new();
        for nonce in 0..10 {
            let tx = make_legacy_tx(sender, BASE_FEE, GAS_LIMIT, nonce, 10);
            swarm.send_transaction(node_1, tx.encoded_2718().into());
            expected.push(tx);
        }
        // a gap (20..30): these can never commit and must not appear
        for nonce in 20..30 {
            let tx = make_legacy_tx(sender, BASE_FEE, GAS_LIMIT, nonce, 10);
            swarm.send_transaction(node_1, tx.encoded_2718().into());
        }

        assert!(
            swarm.run_until_blocks(5, deadline()),
            "stalled before block 5"
        );

        let ids = swarm.node_ids();
        SimVerifier::new()
            .ledger_min_len(2)
            .metrics_happy_path(&ids, &swarm)
            .assert(&swarm);
        assert!(verify_transactions_in_sim(&swarm, expected));
        swarm.assert_agreement();
    }

    #[test]
    fn sanity_7702() {
        let sender_1 = B256::repeat_byte(0xA);
        let sender_2 = B256::repeat_byte(0xB);
        let builders = eth_swarm_config(
            2,
            vec![
                secret_to_eth_address(sender_1),
                secret_to_eth_address(sender_2),
            ],
            |_| ByzantineConfig::default(),
        );
        let mut swarm = eth_sim(builders);
        let node_1 = swarm.node_ids()[0];

        let mut expected = Vec::new();
        let txn1 = make_legacy_tx(sender_1, BASE_FEE, GAS_LIMIT, 0, 10);
        swarm.send_transaction(node_1, txn1.encoded_2718().into());
        expected.push(txn1);

        let auth_list = vec![
            make_signed_authorization(sender_2, secret_to_eth_address(B256::repeat_byte(0x1)), 0),
            make_signed_authorization(sender_2, secret_to_eth_address(B256::repeat_byte(0x3)), 5),
        ];
        let txn2 = make_eip7702_tx(sender_1, BASE_FEE, 1, 1_000_000, 1, auth_list, 0);
        swarm.send_transaction(node_1, txn2.encoded_2718().into());
        expected.push(txn2);

        let txn3 = make_legacy_tx(sender_2, BASE_FEE, GAS_LIMIT, 1, 10);
        swarm.send_transaction(node_1, txn3.encoded_2718().into());
        expected.push(txn3);

        assert!(
            swarm.run_until_blocks(10, deadline()),
            "stalled before block 10"
        );

        let ids = swarm.node_ids();
        SimVerifier::new()
            .ledger_min_len(2)
            .metrics_happy_path(&ids, &swarm)
            .assert(&swarm);
        assert!(verify_transactions_in_sim(&swarm, expected));
        swarm.assert_agreement();
    }

    seq!(N in 0..512 {
        #[test]
        fn test_rand_nonces_7702_~N() {
            rand_nonces_7702(N);
        }
    });

    fn rand_nonces_7702(seed: u64) {
        let test_sender = B256::repeat_byte(0xA);
        let sender_7702 = B256::repeat_byte(0xB);
        let builders = eth_swarm_config(
            2,
            vec![
                secret_to_eth_address(test_sender),
                secret_to_eth_address(sender_7702),
            ],
            |_| ByzantineConfig::default(),
        );
        let mut swarm = eth_sim(builders);
        let node_1 = swarm.node_ids()[0];

        let mut rng = ChaChaRng::seed_from_u64(seed);
        let nonces: Vec<u64> = (0..10).map(|_| rng.gen_range(1..=10)).collect();
        let mut auths = Vec::new();
        for nonce in nonces {
            if rng.gen_bool(0.5) {
                // a legacy tx with a (likely non-sequential) nonce — dropped by
                // the pool, exercises validation under random nonces
                swarm.send_transaction(
                    node_1,
                    make_legacy_tx(test_sender, BASE_FEE, GAS_LIMIT, nonce, 10)
                        .encoded_2718()
                        .into(),
                );
            } else {
                auths.push(make_signed_authorization(
                    test_sender,
                    secret_to_eth_address(B256::repeat_byte(0x1)),
                    nonce,
                ));
            }
        }

        let txn1 = make_legacy_tx(test_sender, BASE_FEE, GAS_LIMIT, 0, 10);
        swarm.send_transaction(node_1, txn1.encoded_2718().into());
        let txn2 = make_eip7702_tx(sender_7702, BASE_FEE, 1, 1_000_000, 1, auths, 0);
        swarm.send_transaction(node_1, txn2.encoded_2718().into());

        assert!(
            swarm.run_until_blocks(10, deadline()),
            "seed {seed}: stalled"
        );

        let ids = swarm.node_ids();
        SimVerifier::new()
            .ledger_min_len(2)
            .metrics_happy_path(&ids, &swarm)
            .assert(&swarm);
        swarm.assert_agreement();
    }

    #[test]
    fn duplicate_nonces_multi_nodes() {
        let sender = B256::repeat_byte(15);
        let builders = eth_swarm_config(2, vec![secret_to_eth_address(sender)], |_| {
            ByzantineConfig::default()
        });
        let mut swarm = eth_sim(builders);
        let node_1 = swarm.node_ids()[0];
        let node_2 = swarm.node_ids()[1];

        let mut expected = Vec::new();
        for nonce in 0..10 {
            let tx = make_legacy_tx(sender, BASE_FEE, GAS_LIMIT, nonce, 10);
            swarm.send_transaction(node_1, tx.encoded_2718().into());
            expected.push(tx);
        }
        assert!(
            swarm.run_until_blocks(5, deadline()),
            "stalled before block 5"
        );
        assert!(verify_transactions_in_sim(&swarm, expected.clone()));

        // duplicate nonces (different value) to node 2 — must not commit
        for nonce in 0..10 {
            let tx = make_legacy_tx(sender, BASE_FEE, GAS_LIMIT, nonce, 1000);
            swarm.send_transaction(node_2, tx.encoded_2718().into());
        }
        assert!(
            swarm.run_until_blocks(8, deadline()),
            "stalled before block 8"
        );

        let ids = swarm.node_ids();
        SimVerifier::new()
            .ledger_min_len(8)
            .metrics_happy_path(&ids, &swarm)
            .assert(&swarm);
        // still only the first 10
        assert!(verify_transactions_in_sim(&swarm, expected));
        swarm.assert_agreement();
    }

    #[test]
    fn committed_nonces() {
        let sender_1 = B256::repeat_byte(15);
        let sender_2 = B256::repeat_byte(16);
        let builders = eth_swarm_config(
            2,
            vec![
                secret_to_eth_address(sender_1),
                secret_to_eth_address(sender_2),
            ],
            |_| ByzantineConfig::default(),
        );
        let mut swarm = eth_sim(builders);
        let node_1 = swarm.node_ids()[0];
        let node_2 = swarm.node_ids()[1];

        let mut expected = Vec::new();
        for nonce in 0..10 {
            let tx1 = make_legacy_tx(sender_1, BASE_FEE, GAS_LIMIT, nonce, 10);
            let tx2 = make_legacy_tx(sender_2, BASE_FEE, GAS_LIMIT, nonce, 10);
            swarm.send_transaction(node_1, tx1.encoded_2718().into());
            swarm.send_transaction(node_1, tx2.encoded_2718().into());
            expected.push(tx1);
            expected.push(tx2);
        }
        assert!(
            swarm.run_until_blocks(10, deadline()),
            "stalled before block 10"
        );

        let ids = swarm.node_ids();
        SimVerifier::new()
            .ledger_min_len(8)
            .metrics_happy_path(&ids, &swarm)
            .assert(&swarm);
        assert!(verify_transactions_in_sim(&swarm, expected.clone()));

        // already-committed nonces (5..10) to node 2 must not re-commit
        for nonce in 5..10 {
            swarm.send_transaction(
                node_2,
                make_legacy_tx(sender_1, BASE_FEE, GAS_LIMIT, nonce, 10)
                    .encoded_2718()
                    .into(),
            );
            swarm.send_transaction(
                node_2,
                make_legacy_tx(sender_2, BASE_FEE, GAS_LIMIT, nonce, 10)
                    .encoded_2718()
                    .into(),
            );
        }
        // fresh nonces (10..20) should commit
        for nonce in 10..20 {
            let tx1 = make_legacy_tx(sender_1, BASE_FEE, GAS_LIMIT, nonce, 10);
            let tx2 = make_legacy_tx(sender_2, BASE_FEE, GAS_LIMIT, nonce, 10);
            swarm.send_transaction(node_2, tx1.encoded_2718().into());
            swarm.send_transaction(node_2, tx2.encoded_2718().into());
            expected.push(tx1);
            expected.push(tx2);
        }
        assert!(
            swarm.run_until_blocks(20, deadline()),
            "stalled before block 20"
        );

        SimVerifier::new()
            .ledger_min_len(18)
            .metrics_happy_path(&ids, &swarm)
            .assert(&swarm);
        assert!(verify_transactions_in_sim(&swarm, expected));
        swarm.assert_agreement();
    }

    #[test]
    fn test_nec() {
        let sender = B256::repeat_byte(15);
        let builders = eth_swarm_config(4, vec![secret_to_eth_address(sender)], |_| {
            ByzantineConfig::default()
        });
        // The round-robin "second node" (2nd by sorted id) proposes in the first
        // `delay` blocks; corrupt its state root so its block 4 fails validation
        // (it produces an equivocating tip). `builders` is in key-gen order, so
        // select by id to match the legacy `states().iter().nth(1)`.
        let mut by_id: Vec<usize> = (0..builders.len()).collect();
        by_id.sort_by_key(|&i| builders[i].id);
        let bad_idx = by_id[1];
        let bad_id = builders[bad_idx].id;
        builders[bad_idx]
            .state_builder
            .state_read
            .lock()
            .unwrap()
            .extra_data = 1;

        let mut swarm =
            SimSwarm::from_builders(0, builders, |_| Network::reliable(CONSENSUS_DELTA));
        // Run the good nodes well past block 10 (leader/follower commit lag means
        // stopping the instant one node hits 10 leaves laggards behind). The bad
        // node is permanently stuck, so its ledger length and the NEC count don't
        // grow while the good nodes keep progressing.
        assert!(
            swarm.run_until_any_blocks(15, deadline()),
            "good nodes did not progress"
        );

        let ids = swarm.node_ids();
        // exactly one NEC is constructed (for the first block the bad node makes)
        let max_nec = ids
            .iter()
            .map(|&id| swarm.with_node(id, |n| n.metrics().consensus_events.created_nec.get()))
            .max()
            .unwrap();
        assert_eq!(max_nec, 1, "expected exactly one NEC");

        for &id in &ids {
            let len = swarm.with_node(id, |n| n.finalized_blocks());
            if id == bad_id {
                // bad node can't validate block 4, so block 3 is never committed
                assert_eq!(len, 2, "bad node ledger length");
            } else {
                assert!(len >= 10, "good node {id} only has {len} blocks");
            }
        }
    }

    #[test]
    fn non_sequential_seqnum() {
        for byz_idx in 0..4 {
            let builders = eth_swarm_config(4, Vec::new(), move |idx| {
                if idx == byz_idx {
                    ByzantineConfig {
                        no_increment_seq_num: true,
                        ..Default::default()
                    }
                } else {
                    ByzantineConfig::default()
                }
            });
            let mut swarm =
                SimSwarm::from_builders(0, builders, |_| Network::reliable(CONSENSUS_DELTA));

            // some node reaches block 20; every node should be at >= 18
            assert!(
                swarm.run_until_any_blocks(20, deadline()),
                "byz_idx {byz_idx}: did not reach block 20"
            );
            SimVerifier::new().ledger_min_len(18).assert(&swarm);
        }
    }

    #[test]
    fn blocksync_missing_nonces() {
        let sender = B256::repeat_byte(15);
        let builders = eth_swarm_config(4, vec![secret_to_eth_address(sender)], |_| {
            ByzantineConfig::default()
        });
        // Blackout the first node during [2s, 7s): the window starts *after*
        // every node is post-statesync, so the blacked-out node is initialized
        // (and will hold txns it's sent), then can only catch up via blocksync
        // once the partition heals.
        let mut swarm = SimSwarm::from_builders(0, builders, move |peers| {
            let first = peers[0];
            Network::reliable(CONSENSUS_DELTA)
                .partition((Time(0) + secs(2))..(Time(0) + secs(7)), [vec![first]])
        });
        let ids = swarm.node_ids();
        let node_1 = ids[0];
        let node_2 = ids[1];

        // post-statesync: every node reaches block 1 before the blackout begins
        swarm.run_until(Time(0) + secs(2));

        // txns 0..10 to a connected node — the blacked-out node misses them
        let mut expected = Vec::new();
        for nonce in 0..10 {
            let tx = make_legacy_tx(sender, BASE_FEE, GAS_LIMIT, nonce, 10);
            swarm.send_transaction(node_2, tx.encoded_2718().into());
            expected.push(tx);
        }
        swarm.run_until(Time(0) + secs(5));
        // the blacked-out node has fallen behind the connected supermajority
        assert!(
            swarm.with_node(node_1, |n| n.finalized_blocks())
                < swarm.with_node(node_2, |n| n.finalized_blocks()),
            "blacked-out node should fall behind"
        );

        // txns 10..20 to the blacked-out node; it proposes them once it catches up
        for nonce in 10..20 {
            let tx = make_legacy_tx(sender, BASE_FEE, GAS_LIMIT, nonce, 10);
            swarm.send_transaction(node_1, tx.encoded_2718().into());
            expected.push(tx);
        }
        // partition heals at 7s; run well past it so the recovered node
        // blocksyncs, becomes leader, and gets its own txns committed everywhere.
        swarm.run_until(Time(0) + secs(30));
        assert!(verify_transactions_in_sim(&swarm, expected));
        swarm.assert_agreement();
    }
}
