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

#[cfg(test)]
mod test {
    use std::{
        collections::{BTreeMap, BTreeSet, HashSet},
        time::Duration,
    };

    use alloy_consensus::{Transaction, TxEnvelope};
    use alloy_primitives::{Address, B256};
    use itertools::Itertools;
    use monad_bls::BlsSignatureCollection;
    use monad_chain_config::{
        revision::{ChainParams, MockChainRevision},
        MockChainConfig,
    };
    use monad_consensus_types::{timeout::HighExtend, RoundCertificate};
    use monad_crypto::certificate_signature::CertificateSignaturePubKey;
    use monad_eth_block_policy::EthBlockPolicy;
    use monad_eth_block_validator::EthBlockValidator;
    use monad_eth_ledger::MockEthLedger;
    use monad_eth_testutil::{make_legacy_tx, secret_to_eth_address};
    use monad_eth_types::EthExecutionProtocol;
    use monad_mock_swarm::{
        mock::TimestamperConfig,
        mock_swarm::{Nodes, SwarmBuilder},
        node::NodeBuilder,
        swarm_relation::SwarmRelation,
        terminator::UntilTerminator,
        verifier::{happy_path_tick_by_block, MockSwarmVerifier},
    };
    use monad_router_scheduler::{NoSerRouterConfig, NoSerRouterScheduler, RouterSchedulerBuilder};
    use monad_secp::{PubKey, SecpSignature};
    use monad_state::{MonadMessage, VerifiedMonadMessage};
    use monad_state_backend::{InMemoryBlockState, InMemoryState, InMemoryStateInner};
    use monad_testutil::swarm::{make_state_configs, swarm_ledger_verification};
    use monad_transformer::{
        DropTransformer, GenericTransformer, GenericTransformerPipeline, LatencyTransformer,
        PartitionTransformer, ID,
    };
    use monad_types::{Balance, NodeId, SeqNum, GENESIS_SEQ_NUM};
    use monad_updaters::{
        ledger::MockableLedger, statesync::MockStateSyncExecutor, txpool::MockTxPoolExecutor,
        val_set::MockValSetUpdaterNop,
    };
    use monad_validator::{
        simple_round_robin::SimpleRoundRobin, validator_set::ValidatorSetFactory,
    };
    use tracing::info;

    pub struct EthSwarm;
    impl SwarmRelation for EthSwarm {
        type SignatureType = SecpSignature;
        type SignatureCollectionType = BlsSignatureCollection<PubKey>;
        type ExecutionProtocolType = EthExecutionProtocol;
        type StateBackendType = InMemoryState<Self::SignatureType, Self::SignatureCollectionType>;
        type BlockPolicyType = EthBlockPolicy<
            Self::SignatureType,
            Self::SignatureCollectionType,
            Self::ChainConfigType,
            Self::ChainRevisionType,
        >;
        type ChainConfigType = MockChainConfig;
        type ChainRevisionType = MockChainRevision;

        type TransportMessage = VerifiedMonadMessage<
            Self::SignatureType,
            Self::SignatureCollectionType,
            Self::ExecutionProtocolType,
        >;

        type BlockValidator = EthBlockValidator<Self::SignatureType, Self::SignatureCollectionType>;
        type ValidatorSetTypeFactory =
            ValidatorSetFactory<CertificateSignaturePubKey<Self::SignatureType>>;
        type LeaderElection = SimpleRoundRobin<CertificateSignaturePubKey<Self::SignatureType>>;
        type Ledger = MockEthLedger<Self::SignatureType, Self::SignatureCollectionType>;

        type RouterScheduler = NoSerRouterScheduler<
            CertificateSignaturePubKey<Self::SignatureType>,
            MonadMessage<
                Self::SignatureType,
                Self::SignatureCollectionType,
                Self::ExecutionProtocolType,
            >,
            VerifiedMonadMessage<
                Self::SignatureType,
                Self::SignatureCollectionType,
                Self::ExecutionProtocolType,
            >,
        >;

        type Pipeline = GenericTransformerPipeline<
            CertificateSignaturePubKey<Self::SignatureType>,
            Self::TransportMessage,
        >;

        type ValSetUpdater = MockValSetUpdaterNop<
            Self::SignatureType,
            Self::SignatureCollectionType,
            Self::ExecutionProtocolType,
        >;
        type TxPoolExecutor = MockTxPoolExecutor<
            Self::SignatureType,
            Self::SignatureCollectionType,
            Self::ExecutionProtocolType,
            Self::BlockPolicyType,
            Self::StateBackendType,
            Self::ChainConfigType,
            Self::ChainRevisionType,
        >;
        type StateSyncExecutor = MockStateSyncExecutor<
            Self::SignatureType,
            Self::SignatureCollectionType,
            Self::ExecutionProtocolType,
        >;
    }

    const CONSENSUS_DELTA: Duration = Duration::from_millis(100);
    const BASE_FEE: u128 = monad_tfm::base_fee::MIN_BASE_FEE as u128;
    const GAS_LIMIT: u64 = 30000;
    const ROUND: u64 = 1;

    static CHAIN_PARAMS: ChainParams = ChainParams {
        tx_limit: 10_000,
        proposal_gas_limit: 300_000_000,
        proposal_byte_limit: 4_000_000,
        max_reserve_balance: 1_000_000_000_000_000_000,
        vote_pace: Duration::from_millis(0),
    };

    fn generate_eth_swarm(
        num_nodes: u16,
        existing_accounts: impl IntoIterator<Item = Address>,
    ) -> Nodes<EthSwarm> {
        let epoch_length = SeqNum(2000);
        let execution_delay = SeqNum(4);

        let existing_nonces: BTreeMap<_, _> =
            existing_accounts.into_iter().map(|acc| (acc, 0)).collect();

        let chain_config = MockChainConfig::new(&CHAIN_PARAMS);

        let create_block_policy = || EthBlockPolicy::new(GENESIS_SEQ_NUM, execution_delay.0);

        let state_configs = make_state_configs::<EthSwarm>(
            num_nodes,
            ValidatorSetFactory::default,
            SimpleRoundRobin::default,
            EthBlockValidator::default,
            create_block_policy,
            || {
                InMemoryStateInner::new(
                    Balance::MAX,
                    execution_delay,
                    InMemoryBlockState::genesis(existing_nonces.clone()),
                )
            },
            execution_delay, // execution_delay
            CONSENSUS_DELTA, // delta
            chain_config,    // chain config
            SeqNum(100),     // state_sync_threshold
        );
        let all_peers: BTreeSet<_> = state_configs
            .iter()
            .map(|state_config| NodeId::new(state_config.key.pubkey()))
            .collect();
        let swarm_config = SwarmBuilder::<EthSwarm>(
            state_configs
                .into_iter()
                .enumerate()
                .map(|(seed, state_builder)| {
                    let validators = state_builder.locked_epoch_validators[0].clone();
                    let state_backend = state_builder.state_backend.clone();
                    NodeBuilder::<EthSwarm>::new(
                        ID::new(NodeId::new(state_builder.key.pubkey())),
                        state_builder,
                        NoSerRouterConfig::new(all_peers.clone()).build(),
                        MockValSetUpdaterNop::new(validators.validators, epoch_length),
                        MockTxPoolExecutor::new(create_block_policy(), state_backend.clone())
                            .with_chain_params(&CHAIN_PARAMS),
                        MockEthLedger::new(state_backend.clone()),
                        MockStateSyncExecutor::new(state_backend),
                        vec![GenericTransformer::Latency(LatencyTransformer::new(
                            CONSENSUS_DELTA,
                        ))],
                        vec![],
                        TimestamperConfig::default(),
                        seed.try_into().unwrap(),
                    )
                })
                .collect(),
        );

        swarm_config.build()
    }

    fn verify_transactions_in_ledger(
        swarm: &Nodes<EthSwarm>,
        node_ids: Vec<ID<PubKey>>,
        txns: Vec<TxEnvelope>,
    ) -> bool {
        let txns: HashSet<_> = HashSet::from_iter(txns.iter().map(|t| *t.tx_hash()));
        for node_id in node_ids {
            let state = swarm.states().get(&node_id).unwrap();
            let mut txns_to_see = txns.clone();
            for (round, block) in state.executor.ledger().get_finalized_blocks() {
                for txn in &block.body().execution_body.transactions {
                    let txn_hash = txn.tx_hash();
                    if txns_to_see.contains(txn_hash) {
                        txns_to_see.remove(txn_hash);
                    } else {
                        println!(
                            "Unexpected transaction in block round {}. SeqNum: {}, NodeID: {}, TxnHash: {}, Nonce: {}",
                            round.0, block.get_seq_num().0, node_id, txn_hash, txn.nonce()
                        );
                        return false;
                    }
                }
            }

            if !txns_to_see.is_empty() {
                println!(
                    "Expected transactions don't exist. NodeID: {}, TxnHashes: {:?}",
                    node_id, txns_to_see
                );
                return false;
            }
        }

        true
    }

    #[test]
    fn non_sequential_nonces() {
        let sender_1_key = B256::repeat_byte(15);
        let mut swarm = generate_eth_swarm(2, vec![secret_to_eth_address(sender_1_key)]);
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

            swarm.send_transaction(node_1_id, alloy_rlp::encode(&eth_txn).into());

            expected_txns.push(eth_txn);
        }

        for nonce in 20..30 {
            let eth_txn = make_legacy_tx(sender_1_key, BASE_FEE, GAS_LIMIT, nonce, 10);

            swarm.send_transaction(node_1_id, alloy_rlp::encode(&eth_txn).into());
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
    fn duplicate_nonces_multi_nodes() {
        let sender_1_key = B256::repeat_byte(15);
        let mut swarm = generate_eth_swarm(2, vec![secret_to_eth_address(sender_1_key)]);

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

            swarm.send_transaction(node_1_id, alloy_rlp::encode(&eth_txn).into());

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

            swarm.send_transaction(node_2_id, alloy_rlp::encode(&eth_txn).into());
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
        let mut swarm = generate_eth_swarm(4, vec![secret_to_eth_address(sender_1_key)]);

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
        let mut swarm = generate_eth_swarm(4, vec![secret_to_eth_address(sender_1_key)]);

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

            swarm.send_transaction(node_1_id, alloy_rlp::encode(&eth_txn_sender_1).into());
            swarm.send_transaction(node_1_id, alloy_rlp::encode(&eth_txn_sender_2).into());

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

            swarm.send_transaction(node_2_id, alloy_rlp::encode(&eth_txn_sender_1).into());
            swarm.send_transaction(node_2_id, alloy_rlp::encode(&eth_txn_sender_2).into());
        }

        // Send transactions with nonces 10..20 to Node 2
        for nonce in 10..20 {
            let eth_txn_sender_1 = make_legacy_tx(sender_1_key, BASE_FEE, GAS_LIMIT, nonce, 10);
            let eth_txn_sender_2 = make_legacy_tx(sender_2_key, BASE_FEE, GAS_LIMIT, nonce, 10);

            swarm.send_transaction(node_2_id, alloy_rlp::encode(&eth_txn_sender_1).into());
            swarm.send_transaction(node_2_id, alloy_rlp::encode(&eth_txn_sender_2).into());

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

        let mut swarm = generate_eth_swarm(4, vec![secret_to_eth_address(sender_1_key)]);
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

            swarm.send_transaction(other_nodes[0], alloy_rlp::encode(&eth_txn).into());

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

            swarm.send_transaction(node_1_id, alloy_rlp::encode(&eth_txn).into());

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
}
