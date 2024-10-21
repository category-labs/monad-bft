mod common;

#[cfg(test)]
mod test {
    use std::{
        collections::{BTreeMap, BTreeSet, HashSet},
        time::Duration,
    };

    use alloy_rlp::Decodable;
    use itertools::Itertools;
    use monad_async_state_verify::{majority_threshold, PeerAsyncStateVerify};
    use monad_consensus_types::payload::{StateRoot, TransactionPayload};
    use monad_crypto::{
        certificate_signature::{CertificateKeyPair, CertificateSignaturePubKey},
        NopPubKey, NopSignature,
    };
    use monad_eth_block_policy::EthBlockPolicy;
    use monad_eth_block_validator::EthValidator;
    use monad_eth_ledger::MockEthLedger;
    use monad_eth_testutil::{make_tx, secret_to_eth_address};
    use monad_eth_tx::EthSignedTransaction;
    use monad_eth_txpool::EthTxPool;
    use monad_eth_types::{Balance, EthAddress};
    use monad_mock_swarm::{
        mock::TimestamperConfig,
        mock_swarm::{Nodes, SwarmBuilder},
        node::NodeBuilder,
        swarm_relation::SwarmRelation,
        terminator::UntilTerminator,
        verifier::{happy_path_tick_by_block, MockSwarmVerifier},
    };
    use monad_multi_sig::MultiSig;
    use monad_router_scheduler::{NoSerRouterConfig, NoSerRouterScheduler, RouterSchedulerBuilder};
    use monad_state::{MonadMessage, VerifiedMonadMessage};
    use monad_state_backend::{InMemoryBlockState, InMemoryState, InMemoryStateInner};
    use monad_testutil::swarm::{make_state_configs, swarm_ledger_verification};
    use monad_transformer::{
        DropTransformer, GenericTransformer, GenericTransformerPipeline, LatencyTransformer,
        PartitionTransformer, ID,
    };
    use monad_types::{NodeId, Round, SeqNum, GENESIS_SEQ_NUM};
    use monad_updaters::{
        ledger::MockableLedger, staked_discovery::MockStakedDiscovery,
        state_root_hash::MockStateRootHashNop, statesync::MockStateSyncExecutor,
    };
    use monad_validator::{
        simple_round_robin::SimpleRoundRobin,
        validator_set::{ValidatorSetFactory, ValidatorSetTypeFactory},
    };
    use reth_primitives::B256;
    use tracing::info;

    pub struct EthSwarm;
    impl SwarmRelation for EthSwarm {
        type SignatureType = NopSignature;
        type SignatureCollectionType = MultiSig<Self::SignatureType>;
        type StateBackendType = InMemoryState;
        type BlockPolicyType = EthBlockPolicy;

        type TransportMessage =
            VerifiedMonadMessage<Self::SignatureType, Self::SignatureCollectionType>;

        type BlockValidator = EthValidator;
        type StateRootValidator = StateRoot;
        type ValidatorSetTypeFactory =
            ValidatorSetFactory<CertificateSignaturePubKey<Self::SignatureType>>;
        type LeaderElection = SimpleRoundRobin<CertificateSignaturePubKey<Self::SignatureType>>;
        type TxPool = EthTxPool;
        type Ledger = MockEthLedger<Self::SignatureType, Self::SignatureCollectionType>;
        type AsyncStateRootVerify = PeerAsyncStateVerify<
            Self::SignatureCollectionType,
            <Self::ValidatorSetTypeFactory as ValidatorSetTypeFactory>::ValidatorSetType,
        >;

        type RouterScheduler = NoSerRouterScheduler<
            CertificateSignaturePubKey<Self::SignatureType>,
            MonadMessage<Self::SignatureType, Self::SignatureCollectionType>,
            VerifiedMonadMessage<Self::SignatureType, Self::SignatureCollectionType>,
        >;

        type Pipeline = GenericTransformerPipeline<
            CertificateSignaturePubKey<Self::SignatureType>,
            Self::TransportMessage,
        >;

        type StateRootHashExecutor =
            MockStateRootHashNop<Self::SignatureType, Self::SignatureCollectionType>;
        type StateSyncExecutor =
            MockStateSyncExecutor<Self::SignatureType, Self::SignatureCollectionType>;
        type DiscoveryExecutor =
            MockStakedDiscovery<Self::SignatureType, Self::SignatureCollectionType>;
    }

    const CONSENSUS_DELTA: Duration = Duration::from_millis(100);
    const BASE_FEE: u128 = 1000;
    const GAS_LIMIT: u64 = 30000;

    fn generate_eth_swarm(
        num_nodes: u16,
        existing_accounts: impl IntoIterator<Item = EthAddress>,
    ) -> Nodes<EthSwarm> {
        let execution_delay = SeqNum(4);

        let existing_nonces: BTreeMap<_, _> =
            existing_accounts.into_iter().map(|acc| (acc, 0)).collect();

        let state_configs = make_state_configs::<EthSwarm>(
            num_nodes,
            ValidatorSetFactory::default,
            SimpleRoundRobin::default,
            EthTxPool::default,
            || EthValidator::new(10_000, 1_000_000, 1337),
            || EthBlockPolicy::new(GENESIS_SEQ_NUM, Balance::MAX, execution_delay.0, 1337),
            || {
                InMemoryStateInner::new(
                    Balance::MAX,
                    execution_delay,
                    InMemoryBlockState::genesis(existing_nonces.clone()),
                )
            },
            || StateRoot::new(execution_delay),
            PeerAsyncStateVerify::new,
            CONSENSUS_DELTA,    // delta
            10,                 // proposal_tx_limit
            SeqNum(2000),       // val_set_update_interval
            Round(50),          // epoch_start_delay
            majority_threshold, // state root quorum threshold
            SeqNum(100),        // state_sync_threshold
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
                    let validators = state_builder.forkpoint.validator_sets[0].clone();
                    let state_backend = state_builder.state_backend.clone();
                    NodeBuilder::<EthSwarm>::new(
                        ID::new(NodeId::new(state_builder.key.pubkey())),
                        state_builder,
                        NoSerRouterConfig::new(all_peers.clone()).build(),
                        MockStateRootHashNop::new(validators.validators.clone(), SeqNum(2000)),
                        MockEthLedger::new(state_backend.clone()),
                        MockStateSyncExecutor::new(
                            state_backend,
                            validators
                                .validators
                                .0
                                .into_iter()
                                .map(|v| v.node_id)
                                .collect(),
                        ),
                        MockStakedDiscovery::default(),
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
        node_ids: Vec<ID<NopPubKey>>,
        txns: Vec<EthSignedTransaction>,
    ) -> bool {
        let txns: HashSet<_> = HashSet::from_iter(txns.iter().map(|t| t.hash()));
        for node_id in node_ids {
            let state = swarm.states().get(&node_id).unwrap();
            let mut txns_to_see = txns.clone();
            for (round, block) in state.executor.ledger().get_blocks() {
                let decoded_txns = match &block.payload.txns {
                    TransactionPayload::List(rlp) => {
                        Vec::<EthSignedTransaction>::decode(&mut rlp.as_ref()).unwrap()
                    }
                    TransactionPayload::Null => Vec::new(),
                };

                let decoded_txn_hashes: HashSet<_> =
                    HashSet::from_iter(decoded_txns.iter().map(|t| t.hash()));
                for txn_hash in decoded_txn_hashes {
                    if txns_to_see.contains(&txn_hash) {
                        txns_to_see.remove(&txn_hash);
                    } else {
                        println!(
                            "Unexpected transaction in block round {}. NodeID: {}, TxnHash: {}",
                            round.0, node_id, txn_hash
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
        let sender_1_key = B256::random();
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
            let eth_txn = make_tx(sender_1_key, BASE_FEE, GAS_LIMIT, nonce, 10);

            swarm.send_transaction(node_1_id, eth_txn.envelope_encoded().into());

            expected_txns.push(eth_txn);
        }

        for nonce in 20..30 {
            let eth_txn = make_tx(sender_1_key, BASE_FEE, GAS_LIMIT, nonce, 10);

            swarm.send_transaction(node_1_id, eth_txn.envelope_encoded().into());
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
        let sender_1_key = B256::random();
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
            let eth_txn = make_tx(sender_1_key, BASE_FEE, GAS_LIMIT, nonce, 10);

            swarm.send_transaction(node_1_id, eth_txn.envelope_encoded().into());

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
            let eth_txn = make_tx(sender_1_key, BASE_FEE, GAS_LIMIT, nonce, 1000);

            swarm.send_transaction(node_2_id, eth_txn.envelope_encoded().into());
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
    fn committed_nonces() {
        let sender_1_key = B256::random();
        let sender_2_key = B256::random();
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
            let eth_txn_sender_1 = make_tx(sender_1_key, BASE_FEE, GAS_LIMIT, nonce, 10);
            let eth_txn_sender_2 = make_tx(sender_2_key, BASE_FEE, GAS_LIMIT, nonce, 10);

            swarm.send_transaction(node_1_id, eth_txn_sender_1.envelope_encoded().into());
            swarm.send_transaction(node_1_id, eth_txn_sender_2.envelope_encoded().into());

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
            let eth_txn_sender_1 = make_tx(sender_1_key, BASE_FEE, GAS_LIMIT, nonce, 10);
            let eth_txn_sender_2 = make_tx(sender_2_key, BASE_FEE, GAS_LIMIT, nonce, 10);

            swarm.send_transaction(node_2_id, eth_txn_sender_1.envelope_encoded().into());
            swarm.send_transaction(node_2_id, eth_txn_sender_2.envelope_encoded().into());
        }

        // Send transactions with nonces 10..20 to Node 2
        for nonce in 10..20 {
            let eth_txn_sender_1 = make_tx(sender_1_key, BASE_FEE, GAS_LIMIT, nonce, 10);
            let eth_txn_sender_2 = make_tx(sender_2_key, BASE_FEE, GAS_LIMIT, nonce, 10);

            swarm.send_transaction(node_2_id, eth_txn_sender_1.envelope_encoded().into());
            swarm.send_transaction(node_2_id, eth_txn_sender_2.envelope_encoded().into());

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
        let sender_1_key = B256::random();

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
            let eth_txn = make_tx(sender_1_key, BASE_FEE, GAS_LIMIT, nonce, 10);

            swarm.send_transaction(other_nodes[0], eth_txn.envelope_encoded().into());

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
            let eth_txn = make_tx(sender_1_key, BASE_FEE, GAS_LIMIT, nonce, 10);

            swarm.send_transaction(node_1_id, eth_txn.envelope_encoded().into());

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
