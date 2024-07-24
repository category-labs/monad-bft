mod common;

#[cfg(test)]
mod test {
    use std::{
        collections::{BTreeSet, HashSet},
        sync::{Arc, Mutex},
        time::Duration,
    };

    use alloy_rlp::Decodable;
    use itertools::Itertools;
    use monad_async_state_verify::{majority_threshold, PeerAsyncStateVerify};
    use monad_consensus_types::{
        block::{Block, BlockType},
        payload::StateRoot,
    };
    use monad_crypto::{
        certificate_signature::{CertificateKeyPair, CertificateSignaturePubKey},
        NopPubKey, NopSignature,
    };
    use monad_eth_block_policy::{nonce::InMemoryState, EthBlockPolicy};
    use monad_eth_block_validator::EthValidator;
    use monad_eth_testutil::make_tx;
    use monad_eth_tx::EthSignedTransaction;
    use monad_eth_txpool::EthTxPool;
    use monad_executor_glue::MonadEvent;
    use monad_mock_swarm::{
        mock_swarm::{Nodes, SwarmBuilder},
        node::NodeBuilder,
        swarm_relation::SwarmRelation,
        terminator::UntilTerminator,
        verifier::{happy_path_tick_by_block, MockSwarmVerifier},
    };
    use monad_multi_sig::MultiSig;
    use monad_router_scheduler::{NoSerRouterConfig, NoSerRouterScheduler, RouterSchedulerBuilder};
    use monad_state::{MonadMessage, VerifiedMonadMessage};
    use monad_testutil::swarm::{make_state_configs, swarm_ledger_verification};
    use monad_transformer::{
        DropTransformer, GenericTransformer, GenericTransformerPipeline, LatencyTransformer,
        PartitionTransformer, ID,
    };
    use monad_types::{NodeId, Round, SeqNum, GENESIS_SEQ_NUM};
    use monad_updaters::{
        ledger::{MockLedger, MockableLedger},
        state_root_hash::MockStateRootHashNop,
    };
    use monad_validator::{
        simple_round_robin::SimpleRoundRobin,
        validator_set::{ValidatorSetFactory, ValidatorSetTypeFactory},
    };
    use reth_primitives::B256;

    pub struct EthSwarm;
    impl SwarmRelation for EthSwarm {
        type SignatureType = NopSignature;
        type SignatureCollectionType = MultiSig<Self::SignatureType>;
        type StateBackendType = Arc<Mutex<InMemoryState>>;
        type BlockPolicyType = EthBlockPolicy;

        type TransportMessage =
            VerifiedMonadMessage<Self::SignatureType, Self::SignatureCollectionType>;

        type BlockValidator = EthValidator;
        type StateRootValidator = StateRoot;
        type ValidatorSetTypeFactory =
            ValidatorSetFactory<CertificateSignaturePubKey<Self::SignatureType>>;
        type LeaderElection = SimpleRoundRobin<CertificateSignaturePubKey<Self::SignatureType>>;
        type TxPool = EthTxPool;
        type Ledger = MockLedger<
            Self::SignatureCollectionType,
            Block<Self::SignatureCollectionType>,
            MonadEvent<Self::SignatureType, Self::SignatureCollectionType>,
        >; // FIXME:
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
    }

    const CONSENSUS_DELTA: Duration = Duration::from_millis(100);

    fn generate_eth_swarm(num_nodes: u16) -> Nodes<EthSwarm> {
        let state_configs = make_state_configs::<EthSwarm>(
            num_nodes, // num_nodes
            ValidatorSetFactory::default,
            SimpleRoundRobin::default,
            EthTxPool::default,
            || EthValidator::new(10_000, 1_000_000),
            || Arc::new(Mutex::new(InMemoryState::default())),
            || {
                EthBlockPolicy::new(
                    GENESIS_SEQ_NUM,
                    SeqNum(4), // state_root_delay
                )
            },
            || {
                StateRoot::new(
                    SeqNum(4), // state_root_delay
                )
            },
            PeerAsyncStateVerify::new,
            CONSENSUS_DELTA,    // delta
            10,                 // proposal_tx_limit
            SeqNum(2000),       // val_set_update_interval
            Round(50),          // epoch_start_delay
            majority_threshold, // state root quorum threshold
            5,                  // max_blocksync_retries
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
                    let state_backend = Arc::clone(&state_builder.state_backend);
                    NodeBuilder::<EthSwarm>::new(
                        ID::new(NodeId::new(state_builder.key.pubkey())),
                        state_builder,
                        NoSerRouterConfig::new(all_peers.clone()).build(),
                        MockStateRootHashNop::new(validators.validators, SeqNum(2000)),
                        MockLedger::default(), // FIXME:
                        vec![GenericTransformer::Latency(LatencyTransformer::new(
                            CONSENSUS_DELTA,
                        ))],
                        vec![],
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
            for block in state.executor.ledger().get_blocks() {
                let decoded_txns =
                    Vec::<EthSignedTransaction>::decode(&mut block.payload.txns.as_ref()).unwrap();
                let decoded_txn_hashes: HashSet<_> =
                    HashSet::from_iter(decoded_txns.iter().map(|t| t.hash()));
                for txn_hash in decoded_txn_hashes {
                    if txns_to_see.contains(&txn_hash) {
                        txns_to_see.remove(&txn_hash);
                    } else {
                        println!(
                            "Unexpected transaction in block {}. NodeID: {}, TxnHash: {}",
                            block.get_seq_num().0,
                            node_id,
                            txn_hash
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
        let mut swarm = generate_eth_swarm(2);
        let node_ids = swarm.states().keys().copied().collect_vec();
        let node_1_id = node_ids[0];

        let sender_1_key = B256::random();
        let mut expected_txns = Vec::new();
        for nonce in 0..10 {
            let eth_txn = make_tx(sender_1_key, 1, 1, nonce, 10);

            swarm.send_transaction(node_1_id, eth_txn.envelope_encoded().into());

            expected_txns.push(eth_txn);
        }

        for nonce in 20..30 {
            let eth_txn = make_tx(sender_1_key, 1, 1, nonce, 10);

            swarm.send_transaction(node_1_id, eth_txn.envelope_encoded().into());
        }

        while swarm
            .step_until(&mut UntilTerminator::new().until_block(4))
            .is_some()
        {}

        let mut verifier = MockSwarmVerifier::default().tick_range(
            happy_path_tick_by_block(4, CONSENSUS_DELTA),
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
        let mut swarm = generate_eth_swarm(2);
        let node_ids = swarm.states().keys().copied().collect_vec();
        let node_1_id = node_ids[0];
        let node_2_id = node_ids[1];

        let mut expected_txns = Vec::new();

        let sender_1_key = B256::random();
        // Send 10 transactions with nonces 0..10 to Node 1. Leader for round 1
        for nonce in 0..10 {
            let eth_txn = make_tx(sender_1_key, 1, 1, nonce, 10);

            swarm.send_transaction(node_1_id, eth_txn.envelope_encoded().into());

            expected_txns.push(eth_txn);
        }

        while swarm
            .step_until(&mut UntilTerminator::new().until_block(4))
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
            let eth_txn = make_tx(sender_1_key, 1, 1, nonce, 1000);

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
        let mut swarm = generate_eth_swarm(2);
        let node_ids = swarm.states().keys().copied().collect_vec();
        let node_1_id = node_ids[0];
        let node_2_id = node_ids[1];

        let mut expected_txns = Vec::new();

        let sender_1_key = B256::random();
        let sender_2_key = B256::random();
        // Send transactions with nonces 0..10 to Node 1. Leader for round 1
        for nonce in 0..10 {
            let eth_txn_sender_1 = make_tx(sender_1_key, 1, 1, nonce, 10);
            let eth_txn_sender_2 = make_tx(sender_2_key, 1, 1, nonce, 10);

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
            let eth_txn_sender_1 = make_tx(sender_1_key, 1, 1, nonce, 10);
            let eth_txn_sender_2 = make_tx(sender_2_key, 1, 1, nonce, 10);

            swarm.send_transaction(node_2_id, eth_txn_sender_1.envelope_encoded().into());
            swarm.send_transaction(node_2_id, eth_txn_sender_2.envelope_encoded().into());
        }

        // Send transactions with nonces 10..20 to Node 2
        for nonce in 10..20 {
            let eth_txn_sender_1 = make_tx(sender_1_key, 1, 1, nonce, 10);
            let eth_txn_sender_2 = make_tx(sender_2_key, 1, 1, nonce, 10);

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
        let mut swarm = generate_eth_swarm(4);
        let node_ids = swarm.states().keys().copied().collect_vec();
        let (node_1_id, other_nodes) = node_ids.split_first().unwrap();
        let node_1_id = *node_1_id;
        let other_nodes = other_nodes.to_owned().to_vec();

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

        let sender_1_key = B256::random();
        // Send transactions with nonces 0..10 to node 2 so that nodes 2, 3 and 4 can make progress
        for nonce in 0..10 {
            let eth_txn = make_tx(sender_1_key, 1, 1, nonce, 10);

            swarm.send_transaction(other_nodes[0], eth_txn.envelope_encoded().into());

            expected_txns.push(eth_txn);
        }

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

        // remove blackout from node 1
        let regular_pipeline = vec![GenericTransformer::Latency(LatencyTransformer::new(
            CONSENSUS_DELTA,
        ))];
        swarm.update_outbound_pipeline_for_all(regular_pipeline);

        // Send transactions with nonces 10..20 to node 1 so that it can propose them after it catches up with blocksync
        for nonce in 10..20 {
            let eth_txn = make_tx(sender_1_key, 1, 1, nonce, 10);

            swarm.send_transaction(node_1_id, eth_txn.envelope_encoded().into());

            expected_txns.push(eth_txn);
        }

        // run sufficiently long so that node 1 can catch and propose the transactions in has in its tx pool
        while swarm
            .step_until(&mut UntilTerminator::new().until_block(30))
            .is_some()
        {}

        assert!(verify_transactions_in_ledger(
            &swarm,
            swarm.states().keys().cloned().collect_vec(),
            expected_txns
        ));
    }
}
