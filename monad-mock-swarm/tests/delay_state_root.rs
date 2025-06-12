#[cfg(test)]
mod test {
    use std::{
        collections::{BTreeMap, BTreeSet, HashSet},
        time::Duration,
    };

    use alloy_consensus::{Transaction, TxEnvelope};
    use alloy_primitives::{Address, B256};
    use itertools::Itertools;
    use monad_chain_config::{
        revision::{ChainParams, MockChainRevision},
        MockChainConfig,
    };
    use monad_crypto::{
        certificate_signature::{CertificateKeyPair, CertificateSignaturePubKey},
        NopPubKey, NopSignature,
    };
    use monad_eth_block_policy::EthBlockPolicy;
    use monad_eth_block_validator::EthValidator;
    use monad_eth_ledger::MockEthLedger;
    use monad_eth_testutil::{make_legacy_tx, secret_to_eth_address};
    use monad_eth_types::{Balance, EthExecutionProtocol, BASE_FEE_PER_GAS};
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
    use monad_testutil::swarm::make_state_configs;
    use monad_transformer::{
        GenericTransformer, GenericTransformerPipeline, LatencyTransformer, ID,
    };
    use monad_types::{NodeId, Round, SeqNum, GENESIS_SEQ_NUM};
    use monad_updaters::{
        ledger::MockableLedger, state_root_hash::MockStateRootHashNop,
        statesync::MockStateSyncExecutor, txpool::MockTxPoolExecutor,
    };
    use monad_validator::{
        simple_round_robin::SimpleRoundRobin, validator_set::ValidatorSetFactory,
    };

    pub struct EthSwarm;
    impl SwarmRelation for EthSwarm {
        type SignatureType = NopSignature;
        type SignatureCollectionType = MultiSig<Self::SignatureType>;
        type ExecutionProtocolType = EthExecutionProtocol;
        type StateBackendType = InMemoryState;
        type BlockPolicyType = EthBlockPolicy<Self::SignatureType, Self::SignatureCollectionType>;
        type ChainConfigType = MockChainConfig;
        type ChainRevisionType = MockChainRevision;

        type TransportMessage = VerifiedMonadMessage<
            Self::SignatureType,
            Self::SignatureCollectionType,
            Self::ExecutionProtocolType,
        >;

        type BlockValidator = EthValidator<
            Self::SignatureType,
            Self::SignatureCollectionType,
            Self::StateBackendType,
        >;
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

        type StateRootHashExecutor = MockStateRootHashNop<
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
        >;
        type StateSyncExecutor = MockStateSyncExecutor<
            Self::SignatureType,
            Self::SignatureCollectionType,
            Self::ExecutionProtocolType,
        >;
    }

    const CONSENSUS_DELTA: Duration = Duration::from_millis(100);
    const GAS_LIMIT: u64 = 30000;

    static CHAIN_PARAMS: ChainParams = ChainParams {
        tx_limit: 10_000,
        proposal_gas_limit: 300_000_000,
        proposal_byte_limit: 4_000_000,
        vote_pace: Duration::from_millis(0),
    };

    fn generate_eth_swarm(
        num_nodes: u16,
        existing_accounts: impl IntoIterator<Item = Address>,
    ) -> Nodes<EthSwarm> {
        let execution_delay = SeqNum(4);

        let existing_nonces: BTreeMap<_, _> =
            existing_accounts.into_iter().map(|acc| (acc, 0)).collect();

        let create_block_policy = || EthBlockPolicy::new(GENESIS_SEQ_NUM, execution_delay.0, 1337);

        let state_configs = make_state_configs::<EthSwarm>(
            num_nodes,
            ValidatorSetFactory::default,
            SimpleRoundRobin::default,
            || EthValidator::new(1337),
            create_block_policy,
            || {
                InMemoryStateInner::new(
                    Balance::MAX,
                    execution_delay,
                    InMemoryBlockState::genesis(existing_nonces.clone()),
                )
            },
            execution_delay,                     // execution_delay
            CONSENSUS_DELTA,                     // delta
            MockChainConfig::new(&CHAIN_PARAMS), // chain config
            SeqNum(2000),                        // val_set_update_interval
            Round(50),                           // epoch_start_delay
            SeqNum(100),                         // state_sync_threshold
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
                        MockTxPoolExecutor::new(create_block_policy(), state_backend.clone()),
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
    fn delay_state_root_block() {
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
            let eth_txn =
                make_legacy_tx(sender_1_key, BASE_FEE_PER_GAS.into(), GAS_LIMIT, nonce, 10);

            swarm.send_transaction(node_1_id, alloy_rlp::encode(&eth_txn).into());

            expected_txns.push(eth_txn);
        }

        // Can not produce blocks until state root is available, state root is available at block 5 = 1 + execution_delay.
        while swarm
            .step_until(&mut UntilTerminator::new().until_block(4))
            .is_some()
        {}

        // Transactions are not in the ledger
        assert!(!verify_transactions_in_ledger(
            &swarm,
            swarm.states().keys().cloned().collect_vec(),
            expected_txns.clone()
        ));

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

        // All transactions should be in the ledger
        assert!(verify_transactions_in_ledger(
            &swarm,
            swarm.states().keys().cloned().collect_vec(),
            expected_txns
        ));
    }

    #[test]
    fn delay_state_root_timer() {
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
            let eth_txn =
                make_legacy_tx(sender_1_key, BASE_FEE_PER_GAS.into(), GAS_LIMIT, nonce, 10);

            swarm.send_transaction(node_1_id, alloy_rlp::encode(&eth_txn).into());

            expected_txns.push(eth_txn);
        }

        // Can not produce blocks until state root is available, state root is available at block 5 = 1 + execution_delay.
        while swarm
            .step_until(&mut UntilTerminator::new().until_block(5))
            .is_some()
        {}

        // Transactions must be in the ledger
        assert!(verify_transactions_in_ledger(
            &swarm,
            swarm.states().keys().cloned().collect_vec(),
            expected_txns.clone()
        ));

        swarm.set_enable_updates(node_1_id, false);

        // Move to the last block that can validate the state root with execution results.
        while swarm
            .step_until(&mut UntilTerminator::new().until_block(9))
            .is_some()
        {}

        for nonce in 10..20 {
            let eth_txn =
                make_legacy_tx(sender_1_key, BASE_FEE_PER_GAS.into(), GAS_LIMIT, nonce, 10);

            swarm.send_transaction(node_1_id, alloy_rlp::encode(&eth_txn).into());

            expected_txns.push(eth_txn);
        }

        while swarm
            .step_until(&mut UntilTerminator::new().until_tick(Duration::from_secs(10)))
            .is_some()
        {}

        // New transactions are not in the ledger
        assert!(!verify_transactions_in_ledger(
            &swarm,
            swarm.states().keys().cloned().collect_vec(),
            expected_txns.clone()
        ));

        swarm.set_enable_updates(node_1_id, true);

        while swarm
            .step_until(&mut UntilTerminator::new().until_block(13))
            .is_some()
        {}

        // All transactions should be in the ledger
        assert!(verify_transactions_in_ledger(
            &swarm,
            swarm.states().keys().cloned().collect_vec(),
            expected_txns
        ));

        println!(
            "node {} metrics {:#?}",
            node_1_id,
            swarm.states().get(&node_1_id).unwrap().state.metrics()
        );
    }
}
