mod common;

mod test {
    use std::{
        collections::{BTreeSet, HashSet},
        time::Duration,
    };

    use itertools::Itertools;
    use monad_consensus_types::{
        block::Block, block_validator::MockValidator, payload::StateRoot, txpool::MockTxPool,
    };
    use monad_crypto::{
        certificate_signature::{CertificateKeyPair, CertificateSignaturePubKey},
        NopPubKey, NopSignature,
    };
    use monad_executor_glue::MonadEvent;
    use monad_mock_swarm::{
        mock_swarm::SwarmBuilder,
        node::{Node, NodeBuilder},
        swarm_relation::{NoSerSwarm, SwarmRelation},
        terminator::UntilTerminator,
    };
    use monad_multi_sig::MultiSig;
    use monad_router_scheduler::{NoSerRouterConfig, NoSerRouterScheduler, RouterSchedulerBuilder};
    use monad_state::{MonadMessage, VerifiedMonadMessage};
    use monad_testutil::swarm::{make_state_configs, swarm_ledger_verification};
    use monad_transformer::{
        DropTransformer, GenericTransformer, GenericTransformerPipeline, LatencyTransformer,
        PartitionTransformer, ID,
    };
    use monad_types::{Epoch, NodeId, Round, SeqNum};
    use monad_updaters::state_root_hash::{MockStateRootHashNop, MockStateRootHashSwap};
    use monad_validator::{
        simple_round_robin::SimpleRoundRobin, validator_set::ValidatorSetFactory,
    };
    use monad_wal::mock::{MockWALogger, MockWALoggerConfig};
    use test_case::test_case;

    pub struct ValidatorSwapSwarm;
    impl SwarmRelation for ValidatorSwapSwarm {
        type SignatureType = NopSignature;
        type SignatureCollectionType = MultiSig<Self::SignatureType>;

        type TransportMessage =
            VerifiedMonadMessage<Self::SignatureType, Self::SignatureCollectionType>;

        type BlockValidator = MockValidator;
        type StateRootValidator = StateRoot;
        type ValidatorSetTypeFactory =
            ValidatorSetFactory<CertificateSignaturePubKey<Self::SignatureType>>;
        type LeaderElection = SimpleRoundRobin<CertificateSignaturePubKey<Self::SignatureType>>;
        type TxPool = MockTxPool;

        type RouterScheduler = NoSerRouterScheduler<
            CertificateSignaturePubKey<Self::SignatureType>,
            MonadMessage<Self::SignatureType, Self::SignatureCollectionType>,
            VerifiedMonadMessage<Self::SignatureType, Self::SignatureCollectionType>,
        >;

        type Pipeline = GenericTransformerPipeline<
            CertificateSignaturePubKey<Self::SignatureType>,
            Self::TransportMessage,
        >;

        type Logger = MockWALogger<MonadEvent<Self::SignatureType, Self::SignatureCollectionType>>;

        type StateRootHashExecutor = MockStateRootHashSwap<
            Block<Self::SignatureCollectionType>,
            Self::SignatureType,
            Self::SignatureCollectionType,
        >;
    }

    fn verify_nodes_in_epoch(nodes: Vec<&Node<impl SwarmRelation>>, epoch: Epoch) {
        assert!(!nodes.is_empty());

        for node in nodes {
            let current_epoch = node
                .state
                .epoch_manager()
                .get_epoch(node.state.consensus().get_current_round());
            assert!(current_epoch == epoch);
        }
    }

    fn verify_nodes_scheduled_epoch(
        nodes: Vec<&Node<impl SwarmRelation>>,
        update_block_num: SeqNum,
        expected_epoch: Epoch,
    ) -> Round {
        assert!(!nodes.is_empty());

        let mut epoch_start_rounds = Vec::new();

        for node in nodes {
            let update_block = node
                .executor
                .ledger()
                .get_blocks()
                .iter()
                .find(|b| b.payload.seq_num == update_block_num)
                .unwrap();
            let update_block_round = update_block.round;
            let epoch_manager = node.state.epoch_manager();
            let epoch_start_round = update_block_round + epoch_manager.epoch_start_delay;

            // verify the epoch is scheduled correctly
            assert_ne!(
                epoch_manager.get_epoch(epoch_start_round - Round(1)),
                expected_epoch
            );
            assert_eq!(epoch_manager.get_epoch(epoch_start_round), expected_epoch);

            epoch_start_rounds.push(epoch_start_round);
        }

        // verify all the nodes agree on the same round for new epoch
        assert!(epoch_start_rounds
            .iter()
            .all(|r| r == &epoch_start_rounds[0]));

        epoch_start_rounds[0]
    }

    #[test]
    fn schedule_and_advance_epoch() {
        let val_set_update_interval = SeqNum(1000);

        let state_configs = make_state_configs::<NoSerSwarm>(
            4, // num_nodes
            ValidatorSetFactory::default,
            SimpleRoundRobin::default,
            MockTxPool::default,
            || MockValidator,
            || {
                StateRoot::new(
                    SeqNum(u64::MAX), // state_root_delay
                )
            },
            Duration::from_millis(2), // delta
            0,                        // proposal_tx_limit
            val_set_update_interval,  // val_set_update_interval
            Round(20),                // epoch_start_delay
        );
        let all_peers: BTreeSet<_> = state_configs
            .iter()
            .map(|state_config| NodeId::new(state_config.key.pubkey()))
            .collect();
        let swarm_config = SwarmBuilder::<NoSerSwarm>(
            state_configs
                .into_iter()
                .enumerate()
                .map(|(seed, state_builder)| {
                    let validators = state_builder.validators.clone();
                    NodeBuilder::<NoSerSwarm>::new(
                        ID::new(NodeId::new(state_builder.key.pubkey())),
                        state_builder,
                        MockWALoggerConfig::default(),
                        NoSerRouterConfig::new(all_peers.clone()).build(),
                        MockStateRootHashNop::new(validators, val_set_update_interval),
                        vec![GenericTransformer::Latency(LatencyTransformer::new(
                            Duration::from_millis(1),
                        ))],
                        seed.try_into().unwrap(),
                    )
                })
                .collect(),
        );

        let mut nodes = swarm_config.build();

        let update_block_num = val_set_update_interval;

        let mut term_before_update_block =
            UntilTerminator::new().until_block((update_block_num.0 - 2) as usize);
        while nodes.step_until(&mut term_before_update_block).is_some() {}
        // all nodes must still be in this epoch
        verify_nodes_in_epoch(nodes.states().values().collect_vec(), Epoch(1));

        let mut term_on_schedule_epoch =
            UntilTerminator::new().until_block(update_block_num.0 as usize);
        while nodes.step_until(&mut term_on_schedule_epoch).is_some() {}

        // all nodes must still be in the same epoch but schedule next epoch
        verify_nodes_in_epoch(nodes.states().values().collect_vec(), Epoch(1));
        let epoch_start_round = verify_nodes_scheduled_epoch(
            nodes.states().values().collect_vec(),
            update_block_num,
            Epoch(2),
        );

        let mut term_on_new_epoch = UntilTerminator::new().until_round(epoch_start_round);
        while nodes.step_until(&mut term_on_new_epoch).is_some() {}

        // all nodes must have advanced to next epoch
        verify_nodes_in_epoch(nodes.states().values().collect_vec(), Epoch(2));
    }

    #[test]
    fn schedule_epoch_after_blocksync() {
        let val_set_update_interval = SeqNum(1000);

        let state_configs = make_state_configs::<NoSerSwarm>(
            4, // num_nodes
            ValidatorSetFactory::default,
            SimpleRoundRobin::default,
            MockTxPool::default,
            || MockValidator,
            || {
                StateRoot::new(
                    SeqNum(u64::MAX), // state_root_delay
                )
            },
            Duration::from_millis(2), // delta
            0,                        // proposal_tx_limit
            val_set_update_interval,  // val_set_update_interval
            Round(20),                // epoch_start_delay
        );
        let all_peers: BTreeSet<_> = state_configs
            .iter()
            .map(|state_config| NodeId::new(state_config.key.pubkey()))
            .collect();

        let regular_pipeline = vec![GenericTransformer::Latency(LatencyTransformer::new(
            Duration::from_millis(1),
        ))];

        let swarm_config = SwarmBuilder::<NoSerSwarm>(
            state_configs
                .into_iter()
                .enumerate()
                .map(|(seed, state_builder)| {
                    let validators = state_builder.validators.clone();
                    NodeBuilder::<NoSerSwarm>::new(
                        ID::new(NodeId::new(state_builder.key.pubkey())),
                        state_builder,
                        MockWALoggerConfig::default(),
                        NoSerRouterConfig::new(all_peers.clone()).build(),
                        MockStateRootHashNop::new(validators, val_set_update_interval),
                        regular_pipeline.clone(),
                        seed.try_into().unwrap(),
                    )
                })
                .collect(),
        );

        let mut nodes = swarm_config.build();

        let update_block_num = val_set_update_interval;

        let mut term_before_update_block =
            UntilTerminator::new().until_block((update_block_num.0 - 1) as usize);
        while nodes.step_until(&mut term_before_update_block).is_some() {}
        // verify all nodes are in epoch 1
        verify_nodes_in_epoch(nodes.states().values().collect_vec(), Epoch(1));

        // blackout one node and let other nodes continue
        let blackout_node_id = nodes.states().values().collect_vec().first().unwrap().id;
        let filter_one_node = HashSet::from([blackout_node_id]);
        let blackout_pipeline = vec![
            GenericTransformer::Latency(LatencyTransformer::new(Duration::from_millis(1))),
            GenericTransformer::Partition(PartitionTransformer(filter_one_node)),
            GenericTransformer::Drop(DropTransformer::new()),
        ];
        nodes.update_pipeline_for_all(blackout_pipeline);

        let mut term_on_schedule_epoch =
            UntilTerminator::new().until_block(update_block_num.0 as usize);
        while nodes.step_until(&mut term_on_schedule_epoch).is_some() {}

        let nodes_vec = nodes.states().values().collect_vec();
        let (blackout_node, running_nodes) = nodes_vec.split_first().unwrap();

        // verify the running nodes scheduled next epoch
        let epoch_start_round =
            verify_nodes_scheduled_epoch(running_nodes.to_vec(), update_block_num, Epoch(2));
        // verify the blackout node didn't schedule next epoch
        assert_eq!(
            blackout_node
                .state
                .epoch_manager()
                .get_epoch(epoch_start_round),
            Epoch(1)
        );

        // remove blackout for the blackout node
        nodes.update_pipeline_for_all(regular_pipeline);

        // run sufficiently long for the blackout node to finish blocksync
        let mut term_on_schedule_epoch_2 =
            UntilTerminator::new().until_block((update_block_num.0 + 10) as usize);
        while nodes.step_until(&mut term_on_schedule_epoch_2).is_some() {}

        // verify all nodes have scheduled next epoch (including blackout node)
        verify_nodes_scheduled_epoch(
            nodes.states().values().collect_vec(),
            update_block_num,
            Epoch(2),
        );
    }

    #[test]
    fn verify_correct_leaders_in_epoch() {
        let val_set_update_interval = SeqNum(1000);

        let state_configs = make_state_configs::<ValidatorSwapSwarm>(
            4, // num_nodes
            ValidatorSetFactory::default,
            SimpleRoundRobin::default,
            MockTxPool::default,
            || MockValidator,
            || {
                StateRoot::new(
                    SeqNum(u64::MAX), // state_root_delay
                )
            },
            Duration::from_millis(2), // delta
            0,                        // proposal_tx_limit
            val_set_update_interval,  // val_set_update_interval
            Round(20),                // epoch_start_delay
        );

        let genesis_validators: Vec<NodeId<NopPubKey>> = state_configs[0]
            .validators
            .0
            .clone()
            .iter()
            .map(|(node_id, _, _)| *node_id)
            .collect();
        let (validators_epoch_2, validators_epoch_3) = genesis_validators.split_at(2);
        // validators for epoch 1 = genesis_validators
        // validators for epoch 2 = validators_epoch_2
        // validators for epoch 3 = validators_epoch_3

        let all_peers: BTreeSet<_> = state_configs
            .iter()
            .map(|state_config| NodeId::new(state_config.key.pubkey()))
            .collect();

        let regular_pipeline = vec![GenericTransformer::Latency(LatencyTransformer::new(
            Duration::from_millis(1),
        ))];

        let swarm_config = SwarmBuilder::<ValidatorSwapSwarm>(
            state_configs
                .into_iter()
                .enumerate()
                .map(|(seed, state_builder)| {
                    let validators = state_builder.validators.clone();
                    NodeBuilder::<ValidatorSwapSwarm>::new(
                        ID::new(NodeId::new(state_builder.key.pubkey())),
                        state_builder,
                        MockWALoggerConfig::default(),
                        NoSerRouterConfig::new(all_peers.clone()).build(),
                        MockStateRootHashSwap::new(validators, val_set_update_interval),
                        regular_pipeline.clone(),
                        seed.try_into().unwrap(),
                    )
                })
                .collect(),
        );

        let mut nodes = swarm_config.build();

        let update_block_num = val_set_update_interval;

        let mut term_on_schedule_epoch_2 =
            UntilTerminator::new().until_block(update_block_num.0 as usize);
        while nodes.step_until(&mut term_on_schedule_epoch_2).is_some() {}

        // all nodes must still be in epoch 1 but schedule epoch 2
        verify_nodes_in_epoch(nodes.states().values().collect_vec(), Epoch(1));
        let epoch_2_start_round = verify_nodes_scheduled_epoch(
            nodes.states().values().collect_vec(),
            update_block_num,
            Epoch(2),
        );

        // terminate well into the second epoch
        let mut term_in_epoch_2 =
            UntilTerminator::new().until_round(epoch_2_start_round + Round(10));
        while nodes.step_until(&mut term_in_epoch_2).is_some() {}

        // all nodes must have advanced to next epoch
        verify_nodes_in_epoch(nodes.states().values().collect_vec(), Epoch(2));

        let next_update_block_num = val_set_update_interval + val_set_update_interval;

        let mut term_on_schedule_epoch_3 =
            UntilTerminator::new().until_block(next_update_block_num.0 as usize);
        while nodes.step_until(&mut term_on_schedule_epoch_3).is_some() {}

        // all nodes must still be in the same epoch but schedule next epoch
        verify_nodes_in_epoch(nodes.states().values().collect_vec(), Epoch(2));
        let epoch_3_start_round = verify_nodes_scheduled_epoch(
            nodes.states().values().collect_vec(),
            next_update_block_num,
            Epoch(3),
        );

        // terminate well into the third epoch
        let mut term_in_epoch_3 =
            UntilTerminator::new().until_round(epoch_3_start_round + Round(10));
        while nodes.step_until(&mut term_in_epoch_3).is_some() {}

        let ledgers = nodes
            .states()
            .values()
            .map(|node| node.executor.ledger().get_blocks().clone())
            .collect_vec();

        for ledger in ledgers {
            for block in ledger {
                if block.round < epoch_2_start_round {
                    assert!(genesis_validators.contains(&block.author));
                } else if block.round < epoch_3_start_round {
                    assert!(validators_epoch_2.contains(&block.author));
                } else {
                    assert!(validators_epoch_3.contains(&block.author));
                }
            }
        }
    }

    #[test_case(SeqNum(100), Round(10), 1000; "update_interval: 100, epoch_start_delay: 10")]
    #[test_case(SeqNum(500), Round(10), 5000; "update_interval: 500, epoch_start_delay: 10")]
    #[test_case(SeqNum(2000), Round(50), 20000; "update_interval: 2000, epoch_start_delay: 50")]
    fn validator_switching(
        val_set_update_interval: SeqNum,
        epoch_start_delay: Round,
        until_block: usize,
    ) {
        let state_configs = make_state_configs::<ValidatorSwapSwarm>(
            4, // num_nodes
            ValidatorSetFactory::default,
            SimpleRoundRobin::default,
            MockTxPool::default,
            || MockValidator,
            || {
                StateRoot::new(
                    SeqNum(4), // state_root_delay
                )
            },
            Duration::from_millis(2), // delta
            0,                        // proposal_tx_limit
            val_set_update_interval,  // val_set_update_interval
            epoch_start_delay,        // epoch_start_delay
        );
        let all_peers: BTreeSet<_> = state_configs
            .iter()
            .map(|state_config| NodeId::new(state_config.key.pubkey()))
            .collect();
        let swarm_config = SwarmBuilder::<ValidatorSwapSwarm>(
            state_configs
                .into_iter()
                .enumerate()
                .map(|(seed, state_builder)| {
                    let validators = state_builder.validators.clone();
                    NodeBuilder::<ValidatorSwapSwarm>::new(
                        ID::new(NodeId::new(state_builder.key.pubkey())),
                        state_builder,
                        MockWALoggerConfig::default(),
                        NoSerRouterConfig::new(all_peers.clone()).build(),
                        MockStateRootHashSwap::new(validators, val_set_update_interval),
                        vec![GenericTransformer::Latency(LatencyTransformer::new(
                            Duration::from_millis(1),
                        ))],
                        seed.try_into().unwrap(),
                    )
                })
                .collect(),
        );

        let mut swarm = swarm_config.build();
        while swarm
            .step_until(&mut UntilTerminator::new().until_block(until_block))
            .is_some()
        {}
        swarm_ledger_verification(&swarm, until_block);
    }
}
