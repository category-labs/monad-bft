mod common;

mod test {
    use std::{
        collections::{BTreeSet, HashSet},
        time::Duration,
    };

    use itertools::Itertools;
    use monad_async_state_verify::{majority_threshold, PeerAsyncStateVerify};
    use monad_consensus_types::{
        block::{BlockType, PassthruBlockPolicy},
        block_validator::MockValidator,
        metrics::Metrics,
        payload::StateRoot,
        txpool::MockTxPool,
    };
    use monad_crypto::{
        certificate_signature::{CertificateKeyPair, CertificateSignaturePubKey},
        NopPubKey, NopSignature,
    };
    use monad_mock_swarm::{
        fetch_metric,
        mock::TimestamperConfig,
        mock_swarm::SwarmBuilder,
        node::{Node, NodeBuilder},
        swarm_relation::{NoSerSwarm, SwarmRelation},
        terminator::UntilTerminator,
        verifier::{happy_path_tick_by_block, happy_path_tick_by_round, MockSwarmVerifier},
    };
    use monad_multi_sig::MultiSig;
    use monad_router_scheduler::{NoSerRouterConfig, NoSerRouterScheduler, RouterSchedulerBuilder};
    use monad_state::{MonadMessage, VerifiedMonadMessage};
    use monad_state_backend::{InMemoryState, InMemoryStateInner};
    use monad_testutil::swarm::{make_state_configs, swarm_ledger_verification};
    use monad_transformer::{
        DropTransformer, GenericTransformer, GenericTransformerPipeline, LatencyTransformer,
        PartitionTransformer, ID,
    };
    use monad_types::{Epoch, NodeId, Round, SeqNum};
    use monad_updaters::{
        ledger::{MockLedger, MockableLedger},
        state_root_hash::{MockStateRootHashNop, MockStateRootHashSwap},
        statesync::MockStateSyncExecutor,
    };
    use monad_validator::{
        simple_round_robin::SimpleRoundRobin,
        validator_set::{ValidatorSetFactory, ValidatorSetTypeFactory},
    };
    use test_case::test_case;
    pub struct ValidatorSwapSwarm;
    impl SwarmRelation for ValidatorSwapSwarm {
        type SignatureType = NopSignature;
        type SignatureCollectionType = MultiSig<Self::SignatureType>;
        type StateBackendType = InMemoryState;
        type BlockPolicyType = PassthruBlockPolicy;

        type TransportMessage =
            VerifiedMonadMessage<Self::SignatureType, Self::SignatureCollectionType>;

        type BlockValidator = MockValidator;
        type StateRootValidator = StateRoot;
        type ValidatorSetTypeFactory =
            ValidatorSetFactory<CertificateSignaturePubKey<Self::SignatureType>>;
        type LeaderElection = SimpleRoundRobin<CertificateSignaturePubKey<Self::SignatureType>>;
        type TxPool = MockTxPool;
        type Ledger = MockLedger<Self::SignatureType, Self::SignatureCollectionType>;
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
            MockStateRootHashSwap<Self::SignatureType, Self::SignatureCollectionType>;
        type StateSyncExecutor =
            MockStateSyncExecutor<Self::SignatureType, Self::SignatureCollectionType>;
    }

    fn verify_nodes_in_epoch(nodes: Vec<&Node<impl SwarmRelation>>, epoch: Epoch) {
        assert!(!nodes.is_empty());

        for node in nodes {
            let current_epoch = node
                .state
                .epoch_manager()
                .get_epoch(
                    node.state
                        .consensus()
                        .expect("consensus is live")
                        .get_current_round(),
                )
                .expect("epoch exists");
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
            let mut update_block = None;
            for block in node.executor.ledger().get_blocks().values() {
                if block.get_seq_num() == update_block_num {
                    update_block = Some(block);
                    break;
                }
            }
            let update_block = update_block.unwrap();

            let update_block_round = update_block.round;
            let epoch_manager = node.state.epoch_manager();
            let epoch_start_round = update_block_round + epoch_manager.epoch_start_delay;

            // verify the epoch is scheduled correctly
            assert_ne!(
                epoch_manager
                    .get_epoch(epoch_start_round - Round(1))
                    .expect("epoch exists"),
                expected_epoch
            );
            assert_eq!(
                epoch_manager
                    .get_epoch(epoch_start_round)
                    .expect("epoch exists"),
                expected_epoch
            );

            epoch_start_rounds.push(epoch_start_round);
        }

        // verify all the nodes agree on the same round for new epoch
        assert!(epoch_start_rounds
            .iter()
            .all(|r| r == &epoch_start_rounds[0]));

        epoch_start_rounds[0]
    }

    fn verify_nodes_not_schedule_epoch(
        nodes: Vec<&Node<impl SwarmRelation>>,
        expected_epoch: Epoch,
    ) {
        assert!(!nodes.is_empty());
        for node in nodes {
            let epoch_manager = node.state.epoch_manager();
            assert!(!epoch_manager.epoch_starts.keys().contains(&expected_epoch));
        }
    }

    #[test]
    fn schedule_and_advance_epoch() {
        let val_set_update_interval = SeqNum(1000);

        let delta = Duration::from_millis(20);
        let state_configs = make_state_configs::<NoSerSwarm>(
            4, // num_nodes
            ValidatorSetFactory::default,
            SimpleRoundRobin::default,
            MockTxPool::default,
            || MockValidator,
            || PassthruBlockPolicy,
            || InMemoryStateInner::genesis(u128::MAX, SeqNum(10_000_000)),
            || {
                StateRoot::new(
                    SeqNum(10_000_000), // state_root_delay
                )
            },
            PeerAsyncStateVerify::new,
            delta,                   // delta
            0,                       // proposal_tx_limit
            val_set_update_interval, // val_set_update_interval
            Round(20),               // epoch_start_delay
            majority_threshold,      // state root quorum threshold
            SeqNum(100),             // state_sync_threshold
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
                    let state_backend = state_builder.state_backend.clone();
                    let validators = state_builder.forkpoint.validator_sets[0].clone();
                    NodeBuilder::<NoSerSwarm>::new(
                        ID::new(NodeId::new(state_builder.key.pubkey())),
                        state_builder,
                        NoSerRouterConfig::new(all_peers.clone()).build(),
                        MockStateRootHashNop::new(
                            validators.validators.clone(),
                            val_set_update_interval,
                        ),
                        MockLedger::new(state_backend.clone()),
                        MockStateSyncExecutor::new(
                            state_backend,
                            validators
                                .validators
                                .0
                                .into_iter()
                                .map(|v| v.node_id)
                                .collect(),
                        ),
                        vec![GenericTransformer::Latency(LatencyTransformer::new(delta))],
                        vec![],
                        TimestamperConfig::default(),
                        seed.try_into().unwrap(),
                    )
                })
                .collect(),
        );

        let mut nodes = swarm_config.build();

        let update_block_num = val_set_update_interval;
        // terminates when any node produced more than `until_block` blocks. we
        // want the longest ledger to be shorter than update_block_num
        let mut term_before_update_block =
            UntilTerminator::new().until_block((update_block_num.0) as usize - 2);
        while nodes.step_until(&mut term_before_update_block).is_some() {}
        // all nodes must still be in this epoch
        verify_nodes_in_epoch(nodes.states().values().collect_vec(), Epoch(1));
        // no one has committed the boundary block
        verify_nodes_not_schedule_epoch(nodes.states().values().collect_vec(), Epoch(2));

        // terminates when one node commits more than `update_block_num` blocks.
        // It ensures every node has committed `update_block_num` blocks
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

        // expect to take (1<timeout> + (epoch_start_round - 1 <the leader
        // enters before delivered to other nodes>) * 2<round trip>) * delta
        let mut verifier = MockSwarmVerifier::default()
            .tick_range(happy_path_tick_by_round(epoch_start_round, delta), delta);

        let node_ids = nodes.states().keys().copied().collect_vec();
        verifier.metrics_happy_path(&node_ids, &nodes);

        assert!(verifier.verify(&nodes));
    }

    #[test]
    fn schedule_epoch_after_blocksync() {
        let val_set_update_interval = SeqNum(1000);

        let delta = Duration::from_millis(20);
        let state_configs = make_state_configs::<NoSerSwarm>(
            4, // num_nodes
            ValidatorSetFactory::default,
            SimpleRoundRobin::default,
            MockTxPool::default,
            || MockValidator,
            || PassthruBlockPolicy,
            || InMemoryStateInner::genesis(u128::MAX, SeqNum(10_000_000)),
            || {
                StateRoot::new(
                    SeqNum(10_000_000), // state_root_delay
                )
            },
            PeerAsyncStateVerify::new,
            delta,                   // delta
            0,                       // proposal_tx_limit
            val_set_update_interval, // val_set_update_interval
            Round(20),               // epoch_start_delay
            majority_threshold,      // state root quorum threshold
            SeqNum(100),             // state_sync_threshold
        );
        let all_peers: BTreeSet<_> = state_configs
            .iter()
            .map(|state_config| NodeId::new(state_config.key.pubkey()))
            .collect();

        let regular_pipeline = vec![GenericTransformer::Latency(LatencyTransformer::new(delta))];

        let swarm_config = SwarmBuilder::<NoSerSwarm>(
            state_configs
                .into_iter()
                .enumerate()
                .map(|(seed, state_builder)| {
                    let state_backend = state_builder.state_backend.clone();
                    let validators = state_builder.forkpoint.validator_sets[0].clone();
                    NodeBuilder::<NoSerSwarm>::new(
                        ID::new(NodeId::new(state_builder.key.pubkey())),
                        state_builder,
                        NoSerRouterConfig::new(all_peers.clone()).build(),
                        MockStateRootHashNop::new(
                            validators.validators.clone(),
                            val_set_update_interval,
                        ),
                        MockLedger::new(state_backend.clone()),
                        MockStateSyncExecutor::new(
                            state_backend,
                            validators
                                .validators
                                .0
                                .into_iter()
                                .map(|v| v.node_id)
                                .collect(),
                        ),
                        regular_pipeline.clone(),
                        vec![],
                        TimestamperConfig::default(),
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
        // verify all nodes are in epoch 1
        verify_nodes_in_epoch(nodes.states().values().collect_vec(), Epoch(1));
        verify_nodes_not_schedule_epoch(nodes.states().values().collect_vec(), Epoch(2));

        let node_ids = nodes.states().keys().copied().collect_vec();
        let mut verifier_before_blackout = MockSwarmVerifier::default().tick_range(
            happy_path_tick_by_block(update_block_num.0 as usize - 2, delta),
            delta,
        );
        verifier_before_blackout.metrics_happy_path(&node_ids, &nodes);
        assert!(verifier_before_blackout.verify(&nodes));

        // blackout one node and let other nodes continue
        let blackout_node_id = nodes.states().values().collect_vec().first().unwrap().id;
        println!("blackout node: {}", blackout_node_id);

        let filter_one_node = HashSet::from([blackout_node_id]);
        let blackout_pipeline = vec![
            GenericTransformer::Latency(LatencyTransformer::new(delta)),
            GenericTransformer::Partition(PartitionTransformer(filter_one_node)),
            GenericTransformer::Drop(DropTransformer::new()),
        ];
        nodes.update_outbound_pipeline_for_all(blackout_pipeline);

        let mut term_on_schedule_epoch =
            UntilTerminator::new().until_block(update_block_num.0 as usize + 1);
        while nodes.step_until(&mut term_on_schedule_epoch).is_some() {}

        let nodes_vec = nodes.states().values().collect_vec();
        let (blackout_node, running_nodes) = nodes_vec.split_first().unwrap();
        let running_nodes_ids = running_nodes.iter().map(|node| node.id).collect_vec();

        // verify the running nodes scheduled next epoch
        let epoch_start_round =
            verify_nodes_scheduled_epoch(running_nodes.to_vec(), update_block_num, Epoch(2));
        // verify the blackout node didn't schedule next epoch
        assert_eq!(
            blackout_node
                .state
                .epoch_manager()
                .get_epoch(epoch_start_round)
                .expect("epoch exists"),
            Epoch(1)
        );

        // remove blackout for the blackout node
        nodes.update_outbound_pipeline_for_all(regular_pipeline);

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

        // during blackout, if the blackout node is a leader for a round,
        // it doesn't collect votes or propose a block. this causes TCs to
        // be formed in two consecutive rounds
        // TODO: add tick assertions. need to account for blackout node consequences.
        // Updating pipelines between subsequent step_until calls doesn't take effect
        // immediately since `pending_inbound_messages` may already be populated.
        let mut verifier_after_blackout = MockSwarmVerifier::default();
        verifier_after_blackout
            .metric_exact(
                &running_nodes_ids,
                fetch_metric!(blocksync_events.blocksync_request),
                0,
            )
            // handle proposal for all blocks in ledger
            .metric_minimum(
                &running_nodes_ids,
                fetch_metric!(consensus_events.handle_proposal),
                update_block_num.0 + 10,
            )
            // vote for all blocks in ledger
            .metric_minimum(
                &running_nodes_ids,
                fetch_metric!(consensus_events.created_vote),
                update_block_num.0 + 10,
            )
            .metric_maximum(
                &vec![blackout_node_id],
                fetch_metric!(blocksync_events.blocksync_request),
                4,
            )
            // initial TC + max timeouts during blackout
            .metric_maximum(
                &node_ids,
                fetch_metric!(consensus_events.local_timeout),
                1 + 10,
            );

        assert!(verifier_after_blackout.verify(&nodes));
    }

    #[test]
    fn verify_correct_leaders_in_epoch() {
        let val_set_update_interval = SeqNum(1000);

        let delta = 40;
        let latency = 20;

        let state_configs = make_state_configs::<ValidatorSwapSwarm>(
            4, // num_nodes
            ValidatorSetFactory::default,
            SimpleRoundRobin::default,
            MockTxPool::default,
            || MockValidator,
            || PassthruBlockPolicy,
            || InMemoryStateInner::genesis(u128::MAX, SeqNum(10_000_000)),
            || {
                StateRoot::new(
                    SeqNum(10_000_000), // state_root_delay
                )
            },
            PeerAsyncStateVerify::new,
            Duration::from_millis(delta), // delta
            0,                            // proposal_tx_limit
            val_set_update_interval,      // val_set_update_interval
            Round(20),                    // epoch_start_delay
            majority_threshold,           // state root quorum threshold
            SeqNum(100),                  // state_sync_threshold
        );

        let genesis_validators: Vec<NodeId<NopPubKey>> = state_configs[0].forkpoint.validator_sets
            [0]
        .validators
        .0
        .clone()
        .iter()
        .map(|vdata| vdata.node_id)
        .collect();
        let (validators_epoch_3, validators_epoch_4) = genesis_validators.split_at(2);
        // validators for epoch 1 = genesis_validators
        // validators for epoch 2 = genesis_validators
        // validators for epoch 3 = validators_epoch_3
        // validators for epoch 4 = validators_epoch_4

        let all_peers: BTreeSet<_> = state_configs
            .iter()
            .map(|state_config| NodeId::new(state_config.key.pubkey()))
            .collect();

        let regular_pipeline = vec![GenericTransformer::Latency(LatencyTransformer::new(
            Duration::from_millis(latency),
        ))];

        let swarm_config = SwarmBuilder::<ValidatorSwapSwarm>(
            state_configs
                .into_iter()
                .enumerate()
                .map(|(seed, state_builder)| {
                    let state_backend = state_builder.state_backend.clone();
                    let validators = state_builder.forkpoint.validator_sets[0].clone();
                    NodeBuilder::<ValidatorSwapSwarm>::new(
                        ID::new(NodeId::new(state_builder.key.pubkey())),
                        state_builder,
                        NoSerRouterConfig::new(all_peers.clone()).build(),
                        MockStateRootHashSwap::new(
                            validators.validators.clone(),
                            val_set_update_interval,
                        ),
                        MockLedger::new(state_backend.clone()),
                        MockStateSyncExecutor::new(
                            state_backend,
                            validators
                                .validators
                                .0
                                .into_iter()
                                .map(|v| v.node_id)
                                .collect(),
                        ),
                        regular_pipeline.clone(),
                        vec![],
                        TimestamperConfig::default(),
                        seed.try_into().unwrap(),
                    )
                })
                .collect(),
        );

        let mut nodes = swarm_config.build();

        let update_block_num_end_1 = val_set_update_interval;

        let mut term_on_schedule_epoch_2 =
            UntilTerminator::new().until_block(update_block_num_end_1.0 as usize + 1);
        while nodes.step_until(&mut term_on_schedule_epoch_2).is_some() {}

        // all nodes must still be in epoch 1 but schedule epoch 2
        verify_nodes_in_epoch(nodes.states().values().collect_vec(), Epoch(1));
        let epoch_2_start_round = verify_nodes_scheduled_epoch(
            nodes.states().values().collect_vec(),
            update_block_num_end_1,
            Epoch(2),
        );

        // terminate well into the second epoch
        let mut term_in_epoch_2 =
            UntilTerminator::new().until_round(epoch_2_start_round + Round(10));
        while nodes.step_until(&mut term_in_epoch_2).is_some() {}

        // all nodes must have advanced to next epoch
        verify_nodes_in_epoch(nodes.states().values().collect_vec(), Epoch(2));

        let update_block_num_end_2 = SeqNum(val_set_update_interval.0 * 2);

        let mut term_on_schedule_epoch_3 =
            UntilTerminator::new().until_block(update_block_num_end_2.0 as usize + 1);
        while nodes.step_until(&mut term_on_schedule_epoch_3).is_some() {}

        // all nodes must still be in the same epoch but schedule next epoch
        verify_nodes_in_epoch(nodes.states().values().collect_vec(), Epoch(2));
        let epoch_3_start_round = verify_nodes_scheduled_epoch(
            nodes.states().values().collect_vec(),
            update_block_num_end_2,
            Epoch(3),
        );

        // terminate well into the third epoch
        let mut term_in_epoch_3 =
            UntilTerminator::new().until_round(epoch_3_start_round + Round(10));
        while nodes.step_until(&mut term_in_epoch_3).is_some() {}

        // all nodes must have advanced to next epoch
        verify_nodes_in_epoch(nodes.states().values().collect_vec(), Epoch(3));

        let update_block_num_end_3 = SeqNum(val_set_update_interval.0 * 3);

        let mut term_on_schedule_epoch_4 =
            UntilTerminator::new().until_block(update_block_num_end_3.0 as usize + 1);
        while nodes.step_until(&mut term_on_schedule_epoch_4).is_some() {}

        // all nodes must still be in the same epoch but schedule next epoch
        verify_nodes_in_epoch(nodes.states().values().collect_vec(), Epoch(3));
        let epoch_4_start_round = verify_nodes_scheduled_epoch(
            nodes.states().values().collect_vec(),
            update_block_num_end_3,
            Epoch(4),
        );

        // terminate well into the fourth epoch
        let mut term_in_epoch_4 =
            UntilTerminator::new().until_round(epoch_4_start_round + Round(10));
        while nodes.step_until(&mut term_in_epoch_4).is_some() {}

        let ledgers = nodes
            .states()
            .values()
            .map(|node| {
                node.executor
                    .ledger()
                    .get_blocks()
                    .values()
                    .cloned()
                    .collect_vec()
            })
            .collect_vec();
        let max_ledger_blocks = ledgers.iter().map(|ledger| ledger.len()).max().unwrap();

        for ledger in ledgers {
            for block in ledger {
                if block.round < epoch_3_start_round {
                    // the first two epochs both have genesis validators as the
                    // validator set
                    assert!(genesis_validators.contains(&block.author));
                } else if block.round < epoch_4_start_round {
                    assert!(validators_epoch_3.contains(&block.author));
                } else {
                    assert!(validators_epoch_4.contains(&block.author));
                }
            }
        }

        let mut verifier = MockSwarmVerifier::default();
        let node_ids = nodes.states().keys().copied().collect_vec();
        verifier
            .metric_exact(
                &node_ids,
                fetch_metric!(blocksync_events.blocksync_request),
                0,
            )
            // handle proposal for all blocks in ledger
            .metric_minimum(
                &node_ids,
                fetch_metric!(consensus_events.handle_proposal),
                max_ledger_blocks as u64,
            )
            // vote for all blocks in ledger
            .metric_minimum(
                &node_ids,
                fetch_metric!(consensus_events.created_vote),
                max_ledger_blocks as u64,
            )
            // initial TC + account for TC whenever emmitted messages
            // are dropped during `step_until`
            .metric_maximum(&node_ids, fetch_metric!(consensus_events.local_timeout), 4);

        assert!(verifier.verify(&nodes));
    }

    #[test_case(SeqNum(100), Round(10), 1000; "update_interval: 100, epoch_start_delay: 10")]
    #[test_case(SeqNum(500), Round(10), 5000; "update_interval: 500, epoch_start_delay: 10")]
    #[test_case(SeqNum(2000), Round(50), 20000; "update_interval: 2000, epoch_start_delay: 50")]
    fn validator_switching(
        val_set_update_interval: SeqNum,
        epoch_start_delay: Round,
        until_block: usize,
    ) {
        let delta = Duration::from_millis(20);
        let state_configs = make_state_configs::<ValidatorSwapSwarm>(
            4, // num_nodes
            ValidatorSetFactory::default,
            SimpleRoundRobin::default,
            MockTxPool::default,
            || MockValidator,
            || PassthruBlockPolicy,
            || InMemoryStateInner::genesis(u128::MAX, SeqNum(4)),
            || {
                StateRoot::new(
                    SeqNum(4), // state_root_delay
                )
            },
            PeerAsyncStateVerify::new,
            delta,                   // delta
            10,                      // proposal_tx_limit
            val_set_update_interval, // val_set_update_interval
            epoch_start_delay,       // epoch_start_delay
            majority_threshold,      // state root quorum threshold
            SeqNum(100),             // state_sync_threshold
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
                    let state_backend = state_builder.state_backend.clone();
                    let validators = state_builder.forkpoint.validator_sets[0].clone();
                    NodeBuilder::<ValidatorSwapSwarm>::new(
                        ID::new(NodeId::new(state_builder.key.pubkey())),
                        state_builder,
                        NoSerRouterConfig::new(all_peers.clone()).build(),
                        MockStateRootHashSwap::new(
                            validators.validators.clone(),
                            val_set_update_interval,
                        ),
                        MockLedger::new(state_backend.clone()),
                        MockStateSyncExecutor::new(
                            state_backend,
                            validators
                                .validators
                                .0
                                .into_iter()
                                .map(|v| v.node_id)
                                .collect(),
                        ),
                        vec![GenericTransformer::Latency(LatencyTransformer::new(delta))],
                        vec![],
                        TimestamperConfig::default(),
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

        // resume tick assertions
        let mut verifier = MockSwarmVerifier::default()
            .tick_range(happy_path_tick_by_block(until_block, delta), delta);
        let node_ids = swarm.states().keys().copied().collect_vec();
        // TODO: all the metrics here should be equal to happy path metrics
        // but since validator switching is mimicked using unstaked validators,
        // there are extra messages sent from unstaked validators which are
        // ignored. should change it back to happy path once they are seperated
        verifier
            .metric_exact(&node_ids, fetch_metric!(consensus_events.local_timeout), 1)
            .metric_exact(
                &node_ids,
                fetch_metric!(consensus_events.remote_timeout_msg),
                3,
            );

        assert!(verifier.verify(&swarm));
    }
}
