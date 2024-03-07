mod common;

mod test {
    use std::{
        collections::{BTreeMap, BTreeSet, HashSet},
        time::Duration,
    };

    use itertools::Itertools;
    use monad_async_state_verify::{majority_threshold, PeerAsyncStateVerify};
    use monad_consensus_types::{
        block_validator::MockValidator, metrics::Metrics, payload::StateRoot, txpool::MockTxPool,
    };
    use monad_crypto::certificate_signature::CertificateKeyPair;
    use monad_mock_swarm::{
        fetch_metric,
        mock_swarm::{Nodes, SwarmBuilder},
        node::NodeBuilder,
        swarm_relation::{MonadMessageNoSerSwarm, NoSerSwarm},
        terminator::{ProgressTerminator, UntilTerminator},
        transformer::{FilterTransformer, MonadMessageTransformer},
        verifier::MockSwarmVerifier,
    };
    use monad_router_scheduler::{NoSerRouterConfig, RouterSchedulerBuilder};
    use monad_testutil::swarm::{
        ledger_verification, make_state_configs, swarm_ledger_verification,
    };
    use monad_transformer::{
        DropTransformer, GenericTransformer, LatencyTransformer, PartitionTransformer,
        PeriodicTransformer, ID,
    };
    use monad_types::{NodeId, Round, SeqNum};
    use monad_updaters::state_root_hash::MockStateRootHashNop;
    use monad_validator::{
        simple_round_robin::SimpleRoundRobin, validator_set::ValidatorSetFactory,
    };
    use monad_wal::mock::MockWALoggerConfig;
    use test_case::test_case;

    #[test]
    fn bsync_timeout_recovery() {
        let delta = Duration::from_millis(1);
        let state_configs = make_state_configs::<MonadMessageNoSerSwarm>(
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
            PeerAsyncStateVerify::new,
            delta,              // delta
            0,                  // proposal_tx_limit
            SeqNum(2000),       // val_set_update_interval
            Round(50),          // epoch_start_delay
            majority_threshold, // state root quorum threshold
            5,                  // max_blocksync_retries
        );
        let all_peers: BTreeSet<_> = state_configs
            .iter()
            .map(|state_config| NodeId::new(state_config.key.pubkey()))
            .collect();

        let filter_peers = HashSet::from([ID::new(*all_peers.first().unwrap())]);

        let mut outbound_pipeline = vec![
            MonadMessageTransformer::Filter(FilterTransformer {
                drop_block_sync: true,
                ..Default::default()
            }),
            MonadMessageTransformer::Latency(LatencyTransformer::new(delta)),
            MonadMessageTransformer::Partition(PartitionTransformer(filter_peers.clone())),
            MonadMessageTransformer::Drop(DropTransformer::new()),
        ];

        let swarm_config = SwarmBuilder::<MonadMessageNoSerSwarm>(
            state_configs
                .into_iter()
                .enumerate()
                .map(|(seed, state_builder)| {
                    let validators = state_builder.validators.clone();
                    NodeBuilder::<MonadMessageNoSerSwarm>::new(
                        ID::new(NodeId::new(state_builder.key.pubkey())),
                        state_builder,
                        MockWALoggerConfig::default(),
                        NoSerRouterConfig::new(all_peers.clone()).build(),
                        MockStateRootHashNop::new(validators, SeqNum(2000)),
                        outbound_pipeline.clone(),
                        vec![],
                        seed.try_into().unwrap(),
                    )
                })
                .collect(),
        );

        let mut swarm = swarm_config.build();

        let verify_but_first = |swarm: &Nodes<MonadMessageNoSerSwarm>| {
            ledger_verification(
                &swarm
                    .states()
                    .values()
                    .filter_map(|node| {
                        if filter_peers.contains(&node.id) {
                            assert_eq!(node.executor.ledger().get_blocks().len(), 0);
                            None
                        } else {
                            Some(node.executor.ledger().get_blocks().clone())
                        }
                    })
                    .collect(),
                10,
            );
        };

        let verify_all = |swarm: &Nodes<MonadMessageNoSerSwarm>| {
            swarm_ledger_verification(swarm, 10);
        };

        let mut terminator = UntilTerminator::new().until_tick(Duration::from_secs(2));

        // first run for 2 seconds, all but first node makes progress
        while swarm.step_until(&mut terminator).is_some() {}

        verify_but_first(&swarm);

        // remove blackout but still ban block sync
        outbound_pipeline = outbound_pipeline[0..2].to_vec();
        swarm.update_outbound_pipeline_for_all(outbound_pipeline.clone());

        // run for 5 sec to allow the blackout node to be aware of the world state,
        // however, it start to attempting block sync, but will not succeed
        terminator = terminator.until_tick(Duration::from_secs(5));
        while swarm.step_until(&mut terminator).is_some() {}

        verify_but_first(&swarm);
        // remove the block sync filter
        outbound_pipeline = outbound_pipeline[1..2].to_vec();
        swarm.update_outbound_pipeline_for_all(outbound_pipeline);

        // run for sufficiently long
        terminator = terminator.until_tick(Duration::from_secs(30));
        while swarm.step_until(&mut terminator).is_some() {}

        // first node should have caught up
        verify_all(&swarm);

        let verifier = MockSwarmVerifier::default().tick_range(Duration::from_secs(30), delta);
        assert!(verifier.verify(&swarm));
    }

    #[test]
    #[should_panic]
    fn lack_of_progress() {
        let delta = Duration::from_millis(1);
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
            PeerAsyncStateVerify::new,
            delta,              // delta
            0,                  // proposal_tx_limit
            SeqNum(2000),       // val_set_update_interval
            Round(50),          // epoch_start_delay
            majority_threshold, // state root quorum threshold
            5,                  // max_blocksync_retries
        );
        let all_peers: BTreeSet<_> = state_configs
            .iter()
            .map(|state_config| NodeId::new(state_config.key.pubkey()))
            .collect();

        let first_node = ID::new(*all_peers.first().unwrap());
        let mut filter_peers = HashSet::new();
        filter_peers.insert(first_node);

        println!("no progress node ID: {:?}", first_node);

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
                        MockStateRootHashNop::new(validators, SeqNum(2000)),
                        vec![
                            GenericTransformer::Latency(LatencyTransformer::new(delta)),
                            GenericTransformer::Partition(PartitionTransformer(
                                filter_peers.clone(),
                            )),
                            GenericTransformer::Drop(DropTransformer::new()),
                        ],
                        vec![],
                        seed.try_into().unwrap(),
                    )
                })
                .collect(),
        );

        let mut swarm = swarm_config.build();
        // step until should panic
        while swarm
            .step_until(&mut ProgressTerminator::new(
                all_peers
                    .iter()
                    .map(|k| (ID::new(*k), 1))
                    .collect::<BTreeMap<_, _>>(),
                Duration::from_secs(1),
            ))
            .is_some()
        {}
        swarm_ledger_verification(&swarm, 20);
    }

    /**
     *  Couple messages gets delayed significantly for 1 second
     */
    #[test]
    fn extreme_delay_recovery_with_block_sync() {
        let delta = Duration::from_millis(1);

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
            PeerAsyncStateVerify::new,
            delta,              // delta
            0,                  // proposal_tx_limit
            SeqNum(2000),       // val_set_update_interval
            Round(50),          // epoch_start_delay
            majority_threshold, // state root quorum threshold
            5,                  // max_blocksync_retries
        );
        let all_peers: BTreeSet<_> = state_configs
            .iter()
            .map(|state_config| NodeId::new(state_config.key.pubkey()))
            .collect();

        let first_node = ID::new(*all_peers.first().unwrap());
        let mut filter_peers = HashSet::new();
        filter_peers.insert(first_node);

        println!("blackout node ID: {:?}", first_node);

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
                        MockStateRootHashNop::new(validators, SeqNum(2000)),
                        vec![
                            GenericTransformer::Latency(LatencyTransformer::new(delta)),
                            GenericTransformer::Partition(PartitionTransformer(
                                filter_peers.clone(),
                            )),
                            GenericTransformer::Periodic(PeriodicTransformer::new(
                                Duration::from_secs(1),
                                Duration::from_secs(2),
                            )),
                            GenericTransformer::Latency(LatencyTransformer::new(
                                Duration::from_millis(400),
                            )),
                        ],
                        vec![],
                        seed.try_into().unwrap(),
                    )
                })
                .collect(),
        );

        let mut swarm = swarm_config.build();
        while swarm
            .step_until(&mut UntilTerminator::new().until_tick(Duration::from_secs(4)))
            .is_some()
        {}
        swarm_ledger_verification(&swarm, 20);

        let ledger_len = swarm
            .states()
            .values()
            .map(|node| node.executor.ledger().get_blocks().len())
            .max()
            .unwrap();
        let running_nodes_ids = swarm
            .states()
            .values()
            .filter_map(|node| (node.id != first_node).then_some(node.id))
            .collect_vec();

        let mut verifier = MockSwarmVerifier::default().tick_range(Duration::from_secs(4), delta);

        verifier
            .metric_exact(
                &running_nodes_ids,
                fetch_metric!(blocksync_events.blocksync_request),
                0,
            )
            // handle proposal for all blocks in ledger
            .metric_minimum(
                &running_nodes_ids,
                fetch_metric!(consensus_events.handle_proposal),
                ledger_len as u64,
            )
            // vote for all blocks in ledger
            .metric_minimum(
                &running_nodes_ids,
                fetch_metric!(consensus_events.created_vote),
                ledger_len as u64,
            );

        assert!(verifier.verify(&swarm));
    }

    #[test_case(4, Duration::from_millis(100),Duration::from_millis(200),Duration::from_secs(4),1; "test 1")]
    #[test_case(50, Duration::from_secs(1),Duration::from_secs(2),Duration::from_secs(4),10; "test 2")]
    #[test_case(50, Duration::from_secs(1),Duration::from_secs(2),Duration::from_secs(4),25; "test 3")]
    #[test_case(50, Duration::from_secs(1),Duration::from_secs(2),Duration::from_secs(4),50; "test 4")]
    #[test_case(10, Duration::from_secs(0),Duration::from_secs(2),Duration::from_secs(4),3; "test 5")]
    #[test_case(10, Duration::from_secs(0),Duration::from_secs(10),Duration::from_secs(20), 3; "test 6")]
    fn black_out_recovery_with_block_sync(
        num_nodes: u16,
        from: Duration,
        to: Duration,
        until: Duration,
        black_out_cnt: usize,
        // giving a high delay so state root doesn't trigger
    ) {
        let delta = Duration::from_millis(1);
        assert!(
            from < to
                && to < until
                && black_out_cnt <= (num_nodes as usize)
                && black_out_cnt >= 1
                && num_nodes >= 4
        );

        let state_configs = make_state_configs::<NoSerSwarm>(
            num_nodes, // num_nodes
            ValidatorSetFactory::default,
            SimpleRoundRobin::default,
            MockTxPool::default,
            || MockValidator,
            || {
                StateRoot::new(
                    SeqNum(u64::MAX), // state_root_delay
                )
            },
            PeerAsyncStateVerify::new,
            delta,              // delta
            0,                  // proposal_tx_limit
            SeqNum(2000),       // val_set_update_interval
            Round(50),          // epoch_start_delay
            majority_threshold, // state root quorum threshold
            5,                  // max_blocksync_retries
        );
        let all_peers: BTreeSet<_> = state_configs
            .iter()
            .map(|state_config| NodeId::new(state_config.key.pubkey()))
            .collect();

        let filter_peers = HashSet::from_iter(
            // FIXME test-2 fails with all_peers iteration order... (eg sorted order)
            state_configs
                .iter()
                .take(black_out_cnt)
                .map(|state_config| ID::new(NodeId::new(state_config.key.pubkey()))),
        );

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
                        MockStateRootHashNop::new(validators, SeqNum(2000)),
                        vec![
                            GenericTransformer::Latency(LatencyTransformer::new(delta)),
                            GenericTransformer::Partition(PartitionTransformer(
                                filter_peers.clone(),
                            )),
                            GenericTransformer::Periodic(PeriodicTransformer::new(from, to)),
                            GenericTransformer::Drop(DropTransformer::new()),
                        ],
                        vec![],
                        seed.try_into().unwrap(),
                    )
                })
                .collect(),
        );

        let mut swarm = swarm_config.build();
        while swarm
            .step_until(&mut UntilTerminator::new().until_tick(until))
            .is_some()
        {}
        swarm_ledger_verification(&swarm, 20);

        let ledger_len = swarm
            .states()
            .values()
            .map(|node| node.executor.ledger().get_blocks().len())
            .max()
            .unwrap();
        let running_nodes_ids = swarm
            .states()
            .values()
            .filter_map(|node| (!filter_peers.contains(&node.id)).then_some(node.id))
            .collect_vec();

        let mut verifier = MockSwarmVerifier::default().tick_range(until, delta);

        verifier
            .metric_exact(
                &running_nodes_ids,
                fetch_metric!(blocksync_events.blocksync_request),
                0,
            )
            // handle proposal for all blocks in ledger
            .metric_minimum(
                &running_nodes_ids,
                fetch_metric!(consensus_events.handle_proposal),
                ledger_len as u64,
            )
            // vote for all blocks in ledger
            .metric_minimum(
                &running_nodes_ids,
                fetch_metric!(consensus_events.created_vote),
                ledger_len as u64,
            );

        assert!(verifier.verify(&swarm));
    }
}
