mod common;

mod test {
    use std::{
        collections::{BTreeMap, HashSet},
        time::Duration,
    };

    use monad_consensus_types::{multi_sig::MultiSig, transaction_validator::MockValidator};
    use monad_crypto::NopSignature;
    use monad_mock_swarm::{
        mock::{MockMempoolConfig, NoSerRouterConfig},
        mock_swarm::{Nodes, ProgressTerminator, UntilTerminator},
        swarm_relation::{MonadMessageNoSerSwarm, NoSerSwarm, SwarmRelation},
        transformer::{
            DropTransformer, FilterTransformer, GenericTransformer, LatencyTransformer,
            MonadMessageTransformer, PartitionTransformer, PeriodicTransformer, ID,
        },
    };
    use monad_testutil::swarm::{get_configs, node_ledger_verification, run_nodes_until};
    use monad_types::NodeId;
    use monad_wal::mock::MockWALoggerConfig;
    use test_case::test_case;

    #[test]
    fn bsync_timeout_recovery() {
        let num_nodes = 4;
        assert!(num_nodes >= 4, "test requires at least 4 nodes");

        let delta = Duration::from_millis(2);
        let (pubkeys, state_configs) = get_configs::<
            <MonadMessageNoSerSwarm as SwarmRelation>::SignatureType,
            <MonadMessageNoSerSwarm as SwarmRelation>::SignatureCollectionType,
            _,
        >(MockValidator, num_nodes, delta, u64::MAX, 0);

        let filter_peers = HashSet::from([ID::new(NodeId(pubkeys[0]))]);

        let mut pipeline = vec![
            MonadMessageTransformer::Filter(FilterTransformer {
                drop_block_sync: true,
                ..Default::default()
            }),
            MonadMessageTransformer::Latency(LatencyTransformer(delta)),
            MonadMessageTransformer::Partition(PartitionTransformer(filter_peers.clone())),
            MonadMessageTransformer::Drop(DropTransformer()),
        ];

        let mut terminator = UntilTerminator::new().until_tick(Duration::from_secs(2));
        let mut nodes = Nodes::<MonadMessageNoSerSwarm>::new(
            pubkeys
                .iter()
                .copied()
                .zip(state_configs)
                .map(|(pubkey, state_config)| {
                    (
                        ID::new(NodeId(pubkey)),
                        state_config,
                        MockWALoggerConfig,
                        NoSerRouterConfig {
                            all_peers: pubkeys.iter().copied().map(NodeId).collect(),
                        },
                        MockMempoolConfig::default(),
                        pipeline.clone(),
                        1,
                    )
                })
                .collect(),
        );

        let verify_but_first = |nodes: &Nodes<MonadMessageNoSerSwarm>| {
            node_ledger_verification(
                &nodes
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

        let verify_all = |nodes: &Nodes<MonadMessageNoSerSwarm>| {
            node_ledger_verification(
                &nodes
                    .states()
                    .values()
                    .map(|node| node.executor.ledger().get_blocks().clone())
                    .collect(),
                10,
            );
        };

        // first run for 2 seconds, all but first node makes progress
        while nodes.step_until(&terminator).is_some() {}

        verify_but_first(&nodes);

        // remove blackout but still ban block sync
        pipeline = pipeline[0..2].to_vec();
        nodes.update_pipeline_for_all(pipeline.clone());

        // run for 5 sec to allow the blackout node to be aware of the world state,
        // however, it start to attempting block sync, but will not succeed
        terminator = terminator.until_tick(Duration::from_secs(5));
        while nodes.step_until(&terminator).is_some() {}

        verify_but_first(&nodes);
        // remove the block sync filter
        pipeline = pipeline[1..2].to_vec();
        nodes.update_pipeline_for_all(pipeline);

        // run for sufficiently long
        terminator = terminator.until_tick(Duration::from_secs(30));
        while nodes.step_until(&terminator).is_some() {}

        // first node should have caught up
        verify_all(&nodes);
    }

    #[test]
    #[should_panic]
    fn lack_of_progress() {
        let num_nodes = 4;
        let delta = Duration::from_millis(2);
        let (pubkeys, state_configs) = get_configs::<NopSignature, MultiSig<NopSignature>, _>(
            MockValidator,
            num_nodes,
            delta,
            u64::MAX,
            0,
        );

        let first_node = ID::new(NodeId(*pubkeys.first().unwrap()));

        let mut filter_peers = HashSet::new();
        filter_peers.insert(first_node);

        println!("no progress node ID: {:?}", first_node);

        let terminator = ProgressTerminator::new(
            pubkeys
                .iter()
                .map(|k| (ID::new(NodeId(*k)), 1))
                .collect::<BTreeMap<_, _>>(),
            Duration::from_secs(1),
        );

        run_nodes_until::<NoSerSwarm, _, _>(
            pubkeys,
            state_configs,
            |all_peers: Vec<_>, _| NoSerRouterConfig {
                all_peers: all_peers.into_iter().collect(),
            },
            MockWALoggerConfig,
            MockMempoolConfig::default(),
            vec![
                GenericTransformer::Latency(LatencyTransformer(Duration::from_millis(1))),
                GenericTransformer::Partition(PartitionTransformer(filter_peers)),
                GenericTransformer::Drop(DropTransformer()),
            ],
            false,
            terminator,
            20,
            1,
        );
    }

    /**
     *  Couple messages gets delayed significantly for 1 second
     */
    #[test]
    fn extreme_delay_recovery_with_block_sync() {
        let num_nodes = 4;
        let delta = Duration::from_millis(2);
        let (pubkeys, state_configs) = get_configs::<NopSignature, MultiSig<NopSignature>, _>(
            MockValidator,
            num_nodes,
            delta,
            u64::MAX,
            0,
        );

        assert!(num_nodes >= 2, "test requires 2 or more nodes");

        let first_node = ID::new(NodeId(*pubkeys.first().unwrap()));

        let mut filter_peers = HashSet::new();
        filter_peers.insert(first_node);

        println!("blackout node ID: {:?}", first_node);

        run_nodes_until::<NoSerSwarm, _, _>(
            pubkeys,
            state_configs,
            |all_peers: Vec<_>, _| NoSerRouterConfig {
                all_peers: all_peers.into_iter().collect(),
            },
            MockWALoggerConfig,
            MockMempoolConfig::default(),
            vec![
                GenericTransformer::Latency(LatencyTransformer(Duration::from_millis(1))),
                GenericTransformer::Partition(PartitionTransformer(filter_peers)),
                GenericTransformer::Periodic(PeriodicTransformer::new(
                    Duration::from_secs(1),
                    Duration::from_secs(2),
                )),
                GenericTransformer::Latency(LatencyTransformer(Duration::from_millis(400))),
            ],
            false,
            UntilTerminator::new().until_tick(Duration::from_secs(4)),
            20,
            1,
        );
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
        assert!(
            from < to && to < until && black_out_cnt <= (num_nodes as usize) && black_out_cnt >= 1
        );

        let delta = Duration::from_millis(2);
        let (pubkeys, state_configs) = get_configs::<NopSignature, MultiSig<NopSignature>, _>(
            MockValidator,
            num_nodes,
            delta,
            u64::MAX,
            0,
        );

        assert!(num_nodes >= 4, "test requires 4 or more nodes");

        let filter_peers = HashSet::from_iter(
            pubkeys
                .iter()
                .take(black_out_cnt)
                .map(|k| ID::new(NodeId(*k))),
        );

        run_nodes_until::<NoSerSwarm, _, _>(
            pubkeys,
            state_configs,
            |all_peers: Vec<_>, _| NoSerRouterConfig {
                all_peers: all_peers.into_iter().collect(),
            },
            MockWALoggerConfig,
            MockMempoolConfig::default(),
            vec![
                GenericTransformer::Latency(LatencyTransformer(Duration::from_millis(1))),
                GenericTransformer::Partition(PartitionTransformer(filter_peers)),
                GenericTransformer::Periodic(PeriodicTransformer::new(from, to)),
                GenericTransformer::Drop(DropTransformer()),
            ],
            false,
            UntilTerminator::new().until_tick(until),
            20,
            1,
        );
    }
}
