mod test {
    use std::{collections::HashSet, f32::INFINITY, time::Duration};

    use monad_block_sync::BlockSyncState;
    use monad_consensus_state::ConsensusState;
    use monad_consensus_types::{
        multi_sig::MultiSig, payload::StateRoot, transaction_validator::MockValidator,
    };
    use monad_crypto::NopSignature;
    use monad_executor_glue::PeerId;
    use monad_mock_swarm::{
        mock::{MockMempool, NoSerRouterConfig, NoSerRouterScheduler},
        transformer::{
            DropTransformer, GenericTransformer, LatencyTransformer, PartitionTransformer,
            PeriodicTransformer,
        },
    };
    use monad_state::{MonadMessage, MonadState};
    use monad_testutil::{
        signing::node_id,
        swarm::{get_configs, run_nodes_until, run_nodes_until_and_verify},
    };
    use monad_validator::{simple_round_robin::SimpleRoundRobin, validator_set::ValidatorSet};
    use monad_wal::mock::{MockWALogger, MockWALoggerConfig};
    use test_case::test_case;

    /**
     *  Couple messages gets delayed significantly for 1 second
     */
    #[test]
    fn extreme_delay() {
        let num_nodes = 4;
        let delta = Duration::from_millis(2);
        let (pubkeys, state_configs) =
            get_configs::<NopSignature, MultiSig<NopSignature>, _>(MockValidator, num_nodes, delta);

        assert!(num_nodes >= 2, "test requires 2 or more nodes");

        let first_node = PeerId(*pubkeys.first().unwrap());

        let mut filter_peers: HashSet<PeerId> = HashSet::new();
        filter_peers.insert(first_node);

        println!("blackout node ID: {:?}", first_node);

        run_nodes_until_and_verify::<
            MonadState<
                ConsensusState<MultiSig<NopSignature>, MockValidator, StateRoot>,
                NopSignature,
                MultiSig<NopSignature>,
                ValidatorSet,
                SimpleRoundRobin,
                BlockSyncState,
            >,
            NopSignature,
            MultiSig<NopSignature>,
            NoSerRouterScheduler<MonadMessage<_, _>>,
            _,
            MockWALogger<_>,
            _,
            _,
            MockValidator,
            MockMempool<_>,
        >(
            pubkeys,
            state_configs,
            |all_peers: Vec<_>, _| NoSerRouterConfig {
                all_peers: all_peers.into_iter().collect(),
            },
            MockWALoggerConfig,
            |_, _| {
                vec![
                    GenericTransformer::Latency(LatencyTransformer(Duration::from_millis(1))), // everyone get delayed no matter what
                    GenericTransformer::Partition(PartitionTransformer(filter_peers.clone())), // partition the victim node
                    GenericTransformer::Periodic(PeriodicTransformer::new(
                        Duration::from_secs(1),
                        Duration::from_secs(2),
                    )),
                    GenericTransformer::Latency(LatencyTransformer(Duration::from_millis(400))),
                ]
            },
            false,
            Duration::from_secs(4),
            usize::MAX,
            20,
        );
    }

    /**
     * This is the integration test for the timeout mechanism, current timeout manager suppose to
     * choose the first member of the validator set as the requester, we permanently censor the first node,
     * if timeout is working properly, then each node that want to block sync will be able to time out and block
     * sync with another neighbour instead
     */
    #[test]
    fn permanent_offline() {
        let num_nodes = 5;
        let delta = Duration::from_millis(2);
        let (pubkeys, state_configs) =
            get_configs::<NopSignature, MultiSig<NopSignature>, _>(MockValidator, num_nodes, delta);
        let offline_node = PeerId(*pubkeys.first().unwrap());
        assert!(num_nodes >= 5, "test requires 5 or more nodes");

        let (nodes, _) = run_nodes_until::<
            MonadState<
                ConsensusState<MultiSig<NopSignature>, MockValidator, StateRoot>,
                NopSignature,
                MultiSig<NopSignature>,
                ValidatorSet,
                SimpleRoundRobin,
                BlockSyncState,
            >,
            NopSignature,
            MultiSig<NopSignature>,
            NoSerRouterScheduler<MonadMessage<_, _>>,
            _,
            MockWALogger<_>,
            _,
            _,
            MockValidator,
            MockMempool<_>,
        >(
            pubkeys,
            state_configs,
            |all_peers: Vec<_>, _| NoSerRouterConfig {
                all_peers: all_peers.into_iter().collect(),
            },
            MockWALoggerConfig,
            |peers, peer| {
                if peer == peers[0] {
                    // suffer permanent outage
                    vec![GenericTransformer::Drop(DropTransformer())]
                } else if peer == peers[1] {
                    // suffer temp outage
                    vec![
                        GenericTransformer::Latency(LatencyTransformer(Duration::from_millis(1))), // everyone get delayed no matter what
                        GenericTransformer::Or(vec![
                            GenericTransformer::And(vec![
                                GenericTransformer::Partition(PartitionTransformer(HashSet::from(
                                    [peer],
                                ))), // partition the victim node
                                GenericTransformer::Periodic(PeriodicTransformer::new(
                                    Duration::from_secs(1),
                                    Duration::from_secs(2),
                                )),
                            ]),
                            GenericTransformer::Partition(PartitionTransformer(HashSet::from([
                                peers[0],
                            ]))),
                        ]),
                        GenericTransformer::Drop(DropTransformer()),
                    ]
                } else {
                    vec![
                        GenericTransformer::Latency(LatencyTransformer(Duration::from_millis(1))),
                        GenericTransformer::Partition(PartitionTransformer(HashSet::from([
                            peers[0]
                        ]))),
                        GenericTransformer::Drop(DropTransformer()),
                    ]
                }
            },
            false,
            Duration::from_secs(4),
            usize::MAX,
        );

        let max_ledger_idx = nodes
            .states()
            .iter()
            .filter_map(|(k, v)| if *k == offline_node { None } else { Some(v) })
            .map(|node| node.executor.ledger().get_blocks().len())
            .max()
            .unwrap();
        assert!(max_ledger_idx > 40); // arbitrary number, but should exceed this easily
        for n in nodes.states().values() {
            if n.id != offline_node {
                assert!(
                    (n.executor.ledger().get_blocks().len() as i32) - (max_ledger_idx as i32) <= 5
                );
            } else {
                assert_eq!(n.executor.ledger().get_blocks().len(), 0);
            }
        }
    }

    #[test_case(4, Duration::from_secs(1),Duration::from_secs(2),Duration::from_secs(4),1; "test 1")]
    #[test_case(50, Duration::from_secs(1),Duration::from_secs(2),Duration::from_secs(4),10; "test 2")]
    #[test_case(50, Duration::from_secs(1),Duration::from_secs(2),Duration::from_secs(4),25; "test 3")]
    #[test_case(50, Duration::from_secs(1),Duration::from_secs(2),Duration::from_secs(4),50; "test 4")]
    #[test_case(10, Duration::from_secs(0),Duration::from_secs(2),Duration::from_secs(4),3; "test 5")]
    #[test_case(10, Duration::from_secs(0),Duration::from_secs(30),Duration::from_secs(60), 3; "test 6")]

    fn black_out(
        num_nodes: u16,
        from: Duration,
        to: Duration,
        until: Duration,
        black_out_cnt: usize,
    ) {
        assert!(
            from < to && to < until && black_out_cnt <= (num_nodes as usize) && black_out_cnt >= 1
        );

        let delta = Duration::from_millis(2);
        let (pubkeys, state_configs) =
            get_configs::<NopSignature, MultiSig<NopSignature>, _>(MockValidator, num_nodes, delta);

        assert!(num_nodes >= 2, "test requires 2 or more nodes");

        let filter_peers: HashSet<PeerId> =
            HashSet::from_iter(pubkeys.iter().take(black_out_cnt).map(|k| PeerId(*k)));

        println!("delayed node ID: {:?}", filter_peers);

        run_nodes_until_and_verify::<
            MonadState<
                ConsensusState<MultiSig<NopSignature>, MockValidator, StateRoot>,
                NopSignature,
                MultiSig<NopSignature>,
                ValidatorSet,
                SimpleRoundRobin,
                BlockSyncState,
            >,
            NopSignature,
            MultiSig<NopSignature>,
            NoSerRouterScheduler<MonadMessage<_, _>>,
            _,
            MockWALogger<_>,
            _,
            _,
            MockValidator,
            MockMempool<_>,
        >(
            pubkeys,
            state_configs,
            |all_peers: Vec<_>, _| NoSerRouterConfig {
                all_peers: all_peers.into_iter().collect(),
            },
            MockWALoggerConfig,
            |_, _| {
                vec![
                    GenericTransformer::Latency(LatencyTransformer(Duration::from_millis(1))), // everyone get delayed no matter what
                    GenericTransformer::Partition(PartitionTransformer(filter_peers.clone())), // partition the victim node
                    GenericTransformer::Periodic(PeriodicTransformer::new(from, to)),
                    GenericTransformer::Drop(DropTransformer()),
                ]
            },
            false,
            until,
            usize::MAX,
            20,
        );
    }
}
