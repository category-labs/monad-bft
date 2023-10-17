mod common;

mod test {
    use std::{
        collections::{BTreeMap, HashSet},
        time::Duration,
    };

    use monad_consensus_types::{multi_sig::MultiSig, transaction_validator::MockValidator};
    use monad_crypto::NopSignature;
    use monad_executor_glue::PeerId;
    use monad_mock_swarm::{
        mock::{MockMempoolConfig, NoSerRouterConfig},
        mock_swarm::{ProgressTerminator, UntilTerminator},
        transformer::{
            DropTransformer, GenericTransformer, LatencyTransformer, PartitionTransformer,
            PeriodicTransformer, ID,
        },
    };
    use monad_testutil::swarm::{get_configs, run_nodes_until};
    use monad_wal::mock::MockWALoggerConfig;
    use test_case::test_case;

    use super::common::NoSerSwarm;

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
        );

        let first_node = ID::new(PeerId(*pubkeys.first().unwrap()));

        let mut filter_peers = HashSet::new();
        filter_peers.insert(first_node);

        println!("no progress node ID: {:?}", first_node);

        let terminator = ProgressTerminator::new(
            pubkeys
                .iter()
                .map(|k| (ID::new(PeerId(*k)), 1))
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
                GenericTransformer::Latency(LatencyTransformer(Duration::from_millis(1))), // everyone get delayed no matter what
                GenericTransformer::Partition(PartitionTransformer(filter_peers)), // partition the victim node
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
        );

        assert!(num_nodes >= 2, "test requires 2 or more nodes");

        let first_node = ID::new(PeerId(*pubkeys.first().unwrap()));

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
                GenericTransformer::Latency(LatencyTransformer(Duration::from_millis(1))), // everyone get delayed no matter what
                GenericTransformer::Partition(PartitionTransformer(filter_peers)), // partition the victim node
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
        );

        assert!(num_nodes >= 2, "test requires 2 or more nodes");

        let first_node = ID::new(PeerId(*pubkeys.first().unwrap()));

        let filter_peers = HashSet::from_iter(
            pubkeys
                .iter()
                .take(black_out_cnt)
                .map(|k| ID::new(PeerId(*k))),
        );

        println!("delayed node ID: {:?}", first_node);

        run_nodes_until::<NoSerSwarm, _, _>(
            pubkeys,
            state_configs,
            |all_peers: Vec<_>, _| NoSerRouterConfig {
                all_peers: all_peers.into_iter().collect(),
            },
            MockWALoggerConfig,
            MockMempoolConfig::default(),
            vec![
                GenericTransformer::Latency(LatencyTransformer(Duration::from_millis(1))), // everyone get delayed no matter what
                GenericTransformer::Partition(PartitionTransformer(filter_peers)), // partition the victim node
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
