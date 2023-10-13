#[cfg(all(test, feature = "monad_test"))]
mod test {
    use std::time::Duration;

    use monad_block_sync::BlockSyncState;
    use monad_consensus_state::ConsensusState;
    use monad_consensus_types::{
        message_signature::MessageSignature, multi_sig::MultiSig, payload::StateRoot,
        signature_collection::SignatureCollection, transaction_validator::MockValidator,
    };
    use monad_crypto::NopSignature;
    use monad_executor::{timed_event::TimedEvent, State};
    use monad_executor_glue::{MonadEvent, PeerId};
    use monad_mock_swarm::{
        mock::{
            MockExecutor, MockMempool, MockMempoolConfig, MockableExecutor, NoSerRouterConfig,
            NoSerRouterScheduler, RouterScheduler,
        },
        mock_swarm::{Node, Nodes},
        transformer::{GenericTransformer, LatencyTransformer, Pipeline},
    };
    use monad_state::{MonadMessage, MonadState};
    use monad_testutil::swarm::{get_configs, node_ledger_verification};
    use monad_types::{Deserializable, Serializable};
    use monad_validator::{simple_round_robin::SimpleRoundRobin, validator_set::ValidatorSet};
    use monad_wal::{
        mock::{MockMemLogger, MockMemLoggerConfig},
        PersistenceLogger,
    };
    use tracing_test::traced_test;

    const CONSENSUS_DELTA: Duration = Duration::from_millis(10);

    type ST = NopSignature;
    type SignatureCollectionType = MultiSig<NopSignature>;
    type TxValType = MockValidator;
    type StateRootValType = StateRoot;
    type RS = NoSerRouterScheduler<MonadMessage<ST, SignatureCollectionType>>;
    type S = MonadState<
        ConsensusState<SignatureCollectionType, TxValType, StateRootValType>,
        ST,
        SignatureCollectionType,
        ValidatorSet,
        SimpleRoundRobin,
        BlockSyncState,
    >;
    type LoggerType = MockMemLogger<TimedEvent<MonadEvent<ST, SignatureCollectionType>>>;
    type ME = MockMempool<ST, SignatureCollectionType>;

    fn run_nodes_until<S, RS, P, LGR, ME, ST, SCT>(
        nodes: &mut Nodes<S, RS, P, LGR, ME, ST, SCT>,
        start_tick: Duration,
        until_tick: Duration,
        until_block: usize,
    ) -> Duration
    where
        S: State<Event = MonadEvent<ST, SCT>, SignatureCollection = SCT>,
        ST: MessageSignature + Unpin,
        SCT: SignatureCollection + Unpin,

        RS: RouterScheduler,
        S::Message: Deserializable<RS::M>,
        S::OutboundMessage: Serializable<RS::M>,
        RS::Serialized: Eq,

        P: Pipeline<RS::Serialized>,
        LGR: PersistenceLogger<Event = TimedEvent<S::Event>>,

        ME: MockableExecutor<Event = S::Event, SignatureCollection = SCT>,

        MockExecutor<S, RS, ME, ST, SCT>: Unpin,
        S::Block: Unpin,
        Node<S, RS, P, LGR, ME, ST, SCT>: Send,
        RS::Serialized: Send,
    {
        let mut max_tick = start_tick;

        while let Some((tick, _, _)) = nodes.step_until(until_tick, until_block) {
            assert!(tick >= max_tick);
            max_tick = tick;
        }

        max_tick
    }

    fn liveness<S, RS, P, LGR, ME, ST, SCT>(
        nodes: &Nodes<S, RS, P, LGR, ME, ST, SCT>,
        last_ledger_len: usize,
    ) -> bool
    where
        S: State<Event = MonadEvent<ST, SCT>, SignatureCollection = SCT>,
        ST: MessageSignature + Unpin,
        SCT: SignatureCollection + Unpin,

        RS: RouterScheduler,
        S::Message: Deserializable<RS::M>,
        S::OutboundMessage: Serializable<RS::M>,
        RS::Serialized: Eq,

        P: Pipeline<RS::Serialized>,
        LGR: PersistenceLogger<Event = TimedEvent<S::Event>>,

        ME: MockableExecutor<Event = S::Event, SignatureCollection = SCT>,

        MockExecutor<S, RS, ME, ST, SCT>: Unpin,
        S::Block: Unpin,
        Node<S, RS, P, LGR, ME, ST, SCT>: Send,
        RS::Serialized: Send,
    {
        let max_ledger_len = nodes
            .states()
            .values()
            .map(|node| node.executor.ledger().get_blocks().len())
            .max()
            .unwrap();
        max_ledger_len > last_ledger_len + 2
    }

    #[traced_test]
    #[test_case::test_case(&[0,1]; "fail 0 1")]
    #[test_case::test_case(&[0,2]; "fail 0 2")]
    #[test_case::test_case(&[0,3]; "fail 0 3")]
    #[test_case::test_case(&[1,2]; "fail 1 2")]
    #[test_case::test_case(&[1,3]; "fail 1 3")]
    #[test_case::test_case(&[2,3]; "fail 2 3")]
    fn replay_one_honest(failure_idx: &[usize]) {
        let default_seed = 1;
        // setup 4 nodes
        let (peers, state_configs) = get_configs::<ST, SignatureCollectionType, TxValType>(
            MockValidator,
            4,
            CONSENSUS_DELTA,
            4,
        );
        let (_, mut state_configs_duplicate) = get_configs::<ST, SignatureCollectionType, TxValType>(
            MockValidator,
            4,
            CONSENSUS_DELTA,
            4,
        );

        let pubkeys = peers;
        let router_scheduler_config = |all_peers: Vec<PeerId>, _: PeerId| NoSerRouterConfig {
            all_peers: all_peers.into_iter().collect(),
        };
        let logger_config = MockMemLoggerConfig::default();
        let pipeline = vec![GenericTransformer::<
            MonadMessage<ST, SignatureCollectionType>,
        >::Latency(LatencyTransformer(
            Duration::from_millis(1),
        ))];
        let phase_one_until = Duration::from_secs(4);
        let phase_one_until_block = 1024;

        let mut nodes = Nodes::<S, RS, _, LoggerType, ME, ST, SignatureCollectionType>::new(
            pubkeys
                .iter()
                .copied()
                .zip(state_configs)
                .map(|(pubkey, state_config)| {
                    (
                        pubkey,
                        state_config,
                        logger_config.clone(),
                        router_scheduler_config(
                            pubkeys.iter().copied().map(PeerId).collect(),
                            PeerId(pubkey),
                        ),
                        MockMempoolConfig::default(),
                        pipeline.clone(),
                        default_seed,
                    )
                })
                .collect(),
        );

        let f0 = failure_idx[0];
        let f1 = failure_idx[1];

        println!("f0 {:?}", pubkeys[f0]);
        println!("f1 {:?}", pubkeys[f1]);

        assert!(f0 < 4);
        assert!(f0 < f1);
        assert!(f1 < 4);

        // play nodes till some step
        let max_tick = Duration::from_nanos(0);
        let max_tick =
            run_nodes_until(&mut nodes, max_tick, phase_one_until, phase_one_until_block);

        // it should've reached some `until` condition, rather than losing liveness
        assert!(liveness::<S, RS, _, LoggerType, ME, _, _>(&nodes, 0));

        let phase_one_length = nodes
            .states()
            .values()
            .map(|node| node.executor.ledger().get_blocks().len())
            .max()
            .unwrap();

        // bring down 2 nodes
        let node0 = nodes
            .remove_state(&PeerId(pubkeys[f0]))
            .expect("peer0 exists");
        let node1 = nodes
            .remove_state(&PeerId(pubkeys[f1]))
            .expect("peer1 exists");

        let phase_two_until = max_tick + Duration::from_secs(4);
        let phase_two_until_block = 2048;

        // run the remaining 2 nodes for some steps, they can't make progress because less than f+1
        let max_tick =
            run_nodes_until(&mut nodes, max_tick, phase_two_until, phase_two_until_block);

        // nodes lost liveness because only 2/4 are online
        assert!(!liveness::<S, RS, _, LoggerType, ME, _, _>(
            &nodes,
            phase_one_length
        ));

        let phase_two_length = nodes
            .states()
            .values()
            .map(|node| node.executor.ledger().get_blocks().len())
            .max()
            .unwrap();

        // bring up failed nodes with the replay logs
        let node0_logger_config = MockMemLoggerConfig::new(node0.logger.log);
        let node1_logger_config = MockMemLoggerConfig::new(node1.logger.log);

        nodes.add_state((
            pubkeys[f0],
            state_configs_duplicate.remove(f0),
            node0_logger_config,
            router_scheduler_config(
                pubkeys.iter().copied().map(PeerId).collect(),
                PeerId(pubkeys[f0]),
            ),
            MockMempoolConfig::default(),
            pipeline.clone(),
            default_seed,
        ));

        nodes.add_state((
            pubkeys[f1],
            state_configs_duplicate.remove(f1 - 1),
            node1_logger_config,
            router_scheduler_config(
                pubkeys.iter().copied().map(PeerId).collect(),
                PeerId(pubkeys[f1]),
            ),
            MockMempoolConfig::default(),
            pipeline,
            default_seed,
        ));

        // assert consensus state is the same after replay
        let node0_consensus = node0.state.consensus();
        let node0_consensus_recovered = nodes
            .states()
            .get(&PeerId(pubkeys[f0]))
            .unwrap()
            .state
            .consensus();
        assert_eq!(node0_consensus, node0_consensus_recovered);

        let node1_consensus = node1.state.consensus();
        let node1_consensus_recovered = nodes
            .states()
            .get(&PeerId(pubkeys[f1]))
            .unwrap()
            .state
            .consensus();
        assert_eq!(node1_consensus, node1_consensus_recovered);

        // the nodes should resume progress
        let phase_three_until = max_tick + Duration::from_secs(4);
        let phase_three_until_block = 3072;

        run_nodes_until(
            &mut nodes,
            max_tick,
            phase_three_until,
            phase_three_until_block,
        );
        assert!(liveness::<S, RS, _, LoggerType, ME, _, _>(
            &nodes,
            phase_two_length
        ));

        node_ledger_verification(
            &nodes
                .states()
                .values()
                .map(|node| node.executor.ledger().get_blocks().clone())
                .collect(),
            phase_one_length + 1,
        );

        // assert that block sync isn't triggered
        logs_assert(|lines: &[&str]| {
            assert!(!lines.is_empty());
            match lines
                .iter()
                .filter(|line| line.contains("monotonic_counter.block_sync_request"))
                .count()
            {
                0 => Ok(()),
                n => Err(format!("Block sync triggered {} times", n)),
            }
        })
    }
}
