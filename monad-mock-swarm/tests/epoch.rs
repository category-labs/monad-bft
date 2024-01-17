mod common;

mod test {
    use std::{collections::HashSet, time::Duration};

    use itertools::Itertools;
    use monad_consensus_state::{ConsensusProcess, ConsensusState};
    use monad_consensus_types::{
        block_validator::MockValidator, payload::StateRoot, txpool::MockTxPool,
    };
    use monad_crypto::{certificate_signature::CertificateSignaturePubKey, NopSignature};
    use monad_executor::{timed_event::TimedEvent, State};
    use monad_executor_glue::MonadEvent;
    use monad_mock_swarm::{
        mock_swarm::{Node, Nodes, UntilTerminator},
        swarm_relation::{NoSerSwarm, SwarmRelation},
    };
    use monad_multi_sig::MultiSig;
    use monad_router_scheduler::{NoSerRouterConfig, NoSerRouterScheduler};
    use monad_state::{MonadMessage, MonadState, VerifiedMonadMessage};
    use monad_testutil::swarm::{create_and_run_nodes, get_configs, SwarmTestConfig};
    use monad_transformer::{
        DropTransformer, GenericTransformer, GenericTransformerPipeline, LatencyTransformer,
        PartitionTransformer, ID,
    };
    use monad_types::{Epoch, NodeId, Round, SeqNum};
    use monad_updaters::state_root_hash::MockStateRootHashSwap;
    use monad_validator::{simple_round_robin::SimpleRoundRobin, validator_set::ValidatorSet};
    use monad_wal::mock::{MockWALogger, MockWALoggerConfig};
    use test_case::test_case;

    pub struct ValidatorSwapSwarm;
    impl SwarmRelation for ValidatorSwapSwarm {
        type SignatureType = NopSignature;
        type SignatureCollectionType = MultiSig<Self::SignatureType>;

        type InboundMessage = MonadMessage<Self::SignatureType, Self::SignatureCollectionType>;
        type OutboundMessage =
            VerifiedMonadMessage<Self::SignatureType, Self::SignatureCollectionType>;
        type TransportMessage = Self::OutboundMessage;

        type TransactionValidator = MockValidator;

        type State = MonadState<
            ConsensusState<
                Self::SignatureType,
                Self::SignatureCollectionType,
                Self::TransactionValidator,
                StateRoot,
            >,
            Self::SignatureType,
            Self::SignatureCollectionType,
            ValidatorSet<CertificateSignaturePubKey<Self::SignatureType>>,
            SimpleRoundRobin,
            MockTxPool,
        >;

        type RouterSchedulerConfig =
            NoSerRouterConfig<CertificateSignaturePubKey<Self::SignatureType>>;
        type RouterScheduler = NoSerRouterScheduler<
            CertificateSignaturePubKey<Self::SignatureType>,
            Self::InboundMessage,
            Self::OutboundMessage,
        >;

        type Pipeline = GenericTransformerPipeline<
            CertificateSignaturePubKey<Self::SignatureType>,
            Self::TransportMessage,
        >;

        type LoggerConfig = MockWALoggerConfig;
        type Logger = MockWALogger<
            TimedEvent<MonadEvent<Self::SignatureType, Self::SignatureCollectionType>>,
        >;

        type StateRootHashExecutor = MockStateRootHashSwap<
            <Self::State as State>::Block,
            Self::SignatureType,
            Self::SignatureCollectionType,
        >;
    }

    fn verify_nodes_in_epoch(nodes: Vec<&Node<NoSerSwarm>>, epoch: Epoch) {
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
        nodes: Vec<&Node<NoSerSwarm>>,
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
        let num_nodes = 4;

        let delta = Duration::from_millis(2);
        let val_set_update_interval = SeqNum(1000);
        let epoch_start_delay = Round(20);

        let (pubkeys, state_configs) = get_configs::<NopSignature, MultiSig<NopSignature>, _>(
            MockValidator,
            num_nodes,
            delta,
            u64::MAX,
            0,
            val_set_update_interval,
            epoch_start_delay,
        );

        let mut nodes = Nodes::<NoSerSwarm>::new(
            pubkeys
                .iter()
                .copied()
                .zip(state_configs)
                .map(|(pubkey, state_config)| {
                    (
                        ID::new(NodeId::new(pubkey)),
                        state_config,
                        MockWALoggerConfig,
                        NoSerRouterConfig {
                            all_peers: pubkeys.iter().copied().map(NodeId::new).collect(),
                        },
                        vec![GenericTransformer::Latency(LatencyTransformer::new(
                            Duration::from_millis(1),
                        ))],
                        1,
                    )
                })
                .collect(),
        );

        let update_block_num = val_set_update_interval;

        let term_before_update_block =
            UntilTerminator::new().until_block((update_block_num.0 - 2) as usize);
        while nodes.step_until(&term_before_update_block).is_some() {}
        // all nodes must still be in this epoch
        verify_nodes_in_epoch(nodes.states().values().collect_vec(), Epoch(1));

        let term_on_schedule_epoch =
            UntilTerminator::new().until_block(update_block_num.0 as usize);
        while nodes.step_until(&term_on_schedule_epoch).is_some() {}

        // all nodes must still be in the same epoch but schedule next epoch
        verify_nodes_in_epoch(nodes.states().values().collect_vec(), Epoch(1));
        let epoch_start_round = verify_nodes_scheduled_epoch(
            nodes.states().values().collect_vec(),
            update_block_num,
            Epoch(2),
        );

        let term_on_new_epoch = UntilTerminator::new().until_round(epoch_start_round);
        while nodes.step_until(&term_on_new_epoch).is_some() {}

        // all nodes must have advanced to next epoch
        verify_nodes_in_epoch(nodes.states().values().collect_vec(), Epoch(2));
    }

    #[test]
    fn schedule_epoch_after_blocksync() {
        let num_nodes = 4;
        // need atleast 4 nodes (1 blackout node)
        assert!(num_nodes >= 4, "need atleast 4 nodes");

        let delta = Duration::from_millis(2);
        let val_set_update_interval = SeqNum(1000);
        let epoch_start_delay = Round(20);

        let (pubkeys, state_configs) = get_configs::<NopSignature, MultiSig<NopSignature>, _>(
            MockValidator,
            num_nodes,
            delta,
            u64::MAX,
            0,
            val_set_update_interval,
            epoch_start_delay,
        );

        let regular_pipeline = vec![GenericTransformer::Latency(LatencyTransformer::new(
            Duration::from_millis(1),
        ))];

        let mut nodes = Nodes::<NoSerSwarm>::new(
            pubkeys
                .iter()
                .copied()
                .zip(state_configs)
                .map(|(pubkey, state_config)| {
                    (
                        ID::new(NodeId::new(pubkey)),
                        state_config,
                        MockWALoggerConfig,
                        NoSerRouterConfig {
                            all_peers: pubkeys.iter().copied().map(NodeId::new).collect(),
                        },
                        regular_pipeline.clone(),
                        1,
                    )
                })
                .collect(),
        );

        let update_block_num = val_set_update_interval;

        let term_before_update_block =
            UntilTerminator::new().until_block((update_block_num.0 - 1) as usize);
        while nodes.step_until(&term_before_update_block).is_some() {}
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

        let term_on_schedule_epoch =
            UntilTerminator::new().until_block(update_block_num.0 as usize);
        while nodes.step_until(&term_on_schedule_epoch).is_some() {}

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
        let term_on_schedule_epoch_2 =
            UntilTerminator::new().until_block((update_block_num.0 + 10) as usize);
        while nodes.step_until(&term_on_schedule_epoch_2).is_some() {}

        // verify all nodes have scheduled next epoch (including blackout node)
        verify_nodes_scheduled_epoch(
            nodes.states().values().collect_vec(),
            update_block_num,
            Epoch(2),
        );
    }

    #[test_case(SeqNum(100), Round(10), 1000; "update_interval: 100, epoch_start_delay: 10")]
    #[test_case(SeqNum(500), Round(10), 5000; "update_interval: 500, epoch_start_delay: 10")]
    #[test_case(SeqNum(2000), Round(50), 20000; "update_interval: 2000, epoch_start_delay: 50")]
    fn validator_switching(
        val_set_update_interval: SeqNum,
        epoch_start_delay: Round,
        until_block: usize,
    ) {
        create_and_run_nodes::<ValidatorSwapSwarm, _, _>(
            MockValidator,
            |all_peers, _| NoSerRouterConfig {
                all_peers: all_peers.into_iter().collect(),
            },
            MockWALoggerConfig,
            vec![GenericTransformer::Latency(LatencyTransformer::new(
                Duration::from_millis(1),
            ))],
            UntilTerminator::new().until_block(until_block),
            SwarmTestConfig {
                num_nodes: 4,
                consensus_delta: Duration::from_millis(2),
                parallelize: false,
                expected_block: until_block,
                state_root_delay: 4,
                seed: 1,
                proposal_size: 0,
                val_set_update_interval,
                epoch_start_delay,
            },
        );
    }
}
