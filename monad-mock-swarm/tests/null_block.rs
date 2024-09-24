#[cfg(test)]
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
        payload::{MissingNextStateRoot, StateRoot, StateRootValidator},
        signature_collection::SignatureCollectionPubKeyType,
        txpool::MockTxPool,
    };
    use monad_crypto::{
        certificate_signature::{CertificateKeyPair, CertificateSignaturePubKey},
        NopSignature,
    };
    use monad_mock_swarm::{
        fetch_metric,
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
    use monad_state_backend::{InMemoryState, InMemoryStateInner};
    use monad_testutil::swarm::{make_state_configs, swarm_ledger_verification};
    use monad_transformer::{
        DropTransformer, GenericTransformer, GenericTransformerPipeline, LatencyTransformer,
        PartitionTransformer, ID,
    };
    use monad_types::{NodeId, Round, SeqNum};
    use monad_updaters::{
        ledger::{MockLedger, MockableLedger},
        state_root_hash::MockStateRootHashNop,
        statesync::MockStateSyncExecutor,
    };
    use monad_validator::{
        simple_round_robin::SimpleRoundRobin,
        validator_set::{ValidatorSetFactory, ValidatorSetTypeFactory},
    };
    use tracing::info;

    pub struct NullBlockSwarm;
    impl SwarmRelation for NullBlockSwarm {
        type SignatureType = NopSignature;
        type SignatureCollectionType = MultiSig<Self::SignatureType>;
        type BlockPolicyType = PassthruBlockPolicy;
        type StateBackendType = InMemoryState;

        type TransportMessage =
            VerifiedMonadMessage<Self::SignatureType, Self::SignatureCollectionType>;

        type BlockValidator = MockValidator;
        type StateRootValidator = Box<dyn StateRootValidator + Send + Sync>;
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
            MockStateRootHashNop<Self::SignatureType, Self::SignatureCollectionType>;
        type StateSyncExecutor =
            MockStateSyncExecutor<Self::SignatureType, Self::SignatureCollectionType>;
    }

    const CONSENSUS_DELTA: Duration = Duration::from_millis(100);

    // generate swarm of four nodes. Node0 uses MissingNextStateRoot, while
    // others uses normal state root validators. As a result, node0 always
    // propose null blocks
    fn generate_swarm() -> (
        Nodes<NullBlockSwarm>,
        NodeId<
            SignatureCollectionPubKeyType<
                <NullBlockSwarm as SwarmRelation>::SignatureCollectionType,
            >,
        >,
    ) {
        let execution_delay = SeqNum(4);

        let mut state_configs = make_state_configs::<NullBlockSwarm>(
            4, // num_nodes
            ValidatorSetFactory::default,
            SimpleRoundRobin::default,
            MockTxPool::default,
            || MockValidator,
            || PassthruBlockPolicy,
            || InMemoryStateInner::genesis(u128::MAX, execution_delay),
            || Box::new(StateRoot::new(execution_delay)),
            PeerAsyncStateVerify::new,
            CONSENSUS_DELTA,
            10,                 // proposal_tx_limit
            SeqNum(2000),       // val_set_update_interval
            Round(50),          // epoch_start_delay
            majority_threshold, // state root quorum threshold
            SeqNum(100),        // state_sync_threshold
        );

        // change state root validator of node0
        let null_proposer_id = NodeId::new(state_configs[0].key.pubkey());
        state_configs[0].state_root_validator = Box::new(MissingNextStateRoot {});

        let all_peers: BTreeSet<_> = state_configs
            .iter()
            .map(|state_config| NodeId::new(state_config.key.pubkey()))
            .collect();
        let swarm_config = SwarmBuilder::<NullBlockSwarm>(
            state_configs
                .into_iter()
                .enumerate()
                .map(|(seed, state_builder)| {
                    let state_backend = state_builder.state_backend.clone();
                    let validators = state_builder.forkpoint.validator_sets[0].clone();
                    NodeBuilder::<NullBlockSwarm>::new(
                        ID::new(NodeId::new(state_builder.key.pubkey())),
                        state_builder,
                        NoSerRouterConfig::new(all_peers.clone()).build(),
                        MockStateRootHashNop::new(validators.validators.clone(), SeqNum(2000)),
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

        (swarm_config.build(), null_proposer_id)
    }

    #[test]
    fn test_null_block() {
        let (mut swarm, null_proposer_id) = generate_swarm();

        let all_nodes = swarm.states().keys().cloned().collect::<Vec<_>>();
        let null_proposer = vec![all_nodes
            .iter()
            .find(|id| id.get_peer_id() == &null_proposer_id)
            .cloned()
            .unwrap()];

        let normal_proposers = swarm
            .states()
            .keys()
            .filter_map(|id| {
                if id.get_peer_id() == &null_proposer_id {
                    None
                } else {
                    Some(*id)
                }
            })
            .collect::<Vec<_>>();
        assert_eq!(normal_proposers.len(), 3);

        while swarm
            .step_until(&mut UntilTerminator::new().until_block(1001))
            .is_some()
        {}

        let min_ledger_len = 1000;
        swarm_ledger_verification(&swarm, min_ledger_len);

        let node_ids = swarm.states().keys().copied().collect_vec();

        let mut verifier = MockSwarmVerifier::default().tick_range(
            happy_path_tick_by_block(1001, CONSENSUS_DELTA),
            CONSENSUS_DELTA,
        );
        verifier.metrics_happy_path(&node_ids, &swarm);

        // override metric verification related to empty blocks and state root
        // updates
        //
        // null proposer doesn't request/need state root updates. Its state root
        // validator always returns None for get_next_state_root
        verifier
            .metric_minimum(
                &null_proposer,
                fetch_metric!(consensus_events.creating_empty_block_proposal),
                250,
            )
            .metric_maximum(
                &null_proposer,
                fetch_metric!(consensus_events.creating_proposal),
                0,
            )
            .metric_minimum(
                &all_nodes,
                fetch_metric!(consensus_events.state_root_update),
                0,
            );
        assert!(verifier.verify(&swarm));

        swarm_ledger_verification(&swarm, 1001);

        // check we have roughly 1/4 empty blocks in the ledger
        let ledger = swarm
            .states()
            .values()
            .next()
            .unwrap()
            .executor
            .ledger()
            .get_blocks();
        let null_blocks_count = ledger
            .iter()
            .filter(|(_r, b)| b.is_empty_block())
            .collect::<Vec<_>>()
            .len();

        assert!(
            null_blocks_count >= 250,
            "null block count {}",
            null_blocks_count
        );
    }

    #[test]
    fn test_blocksync_null_block() {
        let (mut swarm, null_proposer_id) = generate_swarm();
        let all_nodes = swarm.states().keys().copied().collect_vec();
        let (&behind_node, _other_nodes) = all_nodes.split_first().unwrap();
        assert_ne!(behind_node.get_peer_id(), &null_proposer_id);

        info!("catchup node {}", behind_node);

        let filter_catchup_node = HashSet::from([behind_node]);
        let filter_pipeline = vec![
            GenericTransformer::Latency(LatencyTransformer::new(CONSENSUS_DELTA)),
            GenericTransformer::Partition(PartitionTransformer(filter_catchup_node)),
            GenericTransformer::Drop(DropTransformer::new()),
        ];

        swarm.update_outbound_pipeline_for_all(filter_pipeline);

        while swarm
            .step_until(&mut UntilTerminator::new().until_block(50))
            .is_some()
        {}

        // restore normal pipeline
        let regular_pipeline = vec![GenericTransformer::Latency(LatencyTransformer::new(
            CONSENSUS_DELTA,
        ))];
        swarm.update_outbound_pipeline_for_all(regular_pipeline);

        // run sufficiently long so that behind_node can catch up
        while swarm
            .step_until(&mut UntilTerminator::new().until_block(100))
            .is_some()
        {}

        // behind node has synced all block, including null blocks,  and
        // committed same ledger
        swarm_ledger_verification(&swarm, 100);
    }
}
