use std::{collections::BTreeSet, time::Duration};

use itertools::Itertools;
use monad_async_state_verify::{majority_threshold, PeerAsyncStateVerify};
use monad_consensus_types::{
    block::PassthruBlockPolicy, block_validator::MockValidator, metrics::Metrics,
    payload::StateRoot, txpool::MockTxPool,
};
use monad_crypto::{
    certificate_signature::{CertificateKeyPair, CertificateSignaturePubKey},
    NopSignature,
};
use monad_executor_glue::MonadEvent;
use monad_mock_swarm::{
    fetch_metric,
    mock::MockExecutor,
    mock_swarm::{Nodes, SwarmBuilder},
    node::{Node, NodeBuilder},
    swarm_relation::SwarmRelation,
    terminator::UntilTerminator,
    verifier::MockSwarmVerifier,
};
use monad_multi_sig::MultiSig;
use monad_router_scheduler::{NoSerRouterConfig, NoSerRouterScheduler, RouterSchedulerBuilder};
use monad_state::{MonadMessage, VerifiedMonadMessage};
use monad_testutil::swarm::{make_state_configs, swarm_ledger_verification};
use monad_transformer::{GenericTransformer, GenericTransformerPipeline, LatencyTransformer, ID};
use monad_types::{NodeId, Round, SeqNum};
use monad_updaters::state_root_hash::MockStateRootHashNop;
use monad_validator::{
    simple_round_robin::SimpleRoundRobin,
    validator_set::{ValidatorSetFactory, ValidatorSetTypeFactory},
};
use monad_wal::mock::{MockMemLogger, MockMemLoggerConfig};
use tracing_test::traced_test;

const CONSENSUS_DELTA: Duration = Duration::from_millis(10);

struct ReplaySwarm;
impl SwarmRelation for ReplaySwarm {
    type SignatureType = NopSignature;
    type SignatureCollectionType = MultiSig<Self::SignatureType>;
    type BlockPolicyType = PassthruBlockPolicy;

    type TransportMessage =
        VerifiedMonadMessage<Self::SignatureType, Self::SignatureCollectionType>;

    type BlockValidator = MockValidator;
    type StateRootValidator = StateRoot;
    type ValidatorSetTypeFactory =
        ValidatorSetFactory<CertificateSignaturePubKey<Self::SignatureType>>;
    type LeaderElection = SimpleRoundRobin<CertificateSignaturePubKey<Self::SignatureType>>;
    type TxPool = MockTxPool;
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

    type Logger = MockMemLogger<MonadEvent<Self::SignatureType, Self::SignatureCollectionType>>;

    type StateRootHashExecutor =
        MockStateRootHashNop<Self::SignatureType, Self::SignatureCollectionType>;
}

fn run_nodes_until<S>(
    nodes: &mut Nodes<S>,
    start_tick: Duration,
    until_tick: Duration,
    until_block: usize,
) -> Duration
where
    S: SwarmRelation,
    MockExecutor<S>: Unpin,
    Node<S>: Send,
{
    let mut max_tick = start_tick;
    let mut terminator = UntilTerminator::new()
        .until_tick(until_tick)
        .until_block(until_block);

    while let Some((tick, _, _)) = nodes.step_until(&mut terminator) {
        assert!(tick >= max_tick);
        max_tick = tick;
    }

    max_tick
}

fn liveness<S>(nodes: &Nodes<S>, last_ledger_len: usize) -> bool
where
    S: SwarmRelation,

    MockExecutor<S>: Unpin,
    Node<S>: Send,
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
    let state_configs = make_state_configs::<ReplaySwarm>(
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
        PeerAsyncStateVerify::new,
        CONSENSUS_DELTA,    // delta
        0,                  // proposal_tx_limit
        SeqNum(2000),       // val_set_update_interval
        Round(50),          // epoch_start_delay
        majority_threshold, // state root quorum threshold
        5,                  // max_blocksync_retries
        SeqNum(100),        // state_sync_threshold
    );
    let node_ids: Vec<_> = state_configs
        .iter()
        .map(|state_builder| NodeId::new(state_builder.key.pubkey()))
        .collect();
    let all_peers: BTreeSet<_> = node_ids.iter().copied().collect();
    let mut state_configs_duplicates = make_state_configs::<ReplaySwarm>(
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
        PeerAsyncStateVerify::new,
        CONSENSUS_DELTA,    // delta
        0,                  // proposal_tx_limit
        SeqNum(2000),       // val_set_update_interval
        Round(50),          // epoch_start_delay
        majority_threshold, // state root quorum threshold
        5,                  // max_blocksync_retries
        SeqNum(100),        // state_sync_threshold
    );

    let swarm_config = SwarmBuilder::<ReplaySwarm>(
        state_configs
            .into_iter()
            .map(|state_builder| {
                let me = NodeId::new(state_builder.key.pubkey());
                let validators = state_builder.validators.clone();
                NodeBuilder::new(
                    ID::new(me),
                    state_builder,
                    MockMemLoggerConfig::default(),
                    NoSerRouterConfig::new(all_peers.clone()).build(),
                    MockStateRootHashNop::new(validators, SeqNum(2000)),
                    vec![GenericTransformer::Latency(LatencyTransformer::new(
                        Duration::from_millis(1),
                    ))],
                    vec![],
                    default_seed,
                )
            })
            .collect(),
    );

    let mut swarm = swarm_config.build().can_fail_deliver();

    let phase_one_until = Duration::from_secs(4);
    let phase_one_until_block = 1024;

    let f0 = failure_idx[0];
    let f1 = failure_idx[1];

    println!("f0 {:?}", node_ids[f0]);
    println!("f1 {:?}", node_ids[f1]);

    assert!(f0 < 4);
    assert!(f0 < f1);
    assert!(f1 < 4);

    // play nodes till some step
    let max_tick = Duration::from_nanos(0);
    let max_tick = run_nodes_until(&mut swarm, max_tick, phase_one_until, phase_one_until_block);

    // it should've reached some `until` condition, rather than losing liveness
    assert!(liveness::<ReplaySwarm>(&swarm, 0));

    let phase_one_length = swarm
        .states()
        .values()
        .map(|node| node.executor.ledger().get_blocks().len())
        .max()
        .unwrap();
    let mut phase_1_verifier = MockSwarmVerifier::default();
    let ids: Vec<ID<_>> = swarm.states().keys().copied().collect_vec();
    phase_1_verifier.metrics_happy_path(&ids, &swarm);

    assert!(phase_1_verifier.verify(&swarm));

    println!("Phase 1 completes");
    // bring down 2 nodes
    let node0 = swarm
        .remove_state(&ID::new(node_ids[f0]))
        .expect("peer0 exists");
    let node1 = swarm
        .remove_state(&ID::new(node_ids[f1]))
        .expect("peer1 exists");

    let phase_two_until = max_tick + Duration::from_secs(4);
    let phase_two_until_block = 2048;

    // run the remaining 2 nodes for some steps, they can't make progress because less than f+1
    let max_tick = run_nodes_until(&mut swarm, max_tick, phase_two_until, phase_two_until_block);

    // nodes lost liveness because only 2/4 are online
    assert!(!liveness::<ReplaySwarm>(&swarm, phase_one_length));

    let phase_two_length = swarm
        .states()
        .values()
        .map(|node| node.executor.ledger().get_blocks().len())
        .max()
        .unwrap();

    let mut phase_2_verifier = MockSwarmVerifier::default();
    let ids: Vec<ID<_>> = swarm.states().keys().copied().collect_vec();
    phase_2_verifier
        .metric_exact(&ids, fetch_metric!(blocksync_events.blocksync_request), 0)
        // handle proposal for all blocks in ledger
        .metric_minimum(
            &ids,
            fetch_metric!(consensus_events.handle_proposal),
            phase_two_length as u64,
        )
        // vote for all blocks in ledger
        .metric_minimum(
            &ids,
            fetch_metric!(consensus_events.created_vote),
            phase_two_length as u64,
        )
        // enter rounds using QC except round 2
        .metric_minimum(
            &ids,
            fetch_metric!(consensus_events.enter_new_round_qc),
            phase_two_length as u64 - 1,
        )
        // initial TC to enter round 2 only
        .metric_exact(&ids, fetch_metric!(consensus_events.enter_new_round_tc), 1);

    assert!(phase_2_verifier.verify(&swarm));

    println!("Phase 2 completes");
    // bring up failed nodes with the replay logs
    let node0_logger_config = MockMemLoggerConfig::new(node0.logger.log);
    let node1_logger_config = MockMemLoggerConfig::new(node1.logger.log);

    {
        let state_builder = state_configs_duplicates.remove(f0);
        let validators = state_builder.validators.clone();
        swarm.add_state(NodeBuilder::new(
            ID::new(node_ids[f0]),
            state_builder,
            node0_logger_config,
            NoSerRouterConfig::new(all_peers.clone()).build(),
            MockStateRootHashNop::new(validators, SeqNum(2000)),
            vec![GenericTransformer::Latency(LatencyTransformer::new(
                Duration::from_millis(1),
            ))],
            vec![],
            default_seed,
        ));
    }

    {
        let state_builder = state_configs_duplicates.remove(f1 - 1);
        let validators = state_builder.validators.clone();
        swarm.add_state(NodeBuilder::new(
            ID::new(node_ids[f1]),
            state_builder,
            node1_logger_config,
            NoSerRouterConfig::new(all_peers).build(),
            MockStateRootHashNop::new(validators, SeqNum(2000)),
            vec![GenericTransformer::Latency(LatencyTransformer::new(
                Duration::from_millis(1),
            ))],
            vec![],
            default_seed,
        ));
    }

    println!("Replay finishes");

    // assert consensus state is the same after replay
    let node0_consensus = node0.state.consensus();
    let node0_consensus_recovered = swarm
        .states()
        .get(&ID::new(node_ids[f0]))
        .unwrap()
        .state
        .consensus();
    assert_eq!(node0_consensus, node0_consensus_recovered);

    let node1_consensus = node1.state.consensus();
    let node1_consensus_recovered = swarm
        .states()
        .get(&ID::new(node_ids[f1]))
        .unwrap()
        .state
        .consensus();
    assert_eq!(node1_consensus, node1_consensus_recovered);

    // the nodes should resume progress
    let phase_three_until = max_tick + Duration::from_secs(4);
    let phase_three_until_block = 3072;

    run_nodes_until(
        &mut swarm,
        max_tick,
        phase_three_until,
        phase_three_until_block,
    );
    assert!(liveness::<ReplaySwarm>(&swarm, phase_two_length));

    let phase_three_length = swarm
        .states()
        .values()
        .map(|node| node.executor.ledger().get_blocks().len())
        .max()
        .unwrap();

    let mut phase_3_verifier = MockSwarmVerifier::default();
    let ids: Vec<ID<_>> = swarm.states().keys().copied().collect_vec();
    phase_3_verifier
        .metric_exact(&ids, fetch_metric!(blocksync_events.blocksync_request), 0)
        // handle proposal for all blocks in ledger
        .metric_minimum(
            &ids,
            fetch_metric!(consensus_events.handle_proposal),
            phase_three_length as u64,
        )
        // vote for all blocks in ledger
        .metric_minimum(
            &ids,
            fetch_metric!(consensus_events.created_vote),
            phase_three_length as u64,
        )
        // enter rounds using QC except round 2
        .metric_minimum(
            &ids,
            fetch_metric!(consensus_events.enter_new_round_qc),
            phase_three_length as u64 - 1,
        );

    assert!(phase_3_verifier.verify(&swarm));

    swarm_ledger_verification(&swarm, phase_one_length + 1);

    println!("Phase 3 completes");
}
