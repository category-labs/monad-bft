use std::time::{Duration, Instant};

use bytes::Bytes;
use monad_consensus_state::ConsensusState;
use monad_consensus_types::{
    block_validator::MockValidator, bls::BlsSignatureCollection, multi_sig::MultiSig,
    payload::StateRoot,
};
use monad_crypto::NopSignature;
use monad_executor::timed_event::TimedEvent;
use monad_executor_glue::MonadEvent;
use monad_gossip::mock::{MockGossip, MockGossipConfig};
use monad_mock_swarm::{
    mock::{MockMempool, MockMempoolConfig},
    mock_swarm::UntilTerminator,
    swarm_relation::SwarmRelation,
};
use monad_quic::{QuicRouterScheduler, QuicRouterSchedulerConfig};
use monad_state::{MonadMessage, MonadState, VerifiedMonadMessage};
use monad_testutil::swarm::{create_and_run_nodes, SwarmTestConfig};
use monad_transformer::{
    BwTransformer, BytesTransformer, BytesTransformerPipeline, LatencyTransformer,
};
use monad_types::{NodeId, Round};
use monad_validator::{simple_round_robin::SimpleRoundRobin, validator_set::ValidatorSet};
use monad_wal::mock::{MockWALogger, MockWALoggerConfig};

fn setup() -> (
    SwarmTestConfig,
    impl Fn(Vec<NodeId>, NodeId) -> QuicRouterSchedulerConfig<MockGossip>,
    Vec<BytesTransformer>,
    UntilTerminator,
) {
    let zero_instant = Instant::now();
    let stc = SwarmTestConfig {
        num_nodes: 20,
        // intentially smaller than latency so nodes keep timing out
        consensus_delta: Duration::from_millis(30),
        parallelize: false,
        expected_block: 0,
        state_root_delay: u64::MAX,
        seed: 1,
        proposal_size: 0,
    };
    let rsc = move |all_peers: Vec<NodeId>, me: NodeId| QuicRouterSchedulerConfig {
        zero_instant,
        all_peers: all_peers.iter().cloned().collect(),
        me,
        master_seed: 7,
        gossip: MockGossipConfig { all_peers, me }.build(),
    };
    let xfmrs = vec![
        BytesTransformer::Latency(LatencyTransformer(Duration::from_millis(100))),
        BytesTransformer::Bw(BwTransformer::new(4, Duration::from_secs(1))),
    ];
    let terminator = UntilTerminator::new().until_round(Round(10));

    (stc, rsc, xfmrs, terminator)
}

struct NopSwarm;

impl SwarmRelation for NopSwarm {
    type SignatureType = NopSignature;
    type SignatureCollectionType = MultiSig<NopSignature>;

    type InboundMessage = MonadMessage<Self::SignatureType, Self::SignatureCollectionType>;
    type OutboundMessage = VerifiedMonadMessage<Self::SignatureType, Self::SignatureCollectionType>;
    type TransportMessage = Bytes;

    type TransactionValidator = MockValidator;

    type State = MonadState<
        ConsensusState<Self::SignatureCollectionType, Self::TransactionValidator, StateRoot>,
        Self::SignatureType,
        Self::SignatureCollectionType,
        ValidatorSet,
        SimpleRoundRobin,
    >;

    type RouterSchedulerConfig = QuicRouterSchedulerConfig<MockGossip>;
    type RouterScheduler =
        QuicRouterScheduler<MockGossip, Self::InboundMessage, Self::OutboundMessage>;

    type Pipeline = BytesTransformerPipeline;

    type LoggerConfig = MockWALoggerConfig;
    type Logger =
        MockWALogger<TimedEvent<MonadEvent<Self::SignatureType, Self::SignatureCollectionType>>>;

    type MempoolConfig = MockMempoolConfig;
    type MempoolExecutor = MockMempool<Self::SignatureType, Self::SignatureCollectionType>;
}

struct BlsSwarm;

impl SwarmRelation for BlsSwarm {
    type SignatureType = NopSignature;
    type SignatureCollectionType = BlsSignatureCollection;

    type InboundMessage = MonadMessage<Self::SignatureType, Self::SignatureCollectionType>;
    type OutboundMessage = VerifiedMonadMessage<Self::SignatureType, Self::SignatureCollectionType>;
    type TransportMessage = Bytes;

    type TransactionValidator = MockValidator;

    type State = MonadState<
        ConsensusState<Self::SignatureCollectionType, Self::TransactionValidator, StateRoot>,
        Self::SignatureType,
        Self::SignatureCollectionType,
        ValidatorSet,
        SimpleRoundRobin,
    >;

    type RouterScheduler =
        QuicRouterScheduler<MockGossip, Self::InboundMessage, Self::OutboundMessage>;
    type RouterSchedulerConfig = QuicRouterSchedulerConfig<MockGossip>;

    type Pipeline = BytesTransformerPipeline;

    type LoggerConfig = MockWALoggerConfig;
    type Logger =
        MockWALogger<TimedEvent<MonadEvent<Self::SignatureType, Self::SignatureCollectionType>>>;

    type MempoolConfig = MockMempoolConfig;
    type MempoolExecutor = MockMempool<Self::SignatureType, Self::SignatureCollectionType>;
}

fn many_nodes_nop_timeout() -> u128 {
    let (stc, rsc, xfmrs, terminator) = setup();

    let duration = create_and_run_nodes::<NopSwarm, _, _>(
        MockValidator,
        rsc,
        MockWALoggerConfig,
        MockMempoolConfig::default(),
        xfmrs,
        terminator,
        stc,
    );

    duration.as_millis()
}

fn many_nodes_bls_timeout() -> u128 {
    let (stc, rsc, xfmrs, terminator) = setup();

    let duration = create_and_run_nodes::<BlsSwarm, _, _>(
        MockValidator,
        rsc,
        MockWALoggerConfig,
        MockMempoolConfig::default(),
        xfmrs,
        terminator,
        stc,
    );

    duration.as_millis()
}

monad_virtual_bench::virtual_bench_main! {many_nodes_nop_timeout, many_nodes_bls_timeout}
