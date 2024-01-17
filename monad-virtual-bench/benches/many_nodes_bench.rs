use std::time::{Duration, Instant};

use bytes::Bytes;
use monad_bls::BlsSignatureCollection;
use monad_consensus_state::ConsensusState;
use monad_consensus_types::{
    block_validator::MockValidator, payload::StateRoot, txpool::MockTxPool,
};
use monad_crypto::{certificate_signature::CertificateSignaturePubKey, NopSignature};
use monad_executor::{timed_event::TimedEvent, State};
use monad_executor_glue::MonadEvent;
use monad_gossip::mock::{MockGossip, MockGossipConfig};
use monad_mock_swarm::{mock_swarm::UntilTerminator, swarm_relation::SwarmRelation};
use monad_multi_sig::MultiSig;
use monad_quic::{QuicRouterScheduler, QuicRouterSchedulerConfig};
use monad_state::{MonadMessage, MonadState, VerifiedMonadMessage};
use monad_testutil::swarm::{create_and_run_nodes, SwarmTestConfig};
use monad_transformer::{
    BwTransformer, BytesTransformer, BytesTransformerPipeline, LatencyTransformer,
};
use monad_types::{NodeId, Round, SeqNum};
use monad_updaters::state_root_hash::MockStateRootHashNop;
use monad_validator::{simple_round_robin::SimpleRoundRobin, validator_set::ValidatorSet};
use monad_wal::mock::{MockWALogger, MockWALoggerConfig};

type SignatureType = NopSignature;
type NodeIdPubKey = CertificateSignaturePubKey<NopSignature>;

fn setup() -> (
    SwarmTestConfig,
    impl Fn(
        Vec<NodeId<NodeIdPubKey>>,
        NodeId<NodeIdPubKey>,
    ) -> QuicRouterSchedulerConfig<MockGossip<NodeIdPubKey>>,
    Vec<BytesTransformer<NodeIdPubKey>>,
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
        val_set_update_interval: SeqNum(2000),
        epoch_start_delay: Round(50),
    };
    let rsc = move |all_peers: Vec<NodeId<NodeIdPubKey>>, me: NodeId<NodeIdPubKey>| {
        QuicRouterSchedulerConfig {
            zero_instant,
            all_peers: all_peers.iter().cloned().collect(),
            me,
            master_seed: 7,
            gossip: MockGossipConfig { all_peers, me }.build(),
        }
    };
    let xfmrs = vec![
        BytesTransformer::Latency(LatencyTransformer::new(Duration::from_millis(100))),
        BytesTransformer::Bw(BwTransformer::new(4, Duration::from_secs(1))),
    ];
    let terminator = UntilTerminator::new().until_round(Round(10));

    (stc, rsc, xfmrs, terminator)
}

struct NopSwarm;

impl SwarmRelation for NopSwarm {
    type SignatureType = SignatureType;
    type SignatureCollectionType = MultiSig<NopSignature>;

    type InboundMessage = MonadMessage<Self::SignatureType, Self::SignatureCollectionType>;
    type OutboundMessage = VerifiedMonadMessage<Self::SignatureType, Self::SignatureCollectionType>;
    type TransportMessage = Bytes;

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
        ValidatorSet<NodeIdPubKey>,
        SimpleRoundRobin,
        MockTxPool,
    >;

    type RouterSchedulerConfig = QuicRouterSchedulerConfig<MockGossip<NodeIdPubKey>>;
    type RouterScheduler =
        QuicRouterScheduler<MockGossip<NodeIdPubKey>, Self::InboundMessage, Self::OutboundMessage>;

    type Pipeline = BytesTransformerPipeline<NodeIdPubKey>;

    type LoggerConfig = MockWALoggerConfig;
    type Logger =
        MockWALogger<TimedEvent<MonadEvent<Self::SignatureType, Self::SignatureCollectionType>>>;

    type StateRootHashExecutor = MockStateRootHashNop<
        <Self::State as State>::Block,
        Self::SignatureType,
        Self::SignatureCollectionType,
    >;
}

struct BlsSwarm;

impl SwarmRelation for BlsSwarm {
    type SignatureType = SignatureType;
    type SignatureCollectionType = BlsSignatureCollection<NodeIdPubKey>;

    type InboundMessage = MonadMessage<Self::SignatureType, Self::SignatureCollectionType>;
    type OutboundMessage = VerifiedMonadMessage<Self::SignatureType, Self::SignatureCollectionType>;
    type TransportMessage = Bytes;

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
        ValidatorSet<NodeIdPubKey>,
        SimpleRoundRobin,
        MockTxPool,
    >;

    type RouterScheduler =
        QuicRouterScheduler<MockGossip<NodeIdPubKey>, Self::InboundMessage, Self::OutboundMessage>;
    type RouterSchedulerConfig = QuicRouterSchedulerConfig<MockGossip<NodeIdPubKey>>;

    type Pipeline = BytesTransformerPipeline<NodeIdPubKey>;

    type LoggerConfig = MockWALoggerConfig;
    type Logger =
        MockWALogger<TimedEvent<MonadEvent<Self::SignatureType, Self::SignatureCollectionType>>>;

    type StateRootHashExecutor = MockStateRootHashNop<
        <Self::State as State>::Block,
        Self::SignatureType,
        Self::SignatureCollectionType,
    >;
}

fn many_nodes_nop_timeout() -> u128 {
    let (stc, rsc, xfmrs, terminator) = setup();

    let duration = create_and_run_nodes::<NopSwarm, _, _>(
        MockValidator,
        rsc,
        MockWALoggerConfig,
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
        xfmrs,
        terminator,
        stc,
    );

    duration.as_millis()
}

monad_virtual_bench::virtual_bench_main! {many_nodes_nop_timeout, many_nodes_bls_timeout}
