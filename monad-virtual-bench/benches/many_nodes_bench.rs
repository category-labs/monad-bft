use std::time::{Duration, Instant};

use monad_consensus_types::{
    bls::BlsSignatureCollection, multi_sig::MultiSig, transaction_validator::MockValidator,
};
use monad_crypto::NopSignature;
use monad_executor::timed_event::TimedEvent;
use monad_executor_glue::{MonadEvent, PeerId};
use monad_gossip::mock::{MockGossip, MockGossipConfig};
use monad_mock_swarm::{
    mock::{MockMempool, MockMempoolConfig},
    mock_swarm::UntilTerminator,
    swarm_relation::{SwarmRelation, SwarmStateType},
    transformer::{BwTransformer, BytesTransformer, BytesTransformerPipeline, LatencyTransformer},
};
use monad_quic::{QuicRouterScheduler, QuicRouterSchedulerConfig};
use monad_state::{MonadMessage, VerifiedMonadMessage};
use monad_testutil::swarm::{create_and_run_nodes, SwarmTestConfig};
use monad_types::Round;
use monad_wal::mock::{MockWALogger, MockWALoggerConfig};

fn setup() -> (
    SwarmTestConfig,
    impl Fn(Vec<PeerId>, PeerId) -> QuicRouterSchedulerConfig<MockGossipConfig>,
    Vec<BytesTransformer>,
    UntilTerminator,
) {
    let zero_instant = Instant::now();
    let stc = SwarmTestConfig {
        num_nodes: 20,
        consensus_delta: Duration::from_millis(30),
        parallelize: false,
        expected_block: 0,
        state_root_delay: u64::MAX,
        seed: 1,
    };
    let rsc = move |all_peers: Vec<PeerId>, me: PeerId| QuicRouterSchedulerConfig {
        zero_instant,
        all_peers: all_peers.iter().cloned().collect(),
        me,
        tls_key_der: Vec::new(),
        master_seed: 7,
        gossip_config: MockGossipConfig { all_peers },
    };
    let xfmrs = vec![
        BytesTransformer::Latency(LatencyTransformer(Duration::from_millis(100))),
        BytesTransformer::Bw(BwTransformer::new(4)),
    ];
    let terminator = UntilTerminator::new().until_round(Round(10));

    (stc, rsc, xfmrs, terminator)
}

struct NopSwarm;

impl SwarmRelation for NopSwarm {
    type State = SwarmStateType<Self>;
    type SignatureType = NopSignature;
    type SignatureCollectionType = MultiSig<NopSignature>;
    type RouterScheduler = QuicRouterScheduler<MockGossip>;
    type Pipeline = BytesTransformerPipeline;
    type Logger =
        MockWALogger<TimedEvent<MonadEvent<Self::SignatureType, Self::SignatureCollectionType>>>;
    type MempoolExecutor = MockMempool<Self::SignatureType, Self::SignatureCollectionType>;
    type TransactionValidator = MockValidator;
    type LoggerConfig = MockWALoggerConfig;
    type RouterSchedulerConfig = QuicRouterSchedulerConfig<MockGossipConfig>;
    type MempoolConfig = MockMempoolConfig;
    type Message = Vec<u8>;
    type StateMessage = MonadMessage<Self::SignatureType, Self::SignatureCollectionType>;
    type OutboundStateMessage =
        VerifiedMonadMessage<Self::SignatureType, Self::SignatureCollectionType>;
}

struct BlsSwarm;

impl SwarmRelation for BlsSwarm {
    type State = SwarmStateType<Self>;
    type SignatureType = NopSignature;
    type SignatureCollectionType = BlsSignatureCollection;
    type RouterScheduler = QuicRouterScheduler<MockGossip>;
    type Pipeline = BytesTransformerPipeline;
    type Logger =
        MockWALogger<TimedEvent<MonadEvent<Self::SignatureType, Self::SignatureCollectionType>>>;
    type MempoolExecutor = MockMempool<Self::SignatureType, Self::SignatureCollectionType>;
    type TransactionValidator = MockValidator;
    type LoggerConfig = MockWALoggerConfig;
    type RouterSchedulerConfig = QuicRouterSchedulerConfig<MockGossipConfig>;
    type MempoolConfig = MockMempoolConfig;
    type Message = Vec<u8>;
    type StateMessage = MonadMessage<Self::SignatureType, Self::SignatureCollectionType>;
    type OutboundStateMessage =
        VerifiedMonadMessage<Self::SignatureType, Self::SignatureCollectionType>;
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
