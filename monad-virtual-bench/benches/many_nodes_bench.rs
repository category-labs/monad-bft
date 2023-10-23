use std::time::{Duration, Instant};

use monad_block_sync::BlockSyncState;
use monad_consensus_state::ConsensusState;
use monad_consensus_types::{
    bls::BlsSignatureCollection, multi_sig::MultiSig, payload::StateRoot,
    transaction_validator::MockValidator,
};
use monad_crypto::NopSignature;
use monad_executor_glue::PeerId;
use monad_gossip::mock::{MockGossip, MockGossipConfig};
use monad_mock_swarm::{
    mock::{MockMempool, MockMempoolConfig},
    mock_swarm::UntilTerminator,
    transformer::{BwTransformer, BytesTransformer, LatencyTransformer},
};
use monad_quic::{QuicRouterScheduler, QuicRouterSchedulerConfig};
use monad_state::MonadState;
use monad_testutil::swarm::{create_and_run_nodes, SwarmTestConfig};
use monad_validator::{simple_round_robin::SimpleRoundRobin, validator_set::ValidatorSet};
use monad_wal::mock::{MockWALogger, MockWALoggerConfig};

fn setup() -> (
    SwarmTestConfig,
    impl Fn(Vec<PeerId>, PeerId) -> QuicRouterSchedulerConfig<MockGossipConfig>,
    Vec<BytesTransformer>,
    UntilTerminator,
) {
    let zero_instant = Instant::now();
    let stc = SwarmTestConfig {
        num_nodes: 50,
        consensus_delta: Duration::from_millis(10000),
        parallelize: true,
        expected_block: 95,
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
        BytesTransformer::Bw(BwTransformer::new(100)),
    ];
    let terminator = UntilTerminator::new().until_block(100);

    (stc, rsc, xfmrs, terminator)
}

fn many_nodes_nop_sct() -> u128 {
    let (stc, rsc, xfmrs, terminator) = setup();

    let duration = create_and_run_nodes::<
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
        QuicRouterScheduler<MockGossip>,
        _,
        MockWALogger<_>,
        _,
        MockValidator,
        MockMempool<_, _>,
        _,
    >(
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

fn many_nodes_bls_sct() -> u128 {
    let (stc, rsc, xfmrs, terminator) = setup();

    let duration = create_and_run_nodes::<
        MonadState<
            ConsensusState<BlsSignatureCollection, MockValidator, StateRoot>,
            NopSignature,
            BlsSignatureCollection,
            ValidatorSet,
            SimpleRoundRobin,
            BlockSyncState,
        >,
        NopSignature,
        BlsSignatureCollection,
        QuicRouterScheduler<MockGossip>,
        _,
        MockWALogger<_>,
        _,
        MockValidator,
        MockMempool<_, _>,
        _,
    >(
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

monad_virtual_bench::virtual_bench_main! {many_nodes_nop_sct, many_nodes_bls_sct}
