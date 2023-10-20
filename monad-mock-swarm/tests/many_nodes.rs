use std::time::{Duration, Instant};

use monad_block_sync::BlockSyncState;
use monad_consensus_state::ConsensusState;
use monad_consensus_types::{
    multi_sig::MultiSig, payload::StateRoot, transaction_validator::MockValidator,
};
use monad_crypto::NopSignature;
use monad_gossip::mock::{MockGossip, MockGossipConfig};
use monad_mock_swarm::{
    mock::{MockMempool, MockMempoolConfig, NoSerRouterConfig, NoSerRouterScheduler},
    mock_swarm::UntilTerminator,
    transformer::{BwTransformer, BytesTransformer, GenericTransformer, LatencyTransformer},
};
use monad_quic::{QuicRouterScheduler, QuicRouterSchedulerConfig};
use monad_state::{MonadMessage, MonadState};
use monad_testutil::swarm::{create_and_run_nodes, SwarmTestConfig};
use monad_validator::{simple_round_robin::SimpleRoundRobin, validator_set::ValidatorSet};
use monad_wal::mock::{MockWALogger, MockWALoggerConfig};
use tracing_test::traced_test;

#[test]
fn many_nodes_noser() {
    create_and_run_nodes::<
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
        MockValidator,
        MockMempool<_, _>,
        _,
    >(
        MockValidator,
        |all_peers, _| NoSerRouterConfig {
            all_peers: all_peers.into_iter().collect(),
        },
        MockWALoggerConfig,
        MockMempoolConfig::default(),
        vec![GenericTransformer::Latency(LatencyTransformer(
            Duration::from_millis(1),
        ))],
        UntilTerminator::new().until_tick(Duration::from_secs(4)),
        SwarmTestConfig {
            num_nodes: 100,
            consensus_delta: Duration::from_millis(2),
            parallelize: true,
            expected_block: 1024,
            state_root_delay: 4,
            seed: 1,
        },
    );
}

#[test]
fn many_nodes_quic() {
    let zero_instant = Instant::now();

    create_and_run_nodes::<
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
        |all_peers, me| QuicRouterSchedulerConfig {
            zero_instant,
            all_peers: all_peers.iter().cloned().collect(),
            me,

            gossip_config: MockGossipConfig { all_peers },
        },
        MockWALoggerConfig,
        MockMempoolConfig::default(),
        vec![GenericTransformer::Latency::<Vec<u8>>(LatencyTransformer(
            Duration::from_millis(1),
        ))],
        UntilTerminator::new().until_tick(Duration::from_secs(4)),
        SwarmTestConfig {
            num_nodes: 40,
            consensus_delta: Duration::from_millis(10),
            parallelize: true,
            expected_block: 10,
            state_root_delay: 4,
            seed: 1,
        },
    );
}

#[traced_test]
#[test]
fn many_nodes_quic_bw() {
    let zero_instant = Instant::now();

    let swarm_config = SwarmTestConfig {
        num_nodes: 40,
        consensus_delta: Duration::from_millis(1000),
        parallelize: false,
        expected_block: 10,
        state_root_delay: u64::MAX,
        seed: 1,
    };

    let xfmrs = vec![
        BytesTransformer::Latency(LatencyTransformer(Duration::from_millis(1))),
        BytesTransformer::Bw(BwTransformer::new(3)),
    ];

    create_and_run_nodes::<
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
        |all_peers, me| QuicRouterSchedulerConfig {
            zero_instant,
            all_peers: all_peers.iter().cloned().collect(),
            me,

            gossip_config: MockGossipConfig { all_peers },
        },
        MockWALoggerConfig,
        MockMempoolConfig::default(),
        xfmrs,
        UntilTerminator::new().until_tick(Duration::from_secs(100)),
        swarm_config,
    );

    logs_assert(|lines| {
        if lines
            .iter()
            .filter(|line| line.contains("monotonic_counter.bwtransfomer_dropped_msg"))
            .count()
            > 0
        {
            Ok(())
        } else {
            Err(format!("Expected msg to be dropped"))
        }
    });
}
