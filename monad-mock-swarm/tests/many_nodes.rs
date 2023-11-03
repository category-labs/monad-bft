mod common;
use std::time::{Duration, Instant};

use common::{NoSerSwarm, QuicSwarm};
use monad_consensus_types::transaction_validator::MockValidator;
use monad_gossip::mock::MockGossipConfig;
use monad_mock_swarm::{
    mock::{MockMempoolConfig, NoSerRouterConfig},
    mock_swarm::UntilTerminator,
    transformer::{BwTransformer, BytesTransformer, GenericTransformer, LatencyTransformer},
};
use monad_quic::QuicRouterSchedulerConfig;
use monad_testutil::swarm::{create_and_run_nodes, SwarmTestConfig};
use monad_wal::mock::MockWALoggerConfig;

#[test]
fn many_nodes_noser() {
    create_and_run_nodes::<NoSerSwarm, _, _>(
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
            proposal_size: 0,
        },
    );
}

#[test]
fn many_nodes_quic() {
    let zero_instant = Instant::now();

    create_and_run_nodes::<QuicSwarm, _, _>(
        MockValidator,
        |all_peers, me| QuicRouterSchedulerConfig {
            zero_instant,
            all_peers: all_peers.iter().cloned().collect(),
            me,

            tls_key_der: Vec::new(),
            master_seed: 7,

            gossip: MockGossipConfig { all_peers }.build(),
        },
        MockWALoggerConfig,
        MockMempoolConfig::default(),
        vec![BytesTransformer::Latency(LatencyTransformer(
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
            proposal_size: 150,
        },
    );
}

#[test]
fn many_nodes_quic_bw() {
    let zero_instant = Instant::now();

    let swarm_config = SwarmTestConfig {
        num_nodes: 40,
        consensus_delta: Duration::from_millis(1000),
        parallelize: true,
        expected_block: 10,
        state_root_delay: u64::MAX,
        seed: 1,
        proposal_size: 150,
    };

    let xfmrs = vec![
        BytesTransformer::Latency(LatencyTransformer(Duration::from_millis(1))),
        BytesTransformer::Bw(BwTransformer::new(5)),
    ];

    create_and_run_nodes::<QuicSwarm, _, _>(
        MockValidator,
        |all_peers, me| QuicRouterSchedulerConfig {
            zero_instant,
            all_peers: all_peers.iter().cloned().collect(),
            me,

            tls_key_der: Vec::new(),
            master_seed: 7,

            gossip: MockGossipConfig { all_peers }.build(),
        },
        MockWALoggerConfig,
        MockMempoolConfig::default(),
        xfmrs,
        UntilTerminator::new().until_tick(Duration::from_secs(100)),
        swarm_config,
    );
}
