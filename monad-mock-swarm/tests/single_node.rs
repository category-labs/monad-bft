mod common;

use std::time::{Duration, Instant};

use common::QuicSwarm;
use monad_consensus_types::transaction_validator::MockValidator;
use monad_gossip::mock::MockGossipConfig;
use monad_mock_swarm::{
    mock::MockMempoolConfig, mock_swarm::UntilTerminator, swarm_relation::NoSerSwarm,
};
use monad_quic::QuicRouterSchedulerConfig;
use monad_router_scheduler::NoSerRouterConfig;
use monad_testutil::swarm::{create_and_run_nodes, SwarmTestConfig};
use monad_transformer::{BwTransformer, BytesTransformer, GenericTransformer, LatencyTransformer};
use monad_wal::mock::MockWALoggerConfig;
use tracing_test::traced_test;

#[test]
fn two_nodes() {
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
        UntilTerminator::new().until_tick(Duration::from_secs(10)),
        SwarmTestConfig {
            num_nodes: 2,
            consensus_delta: Duration::from_millis(2),
            parallelize: false,
            expected_block: 1024,
            state_root_delay: 4,
            seed: 1,
            proposal_size: 0,
        },
    );
}

#[test]
fn two_nodes_quic() {
    let zero_instant = Instant::now();

    create_and_run_nodes::<QuicSwarm, _, _>(
        MockValidator,
        |all_peers, me| QuicRouterSchedulerConfig {
            zero_instant,
            all_peers: all_peers.iter().cloned().collect(),
            me,

            master_seed: 7,

            gossip: MockGossipConfig { all_peers }.build(),
        },
        MockWALoggerConfig,
        MockMempoolConfig::default(),
        vec![BytesTransformer::Latency(LatencyTransformer(
            Duration::from_millis(1),
        ))],
        UntilTerminator::new().until_tick(Duration::from_secs(10)),
        SwarmTestConfig {
            num_nodes: 2,
            consensus_delta: Duration::from_millis(10),
            parallelize: false,
            expected_block: 256,
            state_root_delay: 4,
            seed: 1,
            proposal_size: 150,
        },
    );
}

#[traced_test]
#[test]
fn two_nodes_quic_bw() {
    let zero_instant = Instant::now();

    create_and_run_nodes::<QuicSwarm, _, _>(
        MockValidator,
        |all_peers, me| QuicRouterSchedulerConfig {
            zero_instant,
            all_peers: all_peers.iter().cloned().collect(),
            me,

            master_seed: 7,

            gossip: MockGossipConfig { all_peers }.build(),
        },
        MockWALoggerConfig,
        MockMempoolConfig::default(),
        vec![
            BytesTransformer::Latency(LatencyTransformer(Duration::from_millis(1))),
            BytesTransformer::Bw(BwTransformer::new(8, Duration::from_secs(1))),
        ],
        UntilTerminator::new().until_tick(Duration::from_secs(5)),
        SwarmTestConfig {
            num_nodes: 2,
            consensus_delta: Duration::from_millis(1000),
            parallelize: false,
            expected_block: 100,
            state_root_delay: 4,
            seed: 1,
            proposal_size: 100,
        },
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
            Err("Expected msg to be dropped".to_owned())
        }
    });
}
