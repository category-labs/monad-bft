mod common;

use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};

use common::QuicSwarm;
use monad_consensus_types::block_validator::MockValidator;
use monad_gossip::mock::MockGossipConfig;
use monad_mock_swarm::{mock_swarm::UntilTerminator, swarm_relation::NoSerSwarm};
use monad_quic::QuicRouterSchedulerConfig;
use monad_router_scheduler::NoSerRouterConfig;
use monad_testutil::swarm::{create_and_run_nodes, SwarmTestConfig};
use monad_tracing_counter::counter::{counter_get, CounterLayer, MetricFilter};
use monad_transformer::{BwTransformer, BytesTransformer, GenericTransformer, LatencyTransformer};
use monad_types::{Round, SeqNum};
use monad_wal::mock::MockWALoggerConfig;
use tracing_core::LevelFilter;
use tracing_subscriber::{filter::Targets, prelude::*, Layer, Registry};

#[test]
fn two_nodes() {
    create_and_run_nodes::<NoSerSwarm, _, _>(
        MockValidator,
        |all_peers, _| NoSerRouterConfig {
            all_peers: all_peers.into_iter().collect(),
        },
        MockWALoggerConfig,
        vec![GenericTransformer::Latency(LatencyTransformer::new(
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
            val_set_update_interval: SeqNum(2000),
            epoch_start_delay: Round(50),
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

            gossip: MockGossipConfig { all_peers, me }.build(),
        },
        MockWALoggerConfig,
        vec![BytesTransformer::Latency(LatencyTransformer::new(
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
            val_set_update_interval: SeqNum(2000),
            epoch_start_delay: Round(50),
        },
    );
}

#[test]
fn two_nodes_quic_bw() {
    let fmt_layer = tracing_subscriber::fmt::layer();
    let counter = Arc::new(RwLock::new(HashMap::new()));
    let counter_layer = CounterLayer::new(Arc::clone(&counter));

    let subscriber = Registry::default()
        .with(
            fmt_layer
                .with_filter(MetricFilter {})
                .with_filter(Targets::new().with_default(LevelFilter::INFO)),
        )
        .with(counter_layer);
    tracing::subscriber::set_global_default(subscriber).expect("unable to set global subscriber");

    let zero_instant = Instant::now();

    create_and_run_nodes::<QuicSwarm, _, _>(
        MockValidator,
        |all_peers, me| QuicRouterSchedulerConfig {
            zero_instant,
            all_peers: all_peers.iter().cloned().collect(),
            me,

            master_seed: 7,

            gossip: MockGossipConfig { all_peers, me }.build(),
        },
        MockWALoggerConfig,
        vec![
            BytesTransformer::Latency(LatencyTransformer::new(Duration::from_millis(1))),
            BytesTransformer::Bw(BwTransformer::new(8, Duration::from_secs(1))),
        ],
        UntilTerminator::new().until_tick(Duration::from_secs(10)),
        SwarmTestConfig {
            num_nodes: 2,
            consensus_delta: Duration::from_millis(1000),
            parallelize: false,
            expected_block: 100,
            state_root_delay: 4,
            seed: 1,
            proposal_size: 100,
            val_set_update_interval: SeqNum(2000),
            epoch_start_delay: Round(50),
        },
    );

    let dropped_msg = counter_get(counter, None, "bwtransformer_dropped_msg");
    assert!(dropped_msg.is_some());
}
