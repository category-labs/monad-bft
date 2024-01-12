mod common;
use std::time::Duration;

use monad_consensus_types::block_validator::MockValidator;
use monad_mock_swarm::{mock_swarm::UntilTerminator, swarm_relation::NoSerSwarm};
use monad_router_scheduler::NoSerRouterConfig;
use monad_testutil::swarm::{create_and_run_nodes, SwarmTestConfig};
use monad_tracing_counter::{
    counter::{CounterLayer, MetricFilter},
    counter_status,
};
use monad_transformer::{GenericTransformer, LatencyTransformer};
use monad_types::{Round, SeqNum};
use monad_wal::mock::MockWALoggerConfig;
use tracing_core::LevelFilter;
use tracing_subscriber::{filter::Targets, prelude::*, Registry};

#[test]
fn two_nodes() {
    let fmt_layer = tracing_subscriber::fmt::layer();
    let counter_layer = CounterLayer::new();

    let subscriber = Registry::default()
        .with(
            fmt_layer
                .with_filter(MetricFilter {})
                .with_filter(Targets::new().with_default(LevelFilter::INFO)),
        )
        .with(counter_layer);

    tracing::subscriber::set_global_default(subscriber).expect("unable to set global subscriber");

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
    counter_status!();
}
