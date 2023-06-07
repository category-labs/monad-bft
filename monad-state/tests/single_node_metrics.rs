use std::time::Duration;

use monad_executor::mock_swarm::LatencyTransformer;
use monad_testutil::swarm::run_nodes;
use monad_tracing_counter::counter::CounterLayer;
use monad_tracing_counter::counter::MetricFilter;
use monad_tracing_counter::counter_status;

use tracing_core::LevelFilter;
use tracing_subscriber::filter::Targets;
use tracing_subscriber::prelude::*;
use tracing_subscriber::Registry;

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

    run_nodes(
        2,
        1024,
        Duration::from_millis(2),
        LatencyTransformer(Duration::from_millis(1)),
    );

    counter_status!();
}
