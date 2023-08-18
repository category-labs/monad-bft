use std::time::Duration;

use monad_executor::{
    transformer::{LatencyTransformer, Transformer, TransformerPipeline},
    xfmr_pipe,
};
use monad_testutil::swarm::run_nodes;
use monad_tracing_counter::{
    counter::{CounterLayer, MetricFilter},
    counter_status,
};
use tracing_core::LevelFilter;
use tracing_subscriber::{filter::Targets, prelude::*, Registry};

#[test]
fn many_nodes() {
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
        100,
        1024,
        Duration::from_millis(2),
        xfmr_pipe!(Transformer::Latency(LatencyTransformer(
            Duration::from_millis(1)
        ))),
    );

    counter_status!();
}
