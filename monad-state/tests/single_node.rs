use std::time::Duration;

use monad_executor::{
    transformer::{LatencyTransformer, Transformer, TransformerPipeline},
    xfmr_pipe,
};
use monad_testutil::swarm::run_nodes;

#[test]
fn two_nodes() {
    tracing_subscriber::fmt::init();

    run_nodes(
        2,
        1024,
        Duration::from_millis(2),
        xfmr_pipe!(Transformer::Latency(LatencyTransformer(
            Duration::from_millis(1)
        ),)),
    );
}
