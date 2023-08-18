use std::time::Duration;

use monad_executor::{
    transformer::{Transformer, TransformerPipeline, XorLatencyTransformer},
    xfmr_pipe,
};
use monad_testutil::swarm::run_nodes;

#[test]
fn two_nodes() {
    tracing_subscriber::fmt::init();

    run_nodes(
        4,
        40,
        Duration::from_millis(101),
        xfmr_pipe!(Transformer::XorLatency(XorLatencyTransformer(
            Duration::from_millis(u8::MAX as u64)
        ))),
    );
}
