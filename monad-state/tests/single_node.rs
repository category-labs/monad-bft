use std::time::Duration;

use monad_executor::mock_swarm::LatencyTransformer;

mod base;

#[cfg(feature = "proto")]
#[test]
fn two_nodes() {
    tracing_subscriber::fmt::init();

    base::run_nodes(
        2,
        1024,
        Duration::from_millis(2),
        LatencyTransformer(Duration::from_millis(1)),
    );
}
