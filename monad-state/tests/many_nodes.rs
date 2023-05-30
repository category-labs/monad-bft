use std::time::Duration;

use monad_executor::mock_swarm::LatencyTransformer;

mod base;

#[test]
fn many_nodes() {
    tracing_subscriber::fmt::init();

    base::run_nodes(
        100,
        1024,
        Duration::from_millis(2),
        LatencyTransformer(Duration::from_millis(1)),
    );
}
