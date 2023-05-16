use std::time::Duration;

use monad_executor::mock_swarm::XorLatencyTransformer;

mod base;

#[test]
fn two_nodes() {
    tracing_subscriber::fmt::init();

    base::run_nodes(
        4,
        40,
        Duration::from_millis(101),
        XorLatencyTransformer(Duration::from_millis(u8::MAX as u64)),
    );
}
