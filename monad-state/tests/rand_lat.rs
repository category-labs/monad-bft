use test_case::test_case;

mod base;

#[test_case(1; "seed1")]
#[test_case(2; "seed2")]
#[test_case(3; "seed3")]
#[test_case(4; "seed4")]
#[test_case(5; "seed5")]
#[test_case(6; "seed6")]
#[test_case(7; "seed7")]
#[test_case(8; "seed8")]
#[test_case(9; "seed9")]
#[test_case(10; "seed10")]
fn nodes_with_random_latency(seed: u64) {
    use monad_executor::mock_swarm::RandLatencyTransformer;
    use std::time::Duration;

    base::run_nodes(
        4,
        2048,
        Duration::from_millis(250),
        RandLatencyTransformer::new(seed, 330),
    );
}
