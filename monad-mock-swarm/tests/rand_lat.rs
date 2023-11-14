mod common;
use std::env;

use common::NoSerSwarm;
use monad_consensus_types::transaction_validator::MockValidator;
use monad_mock_swarm::{
    mock::{MockMempoolConfig, NoSerRouterConfig},
    mock_swarm::UntilTerminator,
    transformer::GenericTransformer,
};
use monad_testutil::swarm::{create_and_run_nodes, SwarmTestConfig};
use monad_wal::mock::MockWALoggerConfig;
use rand::{rngs::StdRng, Rng, SeedableRng};
use test_case::test_case;

#[test]
#[ignore = "cron_test"]
fn nodes_with_random_latency_cron() {
    let round = match env::var("NODES_WITH_RANDOM_LATENCY_ROUND") {
        Ok(v) => v.parse().unwrap(),
        Err(_e) => panic!("NODES_WITH_RANDOM_LATENCY_ROUND is not set"), // default to 1 if not found
    };

    match env::var("RANDOM_TEST_SEED") {
        Ok(v) => {
            let mut seed = v.parse().unwrap();
            let mut generator = StdRng::seed_from_u64(seed);
            for _ in 0..round {
                seed = generator.gen();
                println!("seed is set to be {}", seed);
                nodes_with_random_latency(seed);
            }
        }
        Err(_e) => {
            panic!("RANDOM_TEST_SEED is not set");
        }
    };
}

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
#[test_case(14710580201381303742; "seed11")]
fn nodes_with_random_latency(seed: u64) {
    use std::time::Duration;

    use monad_mock_swarm::transformer::RandLatencyTransformer;

    create_and_run_nodes::<NoSerSwarm, _, _>(
        MockValidator,
        |all_peers, _| NoSerRouterConfig {
            all_peers: all_peers.into_iter().collect(),
        },
        MockWALoggerConfig,
        MockMempoolConfig::default(),
        vec![GenericTransformer::RandLatency(
            RandLatencyTransformer::new(seed, 330),
        )],
        UntilTerminator::new().until_tick(Duration::from_secs(60 * 60)),
        SwarmTestConfig {
            num_nodes: 4,
            consensus_delta: Duration::from_millis(250),
            parallelize: false,
            expected_block: 2048,
            // avoid state_root trigger in rand latency setting
            // TODO-1, cover cases with low state_root_delay once state_sync is done
            state_root_delay: u64::MAX,
            seed: 1,
            proposal_size: 0,
        },
    );
}
