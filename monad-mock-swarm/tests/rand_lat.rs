use std::env;

use monad_block_sync::BlockSyncState;
use monad_consensus_state::ConsensusState;
use monad_consensus_types::{
    multi_sig::MultiSig, payload::StateRoot, transaction_validator::MockValidator,
};
use monad_crypto::NopSignature;
use monad_mock_swarm::{
    mock::{MockMempool, NoSerRouterConfig, NoSerRouterScheduler},
    transformer::GenericTransformer,
};
use monad_state::{MonadMessage, MonadState};
use monad_testutil::swarm::{create_and_run_nodes, SwarmTestConfig};
use monad_validator::{simple_round_robin::SimpleRoundRobin, validator_set::ValidatorSet};
use monad_wal::mock::{MockWALogger, MockWALoggerConfig};
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
fn nodes_with_random_latency(seed: u64) {
    use std::time::Duration;

    use monad_mock_swarm::transformer::RandLatencyTransformer;

    create_and_run_nodes::<
        MonadState<
            ConsensusState<MultiSig<NopSignature>, MockValidator, StateRoot>,
            NopSignature,
            MultiSig<NopSignature>,
            ValidatorSet,
            SimpleRoundRobin,
            BlockSyncState,
        >,
        NopSignature,
        MultiSig<NopSignature>,
        NoSerRouterScheduler<MonadMessage<_, _>>,
        _,
        MockWALogger<_>,
        _,
        MockValidator,
        MockMempool<_, _>,
    >(
        MockValidator,
        |all_peers, _| NoSerRouterConfig {
            all_peers: all_peers.into_iter().collect(),
        },
        MockWALoggerConfig,
        vec![GenericTransformer::<
            MonadMessage<NopSignature, MultiSig<NopSignature>>,
        >::RandLatency(RandLatencyTransformer::new(
            seed, 330,
        ))],
        SwarmTestConfig {
            num_nodes: 4,
            consensus_delta: Duration::from_millis(250),
            parallelize: false,
            until: Duration::from_secs(60 * 60),
            until_block: usize::MAX,
            expected_block: 2048,
            seed: 1,
        },
    );
}
