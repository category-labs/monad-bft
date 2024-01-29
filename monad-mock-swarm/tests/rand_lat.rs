mod common;
use std::{collections::BTreeSet, env};

use monad_async_state_verify::LocalAsyncStateVerify;
use monad_consensus_types::{
    block_validator::MockValidator, payload::StateRoot, txpool::MockTxPool,
};
use monad_crypto::certificate_signature::CertificateKeyPair;
use monad_mock_swarm::{
    mock_swarm::SwarmBuilder, node::NodeBuilder, swarm_relation::NoSerSwarm,
    terminator::UntilTerminator,
};
use monad_router_scheduler::{NoSerRouterConfig, RouterSchedulerBuilder};
use monad_testutil::swarm::{make_state_configs, swarm_ledger_verification};
use monad_transformer::{GenericTransformer, ID};
use monad_types::{NodeId, Round, SeqNum};
use monad_updaters::state_root_hash::MockStateRootHashNop;
use monad_validator::{simple_round_robin::SimpleRoundRobin, validator_set::ValidatorSetFactory};
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

    use monad_transformer::RandLatencyTransformer;

    let state_configs = make_state_configs::<NoSerSwarm>(
        4, // num_nodes
        ValidatorSetFactory::default,
        SimpleRoundRobin::default,
        MockTxPool::default,
        || MockValidator,
        || {
            StateRoot::new(
                // avoid state_root trigger in rand latency setting
                // TODO-1, cover cases with low state_root_delay once state_sync is done
                SeqNum(u64::MAX), // state_root_delay
            )
        },
        LocalAsyncStateVerify::default,
        Duration::from_millis(250), // delta
        0,                          // proposal_tx_limit
        SeqNum(2000),               // val_set_update_interval
        Round(50),                  // epoch_start_delay
    );
    let all_peers: BTreeSet<_> = state_configs
        .iter()
        .map(|state_config| NodeId::new(state_config.key.pubkey()))
        .collect();
    let swarm_config = SwarmBuilder::<NoSerSwarm>(
        state_configs
            .into_iter()
            .enumerate()
            .map(|(seed, state_builder)| {
                let validators = state_builder.validators.clone();
                NodeBuilder::<NoSerSwarm>::new(
                    ID::new(NodeId::new(state_builder.key.pubkey())),
                    state_builder,
                    MockWALoggerConfig::default(),
                    NoSerRouterConfig::new(all_peers.clone()).build(),
                    MockStateRootHashNop::new(validators, SeqNum(2000)),
                    vec![GenericTransformer::RandLatency(
                        RandLatencyTransformer::new(seed.try_into().unwrap(), 330),
                    )],
                    seed.try_into().unwrap(),
                )
            })
            .collect(),
    );

    let mut swarm = swarm_config.build();
    while swarm
        .step_until(&mut UntilTerminator::new().until_tick(Duration::from_secs(60 * 60)))
        .is_some()
    {}
    swarm_ledger_verification(&swarm, 2048);
}
