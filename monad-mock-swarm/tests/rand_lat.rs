use std::{collections::BTreeSet, env};

use itertools::Itertools;

use monad_consensus_types::{
    block::PassthruBlockPolicy, block_validator::MockValidator, metrics::Metrics,
    txpool::MockTxPool,
};
use monad_crypto::certificate_signature::CertificateKeyPair;
use monad_mock_swarm::{
    fetch_metric,
    mock::TimestamperConfig,
    mock_swarm::SwarmBuilder,
    node::NodeBuilder,
    swarm_relation::NoSerSwarm,
    terminator::UntilTerminator,
    verifier::{happy_path_tick_by_block, MockSwarmVerifier},
};
use monad_router_scheduler::{NoSerRouterConfig, RouterSchedulerBuilder};
use monad_state_backend::InMemoryStateInner;
use monad_testutil::swarm::{make_state_configs, swarm_ledger_verification};
use monad_transformer::{GenericTransformer, ID};
use monad_types::{NodeId, Round, SeqNum};
use monad_updaters::{
    ledger::MockLedger, state_root_hash::MockStateRootHashNop, statesync::MockStateSyncExecutor,
};
use monad_validator::{simple_round_robin::SimpleRoundRobin, validator_set::ValidatorSetFactory};
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
#[test_case(11282773634027867923; "seed12")]
#[test_case(11868595526945931122; "seed13")]
#[test_case(4712443726697299681; "seed14")]
fn nodes_with_random_latency(latency_seed: u64) {
    use std::time::Duration;

    use monad_transformer::RandLatencyTransformer;

    let delta = Duration::from_millis(200);
    let vote_pace = Duration::from_millis(10);
    let state_configs = make_state_configs::<NoSerSwarm>(
        4, // num_nodes
        ValidatorSetFactory::default,
        SimpleRoundRobin::default,
        MockTxPool::default,
        || MockValidator,
        || PassthruBlockPolicy,
        || InMemoryStateInner::genesis(u128::MAX, SeqNum(10_000_000)),
        SeqNum(10_000_000), // state_root_delay
        delta,              // delta
        vote_pace,          // vote pace
        0,                  // proposal_tx_limit
        SeqNum(3000),       // val_set_update_interval
        Round(50),          // epoch_start_delay
        SeqNum(100),        // state_sync_threshold
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
                let state_backend = state_builder.state_backend.clone();
                let validators = state_builder.forkpoint.validator_sets[0].clone();
                NodeBuilder::<NoSerSwarm>::new(
                    ID::new(NodeId::new(state_builder.key.pubkey())),
                    state_builder,
                    NoSerRouterConfig::new(all_peers.clone()).build(),
                    MockStateRootHashNop::new(validators.validators.clone(), SeqNum(3000)),
                    MockLedger::new(state_backend.clone()),
                    MockStateSyncExecutor::new(
                        state_backend,
                        validators
                            .validators
                            .0
                            .into_iter()
                            .map(|v| v.node_id)
                            .collect(),
                    ),
                    vec![GenericTransformer::RandLatency(
                        RandLatencyTransformer::new(latency_seed, delta),
                    )],
                    vec![],
                    TimestamperConfig::default(),
                    seed.try_into().unwrap(),
                )
            })
            .collect(),
    );

    let mut swarm = swarm_config.build();
    let last_block = 2000;
    while swarm
        .step_until(&mut UntilTerminator::new().until_block(last_block))
        .is_some()
    {}

    // -5 is arbitrary. this is to ensure that nodes aren't lagging too
    // far behind because of the latency
    let min_ledger_len = last_block - 5;
    let max_blocksync_requests = 40;
    let max_tick = happy_path_tick_by_block(min_ledger_len, delta);
    println!(
        "tick {:?} max tick {:?}",
        swarm.peek_tick().unwrap(),
        max_tick
    );
    let mut verifier = MockSwarmVerifier::default().tick_range(max_tick / 2, max_tick / 2);

    let node_ids = swarm.states().keys().copied().collect_vec();
    verifier
        // the node with the max blocksync requests could have the least
        // blocks in its ledger.
        // the last_block is committed by processing 2 QCs after it. there
        // should be no branching
        .metric_range(
            &node_ids,
            fetch_metric!(consensus_events.process_qc),
            min_ledger_len as u64 - max_blocksync_requests,
            last_block as u64 + 2,
        )
        .metric_maximum(
            &node_ids,
            fetch_metric!(blocksync_events.self_headers_request),
            max_blocksync_requests,
        )
        .metric_maximum(
            &node_ids,
            fetch_metric!(blocksync_events.self_payload_request),
            max_blocksync_requests,
        );

    assert!(verifier.verify(&swarm));

    swarm_ledger_verification(&swarm, min_ledger_len);
}
