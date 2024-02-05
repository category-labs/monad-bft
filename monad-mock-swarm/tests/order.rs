mod common;
use std::{
    collections::{BTreeSet, HashSet},
    env,
    time::Duration,
};

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
use monad_transformer::{
    GenericTransformer, LatencyTransformer, PartitionTransformer, ReplayTransformer,
    TransformerReplayOrder, ID,
};
use monad_types::{NodeId, Round, SeqNum};
use monad_updaters::state_root_hash::MockStateRootHashNop;
use monad_validator::{
    validator_set::ValidatorSetFactory, weighted_round_robin::WeightedRoundRobin,
};
use monad_wal::mock::MockWALoggerConfig;
use rand::{rngs::StdRng, Rng, SeedableRng};
use test_case::test_case;

#[test]
#[ignore = "cron_test"]
fn all_messages_delayed_cron() {
    let round = match env::var("ALL_MESSAGES_DELAYED_ROUND") {
        Ok(v) => v.parse().unwrap(),
        Err(_e) => panic!("ALL_MESSAGES_DELAYED_ROUND is not set"),
    };

    match env::var("RANDOM_TEST_SEED") {
        Ok(v) => {
            let mut seed = v.parse().unwrap();
            let mut generator = StdRng::seed_from_u64(seed);
            for _ in 0..round {
                seed = generator.gen();
                println!("seed for all_messages_delayed_cron is set to be {}", seed);
                all_messages_delayed(TransformerReplayOrder::Random(seed));
            }
        }
        Err(_e) => panic!("RANDOM_TEST_SEED is not set"),
    };
}
#[test_case(TransformerReplayOrder::Forward; "in order")]
#[test_case(TransformerReplayOrder::Reverse; "reverse order")]
#[test_case(TransformerReplayOrder::Random(1); "random seed 1")]
#[test_case(TransformerReplayOrder::Random(2); "random seed 2")]
#[test_case(TransformerReplayOrder::Random(3); "random seed 3")]
#[test_case(TransformerReplayOrder::Random(4); "random seed 4")]
#[test_case(TransformerReplayOrder::Random(5); "random seed 5")]
fn all_messages_delayed(direction: TransformerReplayOrder) {
    let state_configs = make_state_configs::<NoSerSwarm>(
        4, // num_nodes
        ValidatorSetFactory::default,
        WeightedRoundRobin::default,
        MockTxPool::default,
        || MockValidator,
        || {
            StateRoot::new(
                // due to the burst behavior of replay-transformer, its okay to have delay as 1
                // TODO-4?: Make Replay Transformer's stored message not burst within the same Duration
                SeqNum(1), // state_root_delay
            )
        },
        Duration::from_millis(2), // delta
        0,                        // proposal_tx_limit
        SeqNum(2000),             // val_set_update_interval
        Round(50),                // epoch_start_delay
    );
    let all_peers: BTreeSet<_> = state_configs
        .iter()
        .map(|state_config| NodeId::new(state_config.key.pubkey()))
        .collect();

    let first_node = ID::new(*all_peers.first().unwrap());

    let mut filter_peers = HashSet::new();
    filter_peers.insert(first_node);

    println!("delayed node ID: {:?}", first_node);

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
                    vec![
                        GenericTransformer::Latency(LatencyTransformer::new(
                            Duration::from_millis(1),
                        )),
                        GenericTransformer::Partition(PartitionTransformer(filter_peers.clone())),
                        GenericTransformer::Replay(ReplayTransformer::new(
                            Duration::from_millis(500),
                            direction.clone(),
                        )),
                    ],
                    seed.try_into().unwrap(),
                )
            })
            .collect(),
    );

    let mut swarm = swarm_config.build();
    while swarm
        .step_until(&mut UntilTerminator::new().until_tick(Duration::from_secs(1)))
        .is_some()
    {}
    swarm_ledger_verification(&swarm, 20);
}
