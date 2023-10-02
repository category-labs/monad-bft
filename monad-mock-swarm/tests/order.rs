use std::{collections::HashSet, env, time::Duration};

use monad_block_sync::BlockSyncState;
use monad_consensus_state::ConsensusState;
use monad_consensus_types::{
    multi_sig::MultiSig, payload::StateRoot, transaction_validator::MockValidator,
};
use monad_crypto::NopSignature;
use monad_executor_glue::PeerId;
use monad_mock_swarm::{
    mock::{MockMempool, NoSerRouterConfig, NoSerRouterScheduler},
    transformer::{
        GenericTransformer, LatencyTransformer, PartitionTransformer, ReplayTransformer,
        TransformerReplayOrder,
    },
};
use monad_state::{MonadMessage, MonadState};
use monad_testutil::swarm::{get_configs, run_nodes_until};
use monad_validator::{simple_round_robin::SimpleRoundRobin, validator_set::ValidatorSet};
use monad_wal::mock::{MockWALogger, MockWALoggerConfig};
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
    let num_nodes = 4;
    let delta = Duration::from_millis(2);
    let (pubkeys, state_configs) =
        get_configs::<NopSignature, MultiSig<NopSignature>, _>(MockValidator, num_nodes, delta);

    assert!(num_nodes >= 2, "test requires 2 or more nodes");

    let first_node = PeerId(*pubkeys.first().unwrap());

    let mut filter_peers = HashSet::new();
    filter_peers.insert(first_node);

    println!("delayed node ID: {:?}", first_node);

    run_nodes_until::<
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
        pubkeys,
        state_configs,
        |all_peers: Vec<_>, _| NoSerRouterConfig {
            all_peers: all_peers.into_iter().collect(),
        },
        MockWALoggerConfig,
        vec![
            GenericTransformer::Latency(LatencyTransformer(Duration::from_millis(1))),
            GenericTransformer::Partition(PartitionTransformer(filter_peers)),
            GenericTransformer::Replay(ReplayTransformer::new(
                Duration::from_millis(500),
                direction,
            )),
        ],
        false,
        Duration::from_secs(1),
        usize::MAX,
        20,
    );
}
