mod common;
use std::{
    collections::{BTreeSet, HashSet},
    env,
    time::Duration,
};

use itertools::Itertools;
use monad_async_state_verify::{majority_threshold, PeerAsyncStateVerify};
use monad_consensus_types::{
    block_validator::MockValidator, metrics::Metrics, payload::StateRoot, txpool::MockTxPool,
};
use monad_crypto::certificate_signature::CertificateKeyPair;
use monad_mock_swarm::{
    fetch_metric, mock_swarm::SwarmBuilder, node::NodeBuilder, swarm_relation::NoSerSwarm,
    terminator::UntilTerminator, verifier::MockSwarmVerifier,
};
use monad_router_scheduler::{NoSerRouterConfig, RouterSchedulerBuilder};
use monad_testutil::swarm::{make_state_configs, swarm_ledger_verification};
use monad_transformer::{
    GenericTransformer, LatencyTransformer, PartitionTransformer, ReplayTransformer,
    TransformerReplayOrder, ID,
};
use monad_types::{NodeId, Round, SeqNum};
use monad_updaters::state_root_hash::MockStateRootHashNop;
use monad_validator::{simple_round_robin::SimpleRoundRobin, validator_set::ValidatorSetFactory};
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
    // tracing_subscriber::fmt::init();
    let delta = Duration::from_millis(1);
    let max_blocksync_retries = 5;
    let state_configs = make_state_configs::<NoSerSwarm>(
        4, // num_nodes
        ValidatorSetFactory::default,
        SimpleRoundRobin::default,
        MockTxPool::default,
        || MockValidator,
        || {
            StateRoot::new(
                // due to the burst behavior of replay-transformer, its okay to
                // have delay as 1
                //
                // TODO-4?: Make Replay Transformer's stored message not burst
                // within the same Duration
                SeqNum(1), // state_root_delay
            )
        },
        PeerAsyncStateVerify::new,
        delta,                 // delta
        10,                    // proposal_tx_limit
        SeqNum(2000),          // val_set_update_interval
        Round(50),             // epoch_start_delay
        majority_threshold,    // state root quorum threshold
        max_blocksync_retries, // max_blocksync_retries
        SeqNum(100),           // state_sync_threshold
    );
    let all_peers: BTreeSet<_> = state_configs
        .iter()
        .map(|state_config| NodeId::new(state_config.key.pubkey()))
        .collect();

    let first_node = ID::new(*all_peers.first().unwrap());

    let mut filter_peers = HashSet::new();
    filter_peers.insert(first_node);

    println!("delayed node ID: {}", first_node);

    let swarm_config = SwarmBuilder::<NoSerSwarm>(
        state_configs
            .into_iter()
            .enumerate()
            .map(|(seed, state_builder)| {
                let validators = state_builder.forkpoint.validator_set.clone();
                NodeBuilder::<NoSerSwarm>::new(
                    ID::new(NodeId::new(state_builder.key.pubkey())),
                    state_builder,
                    MockWALoggerConfig::default(),
                    NoSerRouterConfig::new(all_peers.clone()).build(),
                    MockStateRootHashNop::new(validators, SeqNum(2000)),
                    vec![GenericTransformer::Latency(LatencyTransformer::new(delta))],
                    vec![
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
    // run the swarm to before the replay and record the longest ledger length
    while swarm
        .step_until(&mut UntilTerminator::new().until_tick(Duration::from_millis(499)))
        .is_some()
    {}

    let longest_ledger_before = swarm
        .states()
        .values()
        .map(|s| s.executor.ledger().get_blocks().len())
        .max()
        .unwrap();

    let nodes_except_first: Vec<ID<_>> = swarm
        .states()
        .keys()
        .filter(|&node_id| *node_id != first_node)
        .copied()
        .collect_vec();

    let mut verifier_before_delayed_messages = MockSwarmVerifier::default();
    verifier_before_delayed_messages
        .metric_exact(
            &nodes_except_first,
            fetch_metric!(blocksync_events.blocksync_request),
            0,
        )
        // handle proposal for all blocks in ledger
        .metric_minimum(
            &nodes_except_first,
            fetch_metric!(consensus_events.handle_proposal),
            longest_ledger_before as u64,
        )
        // vote for all blocks in ledger
        .metric_minimum(
            &nodes_except_first,
            fetch_metric!(consensus_events.created_vote),
            longest_ledger_before as u64,
        )
        // enter rounds using QC except round 2
        .metric_minimum(
            &nodes_except_first,
            fetch_metric!(consensus_events.enter_new_round_qc),
            longest_ledger_before as u64 - 1,
        );

    assert!(verifier_before_delayed_messages.verify(&swarm));

    while swarm
        .step_until(&mut UntilTerminator::new().until_tick(Duration::from_millis(1000)))
        .is_some()
    {}

    // validators should produce the same number of blocks as a working swarm
    // after replay
    //
    // the reverse function of happy_path_tick_by_block
    let expected_blocks = (Duration::from_millis(500).as_millis() / delta.as_millis() - 1) / 2 - 2;
    swarm_ledger_verification(&swarm, longest_ledger_before + expected_blocks as usize);

    let longest_ledger_after = swarm
        .states()
        .values()
        .map(|s| s.executor.ledger().get_blocks().len())
        .max()
        .unwrap();

    let blocksync_requests_range = match direction {
        // when replayed forward, node should populate blocktree in order
        TransformerReplayOrder::Forward => (0, 0),
        // when replayed in reverse, should request every block in ledger
        // +1 is the case where the proposal to commit the last block in
        // ledger is in flight and not delivered to all peers yet
        // +2 is the case where the proposal to commit the last block in
        // ledger is received by all peers
        TransformerReplayOrder::Reverse => (longest_ledger_before + 1, longest_ledger_before + 2),
        // when replayed in random order, could be any number of requests
        // max_blocksync_retries is to ensure that failed blocksync is not
        // triggered too many times
        TransformerReplayOrder::Random(_) => (0, longest_ledger_before + 2 + max_blocksync_retries),
    };

    let mut verifier_after_delayed_messages = MockSwarmVerifier::default();
    verifier_after_delayed_messages
        .metric_exact(
            &nodes_except_first,
            fetch_metric!(blocksync_events.blocksync_request),
            0,
        )
        // handle proposal for all blocks in ledger
        .metric_minimum(
            &nodes_except_first,
            fetch_metric!(consensus_events.handle_proposal),
            longest_ledger_after as u64,
        )
        // vote for all blocks in ledger
        .metric_minimum(
            &nodes_except_first,
            fetch_metric!(consensus_events.created_vote),
            longest_ledger_after as u64,
        )
        // enter rounds using QC except round 2
        .metric_minimum(
            &nodes_except_first,
            fetch_metric!(consensus_events.enter_new_round_qc),
            longest_ledger_after as u64 - 1,
        )
        .metric_range(
            &vec![first_node],
            fetch_metric!(blocksync_events.blocksync_request),
            blocksync_requests_range.0 as u64,
            blocksync_requests_range.1 as u64,
        );

    assert!(verifier_after_delayed_messages.verify(&swarm));
}
