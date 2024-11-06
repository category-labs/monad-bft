mod common;

use std::{
    collections::BTreeSet,
    time::{Duration, Instant},
};

use common::QuicSwarm;
use itertools::Itertools;
use monad_async_state_verify::{majority_threshold, PeerAsyncStateVerify};
use monad_consensus_types::{
    block::PassthruBlockPolicy, block_validator::MockValidator, metrics::Metrics,
    payload::StateRoot, txpool::MockTxPool,
};
use monad_crypto::certificate_signature::CertificateKeyPair;
use monad_gossip::mock::MockGossipConfig;
use monad_mock_swarm::{
    fetch_metric,
    mock::TimestamperConfig,
    mock_swarm::SwarmBuilder,
    node::NodeBuilder,
    swarm_relation::NoSerSwarm,
    terminator::UntilTerminator,
    verifier::{happy_path_tick_by_block, MockSwarmVerifier},
};
use monad_quic::QuicRouterSchedulerConfig;
use monad_router_scheduler::{NoSerRouterConfig, RouterSchedulerBuilder};
use monad_state_backend::InMemoryStateInner;
use monad_testutil::swarm::{make_state_configs, swarm_ledger_verification};
use monad_transformer::{BytesTransformer, GenericTransformer, LatencyTransformer, ID};
use monad_types::{NodeId, Round, SeqNum};
use monad_updaters::{
    ledger::MockLedger, state_root_hash::MockStateRootHashNop, statesync::MockStateSyncExecutor,
};
use monad_validator::{simple_round_robin::SimpleRoundRobin, validator_set::ValidatorSetFactory};

#[test]
fn two_nodes_noser() {
    let delta = Duration::from_millis(100);
    let vote_pace = Duration::from_millis(5);
    let state_configs = make_state_configs::<NoSerSwarm>(
        2, // num_nodes
        ValidatorSetFactory::default,
        SimpleRoundRobin::default,
        MockTxPool::default,
        || MockValidator,
        || PassthruBlockPolicy,
        || InMemoryStateInner::genesis(u128::MAX, SeqNum(4)),
        || {
            StateRoot::new(
                SeqNum(4), // state_root_delay
            )
        },
        PeerAsyncStateVerify::new,
        delta,              // delta
        vote_pace,          // vote pace
        10,                 // proposal_tx_limit
        SeqNum(2000),       // val_set_update_interval
        Round(50),          // epoch_start_delay
        majority_threshold, // state root quorum threshold
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
                    MockStateRootHashNop::new(validators.validators.clone(), SeqNum(2000)),
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
                    vec![GenericTransformer::Latency(LatencyTransformer::new(delta))],
                    vec![],
                    TimestamperConfig::default(),
                    seed.try_into().unwrap(),
                )
            })
            .collect(),
    );

    let mut swarm = swarm_config.build();
    while swarm
        .step_until(&mut UntilTerminator::new().until_block(1026))
        .is_some()
    {}

    let node_ids = swarm.states().keys().copied().collect_vec();
    // the calculation is correct with two nodes because NoSerRouterScheduler
    // always sends the message over the network/transformer, even for it's for
    // self. Otherwise the time is shorter with two nodes, like QUIC test
    let mut verifier =
        MockSwarmVerifier::default().tick_range(happy_path_tick_by_block(1026, delta), delta);
    verifier.metrics_happy_path(&node_ids, &swarm);
    assert!(verifier.verify(&swarm));

    swarm_ledger_verification(&swarm, 1024);
}

#[test]
fn two_nodes_quic_latency() {
    let zero_instant = Instant::now();
    let delta = Duration::from_millis(100);
    let vote_pace = Duration::from_millis(0);

    let state_configs = make_state_configs::<QuicSwarm>(
        2, // num_nodes
        ValidatorSetFactory::default,
        SimpleRoundRobin::default,
        MockTxPool::default,
        || MockValidator,
        || PassthruBlockPolicy,
        || InMemoryStateInner::genesis(u128::MAX, SeqNum(4)),
        || {
            StateRoot::new(
                SeqNum(4), // state_root_delay
            )
        },
        PeerAsyncStateVerify::new,
        delta,              // delta
        vote_pace,          // vote pace
        150,                // proposal_tx_limit
        SeqNum(2000),       // val_set_update_interval
        Round(50),          // epoch_start_delay
        majority_threshold, // state root quorum threshold
        SeqNum(100),        // state_sync_threshold
    );
    let all_peers: BTreeSet<_> = state_configs
        .iter()
        .map(|state_config| NodeId::new(state_config.key.pubkey()))
        .collect();
    let swarm_config = SwarmBuilder::<QuicSwarm>(
        state_configs
            .into_iter()
            .enumerate()
            .map(|(seed, state_builder)| {
                let me = NodeId::new(state_builder.key.pubkey());
                let state_backend = state_builder.state_backend.clone();
                let validators = state_builder.forkpoint.validator_sets[0].clone();
                NodeBuilder::<QuicSwarm>::new(
                    ID::new(me),
                    state_builder,
                    QuicRouterSchedulerConfig::new(
                        zero_instant,
                        all_peers.clone(),
                        me,
                        seed.try_into().unwrap(),
                        MockGossipConfig {
                            all_peers: all_peers.iter().copied().collect(),
                            me,
                            message_delay: Duration::ZERO,
                        }
                        .build(),
                        delta,
                        1000,
                    )
                    .build(),
                    MockStateRootHashNop::new(validators.validators.clone(), SeqNum(2000)),
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
                    vec![BytesTransformer::Latency(LatencyTransformer::new(delta))],
                    vec![],
                    TimestamperConfig::default(),
                    seed.try_into().unwrap(),
                )
            })
            .collect(),
    );

    let mut swarm = swarm_config.build();
    while swarm
        .step_until(&mut UntilTerminator::new().until_block(1000))
        .is_some()
    {}

    let min_ledger_len = 1000;
    swarm_ledger_verification(&swarm, min_ledger_len);

    // 1 extra round trip because of QUIC handshake two nodes with QUIC router
    // scheduler finishes faster than normal happy path because
    // QuicRouterScheduler immediately emit message to self when broadcasting.
    // Every round takes 1 delta instead of 2 delta
    //
    // Being more lenient here because the QUICRouterScheduler and 2 nodes case
    // isn't particular interesting
    let mut verifier = MockSwarmVerifier::default().tick_range(
        happy_path_tick_by_block(1000, delta) / 2 + 2 * delta,
        delta * 10,
    );

    let node_ids = swarm.states().keys().copied().collect_vec();
    verifier
        .metric_exact(
            &node_ids,
            fetch_metric!(blocksync_events.self_headers_request),
            0,
        )
        .metric_exact(
            &node_ids,
            fetch_metric!(blocksync_events.self_payload_request),
            0,
        )
        // handle proposal for all blocks in ledger
        .metric_minimum(
            &node_ids,
            fetch_metric!(consensus_events.handle_proposal),
            min_ledger_len as u64,
        )
        // vote for all blocks in ledger
        .metric_minimum(
            &node_ids,
            fetch_metric!(consensus_events.created_vote),
            min_ledger_len as u64,
        );
    assert!(verifier.verify(&swarm));
}
