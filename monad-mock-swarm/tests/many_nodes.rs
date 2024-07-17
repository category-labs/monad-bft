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
use monad_eth_reserve_balance::{
    state_backend::NopStateBackend, PassthruReserveBalanceCache, ReserveBalanceCacheTrait,
};
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
use monad_testutil::swarm::{make_state_configs, swarm_ledger_verification};
use monad_transformer::{
    BwTransformer, BytesTransformer, GenericTransformer, LatencyTransformer, ID,
};
use monad_types::{NodeId, Round, SeqNum};
use monad_updaters::{ledger::MockLedger, state_root_hash::MockStateRootHashNop};
use monad_validator::{simple_round_robin::SimpleRoundRobin, validator_set::ValidatorSetFactory};

#[test]
fn many_nodes_noser() {
    // block commits every 2∆ on happy path; 20ms * 1024 = 41s
    // but consensus starts with a timeout
    let delta = Duration::from_millis(20);
    let runtime = Duration::from_secs(42);
    let num_expected_blocks = 1024;
    let state_configs = make_state_configs::<NoSerSwarm>(
        40, // num_nodes
        ValidatorSetFactory::default,
        SimpleRoundRobin::default,
        MockTxPool::default,
        || MockValidator,
        || PassthruBlockPolicy,
        || PassthruReserveBalanceCache::new(NopStateBackend, 4),
        || {
            StateRoot::new(
                SeqNum(4), // state_root_delay
            )
        },
        PeerAsyncStateVerify::new,
        delta,              // delta
        0,                  // proposal_tx_limit
        SeqNum(2000),       // val_set_update_interval
        Round(50),          // epoch_start_delay
        majority_threshold, // state root quorum threshold
        5,                  // max_blocksync_retries
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
                let validators = state_builder.forkpoint.validator_sets[0].clone();
                NodeBuilder::<NoSerSwarm>::new(
                    ID::new(NodeId::new(state_builder.key.pubkey())),
                    state_builder,
                    NoSerRouterConfig::new(all_peers.clone()).build(),
                    MockStateRootHashNop::new(validators.validators, SeqNum(2000)),
                    MockLedger::default(),
                    vec![GenericTransformer::Latency(LatencyTransformer::new(delta))],
                    vec![],
                    TimestamperConfig::default(),
                    seed.try_into().unwrap(),
                )
            })
            .collect(),
    );

    let mut swarm = swarm_config.build();
    swarm.batch_step_until(&mut UntilTerminator::new().until_tick(runtime));
    swarm_ledger_verification(&swarm, num_expected_blocks);

    let mut verifier = MockSwarmVerifier::default().tick_range(runtime, delta);
    let node_ids = swarm.states().keys().copied().collect_vec();
    verifier.metrics_happy_path(&node_ids, &swarm);

    assert!(verifier.verify(&swarm));
}

#[test]
fn many_nodes_quic_latency() {
    let zero_instant = Instant::now();

    let delta = Duration::from_millis(100);
    let state_configs = make_state_configs::<QuicSwarm>(
        40, // num_nodes
        ValidatorSetFactory::default,
        SimpleRoundRobin::default,
        MockTxPool::default,
        || MockValidator,
        || PassthruBlockPolicy,
        || PassthruReserveBalanceCache::new(NopStateBackend, 4),
        || {
            StateRoot::new(
                SeqNum(4), // state_root_delay
            )
        },
        PeerAsyncStateVerify::new,
        delta,              // delta
        0,                  // proposal_tx_limit
        SeqNum(2000),       // val_set_update_interval
        Round(50),          // epoch_start_delay
        majority_threshold, // state root quorum threshold
        5,                  // max_blocksync_retries
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
                        delta * 2,
                        1000,
                    )
                    .build(),
                    MockStateRootHashNop::new(validators.validators, SeqNum(2000)),
                    MockLedger::default(),
                    vec![BytesTransformer::Latency(LatencyTransformer::new(delta))],
                    vec![],
                    TimestamperConfig::default(),
                    seed.try_into().unwrap(),
                )
            })
            .collect(),
    );

    let mut swarm = swarm_config.build();
    swarm.batch_step_until(&mut UntilTerminator::new().until_block(100));

    let min_ledger_len = 100;
    swarm_ledger_verification(&swarm, min_ledger_len);

    // quic router scheduler loses the first message when it's handshaking
    // handshake takes less than consensus round timer
    // initial timeout is sent out at t=consensus_round_timer=4*delta
    let mut verifier = MockSwarmVerifier::default()
        .tick_range(happy_path_tick_by_block(100, delta) + 4 * delta, 10 * delta);

    let node_ids = swarm.states().keys().copied().collect_vec();
    verifier
        .metric_exact(
            &node_ids,
            fetch_metric!(blocksync_events.blocksync_request),
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

#[test]
fn many_nodes_quic_bw() {
    let zero_instant = Instant::now();
    let delta = Duration::from_millis(200);

    let state_configs = make_state_configs::<QuicSwarm>(
        40, // num_nodes
        ValidatorSetFactory::default,
        SimpleRoundRobin::default,
        MockTxPool::default,
        || MockValidator,
        || PassthruBlockPolicy,
        || PassthruReserveBalanceCache::new(NopStateBackend, 10_000_000),
        || {
            StateRoot::new(
                SeqNum(10_000_000), // state_root_delay
            )
        },
        PeerAsyncStateVerify::new,
        delta,              // delta
        5000,               // proposal_tx_limit
        SeqNum(2000),       // val_set_update_interval
        Round(50),          // epoch_start_delay
        majority_threshold, // state root quorum threshold
        5,                  // max_blocksync_retries
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
                let validators = state_builder.forkpoint.validator_sets[0].clone();
                NodeBuilder::new(
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
                    MockStateRootHashNop::new(validators.validators, SeqNum(2000)),
                    MockLedger::default(),
                    vec![
                        BytesTransformer::Latency(LatencyTransformer::new(Duration::from_millis(
                            100,
                        ))),
                        BytesTransformer::Bw(BwTransformer::new(1000, Duration::from_millis(30))),
                    ],
                    vec![],
                    TimestamperConfig::default(),
                    seed.try_into().unwrap(),
                )
            })
            .collect(),
    );

    let mut swarm = swarm_config.build();

    let tick1 = swarm
        .batch_step_until(&mut UntilTerminator::new().until_block(100))
        .unwrap();

    let max_block_sync_requests = 10;
    let min_ledger_len = 99;
    swarm_ledger_verification(&swarm, min_ledger_len);

    let mut verifier =
        MockSwarmVerifier::default().tick_range(Duration::from_secs(72), Duration::from_secs(1));

    let node_ids = swarm.states().keys().copied().collect_vec();
    verifier
        .metric_maximum(
            &node_ids,
            fetch_metric!(blocksync_events.blocksync_request),
            max_block_sync_requests,
        )
        .metric_minimum(
            &node_ids,
            fetch_metric!(consensus_events.handle_proposal),
            min_ledger_len as u64 - max_block_sync_requests,
        )
        .metric_minimum(
            &node_ids,
            fetch_metric!(consensus_events.created_vote),
            min_ledger_len as u64 - max_block_sync_requests,
        );
    assert!(verifier.verify(&swarm));

    // The first ~130 rounds are all timing out as the QUIC protocol is learning
    // to pace the message burst. Trying to send all the proposal at the same
    // time instant cannot deliver because the bandwidth limiter allows a rather
    // small burst size.
    //
    // The average block time approaches 350ms as the swarm runs longer. The
    // theoretical minimum block time is (39 nodes * 5000 txns * 32 bytes/txn) /
    // 1 Gbps + 100ms + 100ms = 249.9ms.
    //
    // A long running test is better at checking the average block time

    // let tick2 = swarm
    //     .batch_step_until(&mut UntilTerminator::new().until_block(1000))
    //     .unwrap();

    // let block_time = (tick2 - tick1) / 900;

    // assert!(block_time > Duration::from_millis(250));
    // assert!(block_time < Duration::from_millis(250) + delta * 2);
}
