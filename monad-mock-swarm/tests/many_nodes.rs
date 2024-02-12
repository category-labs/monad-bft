mod common;
use std::{
    collections::BTreeSet,
    time::{Duration, Instant},
};

use common::QuicSwarm;
use monad_async_state_verify::{majority_threshold, PeerAsyncStateVerify};
use monad_consensus_types::{
    block_validator::MockValidator, payload::StateRoot, txpool::MockTxPool,
};
use monad_crypto::certificate_signature::CertificateKeyPair;
use monad_gossip::mock::MockGossipConfig;
use monad_mock_swarm::{
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
use monad_updaters::state_root_hash::MockStateRootHashNop;
use monad_validator::{simple_round_robin::SimpleRoundRobin, validator_set::ValidatorSetFactory};
use monad_wal::mock::MockWALoggerConfig;

#[test]
fn many_nodes_noser() {
    let delta = Duration::from_millis(1);
    let state_configs = make_state_configs::<NoSerSwarm>(
        40, // num_nodes
        ValidatorSetFactory::default,
        SimpleRoundRobin::default,
        MockTxPool::default,
        || MockValidator,
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
                    vec![GenericTransformer::Latency(LatencyTransformer::new(delta))],
                    seed.try_into().unwrap(),
                )
            })
            .collect(),
    );

    let mut swarm = swarm_config.build();
    swarm.batch_step_until(&mut UntilTerminator::new().until_tick(Duration::from_secs(4)));
    swarm_ledger_verification(&swarm, 1024);

    let verifier = MockSwarmVerifier::default().tick_range(Duration::from_secs(4), delta);
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
                let validators = state_builder.validators.clone();
                NodeBuilder::<QuicSwarm>::new(
                    ID::new(me),
                    state_builder,
                    MockWALoggerConfig::default(),
                    QuicRouterSchedulerConfig::new(
                        zero_instant,
                        all_peers.clone(),
                        me,
                        seed.try_into().unwrap(),
                        MockGossipConfig {
                            all_peers: all_peers.iter().copied().collect(),
                            me,
                        }
                        .build(),
                        delta * 2,
                        1000,
                    )
                    .build(),
                    MockStateRootHashNop::new(validators, SeqNum(2000)),
                    vec![BytesTransformer::Latency(LatencyTransformer::new(delta))],
                    seed.try_into().unwrap(),
                )
            })
            .collect(),
    );

    let mut swarm = swarm_config.build();
    swarm.batch_step_until(&mut UntilTerminator::new().until_block(100));
    swarm_ledger_verification(&swarm, 100);

    // quic router scheduler loses the first message when it's handshaking
    // handshake takes less than consensus round timer
    // initial timeout is sent out at t=consensus_round_timer=4*delta
    let verifier = MockSwarmVerifier::default()
        .tick_range(happy_path_tick_by_block(100, delta) + 4 * delta, 10 * delta);
    assert!(verifier.verify(&swarm));
}

#[test]
fn many_nodes_quic_bw() {
    let zero_instant = Instant::now();
    let delta = Duration::from_millis(100);

    let state_configs = make_state_configs::<QuicSwarm>(
        40, // num_nodes
        ValidatorSetFactory::default,
        SimpleRoundRobin::default,
        MockTxPool::default,
        || MockValidator,
        || {
            StateRoot::new(
                SeqNum(u64::MAX), // state_root_delay
            )
        },
        PeerAsyncStateVerify::new,
        delta,              // delta
        5000,               // proposal_tx_limit
        SeqNum(2000),       // val_set_update_interval
        Round(50),          // epoch_start_delay
        majority_threshold, // state root quorum threshold
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
                let validators = state_builder.validators.clone();
                NodeBuilder::new(
                    ID::new(me),
                    state_builder,
                    MockWALoggerConfig::default(),
                    QuicRouterSchedulerConfig::new(
                        zero_instant,
                        all_peers.clone(),
                        me,
                        seed.try_into().unwrap(),
                        MockGossipConfig {
                            all_peers: all_peers.iter().copied().collect(),
                            me,
                        }
                        .build(),
                        delta,
                        1000,
                    )
                    .build(),
                    MockStateRootHashNop::new(validators, SeqNum(2000)),
                    vec![
                        BytesTransformer::Latency(LatencyTransformer::new(Duration::from_millis(
                            100,
                        ))),
                        BytesTransformer::Bw(BwTransformer::new(1000, Duration::from_millis(10))),
                    ],
                    seed.try_into().unwrap(),
                )
            })
            .collect(),
    );

    let mut swarm = swarm_config.build();

    let tick1 = swarm
        .batch_step_until(&mut UntilTerminator::new().until_block(100))
        .unwrap();
    swarm_ledger_verification(&swarm, 100);

    let verifier =
        MockSwarmVerifier::default().tick_range(Duration::from_secs(122), Duration::from_secs(1));
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
