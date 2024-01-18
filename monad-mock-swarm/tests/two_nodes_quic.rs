mod common;

use std::{
    collections::BTreeSet,
    time::{Duration, Instant},
};

use common::QuicSwarm;
use monad_async_state_verify::LocalAsyncStateVerify;
use monad_consensus_types::{
    block_validator::MockValidator, payload::StateRoot, txpool::MockTxPool,
};
use monad_crypto::certificate_signature::CertificateKeyPair;
use monad_gossip::mock::MockGossipConfig;
use monad_mock_swarm::{mock_swarm::SwarmBuilder, node::NodeBuilder, terminator::UntilTerminator};
use monad_quic::QuicRouterSchedulerConfig;
use monad_router_scheduler::RouterSchedulerBuilder;
use monad_testutil::swarm::{make_state_configs, swarm_ledger_verification};
use monad_tracing_counter::counter::counter_get;
use monad_transformer::{BwTransformer, BytesTransformer, LatencyTransformer, ID};
use monad_types::{NodeId, Round, SeqNum};
use monad_updaters::state_root_hash::MockStateRootHashNop;
use monad_validator::{simple_round_robin::SimpleRoundRobin, validator_set::ValidatorSetFactory};
use monad_wal::mock::MockWALoggerConfig;

use crate::common::setup_counter;

#[test]
fn two_nodes_quic() {
    let zero_instant = Instant::now();

    let state_configs = make_state_configs::<QuicSwarm>(
        2, // num_nodes
        ValidatorSetFactory::default,
        SimpleRoundRobin::default,
        MockTxPool::default,
        || MockValidator,
        || {
            StateRoot::new(
                SeqNum(4), // state_root_delay
            )
        },
        LocalAsyncStateVerify::new,
        Duration::from_millis(10), // delta
        150,                       // proposal_tx_limit
        SeqNum(2000),              // val_set_update_interval
        Round(50),                 // epoch_start_delay
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
                    )
                    .build(),
                    MockStateRootHashNop::new(validators, SeqNum(2000)),
                    vec![BytesTransformer::Latency(LatencyTransformer::new(
                        Duration::from_millis(1),
                    ))],
                    seed.try_into().unwrap(),
                )
            })
            .collect(),
    );

    let mut swarm = swarm_config.build();
    while swarm
        .step_until(&mut UntilTerminator::new().until_tick(Duration::from_secs(10)))
        .is_some()
    {}
    swarm_ledger_verification(&swarm, 256);
}

#[test]
fn two_nodes_quic_bw() {
    let counter = setup_counter();

    let zero_instant = Instant::now();

    let state_configs = make_state_configs::<QuicSwarm>(
        2, // num_nodes
        ValidatorSetFactory::default,
        SimpleRoundRobin::default,
        MockTxPool::default,
        || MockValidator,
        || {
            StateRoot::new(
                SeqNum(4), // state_root_delay
            )
        },
        LocalAsyncStateVerify::new,
        Duration::from_millis(1000), // delta
        100,                         // proposal_tx_limit
        SeqNum(2000),                // val_set_update_interval
        Round(50),                   // epoch_start_delay
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
                    )
                    .build(),
                    MockStateRootHashNop::new(validators, SeqNum(2000)),
                    vec![
                        BytesTransformer::Latency(LatencyTransformer::new(Duration::from_millis(
                            1,
                        ))),
                        BytesTransformer::Bw(BwTransformer::new(8, Duration::from_secs(1))),
                    ],
                    seed.try_into().unwrap(),
                )
            })
            .collect(),
    );

    let mut swarm = swarm_config.build();
    while swarm
        .step_until(&mut UntilTerminator::new().until_tick(Duration::from_secs(10)))
        .is_some()
    {}
    swarm_ledger_verification(&swarm, 100);

    let dropped_msg = counter_get(counter, None, "bwtransformer_dropped_msg");
    assert!(dropped_msg.is_some());
}
