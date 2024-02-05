mod common;

use std::{
    collections::{BTreeSet, HashMap},
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};

use common::QuicSwarm;
use monad_consensus_types::{
    block_validator::MockValidator, payload::StateRoot, txpool::MockTxPool,
};
use monad_crypto::certificate_signature::CertificateKeyPair;
use monad_gossip::mock::MockGossipConfig;
use monad_mock_swarm::{
    mock_swarm::SwarmBuilder, node::NodeBuilder, swarm_relation::NoSerSwarm,
    terminator::UntilTerminator,
};
use monad_quic::QuicRouterSchedulerConfig;
use monad_router_scheduler::{NoSerRouterConfig, RouterSchedulerBuilder};
use monad_testutil::swarm::{make_state_configs, swarm_ledger_verification};
use monad_tracing_counter::counter::{counter_get, CounterLayer, MetricFilter};
use monad_transformer::{
    BwTransformer, BytesTransformer, GenericTransformer, LatencyTransformer, ID,
};
use monad_types::{NodeId, Round, SeqNum};
use monad_updaters::state_root_hash::MockStateRootHashNop;
use monad_validator::{
    simple_round_robin::SimpleRoundRobin, validator_set::ValidatorSetFactory,
    weighted_round_robin::WeightedRoundRobin,
};
use monad_wal::mock::MockWALoggerConfig;
use tracing_core::LevelFilter;
use tracing_subscriber::{filter::Targets, prelude::*, Layer, Registry};

#[test]
fn two_nodes() {
    let state_configs = make_state_configs::<NoSerSwarm>(
        2, // num_nodes
        ValidatorSetFactory::default,
        WeightedRoundRobin::default,
        MockTxPool::default,
        || MockValidator,
        || {
            StateRoot::new(
                SeqNum(4), // state_root_delay
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
                    vec![GenericTransformer::Latency(LatencyTransformer::new(
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
    swarm_ledger_verification(&swarm, 1024);
}

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
    let fmt_layer = tracing_subscriber::fmt::layer();
    let counter = Arc::new(RwLock::new(HashMap::new()));
    let counter_layer = CounterLayer::new(Arc::clone(&counter));

    let subscriber = Registry::default()
        .with(
            fmt_layer
                .with_filter(MetricFilter {})
                .with_filter(Targets::new().with_default(LevelFilter::INFO)),
        )
        .with(counter_layer);
    tracing::subscriber::set_global_default(subscriber).expect("unable to set global subscriber");

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
