use std::{
    collections::{BTreeSet, HashSet},
    time::Duration,
};

use monad_chain_config::{revision::ChainParams, MockChainConfig};
use monad_consensus_types::{
    block::PassthruBlockPolicy, block_validator::MockValidator, metrics::StateMetrics,
};
use monad_crypto::certificate_signature::CertificateKeyPair;
use monad_eth_types::Balance;
use monad_mock_swarm::{
    mock::TimestamperConfig, mock_swarm::SwarmBuilder, node::NodeBuilder,
    swarm_relation::NoSerSwarm, terminator::UntilTerminator,
};
use monad_router_scheduler::{NoSerRouterConfig, RouterSchedulerBuilder};
use monad_state_backend::InMemoryStateInner;
use monad_testutil::swarm::{make_state_configs, swarm_ledger_verification};
use monad_transformer::{
    GenericTransformer, LatencyTransformer, PartitionTransformer, RandLatencyTransformer,
    ReplayTransformer, TransformerReplayOrder, ID,
};
use monad_types::{NodeId, Round, SeqNum};
use monad_updaters::{
    ledger::MockLedger, state_root_hash::MockStateRootHashNop, statesync::MockStateSyncExecutor,
    txpool::MockTxPoolExecutor,
};
use monad_validator::{simple_round_robin::SimpleRoundRobin, validator_set::ValidatorSetFactory};

use crate::RandomizedTest;

static CHAIN_PARAMS: ChainParams = ChainParams {
    tx_limit: 10_000,
    proposal_gas_limit: 300_000_000,
    proposal_byte_limit: 4_000_000,
    vote_pace: Duration::from_millis(100),
};

fn random_latency_test(latency_seed: u64) {
    let state_configs = make_state_configs::<NoSerSwarm>(
        4, // num_nodes
        ValidatorSetFactory::default,
        SimpleRoundRobin::default,
        || MockValidator,
        || PassthruBlockPolicy,
        || InMemoryStateInner::genesis(Balance::MAX, SeqNum(4)),
        StateMetrics::default,
        SeqNum(4),                           // execution_delay
        Duration::from_millis(250),          // delta
        MockChainConfig::new(&CHAIN_PARAMS), // chain config
        SeqNum(2000),                        // val_set_update_interval
        Round(50),                           // epoch_start_delay
        SeqNum(100),                         // state_sync_threshold
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
                let validators = state_builder.locked_epoch_validators[0].clone();
                NodeBuilder::<NoSerSwarm>::new(
                    ID::new(NodeId::new(state_builder.key.pubkey())),
                    state_builder,
                    NoSerRouterConfig::new(all_peers.clone()).build(),
                    MockStateRootHashNop::new(validators.validators.clone(), SeqNum(2000)),
                    MockTxPoolExecutor::default(),
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
                        RandLatencyTransformer::new(latency_seed, Duration::from_millis(330)),
                    )],
                    vec![],
                    TimestamperConfig::default(),
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
    swarm_ledger_verification(&swarm, 2048);
}

static CHAIN_PARAMS_NO_VOTE_PACE: ChainParams = ChainParams {
    tx_limit: 10_000,
    proposal_gas_limit: 300_000_000,
    proposal_byte_limit: 4_000_000,
    vote_pace: Duration::from_millis(0),
};

fn delayed_message_test(latency_seed: u64) {
    let state_configs = make_state_configs::<NoSerSwarm>(
        4, // num_nodes
        ValidatorSetFactory::default,
        SimpleRoundRobin::default,
        || MockValidator,
        || PassthruBlockPolicy,
        || InMemoryStateInner::genesis(Balance::MAX, SeqNum(4)),
        StateMetrics::default,
        SeqNum(4),                                        // execution_delay
        Duration::from_millis(2),                         // delta
        MockChainConfig::new(&CHAIN_PARAMS_NO_VOTE_PACE), // chain config
        SeqNum(2000),                                     // val_set_update_interval
        Round(50),                                        // epoch_start_delay
        SeqNum(100),                                      // state_sync_threshold
    );
    let all_peers: BTreeSet<_> = state_configs
        .iter()
        .map(|state_config| NodeId::new(state_config.key.pubkey()))
        .collect();
    let first_node = *all_peers.first().unwrap();
    let mut filter_peers = HashSet::new();
    filter_peers.insert(ID::new(first_node));
    println!("delayed node ID: {:?}", first_node);

    let swarm_config = SwarmBuilder::<NoSerSwarm>(
        state_configs
            .into_iter()
            .enumerate()
            .map(|(seed, state_builder)| {
                let state_backend = state_builder.state_backend.clone();
                let validators = state_builder.locked_epoch_validators[0].clone();
                NodeBuilder::<NoSerSwarm>::new(
                    ID::new(NodeId::new(state_builder.key.pubkey())),
                    state_builder,
                    NoSerRouterConfig::new(all_peers.clone()).build(),
                    MockStateRootHashNop::new(validators.validators.clone(), SeqNum(2000)),
                    MockTxPoolExecutor::default(),
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
                    vec![
                        GenericTransformer::Latency(LatencyTransformer::new(
                            Duration::from_millis(1),
                        )),
                        GenericTransformer::Partition(PartitionTransformer(filter_peers.clone())),
                        GenericTransformer::Replay(ReplayTransformer::new(
                            Duration::from_secs(1),
                            TransformerReplayOrder::Random(latency_seed),
                        )),
                    ],
                    vec![],
                    TimestamperConfig::default(),
                    seed.try_into().unwrap(),
                )
            })
            .collect(),
    );

    let mut swarm = swarm_config.build();
    while swarm
        .step_until(&mut UntilTerminator::new().until_tick(Duration::from_secs(2)))
        .is_some()
    {}
    swarm_ledger_verification(&swarm, 20);
}

inventory::submit!(RandomizedTest {
    name: "random_latency",
    func: random_latency_test,
});

inventory::submit!(RandomizedTest {
    name: "delayed_message",
    func: delayed_message_test,
});
