use std::{collections::BTreeSet, time::Duration};

use criterion::{criterion_group, criterion_main, Criterion};
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
use monad_transformer::{GenericTransformer, LatencyTransformer, ID};
use monad_types::{NodeId, Round, SeqNum};
use monad_updaters::{
    ledger::MockLedger, state_root_hash::MockStateRootHashNop, statesync::MockStateSyncExecutor,
    txpool::MockTxPoolExecutor,
};
use monad_validator::{simple_round_robin::SimpleRoundRobin, validator_set::ValidatorSetFactory};

pub fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("two_nodes", |b| b.iter(two_nodes));
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = criterion_benchmark
}
criterion_main!(benches);

static CHAIN_PARAMS: ChainParams = ChainParams {
    tx_limit: 10_000,
    proposal_gas_limit: 300_000_000,
    proposal_byte_limit: 4_000_000,
    vote_pace: Duration::from_millis(0),
};

fn two_nodes() {
    let state_configs = make_state_configs::<NoSerSwarm>(
        2, // num_nodes
        ValidatorSetFactory::default,
        SimpleRoundRobin::default,
        || MockValidator,
        || PassthruBlockPolicy,
        || InMemoryStateInner::genesis(Balance::MAX, SeqNum(4)),
        StateMetrics::default,
        SeqNum(4),                           // execution_delay
        Duration::from_millis(2),            // delta
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
                let validators = state_builder.forkpoint.validator_sets[0].clone();
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
                    vec![GenericTransformer::Latency(LatencyTransformer::new(
                        Duration::from_millis(1),
                    ))],
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
    swarm_ledger_verification(&swarm, 1024);
}
