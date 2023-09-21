use std::time::Duration;

use criterion::{criterion_group, criterion_main, Criterion};
use monad_block_sync::BlockSyncState;
use monad_consensus_state::ConsensusState;
use monad_consensus_types::{
    multi_sig::MultiSig, payload::StateRoot, transaction_validator::MockValidator,
};
use monad_crypto::NopSignature;
use monad_election::simple_round_robin::SimpleRoundRobin;
use monad_executor::{
    executor::mock::{MockMempool, NoSerRouterConfig, NoSerRouterScheduler},
    transformer::{GenericTransformer, LatencyTransformer},
};
use monad_state::{MonadMessage, MonadState};
use monad_testutil::swarm::create_and_run_nodes;
use monad_validator::validator_set::ValidatorSet;
use monad_wal::mock::{MockWALogger, MockWALoggerConfig};

pub fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("two_nodes", |b| b.iter(two_nodes));
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);

fn two_nodes() {
    create_and_run_nodes::<
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
        MockMempool<_>,
    >(
        MockValidator,
        |all_peers, _| NoSerRouterConfig {
            all_peers: all_peers.into_iter().collect(),
        },
        MockWALoggerConfig,
        vec![GenericTransformer::Latency(LatencyTransformer(
            Duration::from_millis(1),
        ))],
        false,
        2,
        Duration::from_millis(2),
        Duration::from_secs(10),
        1024,
    );
}
