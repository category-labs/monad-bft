use std::time::Duration;

use criterion::{criterion_group, criterion_main, Criterion};
use monad_block_sync::BlockSyncState;
use monad_consensus_state::ConsensusState;
use monad_consensus_types::{multi_sig::MultiSig, transaction_validator::MockValidator};
use monad_crypto::NopSignature;
use monad_executor::{
    executor::mock::{MockMempool, NoSerRouterConfig, NoSerRouterScheduler},
    transformer::{LatencyTransformer, Transformer, TransformerPipeline},
    xfmr_pipe,
};
use monad_state::{MonadMessage, MonadState};
use monad_testutil::swarm::run_nodes;
use monad_validator::{simple_round_robin::SimpleRoundRobin, validator_set::ValidatorSet};
use monad_wal::mock::{MockWALogger, MockWALoggerConfig};

pub fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("two nodes", |b| b.iter(two_nodes));
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);

fn two_nodes() {
    run_nodes::<
        MonadState<
            ConsensusState<NopSignature, MultiSig<NopSignature>, MockValidator>,
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
        xfmr_pipe!(Transformer::Latency(LatencyTransformer(
            Duration::from_millis(1)
        ))),
        2,
        1024,
        Duration::from_millis(2),
    );
}
