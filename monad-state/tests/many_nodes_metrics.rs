use std::time::Duration;

use monad_block_sync::BlockSyncState;
use monad_consensus_state::ConsensusState;
use monad_consensus_types::{multi_sig::MultiSig, transaction_validator::MockValidator};
use monad_crypto::NopSignature;
use monad_executor::{
    executor::mock::{NoSerRouterConfig, NoSerRouterScheduler},
    transformer::{LatencyTransformer, Transformer, TransformerPipeline},
    xfmr_pipe,
};
use monad_state::{MonadMessage, MonadState};
use monad_testutil::swarm::run_nodes;
use monad_tracing_counter::{
    counter::{CounterLayer, MetricFilter},
    counter_status,
};
use monad_validator::{simple_round_robin::SimpleRoundRobin, validator_set::ValidatorSet};
use monad_wal::mock::{MockWALogger, MockWALoggerConfig};
use tracing_core::LevelFilter;
use tracing_subscriber::{filter::Targets, prelude::*, Registry};

#[test]
fn many_nodes() {
    let fmt_layer = tracing_subscriber::fmt::layer();
    let counter_layer = CounterLayer::new();

    let subscriber = Registry::default()
        .with(
            fmt_layer
                .with_filter(MetricFilter {})
                .with_filter(Targets::new().with_default(LevelFilter::INFO)),
        )
        .with(counter_layer);

    tracing::subscriber::set_global_default(subscriber).expect("unable to set global subscriber");

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
    >(
        MockValidator,
        |all_peers, _| NoSerRouterConfig {
            all_peers: all_peers.into_iter().collect(),
        },
        MockWALoggerConfig,
        xfmr_pipe!(Transformer::<
            MonadMessage<NopSignature, MultiSig<NopSignature>>,
        >::Latency(LatencyTransformer(
            Duration::from_millis(1)
        ))),
        100,
        1024,
        Duration::from_millis(2),
    );

    counter_status!();
}
