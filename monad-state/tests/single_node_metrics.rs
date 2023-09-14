use std::time::Duration;

use monad_block_sync::BlockSyncState;
use monad_consensus_state::ConsensusState;
use monad_consensus_types::{
    multi_sig::MultiSig, payload::StateRoot, transaction_validator::MockValidator,
    validation::Sha256Hash,
};
use monad_crypto::NopSignature;
use monad_executor::{
    executor::mock::{MockMempool, NoSerRouterConfig, NoSerRouterScheduler},
    transformer::{LatencyTransformer, Transformer, TransformerPipeline},
    xfmr_pipe,
};
use monad_state::{MonadMessage, MonadState};
use monad_testutil::swarm::create_and_run_nodes;
use monad_tracing_counter::{
    counter::{CounterLayer, MetricFilter},
    counter_status,
};
use monad_validator::{simple_round_robin::SimpleRoundRobin, validator_set::ValidatorSet};
use monad_wal::mock::{MockWALogger, MockWALoggerConfig};
use tracing_core::LevelFilter;
use tracing_subscriber::{filter::Targets, prelude::*, Registry};

#[test]
fn two_nodes() {
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

    create_and_run_nodes::<
        MonadState<
            ConsensusState<NopSignature, MultiSig<NopSignature>, MockValidator, StateRoot>,
            NopSignature,
            MultiSig<NopSignature>,
            ValidatorSet,
            SimpleRoundRobin,
            BlockSyncState,
            Sha256Hash,
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
        false,
        2,
        Duration::from_millis(2),
        Duration::from_secs(10),
        1024,
    );
    counter_status!();
}
