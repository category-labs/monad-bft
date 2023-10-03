use std::time::Duration;

use monad_block_sync::BlockSyncState;
use monad_consensus_state::ConsensusState;
use monad_consensus_types::{
    multi_sig::MultiSig, payload::StateRoot, transaction_validator::MockValidator,
};
use monad_crypto::NopSignature;
use monad_mock_swarm::{
    mock::{MockMempool, NoSerRouterConfig, NoSerRouterScheduler},
    transformer::{GenericTransformer, LatencyTransformer},
};
use monad_state::{MonadMessage, MonadState};
use monad_testutil::swarm::{create_and_run_nodes, SwarmTestConfig};
use monad_tracing_counter::{
    counter::{CounterLayer, MetricFilter},
    counter_status,
};
use monad_validator::{simple_round_robin::SimpleRoundRobin, validator_set::ValidatorSet};
use monad_wal::mock::{MockWALogger, MockWALoggerConfig};
use tracing_core::LevelFilter;
use tracing_subscriber::{filter::Targets, prelude::*, Registry};

#[test]
fn many_nodes_metrics() {
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
        MockMempool<_, _>,
    >(
        MockValidator,
        |all_peers, _| NoSerRouterConfig {
            all_peers: all_peers.into_iter().collect(),
        },
        MockWALoggerConfig,
        vec![GenericTransformer::<
            MonadMessage<NopSignature, MultiSig<NopSignature>>,
        >::Latency(LatencyTransformer(
            Duration::from_millis(1),
        ))],
        SwarmTestConfig {
            num_nodes: 100,
            consensus_delta: Duration::from_millis(2),
            parallelize: false,
            until: Duration::from_secs(4),
            until_block: usize::MAX,
            expected_block: 1024,
            seed: 1,
        },
    );

    counter_status!();
}
