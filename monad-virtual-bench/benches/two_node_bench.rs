use std::time::Duration;

use monad_block_sync::BlockSyncState;
use monad_consensus_state::ConsensusState;
use monad_consensus_types::{
    multi_sig::MultiSig, payload::StateRoot, transaction_validator::MockValidator,
};
use monad_crypto::NopSignature;
use monad_mock_swarm::{
    mock::{MockMempool, MockMempoolConfig, NoSerRouterConfig, NoSerRouterScheduler},
    transformer::{GenericTransformer, LatencyTransformer},
};
use monad_state::{MonadMessage, MonadState};
use monad_testutil::swarm::{create_and_run_nodes, SwarmTestConfig};
use monad_validator::{simple_round_robin::SimpleRoundRobin, validator_set::ValidatorSet};
use monad_wal::mock::{MockWALogger, MockWALoggerConfig};

fn two_nodes() -> u128 {
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
        MockMempoolConfig,
        vec![GenericTransformer::Latency(LatencyTransformer(
            Duration::from_millis(1),
        ))],
        SwarmTestConfig {
            num_nodes: 2,
            consensus_delta: Duration::from_millis(2),
            parallelize: false,
            until: Duration::from_secs(10),
            until_block: 1024,
            expected_block: 1024,
            seed: 1,
        },
    )
    .as_millis()
}

monad_virtual_bench::virtual_bench_main! {two_nodes}
