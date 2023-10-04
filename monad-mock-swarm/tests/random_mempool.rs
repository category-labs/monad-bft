use std::time::Duration;

use monad_block_sync::BlockSyncState;
use monad_consensus_state::ConsensusState;
use monad_consensus_types::{
    multi_sig::MultiSig, payload::StateRoot, transaction_validator::MockValidator,
};
use monad_crypto::NopSignature;
use monad_mock_swarm::{
    mock::{
        MockMempoolRandFail, MockMempoolRandFailConfig, NoSerRouterConfig, NoSerRouterScheduler,
    },
    transformer::{GenericTransformer, LatencyTransformer},
};
use monad_state::{MonadMessage, MonadState};
use monad_testutil::swarm::{create_and_run_nodes, SwarmTestConfig};
use monad_validator::{simple_round_robin::SimpleRoundRobin, validator_set::ValidatorSet};
use monad_wal::mock::{MockWALogger, MockWALoggerConfig};

#[test]
fn random_mempool_failures() {
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
        MockMempoolRandFail<_, _>,
    >(
        MockValidator,
        |all_peers, _| NoSerRouterConfig {
            all_peers: all_peers.into_iter().collect(),
        },
        MockWALoggerConfig,
        MockMempoolRandFailConfig {
            fail_rate: 0.1,
            seed: 0,
        },
        vec![GenericTransformer::Latency(LatencyTransformer(
            Duration::from_millis(1),
        ))],
        SwarmTestConfig {
            num_nodes: 4,
            consensus_delta: Duration::from_millis(2),
            parallelize: false,
            until: Duration::from_secs(7),
            until_block: usize::MAX,
            expected_block: 1024,
            seed: 1,
        },
    );
}
