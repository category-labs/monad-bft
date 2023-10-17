use std::time::Duration;

use monad_block_sync::BlockSyncState;
use monad_consensus_state::ConsensusState;
use monad_consensus_types::{
    multi_sig::MultiSig, payload::StateRoot, transaction_validator::MockValidator,
};
use monad_crypto::NopSignature;
use monad_executor::timed_event::TimedEvent;
use monad_executor_glue::MonadEvent;
use monad_mock_swarm::{
    mock::{
        MockMempoolRandFail, MockMempoolRandFailConfig, NoSerRouterConfig, NoSerRouterScheduler,
    },
    mock_swarm::UntilTerminator,
    swarm_relation::SwarmRelation,
    transformer::{GenericTransformer, GenericTransformerPipeline, LatencyTransformer},
};
use monad_state::{MonadMessage, MonadState, VerifiedMonadMessage};
use monad_testutil::swarm::{create_and_run_nodes, SwarmTestConfig};
use monad_validator::{simple_round_robin::SimpleRoundRobin, validator_set::ValidatorSet};
use monad_wal::mock::{MockWALogger, MockWALoggerConfig};

struct RandFailSwarm;
impl SwarmRelation for RandFailSwarm {
    type STATE = MonadState<
        ConsensusState<MultiSig<NopSignature>, MockValidator, StateRoot>,
        NopSignature,
        MultiSig<NopSignature>,
        ValidatorSet,
        SimpleRoundRobin,
        BlockSyncState,
    >;
    type ST = NopSignature;
    type SCT = MultiSig<Self::ST>;
    type RS = NoSerRouterScheduler<MonadMessage<Self::ST, Self::SCT>>;
    type P = GenericTransformerPipeline<MonadMessage<Self::ST, Self::SCT>>;
    type LGR = MockWALogger<TimedEvent<MonadEvent<Self::ST, Self::SCT>>>;
    type ME = MockMempoolRandFail<Self::ST, Self::SCT>;
    type TVT = MockValidator;
    type LGRCFG = MockWALoggerConfig;
    type RSCFG = NoSerRouterConfig;
    type MPCFG = MockMempoolRandFailConfig;
    type StateMessage = MonadMessage<Self::ST, Self::SCT>;
    type OutboundStateMessage = VerifiedMonadMessage<Self::ST, Self::SCT>;
    type Message = MonadMessage<Self::ST, Self::SCT>;
}

#[test]
fn random_mempool_failures() {
    create_and_run_nodes::<RandFailSwarm, _, _>(
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
        UntilTerminator::new().until_tick(Duration::from_secs(7)),
        SwarmTestConfig {
            num_nodes: 4,
            consensus_delta: Duration::from_millis(2),
            parallelize: false,
            expected_block: 1024,
            state_root_delay: 4,
            seed: 1,
        },
    );
}
