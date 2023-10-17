use monad_block_sync::BlockSyncState;
use monad_consensus_state::ConsensusState;
use monad_consensus_types::{
    multi_sig::MultiSig, payload::StateRoot, transaction_validator::MockValidator,
};
use monad_crypto::NopSignature;
use monad_executor::timed_event::TimedEvent;
use monad_executor_glue::MonadEvent;
use monad_gossip::mock::{MockGossip, MockGossipConfig};
pub use monad_mock_swarm::swarm_relation::NoSerSwarm;
use monad_mock_swarm::{
    mock::{MockMempool, MockMempoolConfig},
    swarm_relation::SwarmRelation,
    transformer::BytesTransformerPipeline,
};
use monad_quic::{QuicRouterScheduler, QuicRouterSchedulerConfig};
use monad_state::{MonadMessage, MonadState, VerifiedMonadMessage};
use monad_validator::{simple_round_robin::SimpleRoundRobin, validator_set::ValidatorSet};
use monad_wal::mock::{MockWALogger, MockWALoggerConfig};

pub struct QuicSwarm;
impl SwarmRelation for QuicSwarm {
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
    type RS = QuicRouterScheduler<MockGossip>;
    type P = BytesTransformerPipeline;
    type LGR = MockWALogger<TimedEvent<MonadEvent<Self::ST, Self::SCT>>>;
    type ME = MockMempool<Self::ST, Self::SCT>;
    type TVT = MockValidator;
    type LGRCFG = MockWALoggerConfig;
    type RSCFG = QuicRouterSchedulerConfig<MockGossipConfig>;
    type MPCFG = MockMempoolConfig;
    type Message = Vec<u8>;
    type StateMessage = MonadMessage<Self::ST, Self::SCT>;
    type OutboundStateMessage = VerifiedMonadMessage<Self::ST, Self::SCT>;
}
