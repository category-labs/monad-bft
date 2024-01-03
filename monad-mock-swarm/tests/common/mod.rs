use bytes::Bytes;
use monad_consensus_state::ConsensusState;
use monad_consensus_types::{
    block_validator::MockValidator, multi_sig::MultiSig, payload::StateRoot,
};
use monad_crypto::NopSignature;
use monad_executor::{timed_event::TimedEvent, State};
use monad_executor_glue::MonadEvent;
use monad_gossip::mock::MockGossip;
use monad_mock_swarm::{mock_txpool::MockTxPool, swarm_relation::SwarmRelation};
use monad_quic::{QuicRouterScheduler, QuicRouterSchedulerConfig};
use monad_state::{MonadMessage, MonadState, VerifiedMonadMessage};
use monad_transformer::BytesTransformerPipeline;
use monad_updaters::state_root_hash::MockStateRootHashNop;
use monad_validator::{simple_round_robin::SimpleRoundRobin, validator_set::ValidatorSet};
use monad_wal::mock::{MockWALogger, MockWALoggerConfig};

pub struct QuicSwarm;

impl SwarmRelation for QuicSwarm {
    type SignatureType = NopSignature;
    type SignatureCollectionType = MultiSig<Self::SignatureType>;

    type InboundMessage = MonadMessage<Self::SignatureType, Self::SignatureCollectionType>;
    type OutboundMessage = VerifiedMonadMessage<Self::SignatureType, Self::SignatureCollectionType>;
    type TransportMessage = Bytes;

    type TransactionValidator = MockValidator;

    type State = MonadState<
        ConsensusState<Self::SignatureCollectionType, Self::TransactionValidator, StateRoot>,
        Self::SignatureType,
        Self::SignatureCollectionType,
        ValidatorSet,
        SimpleRoundRobin,
        MockTxPool,
    >;

    type RouterSchedulerConfig = QuicRouterSchedulerConfig<MockGossip>;
    type RouterScheduler =
        QuicRouterScheduler<MockGossip, Self::InboundMessage, Self::OutboundMessage>;

    type Pipeline = BytesTransformerPipeline;

    type LoggerConfig = MockWALoggerConfig;
    type Logger =
        MockWALogger<TimedEvent<MonadEvent<Self::SignatureType, Self::SignatureCollectionType>>>;

    type StateRootHashExecutor = MockStateRootHashNop<
        <Self::State as State>::Block,
        Self::SignatureType,
        Self::SignatureCollectionType,
    >;
}
