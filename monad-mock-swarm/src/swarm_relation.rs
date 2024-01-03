use monad_consensus_state::ConsensusState;
use monad_consensus_types::{
    block::Block,
    block_validator::{BlockValidator, MockValidator},
    message_signature::MessageSignature,
    multi_sig::MultiSig,
    payload::StateRoot,
    signature_collection::SignatureCollection,
};
use monad_crypto::NopSignature;
use monad_executor::{timed_event::TimedEvent, State};
use monad_executor_glue::{Message, MonadEvent};
use monad_router_scheduler::{NoSerRouterConfig, NoSerRouterScheduler, RouterScheduler};
use monad_state::{MonadConfig, MonadMessage, MonadState, VerifiedMonadMessage};
use monad_transformer::{GenericTransformerPipeline, Pipeline};
use monad_updaters::state_root_hash::{MockStateRootHashNop, MockableStateRootHash};
use monad_validator::{simple_round_robin::SimpleRoundRobin, validator_set::ValidatorSet};
use monad_wal::{
    mock::{MockWALogger, MockWALoggerConfig},
    PersistenceLogger,
};

use crate::{mock_txpool::MockTxPool, transformer::MonadMessageTransformerPipeline};

pub trait SwarmRelation {
    type SignatureType: MessageSignature + Unpin;
    type SignatureCollectionType: SignatureCollection + Unpin;

    type InboundMessage: Message<Event = <Self::State as State>::Event>;
    type OutboundMessage: Clone;
    type TransportMessage: PartialEq + Eq + Send;

    type TransactionValidator: BlockValidator + Clone;

    type State: State<
        Block = Block<Self::SignatureCollectionType>,
        Config = MonadConfig<Self::SignatureCollectionType, Self::TransactionValidator>,
        Event = MonadEvent<Self::SignatureType, Self::SignatureCollectionType>,
        SignatureCollection = Self::SignatureCollectionType,
        Message = Self::InboundMessage,
        OutboundMessage = Self::OutboundMessage,
    >;

    type RouterSchedulerConfig;
    type RouterScheduler: RouterScheduler<
        Config = Self::RouterSchedulerConfig,
        InboundMessage = Self::InboundMessage,
        OutboundMessage = Self::OutboundMessage,
        TransportMessage = Self::TransportMessage,
    >;

    type Pipeline: Pipeline<Self::TransportMessage> + Clone;

    type LoggerConfig: Clone;
    type Logger: PersistenceLogger<
        Config = Self::LoggerConfig,
        Event = TimedEvent<MonadEvent<Self::SignatureType, Self::SignatureCollectionType>>,
    >;

    type StateRootHashExecutor: MockableStateRootHash<
        Block = <Self::State as State>::Block,
        Event = MonadEvent<Self::SignatureType, Self::SignatureCollectionType>,
        SignatureCollection = Self::SignatureCollectionType,
    >;
}

// default swarm relation impl
pub struct NoSerSwarm;
impl SwarmRelation for NoSerSwarm {
    type SignatureType = NopSignature;
    type SignatureCollectionType = MultiSig<Self::SignatureType>;

    type InboundMessage = MonadMessage<Self::SignatureType, Self::SignatureCollectionType>;
    type OutboundMessage = VerifiedMonadMessage<Self::SignatureType, Self::SignatureCollectionType>;
    type TransportMessage = Self::OutboundMessage;

    type TransactionValidator = MockValidator;

    type State = MonadState<
        ConsensusState<Self::SignatureCollectionType, Self::TransactionValidator, StateRoot>,
        Self::SignatureType,
        Self::SignatureCollectionType,
        ValidatorSet,
        SimpleRoundRobin,
        MockTxPool,
    >;

    type RouterSchedulerConfig = NoSerRouterConfig;
    type RouterScheduler = NoSerRouterScheduler<Self::InboundMessage, Self::OutboundMessage>;

    type Pipeline = GenericTransformerPipeline<Self::TransportMessage>;

    type LoggerConfig = MockWALoggerConfig;
    type Logger =
        MockWALogger<TimedEvent<MonadEvent<Self::SignatureType, Self::SignatureCollectionType>>>;

    type StateRootHashExecutor = MockStateRootHashNop<
        <Self::State as State>::Block,
        Self::SignatureType,
        Self::SignatureCollectionType,
    >;
}

pub struct MonadMessageNoSerSwarm;
impl SwarmRelation for MonadMessageNoSerSwarm {
    type SignatureType = NopSignature;
    type SignatureCollectionType = MultiSig<Self::SignatureType>;

    type InboundMessage = MonadMessage<Self::SignatureType, Self::SignatureCollectionType>;
    type OutboundMessage = VerifiedMonadMessage<Self::SignatureType, Self::SignatureCollectionType>;
    type TransportMessage = Self::OutboundMessage;

    type TransactionValidator = MockValidator;

    type State = MonadState<
        ConsensusState<Self::SignatureCollectionType, Self::TransactionValidator, StateRoot>,
        Self::SignatureType,
        Self::SignatureCollectionType,
        ValidatorSet,
        SimpleRoundRobin,
        MockTxPool,
    >;

    type RouterSchedulerConfig = NoSerRouterConfig;
    type RouterScheduler = NoSerRouterScheduler<Self::InboundMessage, Self::OutboundMessage>;

    type Pipeline = MonadMessageTransformerPipeline;

    type LoggerConfig = MockWALoggerConfig;
    type Logger =
        MockWALogger<TimedEvent<MonadEvent<Self::SignatureType, Self::SignatureCollectionType>>>;

    type StateRootHashExecutor = MockStateRootHashNop<
        <Self::State as State>::Block,
        Self::SignatureType,
        Self::SignatureCollectionType,
    >;
}
