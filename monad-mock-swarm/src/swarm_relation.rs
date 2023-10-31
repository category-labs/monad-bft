use monad_block_sync::BlockSyncState;
use monad_consensus_state::ConsensusState;
use monad_consensus_types::{
    block::FullBlock,
    message_signature::MessageSignature,
    multi_sig::MultiSig,
    payload::StateRoot,
    signature_collection::SignatureCollection,
    transaction_validator::{MockValidator, TransactionValidator},
};
use monad_crypto::NopSignature;
use monad_executor::{timed_event::TimedEvent, State};
use monad_executor_glue::MonadEvent;
use monad_state::{MonadConfig, MonadMessage, MonadState, VerifiedMonadMessage};
use monad_types::{Deserializable, Serializable};
use monad_validator::{simple_round_robin::SimpleRoundRobin, validator_set::ValidatorSet};
use monad_wal::{
    mock::{MockWALogger, MockWALoggerConfig},
    PersistenceLogger,
};

use crate::{
    mock::{
        MockMempool, MockMempoolConfig, MockableExecutor, NoSerRouterConfig, NoSerRouterScheduler,
        RouterScheduler,
    },
    transformer::{
        monad_test::MonadMessageTransformerPipeline, GenericTransformerPipeline, Pipeline,
    },
};

pub trait SwarmRelation {
    type State: State<
        Event = MonadEvent<Self::SignatureType, Self::SignatureCollectionType>,
        SignatureCollection = Self::SignatureCollectionType,
        Message = Self::StateMessage,
        OutboundMessage = Self::OutboundStateMessage,
        Block = FullBlock<Self::SignatureCollectionType>,
        Config = MonadConfig<Self::SignatureCollectionType, Self::TransactionValidator>,
    >;
    type SignatureType: MessageSignature + Unpin;
    type SignatureCollectionType: SignatureCollection + Unpin;
    type RouterScheduler: RouterScheduler<
        M = Self::Message,
        Serialized = Self::Message,
        Config = Self::RouterSchedulerConfig,
    >;
    type Pipeline: Pipeline<Self::Message> + Clone;
    type Logger: PersistenceLogger<
        Config = Self::LoggerConfig,
        Event = TimedEvent<MonadEvent<Self::SignatureType, Self::SignatureCollectionType>>,
    >;
    type MempoolExecutor: MockableExecutor<
        Event = MonadEvent<Self::SignatureType, Self::SignatureCollectionType>,
        SignatureCollection = Self::SignatureCollectionType,
        Config = Self::MempoolConfig,
    >;

    type TransactionValidator: TransactionValidator + Clone;
    type StateMessage: Deserializable<Self::Message>;
    type OutboundStateMessage: Serializable<Self::Message>;
    type Message: Clone + PartialEq + Eq + Send;
    type LoggerConfig: Clone;
    type RouterSchedulerConfig;
    type MempoolConfig: Copy;
}
// default swarm relation impl
pub struct NoSerSwarm;

impl SwarmRelation for NoSerSwarm {
    type State = SwarmStateType<Self>;
    type SignatureType = NopSignature;
    type SignatureCollectionType = MultiSig<Self::SignatureType>;
    type RouterScheduler =
        NoSerRouterScheduler<MonadMessage<Self::SignatureType, Self::SignatureCollectionType>>;
    type Pipeline = GenericTransformerPipeline<
        MonadMessage<Self::SignatureType, Self::SignatureCollectionType>,
    >;
    type Logger =
        MockWALogger<TimedEvent<MonadEvent<Self::SignatureType, Self::SignatureCollectionType>>>;
    type MempoolExecutor = MockMempool<Self::SignatureType, Self::SignatureCollectionType>;
    type TransactionValidator = MockValidator;
    type LoggerConfig = MockWALoggerConfig;
    type RouterSchedulerConfig = NoSerRouterConfig;
    type MempoolConfig = MockMempoolConfig;
    type StateMessage = MonadMessage<Self::SignatureType, Self::SignatureCollectionType>;
    type OutboundStateMessage =
        VerifiedMonadMessage<Self::SignatureType, Self::SignatureCollectionType>;
    type Message = MonadMessage<Self::SignatureType, Self::SignatureCollectionType>;
}

// no NoSerSwarm but pipeline is specialized
pub struct MonadMessageNoSerSwarm;

impl SwarmRelation for MonadMessageNoSerSwarm {
    type State = MonadState<
        ConsensusState<MultiSig<NopSignature>, MockValidator, StateRoot>,
        NopSignature,
        MultiSig<NopSignature>,
        ValidatorSet,
        SimpleRoundRobin,
        BlockSyncState,
    >;
    type SignatureType = NopSignature;
    type SignatureCollectionType = MultiSig<Self::SignatureType>;
    type RouterScheduler =
        NoSerRouterScheduler<MonadMessage<Self::SignatureType, Self::SignatureCollectionType>>;
    type Pipeline = MonadMessageTransformerPipeline;
    type Logger =
        MockWALogger<TimedEvent<MonadEvent<Self::SignatureType, Self::SignatureCollectionType>>>;
    type MempoolExecutor = MockMempool<Self::SignatureType, Self::SignatureCollectionType>;
    type TransactionValidator = MockValidator;
    type LoggerConfig = MockWALoggerConfig;
    type RouterSchedulerConfig = NoSerRouterConfig;
    type MempoolConfig = MockMempoolConfig;
    type StateMessage = MonadMessage<Self::SignatureType, Self::SignatureCollectionType>;
    type OutboundStateMessage =
        VerifiedMonadMessage<Self::SignatureType, Self::SignatureCollectionType>;
    type Message = MonadMessage<Self::SignatureType, Self::SignatureCollectionType>;
}

pub type SwarmStateType<S> = MonadState<
    ConsensusState<
        <S as SwarmRelation>::SignatureCollectionType,
        <S as SwarmRelation>::TransactionValidator,
        StateRoot,
    >,
    <S as SwarmRelation>::SignatureType,
    <S as SwarmRelation>::SignatureCollectionType,
    ValidatorSet,
    SimpleRoundRobin,
    BlockSyncState,
>;
