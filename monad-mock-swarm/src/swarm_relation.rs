use monad_consensus_state::ConsensusState;
use monad_consensus_types::{
    block::Block,
    block_validator::{BlockValidator, MockValidator},
    payload::StateRoot,
    signature_collection::SignatureCollection,
    txpool::MockTxPool,
};
use monad_crypto::{
    certificate_signature::{CertificateSignaturePubKey, CertificateSignatureRecoverable},
    NopSignature,
};
use monad_executor::{timed_event::TimedEvent, State};
use monad_executor_glue::{Message, MonadEvent};
use monad_multi_sig::MultiSig;
use monad_router_scheduler::{NoSerRouterConfig, NoSerRouterScheduler, RouterScheduler};
use monad_state::{MonadConfig, MonadMessage, MonadState, VerifiedMonadMessage};
use monad_transformer::{GenericTransformerPipeline, Pipeline};
use monad_updaters::state_root_hash::{MockStateRootHashNop, MockableStateRootHash};
use monad_validator::{simple_round_robin::SimpleRoundRobin, validator_set::ValidatorSet};
use monad_wal::{
    mock::{MockWALogger, MockWALoggerConfig},
    PersistenceLogger,
};

use crate::transformer::MonadMessageTransformerPipeline;

pub trait SwarmRelation {
    type SignatureType: CertificateSignatureRecoverable + Unpin;
    type SignatureCollectionType: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<Self::SignatureType>>
        + Unpin;

    type InboundMessage: Message<
        NodeIdPubKey = CertificateSignaturePubKey<Self::SignatureType>,
        Event = <Self::State as State>::Event,
    >;
    type OutboundMessage: Clone;
    type TransportMessage: PartialEq + Eq + Send;

    type TransactionValidator: BlockValidator + Clone;

    type State: State<
        Block = Block<Self::SignatureCollectionType>,
        Config = MonadConfig<
            Self::SignatureType,
            Self::SignatureCollectionType,
            Self::TransactionValidator,
        >,
        Event = MonadEvent<Self::SignatureType, Self::SignatureCollectionType>,
        NodeIdSignature = Self::SignatureType,
        SignatureCollection = Self::SignatureCollectionType,
        Message = Self::InboundMessage,
        OutboundMessage = Self::OutboundMessage,
    >;

    type RouterSchedulerConfig;
    type RouterScheduler: RouterScheduler<
        NodeIdPublicKey = CertificateSignaturePubKey<Self::SignatureType>,
        Config = Self::RouterSchedulerConfig,
        InboundMessage = Self::InboundMessage,
        OutboundMessage = Self::OutboundMessage,
        TransportMessage = Self::TransportMessage,
    >;

    type Pipeline: Pipeline<
            Self::TransportMessage,
            NodeIdPubKey = CertificateSignaturePubKey<Self::SignatureType>,
        > + Clone;

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
        ConsensusState<
            Self::SignatureType,
            Self::SignatureCollectionType,
            Self::TransactionValidator,
            StateRoot,
        >,
        Self::SignatureType,
        Self::SignatureCollectionType,
        ValidatorSet<CertificateSignaturePubKey<Self::SignatureType>>,
        SimpleRoundRobin,
        MockTxPool,
    >;

    type RouterSchedulerConfig = NoSerRouterConfig<CertificateSignaturePubKey<Self::SignatureType>>;
    type RouterScheduler = NoSerRouterScheduler<
        CertificateSignaturePubKey<Self::SignatureType>,
        Self::InboundMessage,
        Self::OutboundMessage,
    >;

    type Pipeline = GenericTransformerPipeline<
        CertificateSignaturePubKey<Self::SignatureType>,
        Self::TransportMessage,
    >;

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
        ConsensusState<
            Self::SignatureType,
            Self::SignatureCollectionType,
            Self::TransactionValidator,
            StateRoot,
        >,
        Self::SignatureType,
        Self::SignatureCollectionType,
        ValidatorSet<CertificateSignaturePubKey<Self::SignatureType>>,
        SimpleRoundRobin,
        MockTxPool,
    >;

    type RouterSchedulerConfig = NoSerRouterConfig<CertificateSignaturePubKey<Self::SignatureType>>;
    type RouterScheduler = NoSerRouterScheduler<
        CertificateSignaturePubKey<Self::SignatureType>,
        Self::InboundMessage,
        Self::OutboundMessage,
    >;

    type Pipeline =
        MonadMessageTransformerPipeline<CertificateSignaturePubKey<Self::SignatureType>>;

    type LoggerConfig = MockWALoggerConfig;
    type Logger =
        MockWALogger<TimedEvent<MonadEvent<Self::SignatureType, Self::SignatureCollectionType>>>;

    type StateRootHashExecutor = MockStateRootHashNop<
        <Self::State as State>::Block,
        Self::SignatureType,
        Self::SignatureCollectionType,
    >;
}
