use monad_consensus_state::ConsensusState;
use monad_consensus_types::{
    block::Block,
    block_validator::{BlockValidator, MockValidator},
    payload::{StateRoot, StateRootValidator},
    signature_collection::SignatureCollection,
    txpool::{MockTxPool, TxPool},
};
use monad_crypto::{
    certificate_signature::{CertificateSignaturePubKey, CertificateSignatureRecoverable},
    NopSignature,
};
use monad_executor::timed_event::TimedEvent;
use monad_executor_glue::MonadEvent;
use monad_multi_sig::MultiSig;
use monad_router_scheduler::{NoSerRouterConfig, NoSerRouterScheduler, RouterScheduler};
use monad_state::{MonadMessage, MonadState, VerifiedMonadMessage};
use monad_transformer::{GenericTransformerPipeline, Pipeline};
use monad_updaters::state_root_hash::{MockStateRootHashNop, MockableStateRootHash};
use monad_validator::{
    leader_election::LeaderElection,
    simple_round_robin::SimpleRoundRobin,
    validator_set::{ValidatorSet, ValidatorSetType},
};
use monad_wal::{
    mock::{MockWALogger, MockWALoggerConfig},
    PersistenceLogger,
};

use crate::transformer::MonadMessageTransformerPipeline;

pub type SwarmRelationStateType<S> = MonadState<
    ConsensusState<
        <S as SwarmRelation>::SignatureType,
        <S as SwarmRelation>::SignatureCollectionType,
        <S as SwarmRelation>::BlockValidator,
        <S as SwarmRelation>::StateRootValidator,
    >,
    <S as SwarmRelation>::SignatureType,
    <S as SwarmRelation>::SignatureCollectionType,
    <S as SwarmRelation>::ValidatorSet,
    <S as SwarmRelation>::LeaderElection,
    <S as SwarmRelation>::TxPool,
>;
pub trait SwarmRelation {
    type SignatureType: CertificateSignatureRecoverable + Unpin;
    type SignatureCollectionType: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<Self::SignatureType>>
        + Unpin;

    type TransportMessage: PartialEq + Eq + Send + Sync;

    type BlockValidator: BlockValidator + Clone;
    type StateRootValidator: StateRootValidator;
    type ValidatorSet: ValidatorSetType<
        NodeIdPubKey = CertificateSignaturePubKey<Self::SignatureType>,
    >;
    type LeaderElection: LeaderElection;
    type TxPool: TxPool;

    type RouterSchedulerConfig;
    type RouterScheduler: RouterScheduler<
        NodeIdPublicKey = CertificateSignaturePubKey<Self::SignatureType>,
        Config = Self::RouterSchedulerConfig,
        InboundMessage = MonadMessage<Self::SignatureType, Self::SignatureCollectionType>,
        OutboundMessage = VerifiedMonadMessage<Self::SignatureType, Self::SignatureCollectionType>,
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
        Block = Block<Self::SignatureCollectionType>,
        Event = MonadEvent<Self::SignatureType, Self::SignatureCollectionType>,
        SignatureCollection = Self::SignatureCollectionType,
    >;
}

// default swarm relation impl
pub struct NoSerSwarm;
impl SwarmRelation for NoSerSwarm {
    type SignatureType = NopSignature;
    type SignatureCollectionType = MultiSig<Self::SignatureType>;

    type TransportMessage =
        VerifiedMonadMessage<Self::SignatureType, Self::SignatureCollectionType>;

    type BlockValidator = MockValidator;
    type StateRootValidator = StateRoot;
    type ValidatorSet = ValidatorSet<CertificateSignaturePubKey<Self::SignatureType>>;
    type LeaderElection = SimpleRoundRobin;
    type TxPool = MockTxPool;

    type RouterSchedulerConfig = NoSerRouterConfig<CertificateSignaturePubKey<Self::SignatureType>>;
    type RouterScheduler = NoSerRouterScheduler<
        CertificateSignaturePubKey<Self::SignatureType>,
        MonadMessage<Self::SignatureType, Self::SignatureCollectionType>,
        VerifiedMonadMessage<Self::SignatureType, Self::SignatureCollectionType>,
    >;

    type Pipeline = GenericTransformerPipeline<
        CertificateSignaturePubKey<Self::SignatureType>,
        Self::TransportMessage,
    >;

    type LoggerConfig = MockWALoggerConfig;
    type Logger =
        MockWALogger<TimedEvent<MonadEvent<Self::SignatureType, Self::SignatureCollectionType>>>;

    type StateRootHashExecutor = MockStateRootHashNop<
        Block<Self::SignatureCollectionType>,
        Self::SignatureType,
        Self::SignatureCollectionType,
    >;
}

pub struct MonadMessageNoSerSwarm;
impl SwarmRelation for MonadMessageNoSerSwarm {
    type SignatureType = NopSignature;
    type SignatureCollectionType = MultiSig<Self::SignatureType>;

    type TransportMessage =
        VerifiedMonadMessage<Self::SignatureType, Self::SignatureCollectionType>;

    type BlockValidator = MockValidator;
    type StateRootValidator = StateRoot;
    type ValidatorSet = ValidatorSet<CertificateSignaturePubKey<Self::SignatureType>>;
    type LeaderElection = SimpleRoundRobin;
    type TxPool = MockTxPool;

    type RouterSchedulerConfig = NoSerRouterConfig<CertificateSignaturePubKey<Self::SignatureType>>;
    type RouterScheduler = NoSerRouterScheduler<
        CertificateSignaturePubKey<Self::SignatureType>,
        MonadMessage<Self::SignatureType, Self::SignatureCollectionType>,
        VerifiedMonadMessage<Self::SignatureType, Self::SignatureCollectionType>,
    >;

    type Pipeline =
        MonadMessageTransformerPipeline<CertificateSignaturePubKey<Self::SignatureType>>;

    type LoggerConfig = MockWALoggerConfig;
    type Logger =
        MockWALogger<TimedEvent<MonadEvent<Self::SignatureType, Self::SignatureCollectionType>>>;

    type StateRootHashExecutor = MockStateRootHashNop<
        Block<Self::SignatureCollectionType>,
        Self::SignatureType,
        Self::SignatureCollectionType,
    >;
}
