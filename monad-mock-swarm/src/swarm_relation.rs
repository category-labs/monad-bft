use bytes::Bytes;
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
use monad_executor_glue::{MonadEvent, StateRootHashCommand};
use monad_multi_sig::MultiSig;
use monad_router_scheduler::{BytesRouterScheduler, NoSerRouterScheduler, RouterScheduler};
use monad_state::{MonadMessage, MonadState, VerifiedMonadMessage};
use monad_transformer::{GenericTransformerPipeline, Pipeline};
use monad_updaters::state_root_hash::{MockStateRootHashNop, MockableStateRootHash};
use monad_validator::{
    leader_election::LeaderElection,
    simple_round_robin::SimpleRoundRobin,
    validator_set::{BoxedValidatorSetTypeFactory, ValidatorSetFactory, ValidatorSetTypeFactory},
    weighted_round_robin::WeightedRoundRobin,
};
use monad_wal::{mock::MockWALogger, PersistenceLogger};

use crate::{mock::MockExecutor, node::Node, transformer::MonadMessageTransformerPipeline};

pub type SwarmRelationStateType<S> = MonadState<
    <S as SwarmRelation>::SignatureType,
    <S as SwarmRelation>::SignatureCollectionType,
    <S as SwarmRelation>::ValidatorSetTypeFactory,
    <S as SwarmRelation>::LeaderElection,
    <S as SwarmRelation>::TxPool,
    <S as SwarmRelation>::BlockValidator,
    <S as SwarmRelation>::StateRootValidator,
>;
pub trait SwarmRelation
where
    Self: Sized + 'static,
    Node<Self>: Send,
    MockExecutor<Self>: Unpin,
{
    type SignatureType: CertificateSignatureRecoverable;
    type SignatureCollectionType: SignatureCollection<
        NodeIdPubKey = CertificateSignaturePubKey<Self::SignatureType>,
    >;

    type TransportMessage: PartialEq + Eq + Send + Sync + Unpin;

    type BlockValidator: BlockValidator + Send + Sync + Unpin;
    type StateRootValidator: StateRootValidator + Send + Sync + Unpin;
    type ValidatorSetTypeFactory: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<Self::SignatureType>>
        + Send
        + Sync
        + Unpin;
    type LeaderElection: LeaderElection<
            NodeIdPubKey = CertificateSignaturePubKey<Self::SignatureType>,
            NodeSignatureCollection = Self::SignatureCollectionType,
        > + Send
        + Sync
        + Unpin;
    type TxPool: TxPool + Send + Sync + Unpin;

    type RouterScheduler: RouterScheduler<
            NodeIdPublicKey = CertificateSignaturePubKey<Self::SignatureType>,
            InboundMessage = MonadMessage<Self::SignatureType, Self::SignatureCollectionType>,
            OutboundMessage = VerifiedMonadMessage<
                Self::SignatureType,
                Self::SignatureCollectionType,
            >,
            TransportMessage = Self::TransportMessage,
        > + Send
        + Unpin;

    type Pipeline: Pipeline<
            Self::TransportMessage,
            NodeIdPubKey = CertificateSignaturePubKey<Self::SignatureType>,
        > + Send
        + Sync
        + Unpin;

    type Logger: PersistenceLogger<Event = MonadEvent<Self::SignatureType, Self::SignatureCollectionType>>
        + Send
        + Sync
        + Unpin;

    type StateRootHashExecutor: MockableStateRootHash<
            Block = Block<Self::SignatureCollectionType>,
            Event = MonadEvent<Self::SignatureType, Self::SignatureCollectionType>,
            SignatureCollection = Self::SignatureCollectionType,
        > + Send
        + Sync
        + Unpin;
}

pub struct DebugSwarmRelation;
impl SwarmRelation for DebugSwarmRelation {
    type SignatureType = NopSignature;
    type SignatureCollectionType = MultiSig<Self::SignatureType>;

    type TransportMessage = Bytes;

    type BlockValidator = Box<dyn BlockValidator + Send + Sync>;
    type StateRootValidator = Box<dyn StateRootValidator + Send + Sync>;
    type ValidatorSetTypeFactory =
        BoxedValidatorSetTypeFactory<CertificateSignaturePubKey<Self::SignatureType>>;
    type LeaderElection = Box<
        dyn LeaderElection<
                NodeIdPubKey = CertificateSignaturePubKey<Self::SignatureType>,
                NodeSignatureCollection = Self::SignatureCollectionType,
            > + Send
            + Sync,
    >;
    type TxPool = Box<dyn TxPool + Send + Sync>;

    type RouterScheduler = Box<
        dyn RouterScheduler<
                NodeIdPublicKey = CertificateSignaturePubKey<Self::SignatureType>,
                TransportMessage = Self::TransportMessage,
                InboundMessage = MonadMessage<Self::SignatureType, Self::SignatureCollectionType>,
                OutboundMessage = VerifiedMonadMessage<
                    Self::SignatureType,
                    Self::SignatureCollectionType,
                >,
            > + Send
            + Sync,
    >;

    type Pipeline = Box<
        dyn Pipeline<
                Self::TransportMessage,
                NodeIdPubKey = CertificateSignaturePubKey<Self::SignatureType>,
            > + Send
            + Sync,
    >;

    type Logger = Box<
        dyn PersistenceLogger<
                Event = MonadEvent<Self::SignatureType, Self::SignatureCollectionType>,
            > + Send
            + Sync,
    >;

    type StateRootHashExecutor = Box<
        dyn MockableStateRootHash<
                Block = Block<Self::SignatureCollectionType>,
                Event = MonadEvent<Self::SignatureType, Self::SignatureCollectionType>,
                SignatureCollection = Self::SignatureCollectionType,
                Command = StateRootHashCommand<Block<Self::SignatureCollectionType>>,
                Item = MonadEvent<Self::SignatureType, Self::SignatureCollectionType>,
            > + Send
            + Sync,
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
    type ValidatorSetTypeFactory =
        ValidatorSetFactory<CertificateSignaturePubKey<Self::SignatureType>>;
    type LeaderElection = SimpleRoundRobin<
        CertificateSignaturePubKey<Self::SignatureType>,
        Self::SignatureCollectionType,
    >;
    type TxPool = MockTxPool;

    type RouterScheduler = NoSerRouterScheduler<
        CertificateSignaturePubKey<Self::SignatureType>,
        MonadMessage<Self::SignatureType, Self::SignatureCollectionType>,
        VerifiedMonadMessage<Self::SignatureType, Self::SignatureCollectionType>,
    >;

    type Pipeline = GenericTransformerPipeline<
        CertificateSignaturePubKey<Self::SignatureType>,
        Self::TransportMessage,
    >;

    type Logger = MockWALogger<MonadEvent<Self::SignatureType, Self::SignatureCollectionType>>;

    type StateRootHashExecutor = MockStateRootHashNop<
        Block<Self::SignatureCollectionType>,
        Self::SignatureType,
        Self::SignatureCollectionType,
    >;
}

pub struct BytesSwarm;
impl SwarmRelation for BytesSwarm {
    type SignatureType = NopSignature;
    type SignatureCollectionType = MultiSig<Self::SignatureType>;

    type TransportMessage = Bytes;

    type BlockValidator = MockValidator;
    type StateRootValidator = StateRoot;
    type ValidatorSetTypeFactory =
        ValidatorSetFactory<CertificateSignaturePubKey<Self::SignatureType>>;
    type LeaderElection = WeightedRoundRobin<
        CertificateSignaturePubKey<Self::SignatureType>,
        Self::SignatureCollectionType,
    >;
    type TxPool = MockTxPool;

    type RouterScheduler = BytesRouterScheduler<
        CertificateSignaturePubKey<Self::SignatureType>,
        MonadMessage<Self::SignatureType, Self::SignatureCollectionType>,
        VerifiedMonadMessage<Self::SignatureType, Self::SignatureCollectionType>,
    >;

    type Pipeline = GenericTransformerPipeline<
        CertificateSignaturePubKey<Self::SignatureType>,
        Self::TransportMessage,
    >;

    type Logger = MockWALogger<MonadEvent<Self::SignatureType, Self::SignatureCollectionType>>;

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
    type ValidatorSetTypeFactory =
        ValidatorSetFactory<CertificateSignaturePubKey<Self::SignatureType>>;
    type LeaderElection = SimpleRoundRobin<
        CertificateSignaturePubKey<Self::SignatureType>,
        Self::SignatureCollectionType,
    >;
    type TxPool = MockTxPool;

    type RouterScheduler = NoSerRouterScheduler<
        CertificateSignaturePubKey<Self::SignatureType>,
        MonadMessage<Self::SignatureType, Self::SignatureCollectionType>,
        VerifiedMonadMessage<Self::SignatureType, Self::SignatureCollectionType>,
    >;

    type Pipeline =
        MonadMessageTransformerPipeline<CertificateSignaturePubKey<Self::SignatureType>>;

    type Logger = MockWALogger<MonadEvent<Self::SignatureType, Self::SignatureCollectionType>>;

    type StateRootHashExecutor = MockStateRootHashNop<
        Block<Self::SignatureCollectionType>,
        Self::SignatureType,
        Self::SignatureCollectionType,
    >;
}
