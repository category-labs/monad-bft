use bytes::Bytes;
use monad_chain_config::{
    revision::{ChainRevision, MockChainRevision},
    ChainConfig, MockChainConfig,
};
use monad_compress::{nop::NopCompression, CompressionAlgo};
use monad_consensus_types::{
    block::{BlockPolicy, MockExecutionProtocol, PassthruBlockPolicy},
    block_validator::{BlockValidator, MockValidator},
    signature_collection::SignatureCollection,
};
use monad_crypto::{
    certificate_signature::{CertificateSignaturePubKey, CertificateSignatureRecoverable},
    NopSignature,
};
use monad_executor_glue::{
    LedgerCommand, MonadEvent, StateRootHashCommand, StateSyncCommand, TxPoolCommand,
};
use monad_multi_sig::MultiSig;
use monad_router_scheduler::{BytesRouterScheduler, NoSerRouterScheduler, RouterScheduler};
use monad_state::{MonadMessage, MonadState, VerifiedMonadMessage};
use monad_state_backend::{InMemoryState, StateBackend};
use monad_transformer::{GenericTransformerPipeline, Pipeline};
use monad_types::ExecutionProtocol;
use monad_updaters::{
    ledger::{MockLedger, MockableLedger},
    state_root_hash::{MockStateRootHashNop, MockableStateRootHash},
    statesync::{MockStateSyncExecutor, MockableStateSync},
    txpool::{MockTxPoolExecutor, MockableTxPool},
};
use monad_validator::{
    leader_election::LeaderElection,
    simple_round_robin::SimpleRoundRobin,
    validator_set::{BoxedValidatorSetTypeFactory, ValidatorSetFactory, ValidatorSetTypeFactory},
};

use crate::{mock::MockExecutor, node::Node, transformer::MonadMessageTransformerPipeline};
pub type SwarmRelationStateType<S> = MonadState<
    <S as SwarmRelation>::SignatureType,
    <S as SwarmRelation>::SignatureCollectionType,
    <S as SwarmRelation>::ExecutionProtocolType,
    <S as SwarmRelation>::BlockPolicyType,
    <S as SwarmRelation>::StateBackendType,
    <S as SwarmRelation>::ValidatorSetTypeFactory,
    <S as SwarmRelation>::LeaderElection,
    <S as SwarmRelation>::BlockValidator,
    <S as SwarmRelation>::ChainConfigType,
    <S as SwarmRelation>::ChainRevisionType,
    <S as SwarmRelation>::CompressionAlgo,
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
    type ExecutionProtocolType: ExecutionProtocol;
    type BlockPolicyType: BlockPolicy<
            Self::SignatureType,
            Self::SignatureCollectionType,
            Self::ExecutionProtocolType,
            Self::StateBackendType,
        > + Send
        + Sync
        + Unpin;
    type StateBackendType: StateBackend + Send + Sync + Unpin;
    type ChainConfigType: ChainConfig<Self::ChainRevisionType> + Send + Unpin;
    type ChainRevisionType: ChainRevision + Send + Unpin;
    type CompressionAlgo: CompressionAlgo + Send + Unpin;

    type TransportMessage: PartialEq + Eq + Send + Sync + Unpin;

    type BlockValidator: BlockValidator<
            Self::SignatureType,
            Self::SignatureCollectionType,
            Self::ExecutionProtocolType,
            Self::BlockPolicyType,
            Self::StateBackendType,
        > + Send
        + Sync
        + Unpin;
    type ValidatorSetTypeFactory: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<Self::SignatureType>>
        + Send
        + Sync
        + Unpin;
    type LeaderElection: LeaderElection<NodeIdPubKey = CertificateSignaturePubKey<Self::SignatureType>>
        + Send
        + Sync
        + Unpin;
    type Ledger: MockableLedger<
            Signature = Self::SignatureType,
            SignatureCollection = Self::SignatureCollectionType,
            ExecutionProtocol = Self::ExecutionProtocolType,
            Event = MonadEvent<
                Self::SignatureType,
                Self::SignatureCollectionType,
                Self::ExecutionProtocolType,
            >,
        > + Send
        + Unpin;

    type RouterScheduler: RouterScheduler<
            NodeIdPublicKey = CertificateSignaturePubKey<Self::SignatureType>,
            InboundMessage = MonadMessage<
                Self::SignatureType,
                Self::SignatureCollectionType,
                Self::ExecutionProtocolType,
            >,
            OutboundMessage = VerifiedMonadMessage<
                Self::SignatureType,
                Self::SignatureCollectionType,
                Self::ExecutionProtocolType,
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

    type StateRootHashExecutor: MockableStateRootHash<
            Event = MonadEvent<
                Self::SignatureType,
                Self::SignatureCollectionType,
                Self::ExecutionProtocolType,
            >,
            SignatureCollection = Self::SignatureCollectionType,
        > + Send
        + Sync
        + Unpin;
    type TxPoolExecutor: MockableTxPool<
            Signature = Self::SignatureType,
            SignatureCollection = Self::SignatureCollectionType,
            ExecutionProtocol = Self::ExecutionProtocolType,
            BlockPolicy = Self::BlockPolicyType,
            StateBackend = Self::StateBackendType,
            Event = MonadEvent<
                Self::SignatureType,
                Self::SignatureCollectionType,
                Self::ExecutionProtocolType,
            >,
        > + Send
        + Sync
        + Unpin;
    type StateSyncExecutor: MockableStateSync<
            Signature = Self::SignatureType,
            SignatureCollection = Self::SignatureCollectionType,
            ExecutionProtocol = Self::ExecutionProtocolType,
        > + Send
        + Sync
        + Unpin;
}
pub struct DebugSwarmRelation;
impl SwarmRelation for DebugSwarmRelation {
    type SignatureType = NopSignature;
    type SignatureCollectionType = MultiSig<Self::SignatureType>;
    type ExecutionProtocolType = MockExecutionProtocol;
    type BlockPolicyType = PassthruBlockPolicy;
    type StateBackendType = InMemoryState;
    type ChainConfigType = MockChainConfig;
    type ChainRevisionType = MockChainRevision;
    type CompressionAlgo = NopCompression;

    type TransportMessage = Bytes;

    type BlockValidator = Box<
        dyn BlockValidator<
                Self::SignatureType,
                Self::SignatureCollectionType,
                Self::ExecutionProtocolType,
                Self::BlockPolicyType,
                Self::StateBackendType,
            > + Send
            + Sync,
    >;
    type ValidatorSetTypeFactory =
        BoxedValidatorSetTypeFactory<CertificateSignaturePubKey<Self::SignatureType>>;
    type LeaderElection = Box<
        dyn LeaderElection<NodeIdPubKey = CertificateSignaturePubKey<Self::SignatureType>>
            + Send
            + Sync,
    >;
    type Ledger = Box<
        dyn MockableLedger<
                Signature = Self::SignatureType,
                SignatureCollection = Self::SignatureCollectionType,
                ExecutionProtocol = Self::ExecutionProtocolType,
                Event = MonadEvent<
                    Self::SignatureType,
                    Self::SignatureCollectionType,
                    Self::ExecutionProtocolType,
                >,
                Command = LedgerCommand<
                    Self::SignatureType,
                    Self::SignatureCollectionType,
                    Self::ExecutionProtocolType,
                >,
                Item = MonadEvent<
                    Self::SignatureType,
                    Self::SignatureCollectionType,
                    Self::ExecutionProtocolType,
                >,
            > + Send
            + Sync,
    >;
    type RouterScheduler = Box<
        dyn RouterScheduler<
                NodeIdPublicKey = CertificateSignaturePubKey<Self::SignatureType>,
                TransportMessage = Self::TransportMessage,
                InboundMessage = MonadMessage<
                    Self::SignatureType,
                    Self::SignatureCollectionType,
                    Self::ExecutionProtocolType,
                >,
                OutboundMessage = VerifiedMonadMessage<
                    Self::SignatureType,
                    Self::SignatureCollectionType,
                    Self::ExecutionProtocolType,
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

    type StateRootHashExecutor = Box<
        dyn MockableStateRootHash<
                Event = MonadEvent<
                    Self::SignatureType,
                    Self::SignatureCollectionType,
                    Self::ExecutionProtocolType,
                >,
                SignatureCollection = Self::SignatureCollectionType,
                Command = StateRootHashCommand<Self::SignatureCollectionType>,
                Item = MonadEvent<
                    Self::SignatureType,
                    Self::SignatureCollectionType,
                    Self::ExecutionProtocolType,
                >,
            > + Send
            + Sync,
    >;
    type TxPoolExecutor = Box<
        dyn MockableTxPool<
                Signature = Self::SignatureType,
                SignatureCollection = Self::SignatureCollectionType,
                ExecutionProtocol = Self::ExecutionProtocolType,
                BlockPolicy = Self::BlockPolicyType,
                StateBackend = Self::StateBackendType,
                Event = MonadEvent<
                    Self::SignatureType,
                    Self::SignatureCollectionType,
                    Self::ExecutionProtocolType,
                >,
                Command = TxPoolCommand<
                    Self::SignatureType,
                    Self::SignatureCollectionType,
                    Self::ExecutionProtocolType,
                    Self::BlockPolicyType,
                    Self::StateBackendType,
                >,
                Item = MonadEvent<
                    Self::SignatureType,
                    Self::SignatureCollectionType,
                    Self::ExecutionProtocolType,
                >,
            > + Send
            + Sync,
    >;
    type StateSyncExecutor = Box<
        dyn MockableStateSync<
                Signature = Self::SignatureType,
                SignatureCollection = Self::SignatureCollectionType,
                ExecutionProtocol = Self::ExecutionProtocolType,
                Command = StateSyncCommand<Self::SignatureType, Self::ExecutionProtocolType>,
            > + Send
            + Sync,
    >;
}
// default swarm relation impl
pub struct NoSerSwarm;
impl SwarmRelation for NoSerSwarm {
    type SignatureType = NopSignature;
    type SignatureCollectionType = MultiSig<Self::SignatureType>;
    type ExecutionProtocolType = MockExecutionProtocol;
    type BlockPolicyType = PassthruBlockPolicy;
    type StateBackendType = InMemoryState;
    type ChainConfigType = MockChainConfig;
    type ChainRevisionType = MockChainRevision;
    type CompressionAlgo = NopCompression;

    type TransportMessage = VerifiedMonadMessage<
        Self::SignatureType,
        Self::SignatureCollectionType,
        Self::ExecutionProtocolType,
    >;

    type BlockValidator = MockValidator;
    type ValidatorSetTypeFactory =
        ValidatorSetFactory<CertificateSignaturePubKey<Self::SignatureType>>;
    type LeaderElection = SimpleRoundRobin<CertificateSignaturePubKey<Self::SignatureType>>;
    type Ledger =
        MockLedger<Self::SignatureType, Self::SignatureCollectionType, Self::ExecutionProtocolType>;

    type RouterScheduler = NoSerRouterScheduler<
        CertificateSignaturePubKey<Self::SignatureType>,
        MonadMessage<
            Self::SignatureType,
            Self::SignatureCollectionType,
            Self::ExecutionProtocolType,
        >,
        VerifiedMonadMessage<
            Self::SignatureType,
            Self::SignatureCollectionType,
            Self::ExecutionProtocolType,
        >,
    >;

    type Pipeline = GenericTransformerPipeline<
        CertificateSignaturePubKey<Self::SignatureType>,
        Self::TransportMessage,
    >;

    type StateRootHashExecutor = MockStateRootHashNop<
        Self::SignatureType,
        Self::SignatureCollectionType,
        Self::ExecutionProtocolType,
    >;
    type TxPoolExecutor = MockTxPoolExecutor<
        Self::SignatureType,
        Self::SignatureCollectionType,
        Self::ExecutionProtocolType,
        Self::BlockPolicyType,
        Self::StateBackendType,
    >;
    type StateSyncExecutor = MockStateSyncExecutor<
        Self::SignatureType,
        Self::SignatureCollectionType,
        Self::ExecutionProtocolType,
    >;
}

pub struct BytesSwarm;
impl SwarmRelation for BytesSwarm {
    type SignatureType = NopSignature;
    type SignatureCollectionType = MultiSig<Self::SignatureType>;
    type ExecutionProtocolType = MockExecutionProtocol;
    type BlockPolicyType = PassthruBlockPolicy;
    type StateBackendType = InMemoryState;
    type ChainConfigType = MockChainConfig;
    type ChainRevisionType = MockChainRevision;
    type CompressionAlgo = NopCompression;

    type TransportMessage = Bytes;
    type BlockValidator = MockValidator;
    type ValidatorSetTypeFactory =
        ValidatorSetFactory<CertificateSignaturePubKey<Self::SignatureType>>;
    type LeaderElection = SimpleRoundRobin<CertificateSignaturePubKey<Self::SignatureType>>;
    type Ledger =
        MockLedger<Self::SignatureType, Self::SignatureCollectionType, Self::ExecutionProtocolType>;

    type RouterScheduler = BytesRouterScheduler<
        CertificateSignaturePubKey<Self::SignatureType>,
        MonadMessage<
            Self::SignatureType,
            Self::SignatureCollectionType,
            Self::ExecutionProtocolType,
        >,
        VerifiedMonadMessage<
            Self::SignatureType,
            Self::SignatureCollectionType,
            Self::ExecutionProtocolType,
        >,
    >;

    type Pipeline = GenericTransformerPipeline<
        CertificateSignaturePubKey<Self::SignatureType>,
        Self::TransportMessage,
    >;

    type StateRootHashExecutor = MockStateRootHashNop<
        Self::SignatureType,
        Self::SignatureCollectionType,
        Self::ExecutionProtocolType,
    >;
    type TxPoolExecutor = MockTxPoolExecutor<
        Self::SignatureType,
        Self::SignatureCollectionType,
        Self::ExecutionProtocolType,
        Self::BlockPolicyType,
        Self::StateBackendType,
    >;
    type StateSyncExecutor = MockStateSyncExecutor<
        Self::SignatureType,
        Self::SignatureCollectionType,
        Self::ExecutionProtocolType,
    >;
}

pub struct MonadMessageNoSerSwarm;
impl SwarmRelation for MonadMessageNoSerSwarm {
    type SignatureType = NopSignature;
    type SignatureCollectionType = MultiSig<Self::SignatureType>;
    type ExecutionProtocolType = MockExecutionProtocol;
    type BlockPolicyType = PassthruBlockPolicy;
    type StateBackendType = InMemoryState;
    type ChainConfigType = MockChainConfig;
    type ChainRevisionType = MockChainRevision;
    type CompressionAlgo = NopCompression;

    type TransportMessage = VerifiedMonadMessage<
        Self::SignatureType,
        Self::SignatureCollectionType,
        Self::ExecutionProtocolType,
    >;

    type BlockValidator = MockValidator;
    type ValidatorSetTypeFactory =
        ValidatorSetFactory<CertificateSignaturePubKey<Self::SignatureType>>;
    type LeaderElection = SimpleRoundRobin<CertificateSignaturePubKey<Self::SignatureType>>;
    type Ledger =
        MockLedger<Self::SignatureType, Self::SignatureCollectionType, Self::ExecutionProtocolType>;

    type RouterScheduler = NoSerRouterScheduler<
        CertificateSignaturePubKey<Self::SignatureType>,
        MonadMessage<
            Self::SignatureType,
            Self::SignatureCollectionType,
            Self::ExecutionProtocolType,
        >,
        VerifiedMonadMessage<
            Self::SignatureType,
            Self::SignatureCollectionType,
            Self::ExecutionProtocolType,
        >,
    >;

    type Pipeline =
        MonadMessageTransformerPipeline<CertificateSignaturePubKey<Self::SignatureType>>;

    type StateRootHashExecutor = MockStateRootHashNop<
        Self::SignatureType,
        Self::SignatureCollectionType,
        Self::ExecutionProtocolType,
    >;
    type TxPoolExecutor = MockTxPoolExecutor<
        Self::SignatureType,
        Self::SignatureCollectionType,
        Self::ExecutionProtocolType,
        Self::BlockPolicyType,
        Self::StateBackendType,
    >;
    type StateSyncExecutor = MockStateSyncExecutor<
        Self::SignatureType,
        Self::SignatureCollectionType,
        Self::ExecutionProtocolType,
    >;
}
