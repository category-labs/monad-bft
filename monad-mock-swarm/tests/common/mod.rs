use bytes::Bytes;
use monad_async_state_verify::PeerAsyncStateVerify;
use monad_consensus_types::{
    block::PassthruBlockPolicy, block_validator::MockValidator, payload::StateRoot,
    txpool::MockTxPool,
};
use monad_crypto::{certificate_signature::CertificateSignaturePubKey, NopSignature};
use monad_gossip::mock::MockGossip;
use monad_mock_swarm::swarm_relation::SwarmRelation;
use monad_multi_sig::MultiSig;
use monad_quic::QuicRouterScheduler;
use monad_state::{MonadMessage, VerifiedMonadMessage};
use monad_state_backend::InMemoryState;
use monad_transformer::BytesTransformerPipeline;
use monad_updaters::{
    ledger::MockLedger, staked_discovery::MockStakedDiscovery,
    state_root_hash::MockStateRootHashNop, statesync::MockStateSyncExecutor,
};
use monad_validator::{
    simple_round_robin::SimpleRoundRobin,
    validator_set::{ValidatorSetFactory, ValidatorSetTypeFactory},
};

pub struct QuicSwarm;

impl SwarmRelation for QuicSwarm {
    type SignatureType = NopSignature;
    type SignatureCollectionType = MultiSig<Self::SignatureType>;
    type StateBackendType = InMemoryState;
    type BlockPolicyType = PassthruBlockPolicy;

    type TransportMessage = Bytes;

    type BlockValidator = MockValidator;
    type StateRootValidator = StateRoot;
    type ValidatorSetTypeFactory =
        ValidatorSetFactory<CertificateSignaturePubKey<Self::SignatureType>>;
    type LeaderElection = SimpleRoundRobin<CertificateSignaturePubKey<Self::SignatureType>>;
    type TxPool = MockTxPool;
    type AsyncStateRootVerify = PeerAsyncStateVerify<
        Self::SignatureCollectionType,
        <Self::ValidatorSetTypeFactory as ValidatorSetTypeFactory>::ValidatorSetType,
    >;

    type RouterScheduler = QuicRouterScheduler<
        MockGossip<CertificateSignaturePubKey<Self::SignatureType>>,
        MonadMessage<Self::SignatureType, Self::SignatureCollectionType>,
        VerifiedMonadMessage<Self::SignatureType, Self::SignatureCollectionType>,
    >;
    type Ledger = MockLedger<Self::SignatureType, Self::SignatureCollectionType>;

    type Pipeline = BytesTransformerPipeline<CertificateSignaturePubKey<Self::SignatureType>>;

    type StateRootHashExecutor =
        MockStateRootHashNop<Self::SignatureType, Self::SignatureCollectionType>;
    type StateSyncExecutor =
        MockStateSyncExecutor<Self::SignatureType, Self::SignatureCollectionType>;
    type DiscoveryExecutor =
        MockStakedDiscovery<Self::SignatureType, Self::SignatureCollectionType>;
}
