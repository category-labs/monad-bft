use bytes::Bytes;
use monad_async_state_verify::PeerAsyncStateVerify;
use monad_consensus_types::{
    block::PassthruBlockPolicy, block_validator::MockValidator, payload::StateRoot,
    txpool::MockTxPool,
};
use monad_crypto::{certificate_signature::CertificateSignaturePubKey, NopSignature};
use monad_eth_reserve_balance::PassthruReserveBalanceCache;
use monad_gossip::mock::MockGossip;
use monad_mock_swarm::swarm_relation::SwarmRelation;
use monad_multi_sig::MultiSig;
use monad_quic::QuicRouterScheduler;
use monad_state::{MonadMessage, VerifiedMonadMessage};
use monad_transformer::BytesTransformerPipeline;
use monad_updaters::state_root_hash::MockStateRootHashNop;
use monad_validator::{
    simple_round_robin::SimpleRoundRobin,
    validator_set::{ValidatorSetFactory, ValidatorSetTypeFactory},
};

pub struct QuicSwarm;

impl SwarmRelation for QuicSwarm {
    type SignatureType = NopSignature;
    type SignatureCollectionType = MultiSig<Self::SignatureType>;
    type BlockPolicyType = PassthruBlockPolicy;
    type ReserveBalanceCacheType = PassthruReserveBalanceCache;

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

    type Pipeline = BytesTransformerPipeline<CertificateSignaturePubKey<Self::SignatureType>>;

    type StateRootHashExecutor =
        MockStateRootHashNop<Self::SignatureType, Self::SignatureCollectionType>;
}
