use bytes::Bytes;
use monad_consensus_types::{
    block::Block, block_validator::MockValidator, payload::StateRoot, txpool::MockTxPool,
};
use monad_crypto::{certificate_signature::CertificateSignaturePubKey, NopSignature};
use monad_executor::timed_event::TimedEvent;
use monad_executor_glue::MonadEvent;
use monad_gossip::mock::MockGossip;
use monad_mock_swarm::swarm_relation::SwarmRelation;
use monad_multi_sig::MultiSig;
use monad_quic::{QuicRouterScheduler, QuicRouterSchedulerConfig};
use monad_state::{MonadMessage, VerifiedMonadMessage};
use monad_transformer::BytesTransformerPipeline;
use monad_updaters::state_root_hash::MockStateRootHashNop;
use monad_validator::{simple_round_robin::SimpleRoundRobin, validator_set::ValidatorSet};
use monad_wal::mock::{MockWALogger, MockWALoggerConfig};

pub struct QuicSwarm;

impl SwarmRelation for QuicSwarm {
    type SignatureType = NopSignature;
    type SignatureCollectionType = MultiSig<Self::SignatureType>;

    type TransportMessage = Bytes;

    type BlockValidator = MockValidator;
    type StateRootValidator = StateRoot;
    type ValidatorSet = ValidatorSet<CertificateSignaturePubKey<Self::SignatureType>>;
    type LeaderElection = SimpleRoundRobin;
    type TxPool = MockTxPool;

    type RouterSchedulerConfig =
        QuicRouterSchedulerConfig<MockGossip<CertificateSignaturePubKey<Self::SignatureType>>>;
    type RouterScheduler = QuicRouterScheduler<
        MockGossip<CertificateSignaturePubKey<Self::SignatureType>>,
        MonadMessage<Self::SignatureType, Self::SignatureCollectionType>,
        VerifiedMonadMessage<Self::SignatureType, Self::SignatureCollectionType>,
    >;

    type Pipeline = BytesTransformerPipeline<CertificateSignaturePubKey<Self::SignatureType>>;

    type LoggerConfig = MockWALoggerConfig;
    type Logger =
        MockWALogger<TimedEvent<MonadEvent<Self::SignatureType, Self::SignatureCollectionType>>>;

    type StateRootHashExecutor = MockStateRootHashNop<
        Block<Self::SignatureCollectionType>,
        Self::SignatureType,
        Self::SignatureCollectionType,
    >;
}
