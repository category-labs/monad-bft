use monad_consensus_types::{multi_sig::MultiSig, transaction_validator::MockValidator};
use monad_crypto::NopSignature;
use monad_executor::timed_event::TimedEvent;
use monad_executor_glue::MonadEvent;
use monad_gossip::mock::{MockGossip, MockGossipConfig};
pub use monad_mock_swarm::swarm_relation::NoSerSwarm;
use monad_mock_swarm::{
    mock::{MockMempool, MockMempoolConfig},
    swarm_relation::{SwarmRelation, SwarmStateType},
    transformer::BytesTransformerPipeline,
};
use monad_quic::{QuicRouterScheduler, QuicRouterSchedulerConfig};
use monad_state::{MonadMessage, VerifiedMonadMessage};
use monad_wal::mock::{MockWALogger, MockWALoggerConfig};

pub struct QuicSwarm;
impl SwarmRelation for QuicSwarm {
    type State = SwarmStateType<Self>;
    type SignatureType = NopSignature;
    type SignatureCollectionType = MultiSig<Self::SignatureType>;
    type RouterScheduler = QuicRouterScheduler<MockGossip>;
    type Pipeline = BytesTransformerPipeline;
    type Logger =
        MockWALogger<TimedEvent<MonadEvent<Self::SignatureType, Self::SignatureCollectionType>>>;
    type MempoolExecutor = MockMempool<Self::SignatureType, Self::SignatureCollectionType>;
    type TransactionValidator = MockValidator;
    type LoggerConfig = MockWALoggerConfig;
    type RouterSchedulerConfig = QuicRouterSchedulerConfig<MockGossipConfig>;
    type MempoolConfig = MockMempoolConfig;
    type Message = Vec<u8>;
    type StateMessage = MonadMessage<Self::SignatureType, Self::SignatureCollectionType>;
    type OutboundStateMessage =
        VerifiedMonadMessage<Self::SignatureType, Self::SignatureCollectionType>;
}
