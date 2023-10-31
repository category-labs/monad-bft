use std::time::Duration;

use monad_block_sync::BlockSyncState;
use monad_consensus_state::ConsensusState;
use monad_consensus_types::{
    bls::BlsSignatureCollection, payload::StateRoot, transaction_validator::MockValidator,
};
use monad_crypto::secp256k1::SecpSignature;
use monad_executor::timed_event::TimedEvent;
use monad_executor_glue::MonadEvent;
use monad_mock_swarm::{
    mock::{MockMempool, MockMempoolConfig, NoSerRouterConfig, NoSerRouterScheduler},
    mock_swarm::UntilTerminator,
    swarm_relation::SwarmRelation,
    transformer::{GenericTransformer, GenericTransformerPipeline, LatencyTransformer},
};
use monad_state::{MonadMessage, MonadState, VerifiedMonadMessage};
use monad_testutil::swarm::{create_and_run_nodes, SwarmTestConfig};
use monad_validator::{simple_round_robin::SimpleRoundRobin, validator_set::ValidatorSet};
use monad_wal::mock::{MockWALogger, MockWALoggerConfig};

struct BLSSwarm;
impl SwarmRelation for BLSSwarm {
    type State = MonadState<
        ConsensusState<Self::SignatureCollectionType, MockValidator, StateRoot>,
        Self::SignatureType,
        Self::SignatureCollectionType,
        ValidatorSet,
        SimpleRoundRobin,
        BlockSyncState,
    >;
    type SignatureType = SecpSignature;
    type SignatureCollectionType = BlsSignatureCollection;
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

type SignatureType = SecpSignature;
type SignatureCollectionType = BlsSignatureCollection;

#[test]
fn two_nodes_bls() {
    tracing_subscriber::fmt::init();

    create_and_run_nodes::<BLSSwarm, _, _>(
        MockValidator,
        |all_peers, _| NoSerRouterConfig {
            all_peers: all_peers.into_iter().collect(),
        },
        MockWALoggerConfig,
        MockMempoolConfig::default(),
        vec![GenericTransformer::Latency::<
            MonadMessage<SignatureType, SignatureCollectionType>,
        >(LatencyTransformer(Duration::from_millis(1)))],
        UntilTerminator::new().until_tick(Duration::from_secs(10)),
        SwarmTestConfig {
            num_nodes: 2,
            consensus_delta: Duration::from_millis(2),
            parallelize: false,
            expected_block: 128,
            state_root_delay: 4,
            seed: 1,
        },
    );
}
