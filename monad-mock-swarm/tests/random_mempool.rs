use std::time::Duration;

use monad_consensus_state::ConsensusState;
use monad_consensus_types::{
    multi_sig::MultiSig, payload::StateRoot, transaction_validator::MockValidator,
};
use monad_crypto::NopSignature;
use monad_executor::timed_event::TimedEvent;
use monad_executor_glue::MonadEvent;
use monad_mock_swarm::{
    mock::{MockMempoolMessage, MockMempoolRandFail, MockMempoolRandFailConfig},
    mock_swarm::UntilTerminator,
    swarm_relation::SwarmRelation,
};
use monad_router_scheduler::{NoSerRouterConfig, NoSerRouterScheduler};
use monad_state::{MonadMessage, MonadState, VerifiedMonadMessage};
use monad_testutil::swarm::{create_and_run_nodes, SwarmTestConfig};
use monad_transformer::{GenericTransformer, GenericTransformerPipeline, LatencyTransformer};
use monad_validator::{simple_round_robin::SimpleRoundRobin, validator_set::ValidatorSet};
use monad_wal::mock::{MockWALogger, MockWALoggerConfig};

struct RandFailSwarm;
impl SwarmRelation for RandFailSwarm {
    type SignatureType = NopSignature;
    type SignatureCollectionType = MultiSig<Self::SignatureType>;

    type InboundMessage = MonadMessage<Self::SignatureType, Self::SignatureCollectionType>;
    type OutboundMessage = VerifiedMonadMessage<Self::SignatureType, Self::SignatureCollectionType>;
    type TransportMessage = Self::OutboundMessage;

    type TransactionValidator = MockValidator;

    type State = MonadState<
        ConsensusState<MultiSig<Self::SignatureType>, Self::TransactionValidator, StateRoot>,
        Self::SignatureType,
        Self::SignatureCollectionType,
        ValidatorSet,
        SimpleRoundRobin,
    >;

    type RouterSchedulerConfig = NoSerRouterConfig;
    type RouterScheduler = NoSerRouterScheduler<Self::InboundMessage, Self::OutboundMessage>;

    type Pipeline = GenericTransformerPipeline<Self::TransportMessage>;

    type LoggerConfig = MockWALoggerConfig;
    type Logger =
        MockWALogger<TimedEvent<MonadEvent<Self::SignatureType, Self::SignatureCollectionType>>>;

    type MempoolInboundMessage = MockMempoolMessage;
    type MempoolOutboundMessage = MockMempoolMessage;
    type MempoolTransportMessage = MockMempoolMessage;

    type MempoolConfig = MockMempoolRandFailConfig;
    type MempoolExecutor = MockMempoolRandFail<Self::SignatureType, Self::SignatureCollectionType>;
    type MempoolRouterSchedulerConfig = NoSerRouterConfig;
    type MempoolRouterScheduler =
        NoSerRouterScheduler<Self::MempoolInboundMessage, Self::MempoolOutboundMessage>;
}

#[test]
fn random_mempool_failures() {
    create_and_run_nodes::<RandFailSwarm, _, _, _>(
        MockValidator,
        |all_peers, _| NoSerRouterConfig {
            all_peers: all_peers.into_iter().collect(),
        },
        |all_peers, _| NoSerRouterConfig {
            all_peers: all_peers.into_iter().collect(),
        },
        MockWALoggerConfig,
        MockMempoolRandFailConfig {
            fail_rate: 0.1,
            seed: 0,
        },
        vec![GenericTransformer::Latency(LatencyTransformer(
            Duration::from_millis(1),
        ))],
        UntilTerminator::new().until_tick(Duration::from_secs(7)),
        SwarmTestConfig {
            num_nodes: 4,
            consensus_delta: Duration::from_millis(2),
            parallelize: false,
            expected_block: 1024,
            state_root_delay: 4,
            seed: 1,
            proposal_size: 0,
        },
    );
}
