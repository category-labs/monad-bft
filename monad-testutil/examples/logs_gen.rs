use std::{path::PathBuf, time::Duration};

use monad_consensus_state::ConsensusState;
use monad_consensus_types::{
    block_validator::MockValidator, multi_sig::MultiSig, payload::StateRoot,
};
use monad_crypto::NopSignature;
use monad_executor::{timed_event::TimedEvent, State};
use monad_executor_glue::MonadEvent;
use monad_mock_swarm::{
    mock_swarm::{Nodes, UntilTerminator},
    mock_txpool::MockTxPool,
    swarm_relation::SwarmRelation,
};
use monad_router_scheduler::{NoSerRouterConfig, NoSerRouterScheduler};
use monad_state::{MonadMessage, MonadState, VerifiedMonadMessage};
use monad_testutil::swarm::get_configs;
use monad_transformer::{GenericTransformer, GenericTransformerPipeline, LatencyTransformer, ID};
use monad_types::{NodeId, Round, SeqNum};
use monad_updaters::state_root_hash::MockStateRootHashNop;
use monad_validator::{simple_round_robin::SimpleRoundRobin, validator_set::ValidatorSet};
use monad_wal::wal::{WALogger, WALoggerConfig};

pub struct LogSwarm;

impl SwarmRelation for LogSwarm {
    type SignatureType = NopSignature;
    type SignatureCollectionType = MultiSig<Self::SignatureType>;

    type InboundMessage = MonadMessage<Self::SignatureType, Self::SignatureCollectionType>;
    type OutboundMessage = VerifiedMonadMessage<Self::SignatureType, Self::SignatureCollectionType>;
    type TransportMessage = Self::OutboundMessage;

    type TransactionValidator = MockValidator;

    type State = MonadState<
        ConsensusState<Self::SignatureCollectionType, Self::TransactionValidator, StateRoot>,
        Self::SignatureType,
        Self::SignatureCollectionType,
        ValidatorSet,
        SimpleRoundRobin,
        MockTxPool,
    >;

    type RouterSchedulerConfig = NoSerRouterConfig;
    type RouterScheduler = NoSerRouterScheduler<Self::InboundMessage, Self::OutboundMessage>;

    type Pipeline = GenericTransformerPipeline<Self::TransportMessage>;

    type LoggerConfig = WALoggerConfig;
    type Logger =
        WALogger<TimedEvent<MonadEvent<Self::SignatureType, Self::SignatureCollectionType>>>;

    type StateRootHashExecutor = MockStateRootHashNop<
        <Self::State as State>::Block,
        Self::SignatureType,
        Self::SignatureCollectionType,
    >;
}

pub fn generate_log(
    num_nodes: u16,
    num_blocks: usize,
    delta: Duration,
    state_root_delay: u64,
    proposal_size: usize,
    val_set_update_interval: SeqNum,
    epoch_start_delay: Round,
) {
    let (pubkeys, state_configs) = get_configs::<
        <LogSwarm as SwarmRelation>::SignatureType,
        <LogSwarm as SwarmRelation>::SignatureCollectionType,
        _,
    >(
        MockValidator,
        num_nodes,
        delta,
        state_root_delay,
        proposal_size,
        val_set_update_interval,
        epoch_start_delay,
    );
    let file_path_vec = pubkeys.iter().map(|pubkey| WALoggerConfig {
        file_path: PathBuf::from(format!("{:?}.log", pubkey)),
        sync: false,
    });
    let pipeline = vec![GenericTransformer::Latency(LatencyTransformer(
        Duration::from_millis(100),
    ))];
    let peers = pubkeys
        .iter()
        .copied()
        .zip(state_configs)
        .zip(file_path_vec)
        .map(|((a, b), c)| {
            (
                ID::new(NodeId(a)),
                b,
                c,
                NoSerRouterConfig {
                    all_peers: pubkeys.iter().map(|pubkey| NodeId(*pubkey)).collect(),
                },
                pipeline.clone(),
                1,
            )
        })
        .collect::<Vec<_>>();
    let mut nodes = Nodes::<LogSwarm>::new(peers);

    let term = UntilTerminator::new().until_block(num_blocks);

    while nodes.step_until(&term).is_some() {}
}

fn main() {
    generate_log(
        4,
        10,
        Duration::from_millis(101),
        4,
        0,
        SeqNum(2000),
        Round(50),
    );
    println!("Logs Generated!");
}
