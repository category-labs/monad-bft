use std::{path::PathBuf, time::Duration};

use monad_block_sync::BlockSyncState;
use monad_consensus_state::ConsensusState;
use monad_consensus_types::{
    multi_sig::MultiSig, payload::NopStateRoot, transaction_validator::MockValidator,
};
use monad_crypto::NopSignature;
use monad_executor::timed_event::TimedEvent;
use monad_executor_glue::{MonadEvent, PeerId};
use monad_mock_swarm::{
    mock::{MockMempool, MockMempoolConfig, NoSerRouterConfig, NoSerRouterScheduler},
    mock_swarm::{Nodes, UntilTerminator},
    swarm_relation::SwarmRelation,
    transformer::{GenericTransformer, GenericTransformerPipeline, LatencyTransformer, ID},
};
use monad_state::{MonadMessage, MonadState, VerifiedMonadMessage};
use monad_testutil::swarm::get_configs;
use monad_validator::{simple_round_robin::SimpleRoundRobin, validator_set::ValidatorSet};
use monad_wal::wal::{WALogger, WALoggerConfig};

pub struct LogSwarm;

impl SwarmRelation for LogSwarm {
    type STATE = MonadState<
        ConsensusState<Self::SCT, MockValidator, NopStateRoot>,
        Self::ST,
        Self::SCT,
        ValidatorSet,
        SimpleRoundRobin,
        BlockSyncState,
    >;
    type ST = NopSignature;
    type SCT = MultiSig<Self::ST>;
    type RS = NoSerRouterScheduler<MonadMessage<Self::ST, Self::SCT>>;
    type P = GenericTransformerPipeline<MonadMessage<Self::ST, Self::SCT>>;
    type LGR = WALogger<TimedEvent<MonadEvent<Self::ST, Self::SCT>>>;
    type ME = MockMempool<Self::ST, Self::SCT>;
    type TVT = MockValidator;
    type LGRCFG = WALoggerConfig;
    type RSCFG = NoSerRouterConfig;
    type MPCFG = MockMempoolConfig;
    type StateMessage = MonadMessage<Self::ST, Self::SCT>;
    type OutboundStateMessage = VerifiedMonadMessage<Self::ST, Self::SCT>;
    type Message = MonadMessage<Self::ST, Self::SCT>;
}

pub fn generate_log(num_nodes: u16, num_blocks: usize, delta: Duration, state_root_delay: u64) {
    let (pubkeys, state_configs) = get_configs::<
        <LogSwarm as SwarmRelation>::ST,
        <LogSwarm as SwarmRelation>::SCT,
        _,
    >(MockValidator, num_nodes, delta, state_root_delay);
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
                ID::new(PeerId(a)),
                b,
                c,
                NoSerRouterConfig {
                    all_peers: pubkeys.iter().map(|pubkey| PeerId(*pubkey)).collect(),
                },
                MockMempoolConfig::default(),
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
    generate_log(4, 10, Duration::from_millis(101), 4);
    println!("Logs Generated!");
}
