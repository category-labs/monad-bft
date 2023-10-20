use std::{path::PathBuf, time::Duration};

use monad_block_sync::BlockSyncState;
use monad_consensus_state::ConsensusState;
use monad_consensus_types::{
    multi_sig::MultiSig, payload::NopStateRoot, transaction_validator::MockValidator,
};
use monad_crypto::NopSignature;
use monad_executor::{timed_event::TimedEvent, State};
use monad_executor_glue::{MonadEvent, PeerId};
use monad_mock_swarm::{
    mock::{MockMempool, MockMempoolConfig, NoSerRouterConfig, NoSerRouterScheduler},
    mock_swarm::{Nodes, UntilTerminator},
    transformer::{GenericTransformer, Pipeline, ID},
};
use monad_state::MonadState;
use monad_testutil::swarm::get_configs;
use monad_validator::{simple_round_robin::SimpleRoundRobin, validator_set::ValidatorSet};
use monad_wal::wal::{WALogger, WALoggerConfig};

type SignatureType = NopSignature;
type SignatureCollectionType = MultiSig<SignatureType>;
type MS = MonadState<
    ConsensusState<SignatureCollectionType, MockValidator, NopStateRoot>,
    SignatureType,
    SignatureCollectionType,
    ValidatorSet,
    SimpleRoundRobin,
    BlockSyncState,
>;
type MM = <MS as State>::Message;
type ME = MonadEvent<SignatureType, SignatureCollectionType>;

pub fn generate_log<P: Pipeline<MM>>(
    num_nodes: u16,
    num_blocks: usize,
    delta: Duration,
    state_root_delay: u64,
    pipeline: P,
) where
    P: Clone,
    P: Send,
{
    type WALoggerType = WALogger<TimedEvent<ME>>;
    let (pubkeys, state_configs) = get_configs::<NopSignature, MultiSig<NopSignature>, _>(
        MockValidator,
        num_nodes,
        delta,
        state_root_delay,
    );
    let file_path_vec = pubkeys.iter().map(|pubkey| WALoggerConfig {
        file_path: PathBuf::from(format!("{:?}.log", pubkey)),
        sync: false,
    });
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
    let mut nodes = Nodes::<
        MS,
        NoSerRouterScheduler<MM>,
        P,
        WALoggerType,
        MockMempool<SignatureType, SignatureCollectionType>,
        SignatureType,
        SignatureCollectionType,
    >::new(peers);

    let term = UntilTerminator::new().until_block(num_blocks);

    while nodes.step_until(&term).is_some() {}
}

fn main() {
    use monad_mock_swarm::transformer::LatencyTransformer;
    generate_log(
        4,
        10,
        Duration::from_millis(101),
        4,
        vec![GenericTransformer::Latency(LatencyTransformer(
            Duration::from_millis(100),
        ))],
    );
    println!("Logs Generated!");
}
