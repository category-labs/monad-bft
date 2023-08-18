use std::{path::PathBuf, time::Duration};

use monad_block_sync::BlockSyncState;
use monad_consensus_state::ConsensusState;
use monad_consensus_types::{multi_sig::MultiSig, transaction_validator::MockValidator};
use monad_crypto::NopSignature;
use monad_executor::{
    executor::mock::NoSerRouterScheduler,
    mock_swarm::Nodes,
    timed_event::TimedEvent,
    transformer::{Pipeline, Transformer, TransformerPipeline},
    xfmr_pipe, State,
};
use monad_state::{MonadEvent, MonadState};
use monad_testutil::swarm::get_configs;
use monad_validator::{simple_round_robin::SimpleRoundRobin, validator_set::ValidatorSet};
use monad_wal::wal::{WALogger, WALoggerConfig};

type SignatureType = NopSignature;
type SignatureCollectionType = MultiSig<SignatureType>;
type MS = MonadState<
    ConsensusState<SignatureType, SignatureCollectionType, MockValidator>,
    SignatureType,
    SignatureCollectionType,
    ValidatorSet,
    SimpleRoundRobin,
    BlockSyncState,
>;
type MM = <MS as State>::Message;

pub fn generate_log<T: Pipeline<MM>>(
    num_nodes: u16,
    num_blocks: usize,
    delta: Duration,
    transformer: T,
) where
    T: Clone,
{
    type WALoggerType = WALogger<TimedEvent<MonadEvent<SignatureType, SignatureCollectionType>>>;
    let (pubkeys, state_configs) = get_configs(num_nodes, delta);
    let binding = pubkeys.clone();
    let file_path_vec = binding.iter().map(|pubkey| WALoggerConfig {
        file_path: PathBuf::from(format!("{:?}.log", pubkey)),
        sync: false,
    });
    let peers = pubkeys
        .into_iter()
        .zip(state_configs)
        .zip(file_path_vec)
        .map(|((a, b), c)| (a, b, c))
        .collect::<Vec<_>>();
    let mut nodes = Nodes::<MS, NoSerRouterScheduler<MM>, T, WALoggerType>::new(peers, transformer);

    while let Some((duration, id, event)) = nodes.step() {
        if nodes
            .states()
            .values()
            .next()
            .unwrap()
            .0
            .ledger()
            .get_blocks()
            .len()
            > num_blocks
        {
            break;
        }
    }
}

fn main() {
    use monad_executor::transformer::LatencyTransformer;
    generate_log(
        4,
        10,
        Duration::from_millis(101),
        xfmr_pipe!(Transformer::Latency(LatencyTransformer(
            Duration::from_millis(100),
        ))),
    );
    println!("Logs Generated!");
}
