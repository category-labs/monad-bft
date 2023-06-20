use std::path::PathBuf;
use std::time::Duration;

use monad_consensus_types::multi_sig::MultiSig;
use monad_crypto::NopSignature;
use monad_executor::{
    mock_swarm::{Nodes, Transformer},
    timed_event::TimedEvent,
    State,
};
use monad_state::{MonadEvent, MonadState};
use monad_testutil::swarm::get_configs;
use monad_wal::wal::{WALogger, WALoggerConfig};

type MS = MonadState<SignatureType, SignatureCollectionType>;
type MM = <MS as State>::Message;
type SignatureType = NopSignature;
type SignatureCollectionType = MultiSig<SignatureType>;

pub fn generate_log<T: Transformer<MM>>(
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
    });
    let peers = pubkeys
        .into_iter()
        .zip(state_configs)
        .zip(file_path_vec)
        .map(|((a, b), c)| (a, b, c))
        .collect::<Vec<_>>();
    let mut nodes = Nodes::<MS, T, WALoggerType>::new(peers, transformer);

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
    use monad_executor::mock_swarm::LatencyTransformer;
    generate_log(
        4,
        10,
        Duration::from_millis(101),
        LatencyTransformer(Duration::from_millis(100)),
    );
    println!("Logs Generated!");
}
