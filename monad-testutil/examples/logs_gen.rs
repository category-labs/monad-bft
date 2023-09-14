use std::{path::PathBuf, time::Duration};

use monad_block_sync::BlockSyncState;
use monad_consensus_state::ConsensusState;
use monad_consensus_types::{
    multi_sig::MultiSig, payload::NopStateRoot, transaction_validator::MockValidator,
};
use monad_crypto::NopSignature;
use monad_executor::{
    executor::mock::{MockMempool, NoSerRouterConfig, NoSerRouterScheduler},
    mock_swarm::Nodes,
    timed_event::TimedEvent,
    transformer::{Pipeline, Transformer, TransformerPipeline},
    xfmr_pipe, PeerId, State,
};
use monad_state::{MonadEvent, MonadState};
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
type ME = <MS as State>::Event;

pub fn generate_log<P: Pipeline<MM>>(
    num_nodes: u16,
    num_blocks: usize,
    delta: Duration,
    pipeline: P,
) where
    P: Clone,
    P: Send,
{
    type WALoggerType = WALogger<TimedEvent<MonadEvent<SignatureType, SignatureCollectionType>>>;
    let (pubkeys, state_configs) =
        get_configs::<NopSignature, MultiSig<NopSignature>, _>(MockValidator, num_nodes, delta);
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
                a,
                b,
                c,
                NoSerRouterConfig {
                    all_peers: pubkeys.iter().map(|pubkey| PeerId(*pubkey)).collect(),
                },
                pipeline.clone(),
            )
        })
        .collect::<Vec<_>>();
    let mut nodes =
        Nodes::<MS, NoSerRouterScheduler<MM>, P, WALoggerType, MockMempool<ME>>::new(peers);

    while let Some((duration, id, event)) = nodes.step() {
        if nodes
            .states()
            .values()
            .next()
            .unwrap()
            .executor
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
