use std::time::Duration;

use monad_block_sync::BlockSyncState;
use monad_consensus_state::ConsensusState;
use monad_consensus_types::{bls::BlsSignatureCollection, transaction_validator::MockValidator};
use monad_crypto::secp256k1::SecpSignature;
use monad_executor::{
    executor::mock::{MockMempool, NoSerRouterConfig, NoSerRouterScheduler},
    transformer::{LatencyTransformer, Transformer, TransformerPipeline},
    xfmr_pipe,
};
use monad_state::{MonadMessage, MonadState};
use monad_testutil::swarm::run_nodes;
use monad_validator::{simple_round_robin::SimpleRoundRobin, validator_set::ValidatorSet};
use monad_wal::mock::{MockWALogger, MockWALoggerConfig};

type SignatureType = SecpSignature;
type SignatureCollectionType = BlsSignatureCollection;

#[test]
fn two_nodes_bls() {
    tracing_subscriber::fmt::init();

    run_nodes::<
        MonadState<
            ConsensusState<SignatureType, SignatureCollectionType, MockValidator>,
            SignatureType,
            SignatureCollectionType,
            ValidatorSet,
            SimpleRoundRobin,
            BlockSyncState,
        >,
        SignatureType,
        SignatureCollectionType,
        NoSerRouterScheduler<MonadMessage<_, _>>,
        _,
        MockWALogger<_>,
        _,
        MockValidator,
        MockMempool<_>,
    >(
        MockValidator,
        |all_peers, _| NoSerRouterConfig {
            all_peers: all_peers.into_iter().collect(),
        },
        MockWALoggerConfig,
        xfmr_pipe!(Transformer::Latency::<
            MonadMessage<SignatureType, SignatureCollectionType>,
        >(LatencyTransformer(Duration::from_millis(1)))),
        2,
        128,
        Duration::from_millis(2),
    );
}
