use std::{collections::HashSet, time::Duration};

use monad_block_sync::BlockSyncState;
use monad_consensus_state::ConsensusState;
use monad_consensus_types::{
    multi_sig::MultiSig, payload::NopStateRoot, transaction_validator::MockValidator,
};
use monad_crypto::NopSignature;
use monad_executor::{
    executor::mock::{MockMempool, NoSerRouterConfig, NoSerRouterScheduler},
    transformer::{
        LatencyTransformer, PartitionTransformer, RandLatencyTransformer, ReplayTransformer,
        Transformer, TransformerPipeline, TransformerReplayOrder,
    },
    xfmr_pipe, PeerId,
};
use monad_state::{MonadMessage, MonadState};
use monad_testutil::swarm::{get_configs, run_nodes, run_nodes_until};
use monad_validator::{simple_round_robin::SimpleRoundRobin, validator_set::ValidatorSet};
use monad_wal::mock::{MockWALogger, MockWALoggerConfig};

use crate::RandomizedTest;

fn random_latency_test(seed: u64) {
    run_nodes::<
        MonadState<
            ConsensusState<NopSignature, MultiSig<NopSignature>, MockValidator, NopStateRoot>,
            NopSignature,
            MultiSig<NopSignature>,
            ValidatorSet,
            SimpleRoundRobin,
            BlockSyncState,
        >,
        NopSignature,
        MultiSig<NopSignature>,
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
        xfmr_pipe!(Transformer::RandLatency(RandLatencyTransformer::new(
            seed, 330,
        ))),
        4,
        2048,
        Duration::from_millis(250),
    );
}

fn delayed_message_test(seed: u64) {
    let num_nodes = 4;
    let delta = Duration::from_millis(2);
    let (pubkeys, state_configs) =
        get_configs::<NopSignature, MultiSig<NopSignature>, _>(MockValidator, num_nodes, delta);

    assert!(num_nodes >= 2, "test requires 2 or more nodes");

    let first_node = PeerId(*pubkeys.first().unwrap());

    let mut filter_peers = HashSet::new();
    filter_peers.insert(first_node);

    println!("delayed node ID: {:?}", first_node);

    run_nodes_until::<
        MonadState<
            ConsensusState<NopSignature, MultiSig<NopSignature>, MockValidator, NopStateRoot>,
            NopSignature,
            MultiSig<NopSignature>,
            ValidatorSet,
            SimpleRoundRobin,
            BlockSyncState,
        >,
        NopSignature,
        MultiSig<NopSignature>,
        NoSerRouterScheduler<MonadMessage<_, _>>,
        _,
        MockWALogger<_>,
        _,
        MockValidator,
        MockMempool<_>,
    >(
        pubkeys,
        state_configs,
        |all_peers: Vec<_>, _| NoSerRouterConfig {
            all_peers: all_peers.into_iter().collect(),
        },
        MockWALoggerConfig,
        xfmr_pipe!(
            Transformer::Latency(LatencyTransformer(Duration::from_millis(1))),
            Transformer::Partition(PartitionTransformer(filter_peers)),
            Transformer::Replay(ReplayTransformer::new(
                Duration::from_secs(1),
                TransformerReplayOrder::Random(seed),
            ))
        ),
        Duration::from_secs(2),
    );
}

inventory::submit!(RandomizedTest {
    name: "random_latency",
    func: random_latency_test,
});

inventory::submit!(RandomizedTest {
    name: "delayed_message",
    func: delayed_message_test,
});
