use std::{collections::HashSet, time::Duration};

use monad_block_sync::BlockSyncState;
use monad_consensus_state::ConsensusState;
use monad_consensus_types::{
    multi_sig::MultiSig, payload::NopStateRoot, transaction_validator::MockValidator,
};
use monad_crypto::NopSignature;
use monad_executor_glue::PeerId;
use monad_mock_swarm::{
    mock::{MockMempool, MockMempoolConfig, NoSerRouterConfig, NoSerRouterScheduler},
    mock_swarm::UntilTerminator,
    transformer::{
        GenericTransformer, LatencyTransformer, PartitionTransformer, RandLatencyTransformer,
        ReplayTransformer, TransformerReplayOrder, ID,
    },
};
use monad_state::{MonadMessage, MonadState};
use monad_testutil::swarm::{create_and_run_nodes, get_configs, run_nodes_until, SwarmTestConfig};
use monad_validator::{simple_round_robin::SimpleRoundRobin, validator_set::ValidatorSet};
use monad_wal::mock::{MockWALogger, MockWALoggerConfig};

use crate::RandomizedTest;

fn random_latency_test(seed: u64) {
    create_and_run_nodes::<
        MonadState<
            ConsensusState<MultiSig<NopSignature>, MockValidator, NopStateRoot>,
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
        MockMempool<_, _>,
        _,
    >(
        MockValidator,
        |all_peers, _| NoSerRouterConfig {
            all_peers: all_peers.into_iter().collect(),
        },
        MockWALoggerConfig,
        MockMempoolConfig::default(),
        vec![GenericTransformer::RandLatency(
            RandLatencyTransformer::new(seed, 330),
        )],
        UntilTerminator::new().until_tick(Duration::from_secs(10)),
        SwarmTestConfig {
            num_nodes: 4,
            consensus_delta: Duration::from_millis(250),
            parallelize: false,
            expected_block: 2048,
            state_root_delay: 4,
            seed: 1,
        },
    );
}

fn delayed_message_test(seed: u64) {
    let num_nodes = 4;
    let delta = Duration::from_millis(2);
    let (pubkeys, state_configs) =
        get_configs::<NopSignature, MultiSig<NopSignature>, _>(MockValidator, num_nodes, delta, 4);

    assert!(num_nodes >= 2, "test requires 2 or more nodes");

    let first_node = PeerId(*pubkeys.first().unwrap());

    let mut filter_peers = HashSet::new();
    filter_peers.insert(ID::new(first_node));

    println!("delayed node ID: {:?}", first_node);

    run_nodes_until::<
        MonadState<
            ConsensusState<MultiSig<NopSignature>, MockValidator, NopStateRoot>,
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
        MockMempool<_, _>,
        _,
    >(
        pubkeys,
        state_configs,
        |all_peers: Vec<_>, _| NoSerRouterConfig {
            all_peers: all_peers.into_iter().collect(),
        },
        MockWALoggerConfig,
        MockMempoolConfig::default(),
        vec![
            GenericTransformer::Latency(LatencyTransformer(Duration::from_millis(1))),
            GenericTransformer::Partition(PartitionTransformer(filter_peers)),
            GenericTransformer::Replay(ReplayTransformer::new(
                Duration::from_secs(1),
                TransformerReplayOrder::Random(seed),
            )),
        ],
        false,
        UntilTerminator::new().until_tick(Duration::from_secs(2)),
        20,
        1,
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
