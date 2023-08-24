use std::{collections::HashSet, time::Duration};

use monad_block_sync::BlockSyncState;
use monad_consensus_state::ConsensusState;
use monad_consensus_types::{multi_sig::MultiSig, transaction_validator::MockValidator};
use monad_crypto::NopSignature;
use monad_executor::{
    executor::mock::{NoSerRouterConfig, NoSerRouterScheduler},
    transformer::{
        DropTransformer, LatencyTransformer, PartitionTransformer, PeriodicTranformer, Transformer,
        TransformerPipeline,
    },
    PeerId,
};
use monad_state::{MonadMessage, MonadState};
use monad_testutil::swarm::{get_configs, run_nodes_until_step};
use monad_validator::{simple_round_robin::SimpleRoundRobin, validator_set::ValidatorSet};
use monad_wal::mock::{MockWALogger, MockWALoggerConfig};

/**
 *  Simulate the situtation where around step 20, first node lost contact
 *  completely with outside world for about 50 messages (both in and out)
 *
 *  at the moment before block-sync is ready, this will cause certain
 *  nodes to never able to commit again because there is no catch up mechanims
 *
 */
#[test]
#[ignore = "block_sync not ready"] // once block sync is completed this will be removed
fn black_out() {
    let num_nodes = 4;
    let delta = Duration::from_millis(2);
    let (pubkeys, state_configs) =
        get_configs::<NopSignature, MultiSig<NopSignature>, _>(MockValidator, num_nodes, delta);

    assert!(num_nodes >= 2, "test requires 2 or more nodes");

    let first_node = PeerId(*pubkeys.first().unwrap());

    let mut filter_peers: HashSet<PeerId> = HashSet::new();
    filter_peers.insert(first_node);

    println!("delayed node ID: {:?}", first_node);

    run_nodes_until_step::<
        MonadState<
            ConsensusState<NopSignature, MultiSig<NopSignature>, MockValidator>,
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
    >(
        pubkeys,
        state_configs,
        |all_peers: Vec<_>, _| NoSerRouterConfig {
            all_peers: all_peers.into_iter().collect(),
        },
        MockWALoggerConfig,
        TransformerPipeline::new(vec![
            Transformer::Latency(LatencyTransformer(Duration::from_millis(1))), // everyone get delayed no matter what
            Transformer::Partition(PartitionTransformer(filter_peers)), // partition the victim node
            Transformer::Periodic(PeriodicTranformer::new(20, 50)),
            Transformer::Drop(DropTransformer()),
        ]),
        400,
    );
}

/**
 *  Similarly, if there is a couple message that is extremely delayed (but not lost)
 *  Block sync should allow certain nodes to catch up reasonably fast
 *
 *  (precise parameter still neet tuning once block sync is done)
 *
 */
#[test]
#[ignore = "block_sync not ready"] // once block sync is completed this will be removed
fn extreme_delay() {
    let num_nodes = 4;
    let delta = Duration::from_millis(2);
    let (pubkeys, state_configs) =
        get_configs::<NopSignature, MultiSig<NopSignature>, _>(MockValidator, num_nodes, delta);

    assert!(num_nodes >= 2, "test requires 2 or more nodes");

    let first_node = PeerId(*pubkeys.first().unwrap());

    let mut filter_peers: HashSet<PeerId> = HashSet::new();
    filter_peers.insert(first_node);

    println!("delayed node ID: {:?}", first_node);

    run_nodes_until_step::<
        MonadState<
            ConsensusState<NopSignature, MultiSig<NopSignature>, MockValidator>,
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
    >(
        pubkeys,
        state_configs,
        |all_peers: Vec<_>, _| NoSerRouterConfig {
            all_peers: all_peers.into_iter().collect(),
        },
        MockWALoggerConfig,
        TransformerPipeline::new(vec![
            Transformer::Latency(LatencyTransformer(Duration::from_millis(1))), // everyone get delayed no matter what
            Transformer::Partition(PartitionTransformer(filter_peers)), // partition the victim node
            Transformer::Periodic(PeriodicTranformer::new(20, 20)),
            Transformer::Latency(LatencyTransformer(Duration::from_millis(400))), // delayed by a whole 2 seconds
        ]),
        800,
    );
}
