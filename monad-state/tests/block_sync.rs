use std::{collections::HashSet, time::Duration};

use monad_block_sync::BlockSyncState;
use monad_consensus_state::ConsensusState;
use monad_consensus_types::{multi_sig::MultiSig, transaction_validator::MockValidator};
use monad_crypto::NopSignature;
use monad_executor::{
    executor::mock::{MockMempool, NoSerRouterConfig, NoSerRouterScheduler},
    mock_swarm::NodesConfig,
    transformer::{
        DropTransformer, LatencyTransformer, PartitionTransformer, PeriodicTransformer,
        Transformer, TransformerPipeline,
    },
    PeerId,
};
use monad_state::{MonadMessage, MonadState};
use monad_testutil::swarm::{get_configs, prep_peers, run_nodes_until_step};
use monad_validator::{simple_round_robin::SimpleRoundRobin, validator_set::ValidatorSet};
use monad_wal::mock::{MockWALogger, MockWALoggerConfig};

type S = MonadState<
    ConsensusState<NopSignature, MultiSig<NopSignature>, MockValidator>,
    NopSignature,
    MultiSig<NopSignature>,
    ValidatorSet,
    SimpleRoundRobin,
    BlockSyncState,
>;
/**
*  Simulate at step 20, first node lost contact
*  completely with outside world for 50 messages (both in and out)
*/
#[test]
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
    let nodes_config = NodesConfig {
        peers: prep_peers::<
            S,
            NopSignature,
            MultiSig<NopSignature>,
            NoSerRouterScheduler<MonadMessage<_, _>>,
            _,
            MockWALogger<_>,
            MockValidator,
        >(
            pubkeys,
            state_configs,
            |all_peers: Vec<_>, _| NoSerRouterConfig {
                all_peers: all_peers.into_iter().collect(),
            },
            MockWALoggerConfig,
        ),
        pipeline: TransformerPipeline::new(vec![
            Transformer::Latency(LatencyTransformer(Duration::from_millis(1))),
            Transformer::Partition(PartitionTransformer(filter_peers)),
            Transformer::Periodic(PeriodicTransformer::new(20, 50)),
            Transformer::Drop(DropTransformer()),
        ]),
        ..Default::default()
    };

    run_nodes_until_step::<
        S,
        NopSignature,
        MultiSig<NopSignature>,
        NoSerRouterScheduler<MonadMessage<_, _>>,
        MockWALogger<_>,
        _,
        MockValidator,
        MockMempool<_>,
    >(nodes_config, 2000);
}
/**
 *  Couple messages gets delayed significantly at step 20
 */
#[test]
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
    let nodes_config = NodesConfig {
        peers: prep_peers::<
            S,
            NopSignature,
            MultiSig<NopSignature>,
            NoSerRouterScheduler<MonadMessage<_, _>>,
            _,
            MockWALogger<_>,
            MockValidator,
        >(
            pubkeys,
            state_configs,
            |all_peers: Vec<_>, _| NoSerRouterConfig {
                all_peers: all_peers.into_iter().collect(),
            },
            MockWALoggerConfig,
        ),
        pipeline: TransformerPipeline::new(vec![
            Transformer::Latency(LatencyTransformer(Duration::from_millis(1))),
            Transformer::Partition(PartitionTransformer(filter_peers)),
            Transformer::Periodic(PeriodicTransformer::new(20, 50)),
            Transformer::Latency(LatencyTransformer(Duration::from_millis(400))),
        ]),
        ..Default::default()
    };
    run_nodes_until_step::<
        S,
        NopSignature,
        MultiSig<NopSignature>,
        NoSerRouterScheduler<MonadMessage<_, _>>,
        MockWALogger<_>,
        _,
        MockValidator,
        MockMempool<_>,
    >(nodes_config, 2000);
}
