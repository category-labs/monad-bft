use std::{collections::HashSet, time::Duration};

use monad_executor::{
    transformer::{
        DropTransformer, LatencyTransformer, PartitionTransformer, PeriodicTranformer, Transformer,
        TransformerPipeline,
    },
    PeerId,
};
use monad_testutil::swarm::{get_configs, run_nodes_until_step};

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
    let (pubkeys, state_configs) = get_configs(num_nodes, delta);

    assert!(num_nodes >= 2, "test requires 2 or more nodes");

    let first_node = PeerId(*pubkeys.first().unwrap());

    let mut filter_peers: HashSet<PeerId> = HashSet::new();
    filter_peers.insert(first_node);

    println!("delayed node ID: {:?}", first_node);

    run_nodes_until_step(
        TransformerPipeline::new(vec![
            Transformer::Latency(LatencyTransformer(Duration::from_millis(1))), // everyone get delayed no matter what
            Transformer::Partition(PartitionTransformer(filter_peers)), // partition the victim node
            Transformer::Periodic(PeriodicTranformer::new(20, 50)),
            Transformer::Drop(DropTransformer()),
        ]),
        pubkeys,
        state_configs,
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
    let (pubkeys, state_configs) = get_configs(num_nodes, delta);

    assert!(num_nodes >= 2, "test requires 2 or more nodes");

    let first_node = PeerId(*pubkeys.first().unwrap());

    let mut filter_peers: HashSet<PeerId> = HashSet::new();
    filter_peers.insert(first_node);

    println!("delayed node ID: {:?}", first_node);

    run_nodes_until_step(
        TransformerPipeline::new(vec![
            Transformer::Latency(LatencyTransformer(Duration::from_millis(1))), // everyone get delayed no matter what
            Transformer::Partition(PartitionTransformer(filter_peers)), // partition the victim node
            Transformer::Periodic(PeriodicTranformer::new(20, 20)),
            Transformer::Latency(LatencyTransformer(Duration::from_millis(400))), // delayed by a whole 2 seconds
        ]),
        pubkeys,
        state_configs,
        800,
    );
}
