use std::{collections::HashSet, time::Duration};

use monad_executor::{
    transformer::{
        LatencyTransformer, PartitionTransformer, RandLatencyTransformer, ReplayTransformer,
        Transformer, TransformerPipeline, TransformerReplayOrder,
    },
    xfmr_pipe, PeerId,
};
use monad_testutil::swarm::{get_configs, run_nodes, run_nodes_until_step};

use crate::RandomizedTest;

fn random_latency_test(seed: u64) {
    run_nodes(
        4,
        2048,
        Duration::from_millis(250),
        xfmr_pipe!(Transformer::RandLatency(RandLatencyTransformer::new(
            seed, 330,
        ))),
    );
}

fn delayed_message_test(seed: u64) {
    let num_nodes = 4;
    let delta = Duration::from_millis(2);
    let (pubkeys, state_configs) = get_configs(num_nodes, delta);

    assert!(num_nodes >= 2, "test requires 2 or more nodes");

    let first_node = PeerId(*pubkeys.first().unwrap());

    let mut filter_peers = HashSet::new();
    filter_peers.insert(first_node);

    println!("delayed node ID: {:?}", first_node);

    run_nodes_until_step(
        xfmr_pipe!(
            Transformer::Latency(LatencyTransformer(Duration::from_millis(1))),
            Transformer::Partition(PartitionTransformer(filter_peers)),
            Transformer::Replay(ReplayTransformer::new(
                200,
                TransformerReplayOrder::Random(seed),
            ))
        ),
        pubkeys,
        state_configs,
        400,
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
