use std::{collections::HashSet, time::Duration};

use monad_executor::{
    mock_swarm::{LatencyTransformer, RandLatencyTransformer, Transformer},
    PeerId,
};
use monad_testutil::swarm::{
    get_configs, run_nodes, run_one_delayed_node, PartitionThenReplayTransformer,
    TransformerReplayOrder,
};

use crate::RandomizedTest;

fn random_latency_test(seed: u64) {
    run_nodes(
        4,
        2048,
        Duration::from_millis(250),
        RandLatencyTransformer::new(seed, 330),
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

    run_one_delayed_node(
        vec![
            LatencyTransformer(Duration::from_millis(1)).boxed(),
            PartitionThenReplayTransformer::new(
                filter_peers,
                200,
                TransformerReplayOrder::Random(seed),
            )
            .boxed(),
        ],
        pubkeys,
        state_configs,
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
