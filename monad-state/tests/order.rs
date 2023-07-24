use std::{collections::HashSet, time::Duration};

use monad_executor::{
    mock_swarm::{LatencyTransformer, Transformer},
    PeerId,
};
use monad_testutil::swarm::{
    get_configs, run_one_delayed_node, PartitionThenReplayTransformer, TransformerReplayOrder,
};
use test_case::test_case;

#[test_case(TransformerReplayOrder::Forward; "in order")]
#[test_case(TransformerReplayOrder::Reverse; "reverse order")]
#[test_case(TransformerReplayOrder::Random(1); "random seed 1")]
#[test_case(TransformerReplayOrder::Random(2); "random seed 2")]
#[test_case(TransformerReplayOrder::Random(3); "random seed 3")]
#[test_case(TransformerReplayOrder::Random(4); "random seed 4")]
#[test_case(TransformerReplayOrder::Random(5); "random seed 5")]
fn all_messages_delayed(direction: TransformerReplayOrder) {
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
            PartitionThenReplayTransformer::new(filter_peers, 200, direction).boxed(),
        ],
        pubkeys,
        state_configs,
    );
}
