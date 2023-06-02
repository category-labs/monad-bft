use std::collections::HashSet;
use std::time::Duration;

use monad_executor::mock_swarm::{LatencyTransformer, Transformer};
use test_case::test_case;

use crate::base::PartitionThenReplayTransformer;
use monad_executor::PeerId;

mod base;

#[test_case(false; "in order")]
#[test_case(true; "reverse order")]
fn all_messages_delayed(direction: bool) {
    let num_nodes = 4;
    let delta = Duration::from_millis(2);
    let (pubkeys, state_configs) = base::get_configs(num_nodes, delta);

    assert!(num_nodes >= 2, "test requires 2 or more nodes");

    let first_node = PeerId(*pubkeys.first().unwrap());

    let mut filter_peers = HashSet::new();
    filter_peers.insert(first_node);

    println!("delayed node ID: {:?}", first_node);

    base::run_one_delayed_node(
        vec![
            LatencyTransformer(Duration::from_millis(1)).boxed(),
            PartitionThenReplayTransformer {
                peers: filter_peers,
                filtered_msgs: Vec::new(),
                cnt: 0,
                cnt_limit: 200,
                reverse: direction,
            }
            .boxed(),
        ],
        pubkeys,
        state_configs,
    );
}
