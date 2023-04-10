use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use monad_crypto::secp256k1::KeyPair;
use monad_executor::mock_swarm::Nodes;
use monad_state::{MonadConfig, MonadState};
use monad_testutil::signing::create_keys;

// FIXME this isn't actually single node, factor this out and name better
#[test]
fn single_node() {
    const NUM_NODES: u16 = 2;

    let keys = create_keys(NUM_NODES as u32);
    let pubkeys = keys.iter().map(KeyPair::pubkey).collect::<Vec<_>>();

    let state_configs = keys
        .into_iter()
        .zip(std::iter::repeat(pubkeys.clone()))
        .map(|(key, pubkeys)| MonadConfig {
            key,
            validators: pubkeys,
        })
        .collect::<Vec<_>>();

    let mut nodes = Nodes::<MonadState, _>::new(
        pubkeys.into_iter().zip(state_configs).collect(),
        |_node_1, _node_2| Duration::from_millis(1),
    );

    while let Some((duration, id, event)) = nodes.step() {
        // println!("{duration:?} => {id:?} => {event:?}");

        if nodes.states().values().next().unwrap().1.ledger().len() > 1024 {
            break;
        }
    }

    // this makes sure that block histories match
    // FIXME make this code better.... -.-
    let node_blocks = nodes
        .states()
        .values()
        .map(|(_, state)| {
            state
                .ledger()
                .into_iter()
                .map(|b| b.get_id())
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let mut counter = 0;
    loop {
        let counter_node_blocks = node_blocks
            .iter()
            .filter_map(|blocks| blocks.get(counter))
            .collect::<Vec<_>>();
        if counter_node_blocks.len() != node_blocks.len() {
            break;
        }
        assert!(
            counter_node_blocks
                .into_iter()
                .collect::<HashSet<_>>()
                .len()
                == 1
        );
        counter += 1;
    }
}
