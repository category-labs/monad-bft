use std::{collections::HashSet, time::Duration};

use monad_consensus::{
    types::quorum_certificate::genesis_vote_info, validation::hashing::Sha256Hash,
};
use monad_crypto::secp256k1::KeyPair;
use monad_executor::mock_swarm::Nodes;
use monad_state::{MonadConfig, MonadState, SignatureCollectionType};
use monad_testutil::signing::{create_keys, get_genesis_config};

pub fn run_nodes(num_nodes: u16, num_blocks: usize) {
    let keys = create_keys(num_nodes as u32);
    let pubkeys = keys.iter().map(KeyPair::pubkey).collect::<Vec<_>>();
    let (genesis_block, genesis_sigs) =
        get_genesis_config::<Sha256Hash, SignatureCollectionType>(&keys);

    let state_configs = keys
        .into_iter()
        .zip(std::iter::repeat(pubkeys.clone()))
        .map(|(key, pubkeys)| MonadConfig {
            key,
            validators: pubkeys,

            delta: Duration::from_millis(2),
            genesis_block: genesis_block.clone(),
            genesis_vote_info: genesis_vote_info(genesis_block.get_id()),
            genesis_signatures: genesis_sigs.clone(),
        })
        .collect::<Vec<_>>();

    let mut nodes = Nodes::<MonadState, _>::new(
        pubkeys.into_iter().zip(state_configs).collect(),
        |_node_1, _node_2| Duration::from_millis(1),
    );

    while let Some((duration, id, event)) = nodes.step() {
        if nodes.states().values().next().unwrap().1.ledger().len() > num_blocks {
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
