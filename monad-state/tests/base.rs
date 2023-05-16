use std::{collections::HashSet, time::Duration};

use monad_consensus::{
    signatures::aggregate_signature::AggregateSignatures,
    types::quorum_certificate::genesis_vote_info, validation::hashing::Sha256Hash,
};
use monad_crypto::{secp256k1::KeyPair, secp256k1::PubKey, NopSignature};
use monad_executor::{
    mock_swarm::{Nodes, Transformer},
    State,
};
use monad_state::{MonadConfig, MonadState};
use monad_testutil::signing::{create_keys, get_genesis_config};
use monad_types::BlockId;

type SignatureType = NopSignature;
type SignatureCollectionType = AggregateSignatures<SignatureType>;
type MS = MonadState<SignatureType, SignatureCollectionType>;
type MM = <MS as State>::Message;

pub fn get_configs(
    num_nodes: u16,
    delta: Duration,
) -> (Vec<PubKey>, Vec<MonadConfig<SignatureCollectionType>>) {
    let keys = create_keys(num_nodes as u32);
    let pubkeys = keys.iter().map(KeyPair::pubkey).collect::<Vec<_>>();
    let (genesis_block, genesis_sigs) =
        get_genesis_config::<Sha256Hash, SignatureCollectionType>(keys.iter());

    let state_configs = keys
        .into_iter()
        .zip(std::iter::repeat(pubkeys.clone()))
        .map(|(key, pubkeys)| MonadConfig {
            key,
            validators: pubkeys,

            delta,
            genesis_block: genesis_block.clone(),
            genesis_vote_info: genesis_vote_info(genesis_block.get_id()),
            genesis_signatures: genesis_sigs.clone(),
        })
        .collect::<Vec<_>>();

    (pubkeys, state_configs)
}

fn node_ledger_verification(node_blocks: &Vec<Vec<BlockId>>) {
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

pub fn run_nodes<T: Transformer<MM>>(
    num_nodes: u16,
    num_blocks: usize,
    delta: Duration,
    transformer: T,
) {
    let (pubkeys, state_configs) = get_configs(num_nodes, delta);

    let mut nodes = Nodes::<MS, T>::new(
        pubkeys.into_iter().zip(state_configs).collect(),
        transformer,
    );

    while let Some((duration, id, event)) = nodes.step() {
        if nodes.states().values().next().unwrap().1.ledger().len() > num_blocks {
            break;
        }
    }

    // FIXME make this code better.... -.-
    let node_blocks = nodes
        .states()
        .values()
        .map(|(_, state)| {
            state
                .ledger()
                .iter()
                .map(|b| b.get_id())
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    node_ledger_verification(&node_blocks);
}
