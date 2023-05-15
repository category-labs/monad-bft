use std::{collections::HashSet, time::Duration};

use monad_consensus::{
    signatures::aggregate_signature::AggregateSignatures,
    types::quorum_certificate::genesis_vote_info, validation::hashing::Sha256Hash,
};
use monad_crypto::{secp256k1::KeyPair, secp256k1::PubKey, NopSignature};
use monad_executor::mock_swarm::Nodes;
use monad_state::{MonadConfig, MonadState};
use monad_testutil::signing::{create_keys, get_genesis_config};
use monad_types::BlockId;

type SignatureType = NopSignature;
type SignatureCollectionType = AggregateSignatures<SignatureType>;

pub fn get_configs(
    num_nodes: u16,
    delta_ms: u64,
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

            delta: Duration::from_millis(delta_ms),
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

pub fn run_nodes(num_nodes: u16, num_blocks: usize) {
    let (pubkeys, state_configs) = get_configs(num_nodes, 2);

    let mut nodes = Nodes::<MonadState<SignatureType, SignatureCollectionType>, _>::new(
        pubkeys.into_iter().zip(state_configs).collect(),
        |_node_1, _node_2| Duration::from_millis(1),
        Vec::new(),
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

pub fn run_nodes_msg_delays(num_nodes: u16, num_blocks: usize) {
    let (pubkeys, state_configs) = get_configs(num_nodes, 101);

    let mut nodes = Nodes::<MonadState<SignatureType, SignatureCollectionType>, _>::new(
        pubkeys.into_iter().zip(state_configs).collect(),
        |node_1, node_2| {
            let mut ck = 0;
            for b in node_1.0.bytes() {
                ck ^= b;
            }
            for b in node_2.0.bytes() {
                ck ^= b;
            }
            Duration::from_millis(ck as u64 % 600)
        },
        Vec::new(),
    );

    while let Some((duration, id, event)) = nodes.step() {
        if nodes.states().values().next().unwrap().1.ledger().len() > num_blocks {
            break;
        }
    }

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
