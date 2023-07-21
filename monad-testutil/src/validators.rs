use monad_consensus_types::signature::{SignatureCollection, SignatureCollectionKeyPairType};
use monad_crypto::{bls12_381::BlsKeyPair, secp256k1::KeyPair, GenericKeyPair};
use monad_types::{NodeId, Round, Stake};
use monad_validator::{
    leader_election::LeaderElection,
    validator_property::{ValidatorSetProperty, ValidatorSetPropertyType},
    validator_set::{ValidatorSet, ValidatorSetType},
};

use crate::signing::{create_keys, create_keys_bls, create_voting_keys};

pub struct MockLeaderElection {
    leader: NodeId,
}

impl LeaderElection for MockLeaderElection {
    fn new() -> Self {
        let mut key: [u8; 32] = [128; 32];
        let keypair = KeyPair::from_bytes(&mut key).unwrap();
        let leader = keypair.pubkey();
        MockLeaderElection {
            leader: NodeId(leader),
        }
    }

    fn get_leader(&self, _round: Round, _validator_list: &[NodeId]) -> NodeId {
        self.leader
    }
}

pub fn create_keys_w_validators<SCT: SignatureCollection>(
    num_nodes: u32,
) -> (
    Vec<KeyPair>,
    Vec<BlsKeyPair>,
    Vec<SignatureCollectionKeyPairType<SCT>>,
    ValidatorSet,
    ValidatorSetProperty,
) {
    let keys = create_keys(num_nodes);
    let blskeys = create_keys_bls(num_nodes);
    let voting_keys = create_voting_keys::<SCT>(num_nodes);

    let staking_list = keys
        .iter()
        .map(|k| NodeId(k.pubkey()))
        .zip(std::iter::repeat(Stake(1)))
        .collect::<Vec<_>>();

    let prop_list = keys
        .iter()
        .map(|k| NodeId(k.pubkey()))
        .zip(blskeys.iter().map(|k| k.pubkey()))
        .collect::<Vec<_>>();

    let validators = ValidatorSet::new(staking_list).expect("create validator set");
    let validators_property =
        ValidatorSetProperty::new(prop_list).expect("create validator set property");

    (keys, blskeys, voting_keys, validators, validators_property)
}
