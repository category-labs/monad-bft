use monad_consensus_types::{
    certificate_signature::CertificateKeyPair,
    signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
    voting::ValidatorMapping,
};
use monad_crypto::secp256k1::KeyPair;
use monad_types::{NodeId, Round, Stake};
use monad_validator::{
    leader_election::LeaderElection,
    validator_set::{ValidatorSet, ValidatorSetType},
};

use crate::signing::{create_certificate_keys, create_keys};

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
    Vec<SignatureCollectionKeyPairType<SCT>>,
    ValidatorSet,
    ValidatorMapping<SignatureCollectionKeyPairType<SCT>>,
) {
    let keys = create_keys(num_nodes);
    let certificate_keys = create_certificate_keys::<SCT>(num_nodes);

    let staking_list = keys
        .iter()
        .map(|k| NodeId(k.pubkey()))
        .zip(std::iter::repeat(Stake(1)))
        .collect::<Vec<_>>();

    let voting_identity = keys
        .iter()
        .map(|k| NodeId(k.pubkey()))
        .zip(certificate_keys.iter().map(|k| k.pubkey()))
        .collect::<Vec<_>>();

    let validators = ValidatorSet::new(staking_list).expect("create validator set");
    let validator_mapping = ValidatorMapping::new(voting_identity);

    (keys, certificate_keys, validators, validator_mapping)
}
