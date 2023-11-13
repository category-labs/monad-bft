use monad_consensus_types::{
    certificate_signature::CertificateKeyPair,
    signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
    voting::ValidatorMapping,
};
use monad_crypto::secp256k1::KeyPair;
use monad_types::{Epoch, NodeId, Stake};
use monad_validator::validator_set::{ValidatorSet, ValidatorSetType};

use crate::signing::{create_certificate_keys, create_keys};

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
    let (validators, validator_mapping) =
        complete_keys_w_validators::<SCT>(&keys, &certificate_keys);
    (keys, certificate_keys, validators, validator_mapping)
}

pub fn complete_keys_w_validators<SCT: SignatureCollection>(
    keys: &[KeyPair],
    certificate_keys: &[SignatureCollectionKeyPairType<SCT>],
) -> (
    ValidatorSet,
    ValidatorMapping<SignatureCollectionKeyPairType<SCT>>,
) {
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

    let validators = ValidatorSet::new(staking_list, Epoch(1)).expect("create validator set");
    let validator_mapping = ValidatorMapping::new(voting_identity);

    (validators, validator_mapping)
}
