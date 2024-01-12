use monad_consensus_types::{
    signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
    voting::ValidatorMapping,
};
use monad_crypto::certificate_signature::{
    CertificateKeyPair, CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_types::{NodeId, Stake};
use monad_validator::validator_set::{ValidatorSet, ValidatorSetType};

use crate::signing::{create_certificate_keys, create_keys};

pub fn create_keys_w_validators<ST, SCT>(
    num_nodes: u32,
) -> (
    Vec<ST::KeyPairType>,
    Vec<SignatureCollectionKeyPairType<SCT>>,
    ValidatorSet<CertificateSignaturePubKey<ST>>,
    ValidatorMapping<CertificateSignaturePubKey<ST>, SignatureCollectionKeyPairType<SCT>>,
)
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    let keys = create_keys::<ST>(num_nodes);
    let certificate_keys = create_certificate_keys::<SCT>(num_nodes);
    let (validators, validator_mapping) =
        complete_keys_w_validators::<ST, SCT>(&keys, &certificate_keys);
    (keys, certificate_keys, validators, validator_mapping)
}

pub fn complete_keys_w_validators<ST, SCT>(
    keys: &[ST::KeyPairType],
    certificate_keys: &[SignatureCollectionKeyPairType<SCT>],
) -> (
    ValidatorSet<CertificateSignaturePubKey<ST>>,
    ValidatorMapping<CertificateSignaturePubKey<ST>, SignatureCollectionKeyPairType<SCT>>,
)
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    let staking_list = keys
        .iter()
        .map(|k| NodeId::new(k.pubkey()))
        .zip(std::iter::repeat(Stake(1)))
        .collect::<Vec<_>>();

    let voting_identity = keys
        .iter()
        .map(|k| NodeId::new(k.pubkey()))
        .zip(certificate_keys.iter().map(|k| k.pubkey()))
        .collect::<Vec<_>>();

    let validators = ValidatorSet::new(staking_list).expect("create validator set");
    let validator_mapping = ValidatorMapping::new(voting_identity);

    (validators, validator_mapping)
}
