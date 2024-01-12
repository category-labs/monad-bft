use monad_crypto::{
    certificate_signature::{
        CertificateKeyPair, CertificateSignature, CertificateSignaturePubKey,
        CertificateSignatureRecoverable,
    },
    hasher::{Hasher, HasherType},
};
use monad_testutil::signing::create_keys;
use monad_types::NodeId;

use crate::{
    signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
    voting::ValidatorMapping,
};

pub(crate) fn get_certificate_key<SCT: SignatureCollection>(
    seed: u64,
) -> SignatureCollectionKeyPairType<SCT> {
    let mut hasher = HasherType::new();
    hasher.update(seed.to_le_bytes());
    let mut hash = hasher.hash();
    <SignatureCollectionKeyPairType<SCT> as CertificateKeyPair>::from_bytes(&mut hash.0).unwrap()
}

pub(crate) fn create_certificate_keys<SCT: SignatureCollection>(
    num: u32,
) -> Vec<SignatureCollectionKeyPairType<SCT>> {
    let mut res = Vec::new();
    for i in 0..num {
        let keypair = get_certificate_key::<SCT>(i.into());
        res.push(keypair);
    }
    res
}

pub(crate) fn setup_sigcol_test<
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
>(
    num: u32,
) -> (
    Vec<(
        NodeId<SCT::NodeIdPubKey>,
        SignatureCollectionKeyPairType<SCT>,
    )>,
    ValidatorMapping<SCT::NodeIdPubKey, SignatureCollectionKeyPairType<SCT>>,
) {
    let node_ids = create_keys::<ST>(num)
        .into_iter()
        .map(|k| NodeId::new(k.pubkey()))
        .collect::<Vec<_>>();
    let keys = create_certificate_keys::<SCT>(num);

    let voting_keys = node_ids.into_iter().zip(keys).collect::<Vec<_>>();

    let voting_identity = voting_keys
        .iter()
        .map(|(node_id, key)| (*node_id, key.pubkey()))
        .collect::<Vec<_>>();

    let validator_mapping = ValidatorMapping::new(voting_identity);

    (voting_keys, validator_mapping)
}

pub(crate) fn get_sigs<'a, SCT: SignatureCollection>(
    msg: &[u8],
    iter: impl Iterator<
        Item = &'a (
            NodeId<SCT::NodeIdPubKey>,
            SignatureCollectionKeyPairType<SCT>,
        ),
    >,
) -> Vec<(NodeId<SCT::NodeIdPubKey>, SCT::SignatureType)> {
    let mut sigs = Vec::new();
    for (node_id, key) in iter {
        let sig = <SCT::SignatureType as CertificateSignature>::sign(msg, key);
        sigs.push((*node_id, sig));
    }
    sigs
}
