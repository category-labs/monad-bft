use monad_types::{Hash, NodeId};

use crate::{
    certificate_signature::{CertificateKeyPair, CertificateSignature},
    validation::{Hashable, Hasher},
    voting::ValidatorMapping,
};

pub type SignatureCollectionKeyPairType<SCT> =
    <<SCT as SignatureCollection>::SignatureType as CertificateSignature>::KeyPairType;
pub type SignatureCollectionPubKeyType<SCT> =
    <SignatureCollectionKeyPairType<SCT> as CertificateKeyPair>::PubKeyType;

pub trait SignatureCollection: Clone + Hashable + Send + Sync + std::fmt::Debug + 'static {
    type SignatureError: std::error::Error + Send + Sync;
    type SignatureType: CertificateSignature;

    fn new(
        sigs: Vec<(NodeId, Self::SignatureType)>,
        validator_mapping: &ValidatorMapping<SignatureCollectionKeyPairType<Self>>,
        msg: &[u8],
    ) -> Result<Self, Self::SignatureError>;

    // hash of all the signatures
    fn get_hash<H: Hasher>(&self) -> Hash;

    fn verify(
        &self,
        validator_mapping: &ValidatorMapping<SignatureCollectionKeyPairType<Self>>,
        msg: &[u8],
    ) -> Result<Vec<NodeId>, Self::SignatureError>;

    // TODO: deprecate this function: only used by tests
    fn num_signatures(&self) -> usize;
}
