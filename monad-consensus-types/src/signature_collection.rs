use std::{collections::HashSet, fmt::Debug};

use monad_crypto::{
    certificate_signature::{CertificateKeyPair, CertificateSignature, PubKey},
    hasher::{Hash, Hashable},
};
use monad_types::NodeId;

use crate::voting::ValidatorMapping;

pub type SignatureCollectionKeyPairType<SCT> =
    <<SCT as SignatureCollection>::SignatureType as CertificateSignature>::KeyPairType;
pub type SignatureCollectionPubKeyType<SCT> =
    <SignatureCollectionKeyPairType<SCT> as CertificateKeyPair>::PubKeyType;

#[derive(Debug, PartialEq, Eq)]
pub enum SignatureCollectionError<PT: PubKey, S> {
    NodeIdNotInMapping(Vec<(NodeId<PT>, S)>),
    // only possible for non-deterministic signature
    ConflictingSignatures((NodeId<PT>, S, S)),
    InvalidSignaturesCreate(Vec<(NodeId<PT>, S)>),
    InvalidSignaturesVerify,
    DeserializeError(String),
}

impl<PT: PubKey, S: CertificateSignature> std::fmt::Display for SignatureCollectionError<PT, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SignatureCollectionError::NodeIdNotInMapping(v) => {
                write!(f, "NodeId not in validator mapping: {v:?}")
            }
            SignatureCollectionError::ConflictingSignatures((node_id, s1, s2)) => {
                write!(
                    f,
                    "Conflicting signatures from {node_id:?}\ns1: {s1:?}\ns2: {s2:?}"
                )
            }
            SignatureCollectionError::InvalidSignaturesCreate(sig) => {
                write!(f, "Invalid signature on create: ({sig:?})")
            }
            SignatureCollectionError::InvalidSignaturesVerify => {
                write!(f, "Invalid signature on verify")
            }
            SignatureCollectionError::DeserializeError(err) => {
                write!(f, "Deserialization error {:?}", err)
            }
        }
    }
}

impl<PT: PubKey, S: CertificateSignature> std::error::Error for SignatureCollectionError<PT, S> {}

pub trait SignatureCollection:
    Clone + Hashable + Eq + Debug + Send + Sync + Unpin + 'static
{
    type NodeIdPubKey: PubKey;
    type SignatureType: CertificateSignature + Unpin;

    fn new(
        sigs: impl IntoIterator<Item = (NodeId<Self::NodeIdPubKey>, Self::SignatureType)>,
        validator_mapping: &ValidatorMapping<
            Self::NodeIdPubKey,
            SignatureCollectionKeyPairType<Self>,
        >,
        msg: &[u8],
    ) -> Result<Self, SignatureCollectionError<Self::NodeIdPubKey, Self::SignatureType>>;

    // hash of all the signatures
    fn get_hash(&self) -> Hash;

    fn verify(
        &self,
        validator_mapping: &ValidatorMapping<
            Self::NodeIdPubKey,
            SignatureCollectionKeyPairType<Self>,
        >,
        msg: &[u8],
    ) -> Result<
        Vec<NodeId<Self::NodeIdPubKey>>,
        SignatureCollectionError<Self::NodeIdPubKey, Self::SignatureType>,
    >;

    /**
     * Get participants doesn't verify the validity of the certificate,
     * but retrieve any valid nodeId participated given a validator mapping.
     */
    fn get_participants(
        &self,
        validator_mapping: &ValidatorMapping<
            Self::NodeIdPubKey,
            SignatureCollectionKeyPairType<Self>,
        >,
        msg: &[u8],
    ) -> HashSet<NodeId<Self::NodeIdPubKey>>;
    // TODO-4: deprecate this function: only used by tests
    fn num_signatures(&self) -> usize;

    fn serialize(&self) -> Vec<u8>;
    fn deserialize(
        data: &[u8],
    ) -> Result<Self, SignatureCollectionError<Self::NodeIdPubKey, Self::SignatureType>>;
}
