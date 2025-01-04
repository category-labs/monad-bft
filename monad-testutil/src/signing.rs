use std::{collections::HashSet, marker::PhantomData};

use alloy_rlp::{Decodable, Encodable};
use monad_consensus::validation::signing::{Unvalidated, Unverified};
use monad_consensus_types::{
    signature_collection::{
        SignatureCollection, SignatureCollectionError, SignatureCollectionKeyPairType,
    },
    voting::ValidatorMapping,
};
use monad_crypto::{
    certificate_signature::{
        CertificateKeyPair, CertificateSignaturePubKey, CertificateSignatureRecoverable,
    },
    hasher::{Hash, Hashable, Hasher, HasherType},
};
use monad_types::NodeId;

#[derive(Clone, Default, Debug, PartialEq, Eq)]
pub struct MockSignatures<ST: CertificateSignatureRecoverable> {
    pubkey: Vec<CertificateSignaturePubKey<ST>>,
}

impl<ST: CertificateSignatureRecoverable> MockSignatures<ST> {
    pub fn with_pubkeys(pubkeys: &[CertificateSignaturePubKey<ST>]) -> Self {
        Self {
            pubkey: pubkeys.to_vec(),
        }
    }
}

impl<ST: CertificateSignatureRecoverable> Hashable for MockSignatures<ST> {
    fn hash(&self, _state: &mut impl Hasher) {}
}

impl<ST: CertificateSignatureRecoverable> SignatureCollection for MockSignatures<ST> {
    type NodeIdPubKey = CertificateSignaturePubKey<ST>;
    type SignatureType = ST;

    fn new(
        _sigs: impl IntoIterator<Item = (NodeId<Self::NodeIdPubKey>, Self::SignatureType)>,
        _validator_mapping: &ValidatorMapping<
            Self::NodeIdPubKey,
            SignatureCollectionKeyPairType<Self>,
        >,
        _msg: &[u8],
    ) -> Result<Self, SignatureCollectionError<Self::NodeIdPubKey, Self::SignatureType>> {
        Ok(Self { pubkey: Vec::new() })
    }

    fn get_hash(&self) -> Hash {
        Default::default()
    }

    fn verify(
        &self,
        _validator_mapping: &ValidatorMapping<
            Self::NodeIdPubKey,
            SignatureCollectionKeyPairType<Self>,
        >,
        _msg: &[u8],
    ) -> Result<
        Vec<NodeId<Self::NodeIdPubKey>>,
        SignatureCollectionError<Self::NodeIdPubKey, Self::SignatureType>,
    > {
        Ok(self
            .pubkey
            .iter()
            .map(|pubkey| NodeId::new(*pubkey))
            .collect())
    }

    fn get_participants(
        &self,
        _validator_mapping: &ValidatorMapping<
            Self::NodeIdPubKey,
            SignatureCollectionKeyPairType<Self>,
        >,
        _msg: &[u8],
    ) -> HashSet<NodeId<Self::NodeIdPubKey>> {
        HashSet::from_iter(self.pubkey.iter().map(|pubkey| NodeId::new(*pubkey)))
    }

    fn num_signatures(&self) -> usize {
        self.pubkey.len()
    }

    fn serialize(&self) -> Vec<u8> {
        unreachable!()
    }

    fn deserialize(
        _data: &[u8],
    ) -> Result<Self, SignatureCollectionError<Self::NodeIdPubKey, Self::SignatureType>> {
        unreachable!()
    }
}

impl<ST: CertificateSignatureRecoverable> Encodable for MockSignatures<ST> {
    fn encode(&self, _out: &mut dyn alloy_rlp::BufMut) {}
}

impl<ST: CertificateSignatureRecoverable> Decodable for MockSignatures<ST> {
    fn decode(_buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        unreachable!()
    }
}

pub fn node_id<ST: CertificateSignatureRecoverable>() -> NodeId<CertificateSignaturePubKey<ST>> {
    let mut privkey: [u8; 32] = [127; 32];
    let keypair = ST::KeyPairType::from_bytes(&mut privkey).unwrap();
    NodeId::new(keypair.pubkey())
}

pub fn create_keys<ST: CertificateSignatureRecoverable>(num_keys: u32) -> Vec<ST::KeyPairType> {
    let mut res = Vec::new();
    for i in 0..num_keys {
        let keypair = get_key::<ST>(i.into());
        res.push(keypair);
    }

    res
}

pub fn create_certificate_keys<SCT: SignatureCollection>(
    num_keys: u32,
) -> Vec<SignatureCollectionKeyPairType<SCT>> {
    let mut res = Vec::new();
    for i in 0..num_keys {
        // (i+u32::MAX) makes sure that the MessageKeyPair != CertificateKeyPair
        // so we don't accidentally mis-sign stuff without test noticing
        let keypair = get_certificate_key::<SCT>(i as u64 + u32::MAX as u64);
        res.push(keypair);
    }
    res
}

pub fn create_seed_for_certificate_keys<SCT: SignatureCollection>(num_keys: u32) -> Vec<u64> {
    (0..num_keys).map(|i| i as u64 + u32::MAX as u64).collect()
}

pub struct TestSigner<S> {
    _p: PhantomData<S>,
}

impl<ST: CertificateSignatureRecoverable> TestSigner<ST> {
    pub fn sign_object<T: Hashable>(o: T, key: &ST::KeyPairType) -> Unverified<ST, Unvalidated<T>> {
        let msg = HasherType::hash_object(&o);
        let sig = ST::sign(msg.as_ref(), key);

        Unverified::new(Unvalidated::new(o), sig)
    }
}

impl<ST: CertificateSignatureRecoverable> TestSigner<ST> {
    pub fn sign_incorrect_object<T: Hashable>(
        signed_object: T,
        unsigned_object: T,
        key: &ST::KeyPairType,
    ) -> Unverified<ST, Unvalidated<T>> {
        let msg = HasherType::hash_object(&signed_object);
        let sig = ST::sign(msg.as_ref(), key);

        Unverified::new(Unvalidated::new(unsigned_object), sig)
    }
}

pub fn get_key<ST: CertificateSignatureRecoverable>(seed: u64) -> ST::KeyPairType {
    let mut hasher = HasherType::new();
    hasher.update(seed.to_le_bytes());
    let mut hash = hasher.hash();
    <ST::KeyPairType as CertificateKeyPair>::from_bytes(&mut hash.0).unwrap()
}

// FIXME a lot of these functions can be collapsed now that CertificateSignature is generic
pub fn get_certificate_key<SCT: SignatureCollection>(
    seed: u64,
) -> SignatureCollectionKeyPairType<SCT> {
    let mut hasher = HasherType::new();
    hasher.update(seed.to_le_bytes());
    let mut hash = hasher.hash();
    <SignatureCollectionKeyPairType<SCT> as CertificateKeyPair>::from_bytes(&mut hash.0).unwrap()
}
