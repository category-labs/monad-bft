// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::marker::PhantomData;

use alloy_rlp::{RlpDecodable, RlpEncodable};
use monad_consensus::validation::signing::{Unvalidated, Unverified};
use monad_crypto::{
    certificate_signature::{
        CertificateKeyPair, CertificateSignaturePubKey, CertificateSignatureRecoverable,
    },
    hasher::{Hasher, HasherType},
    signing_domain::{self, SigningDomain},
};
use monad_types::NodeId;
use monad_validator::{
    signature_collection::{
        SignatureCollection, SignatureCollectionError, SignatureCollectionKeyPairType,
    },
    validator_mapping::ValidatorMapping,
};
use serde::{Deserialize, Serialize};

#[derive(
    Clone, Default, Debug, PartialEq, Eq, RlpEncodable, RlpDecodable, Serialize, Deserialize,
)]
#[serde(bound(
    serialize = "CertificateSignaturePubKey<ST>: Serialize",
    deserialize = "CertificateSignaturePubKey<ST>: Deserialize<'de>"
))]
pub struct MockSignatures<ST>
where
    ST: CertificateSignatureRecoverable,
{
    pubkey: Vec<CertificateSignaturePubKey<ST>>,
}

impl<ST> MockSignatures<ST>
where
    ST: CertificateSignatureRecoverable,
    CertificateSignaturePubKey<ST>: Serialize + for<'de> Deserialize<'de>,
{
    pub fn with_pubkeys(pubkeys: &[CertificateSignaturePubKey<ST>]) -> Self {
        Self {
            pubkey: pubkeys.to_vec(),
        }
    }
}

impl<ST> SignatureCollection for MockSignatures<ST>
where
    ST: CertificateSignatureRecoverable,
    CertificateSignaturePubKey<ST>: Serialize + for<'de> Deserialize<'de>,
{
    type NodeIdPubKey = CertificateSignaturePubKey<ST>;
    type SignatureType = ST;

    fn new<SD: SigningDomain>(
        _sigs: impl IntoIterator<Item = (NodeId<Self::NodeIdPubKey>, Self::SignatureType)>,
        _validator_mapping: &ValidatorMapping<
            Self::NodeIdPubKey,
            SignatureCollectionKeyPairType<Self>,
        >,
        _msg: &[u8],
    ) -> Result<Self, SignatureCollectionError<Self::NodeIdPubKey, Self::SignatureType>> {
        Ok(Self { pubkey: Vec::new() })
    }

    fn verify<SD: SigningDomain>(
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
    pub fn sign_object<T: alloy_rlp::Encodable>(
        o: T,
        key: &ST::KeyPairType,
    ) -> Unverified<ST, Unvalidated<T>> {
        let msg = alloy_rlp::encode(&o);
        let sig = ST::sign::<signing_domain::ConsensusMessage>(msg.as_ref(), key);

        Unverified::new(Unvalidated::new(o), sig)
    }
}

impl<ST: CertificateSignatureRecoverable> TestSigner<ST> {
    pub fn sign_incorrect_object<T: alloy_rlp::Encodable>(
        signed_object: T,
        unsigned_object: T,
        key: &ST::KeyPairType,
    ) -> Unverified<ST, Unvalidated<T>> {
        let msg = alloy_rlp::encode(&signed_object);
        let sig = ST::sign::<signing_domain::ConsensusMessage>(msg.as_ref(), key);

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
