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

use std::fmt::Debug;

use alloy_rlp::{Decodable, Encodable};
use monad_crypto::{
    certificate_signature::{CertificateKeyPair, CertificateSignature, PubKey},
    signing_domain::SigningDomain,
};
use monad_types::NodeId;
use serde::{Deserialize, Serialize};

use crate::validator_mapping::ValidatorMapping;

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
    Clone
    + Eq
    + Debug
    + Send
    + Sync
    + Unpin
    + Encodable
    + Decodable
    + Serialize
    + for<'a> Deserialize<'a>
    + 'static
{
    type NodeIdPubKey: PubKey;
    type SignatureType: CertificateSignature + Unpin;

    fn new<SD: SigningDomain>(
        sigs: impl IntoIterator<Item = (NodeId<Self::NodeIdPubKey>, Self::SignatureType)>,
        validator_mapping: &ValidatorMapping<
            Self::NodeIdPubKey,
            SignatureCollectionKeyPairType<Self>,
        >,
        msg: &[u8],
    ) -> Result<Self, SignatureCollectionError<Self::NodeIdPubKey, Self::SignatureType>>;

    fn verify<SD: SigningDomain>(
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

    fn num_signatures(&self) -> usize;

    fn serialize(&self) -> Vec<u8>;
    fn deserialize(
        data: &[u8],
    ) -> Result<Self, SignatureCollectionError<Self::NodeIdPubKey, Self::SignatureType>>;
}
