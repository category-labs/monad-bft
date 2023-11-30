use monad_proto::{
    error::ProtoError,
    proto::basic::{ProtoHash, ProtoPubkey},
};
use zerocopy::AsBytes;

use crate::{bls12_381::BlsPubKey, hasher::Hash, PubKey};

impl From<&PubKey> for ProtoPubkey {
    fn from(value: &PubKey) -> Self {
        Self {
            pubkey: value.bytes(),
        }
    }
}

impl From<&BlsPubKey> for ProtoPubkey {
    fn from(value: &BlsPubKey) -> Self {
        Self {
            pubkey: value.serialize(),
        }
    }
}

impl TryFrom<ProtoPubkey> for PubKey {
    type Error = ProtoError;

    fn try_from(value: ProtoPubkey) -> Result<Self, Self::Error> {
        PubKey::from_slice(value.pubkey.as_bytes())
            .map_err(|e| ProtoError::CryptoError(format!("{}", e)))
    }
}

impl TryFrom<ProtoPubkey> for BlsPubKey {
    type Error = ProtoError;

    fn try_from(value: ProtoPubkey) -> Result<Self, Self::Error> {
        BlsPubKey::deserialize(value.pubkey.as_slice())
            .map_err(|e| ProtoError::CryptoError(format!("{}", e)))
    }
}

impl From<&Hash> for ProtoHash {
    fn from(value: &Hash) -> Self {
        Self {
            hash: value.to_vec(),
        }
    }
}

impl TryFrom<ProtoHash> for Hash {
    type Error = ProtoError;
    fn try_from(value: ProtoHash) -> Result<Self, Self::Error> {
        Ok(Self(value.hash.try_into().map_err(|e: Vec<_>| {
            Self::Error::WrongHashLen(format!("{}", e.len()))
        })?))
    }
}
