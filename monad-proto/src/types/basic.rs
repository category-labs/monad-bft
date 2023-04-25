use zerocopy::AsBytes;

use monad_crypto::secp256k1::PubKey;
use monad_types::{Hash, NodeId, Round};

use crate::error::ProtoError;

pub(crate) use crate::proto::basic::*;

impl From<&PubKey> for ProtoPubkey {
    fn from(value: &PubKey) -> Self {
        Self {
            pubkey: value.bytes(),
        }
    }
}

impl TryFrom<ProtoPubkey> for PubKey {
    type Error = ProtoError;

    fn try_from(value: ProtoPubkey) -> Result<Self, Self::Error> {
        Ok(PubKey::from_slice(value.pubkey.as_bytes())?)
    }
}

impl From<&NodeId> for ProtoNodeId {
    fn from(nodeid: &NodeId) -> Self {
        Self {
            pubkey: Some((&(nodeid.0)).into()),
        }
    }
}

impl TryFrom<ProtoNodeId> for NodeId {
    type Error = ProtoError;
    fn try_from(value: ProtoNodeId) -> Result<Self, Self::Error> {
        Ok(Self(
            value
                .pubkey
                .ok_or(ProtoError::MissingRequiredField("NodeId.0".to_owned()))?
                .try_into()?,
        ))
    }
}

impl From<&Round> for ProtoRound {
    fn from(value: &Round) -> Self {
        ProtoRound { round: value.0 }
    }
}

impl TryFrom<ProtoRound> for Round {
    type Error = ProtoError;
    fn try_from(value: ProtoRound) -> Result<Self, Self::Error> {
        Ok(Self(value.round))
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
        value
            .hash
            .try_into()
            .map_err(|e: Vec<_>| Self::Error::WrongHashLen(format!("{}", e.len())))
    }
}
