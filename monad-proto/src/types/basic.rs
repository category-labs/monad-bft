use zerocopy::AsBytes;

use monad_crypto::secp256k1::PubKey;
use monad_types::{Hash, NodeId, Round};

use crate::error::ProtoError;

include!(concat!(env!("OUT_DIR"), "/monad_proto.basic.rs"));

impl From<&NodeId> for ProtoNodeId {
    fn from(nodeid: &NodeId) -> Self {
        ProtoNodeId {
            id: nodeid.0.bytes(),
        }
    }
}

impl TryFrom<ProtoNodeId> for NodeId {
    type Error = ProtoError;
    fn try_from(value: ProtoNodeId) -> Result<Self, Self::Error> {
        Ok(Self(PubKey::from_slice(value.id.as_bytes())?))
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
