use monad_proto::error::ProtoError;
use monad_proto::proto::basic::{ProtoBlockId, ProtoHash, ProtoNodeId, ProtoRound};

use crate::{BlockId, Hash, NodeId, Round};

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
        Ok(Self(value.hash.try_into().map_err(|e: Vec<_>| {
            Self::Error::WrongHashLen(format!("{}", e.len()))
        })?))
    }
}

impl From<&BlockId> for ProtoBlockId {
    fn from(value: &BlockId) -> Self {
        Self {
            bid: Some((&(value.0)).into()),
        }
    }
}

impl TryFrom<ProtoBlockId> for BlockId {
    type Error = ProtoError;

    fn try_from(value: ProtoBlockId) -> Result<Self, Self::Error> {
        Ok(Self(
            value
                .bid
                .ok_or(Self::Error::MissingRequiredField(
                    "ProtoBlockId.bid".to_owned(),
                ))?
                .try_into()?,
        ))
    }
}
