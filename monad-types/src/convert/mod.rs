use monad_proto::{
    error::ProtoError,
    proto::basic::{ProtoBlockId, ProtoEpoch, ProtoHash, ProtoNodeId, ProtoRound, ProtoStake},
};

use crate::{BlockId, Epoch, Hash, NodeId, Round, Stake};

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

impl From<&Epoch> for ProtoEpoch {
    fn from(value: &Epoch) -> Self {
        ProtoEpoch { epoch: value.0 }
    }
}

impl TryFrom<ProtoEpoch> for Epoch {
    type Error = ProtoError;
    fn try_from(value: ProtoEpoch) -> Result<Self, Self::Error> {
        Ok(Self(value.epoch))
    }
}

impl From<&Stake> for ProtoStake {
    fn from(value: &Stake) -> Self {
        ProtoStake { stake: value.0 }
    }
}

impl TryFrom<ProtoStake> for Stake {
    type Error = ProtoError;
    fn try_from(value: ProtoStake) -> Result<Self, Self::Error> {
        Ok(Self(value.stake))
    }
}
