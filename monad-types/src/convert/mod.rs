use monad_proto::{
    error::ProtoError,
    proto::basic::{ProtoBlockId, ProtoEpoch, ProtoNodeId, ProtoRound, ProtoSeqNum, ProtoStake},
};

use crate::{BlockId, Epoch, NodeId, Round, SeqNum, Stake};

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

impl From<&SeqNum> for ProtoSeqNum {
    fn from(value: &SeqNum) -> Self {
        ProtoSeqNum { seq_num: value.0 }
    }
}

impl TryFrom<ProtoSeqNum> for SeqNum {
    type Error = ProtoError;
    fn try_from(value: ProtoSeqNum) -> Result<Self, Self::Error> {
        Ok(Self(value.seq_num))
    }
}
