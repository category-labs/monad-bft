use monad_crypto::certificate_signature::PubKey;
use monad_proto::{
    error::ProtoError,
    proto::basic::{
        ProtoBlockId, ProtoEpoch, ProtoMonadVersion, ProtoNodeId, ProtoPubkey, ProtoRound,
        ProtoSeqNum, ProtoStake,
    },
};

use crate::{BlockId, Epoch, MonadVersion, NodeId, Round, SeqNum, Stake};

impl<P: PubKey> From<&NodeId<P>> for ProtoNodeId {
    fn from(nodeid: &NodeId<P>) -> Self {
        Self {
            pubkey: Some(pubkey_to_proto(&nodeid.0)),
        }
    }
}

impl<P: PubKey> TryFrom<ProtoNodeId> for NodeId<P> {
    type Error = ProtoError;
    fn try_from(value: ProtoNodeId) -> Result<Self, Self::Error> {
        Ok(Self::new(proto_to_pubkey(value.pubkey.ok_or(
            ProtoError::MissingRequiredField("NodeId.0".to_owned()),
        )?)?))
    }
}

pub fn pubkey_to_proto(pubkey: &impl PubKey) -> ProtoPubkey {
    ProtoPubkey {
        pubkey: pubkey.bytes().into(),
    }
}

pub fn proto_to_pubkey<P: PubKey>(pubkey: ProtoPubkey) -> Result<P, ProtoError> {
    P::from_bytes(&pubkey.pubkey).map_err(|e| ProtoError::CryptoError(format!("{}", e)))
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

impl From<&MonadVersion> for ProtoMonadVersion {
    fn from(value: &MonadVersion) -> Self {
        ProtoMonadVersion {
            protocol_version: value.protocol_version,
            client_version_maj: value.client_version_maj.into(),
            client_version_min: value.client_version_min.into(),
            hash_version: value.hash_version.into(),
            serialize_version: value.serialize_version.into(),
        }
    }
}

impl TryFrom<ProtoMonadVersion> for MonadVersion {
    type Error = ProtoError;
    fn try_from(value: ProtoMonadVersion) -> Result<Self, Self::Error> {
        let client_version_maj = value.client_version_maj.try_into().map_err(|_| {
            Self::Error::DeserializeError("client version exceeds max value".to_string())
        })?;
        let client_version_min = value.client_version_min.try_into().map_err(|_| {
            Self::Error::DeserializeError("client version exceeds max value".to_string())
        })?;

        let serialize_version = value.serialize_version.try_into().map_err(|_| {
            Self::Error::DeserializeError("serialize version exceeds max value".to_string())
        })?;
        let hash_version = value.hash_version.try_into().map_err(|_| {
            Self::Error::DeserializeError("hash version exceeds max value".to_string())
        })?;

        Ok(Self {
            protocol_version: value.protocol_version,
            client_version_maj,
            client_version_min,
            serialize_version,
            hash_version,
        })
    }
}
