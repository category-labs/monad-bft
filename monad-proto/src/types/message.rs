use std::ops::Deref;

use monad_consensus::types::consensus_message::ConsensusMessage;
use prost::Message;

use monad_consensus::types::message::{
    ProposalMessage as ConsensusTypePropMsg, TimeoutMessage as ConsensusTypeTmoMsg, VoteMessage,
};
use monad_consensus::validation::signing::{Unverified, Verified};
use monad_crypto::secp256k1::SecpSignature;

use crate::error::ProtoError;

use super::signing::AggSecpSignature;

type TimeoutMessage = ConsensusTypeTmoMsg<SecpSignature, AggSecpSignature>;
type ProposalMessage = ConsensusTypePropMsg<SecpSignature, AggSecpSignature>;
type VerifiedConsensusMessage =
    Verified<SecpSignature, ConsensusMessage<SecpSignature, AggSecpSignature>>;
type UnverifiedConsensusMessage =
    Unverified<SecpSignature, ConsensusMessage<SecpSignature, AggSecpSignature>>;

pub(crate) use crate::proto::message::*;

impl From<&VoteMessage> for ProtoVoteMessage {
    fn from(value: &VoteMessage) -> Self {
        ProtoVoteMessage {
            vote_info: Some((&value.vote_info).into()),
            ledger_commit_info: Some((&value.ledger_commit_info).into()),
        }
    }
}

impl TryFrom<ProtoVoteMessage> for VoteMessage {
    type Error = ProtoError;

    fn try_from(value: ProtoVoteMessage) -> Result<Self, Self::Error> {
        Ok(Self {
            vote_info: value
                .vote_info
                .ok_or(Self::Error::MissingRequiredField(
                    "VoteMessage.vote_info".to_owned(),
                ))?
                .try_into()?,
            ledger_commit_info: value
                .ledger_commit_info
                .ok_or(Self::Error::MissingRequiredField(
                    "VoteMessage.ledger_commit_info".to_owned(),
                ))?
                .try_into()?,
        })
    }
}

impl From<&TimeoutMessage> for ProtoTimeoutMessage {
    fn from(value: &TimeoutMessage) -> Self {
        ProtoTimeoutMessage {
            tminfo: Some((&value.tminfo).into()),
            last_round_tc: (value.last_round_tc.as_ref().map(|v| v.into())),
        }
    }
}

impl TryFrom<ProtoTimeoutMessage> for TimeoutMessage {
    type Error = ProtoError;
    fn try_from(value: ProtoTimeoutMessage) -> Result<Self, Self::Error> {
        Ok(Self {
            tminfo: value
                .tminfo
                .ok_or(Self::Error::MissingRequiredField(
                    "TmoMsg<AggSig>.tminfo".to_owned(),
                ))?
                .try_into()?,

            last_round_tc: value.last_round_tc.map(|v| v.try_into()).transpose()?,
        })
    }
}

impl From<&ProposalMessage> for ProtoProposalMessageAggSig {
    fn from(value: &ProposalMessage) -> Self {
        Self {
            block: Some((&value.block).into()),
            last_round_tc: value.last_round_tc.as_ref().map(|v| v.into()),
        }
    }
}

impl TryFrom<ProtoProposalMessageAggSig> for ProposalMessage {
    type Error = ProtoError;

    fn try_from(value: ProtoProposalMessageAggSig) -> Result<Self, Self::Error> {
        Ok(Self {
            block: value
                .block
                .ok_or(Self::Error::MissingRequiredField(
                    "ProposalMessage<AggregateSignatures>.block".to_owned(),
                ))?
                .try_into()?,
            last_round_tc: value.last_round_tc.map(|v| v.try_into()).transpose()?,
        })
    }
}

impl From<&VerifiedConsensusMessage> for ProtoUnverifiedConsensusMessage {
    fn from(value: &VerifiedConsensusMessage) -> Self {
        let oneof_message = match value.deref() {
            ConsensusMessage::Proposal(msg) => {
                proto_unverified_consensus_message::OneofMessage::Proposal(msg.into())
            }
            ConsensusMessage::Vote(msg) => {
                proto_unverified_consensus_message::OneofMessage::Vote(msg.into())
            }
            ConsensusMessage::Timeout(msg) => {
                proto_unverified_consensus_message::OneofMessage::Timeout(msg.into())
            }
        };
        Self {
            oneof_message: Some(oneof_message),
            author_signature: Some(value.author_signature().into()),
        }
    }
}

impl TryFrom<ProtoUnverifiedConsensusMessage> for UnverifiedConsensusMessage {
    type Error = ProtoError;

    fn try_from(value: ProtoUnverifiedConsensusMessage) -> Result<Self, Self::Error> {
        let message = match value.oneof_message {
            Some(proto_unverified_consensus_message::OneofMessage::Proposal(msg)) => {
                ConsensusMessage::Proposal(msg.try_into()?)
            }
            Some(proto_unverified_consensus_message::OneofMessage::Timeout(msg)) => {
                ConsensusMessage::Timeout(msg.try_into()?)
            }
            Some(proto_unverified_consensus_message::OneofMessage::Vote(msg)) => {
                ConsensusMessage::Vote(msg.try_into()?)
            }
            None => Err(ProtoError::MissingRequiredField(
                "Unverified<ConsensusMessage>.oneofmessage".to_owned(),
            ))?,
        };
        let signature: SecpSignature = value
            .author_signature
            .ok_or(Self::Error::MissingRequiredField(
                "Unverified<ConsensusMessage>.signature".to_owned(),
            ))?
            .try_into()?;
        Ok(Unverified::new(message, signature))
    }
}

pub fn serialize_verified_consensus_message(msg: &VerifiedConsensusMessage) -> Vec<u8> {
    let proto_msg: ProtoUnverifiedConsensusMessage = msg.into();
    proto_msg.encode_to_vec()
}

pub fn deserialize_unverified_consensus_message(
    data: &[u8],
) -> Result<UnverifiedConsensusMessage, ProtoError> {
    let msg = ProtoUnverifiedConsensusMessage::decode(data)?;
    msg.try_into()
}
