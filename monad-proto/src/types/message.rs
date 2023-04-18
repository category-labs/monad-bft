use prost::Message;

use monad_consensus::types::consensus_message::{
    SignedConsensusMessage, VerifiedConsensusMessage as ConsensusTypeVerifiedMessage,
};
use monad_consensus::types::message::{
    ProposalMessage as ConsensusTypePropMsg, TimeoutMessage as ConsensusTypeTmoMsg, VoteMessage,
};
use monad_consensus::validation::signing::{Unverified, Verified};
use monad_crypto::secp256k1::SecpSignature;

use crate::error::ProtoError;

use super::signing::AggSecpSignature;

type VerifiedVoteMessage = Verified<SecpSignature, VoteMessage>;
type UnverifiedVoteMessage = Unverified<SecpSignature, VoteMessage>;
type TimeoutMessage = ConsensusTypeTmoMsg<SecpSignature, AggSecpSignature>;
type VerifiedTimeoutMessage = Verified<SecpSignature, TimeoutMessage>;
type UnverifiedTimeoutMessage = Unverified<SecpSignature, TimeoutMessage>;
type ProposalMessage = ConsensusTypePropMsg<SecpSignature, AggSecpSignature>;
type VerifiedProposalMessage = Verified<SecpSignature, ProposalMessage>;
type UnverifiedProposalMessage = Unverified<SecpSignature, ProposalMessage>;
type VerifiedConsensusMessage = ConsensusTypeVerifiedMessage<SecpSignature, AggSecpSignature>;
type UnverifiedConsensusMessage = SignedConsensusMessage<SecpSignature, AggSecpSignature>;

include!(concat!(env!("OUT_DIR"), "/monad_proto.message.rs"));

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

impl From<&VerifiedVoteMessage> for ProtoUnverifiedVoteMessage {
    fn from(value: &VerifiedVoteMessage) -> Self {
        Self {
            vote_msg: Some((&(**value)).into()),
            author_signature: Some(value.author_signature().into()),
        }
    }
}

impl TryFrom<ProtoUnverifiedVoteMessage> for UnverifiedVoteMessage {
    type Error = ProtoError;
    fn try_from(value: ProtoUnverifiedVoteMessage) -> Result<Self, Self::Error> {
        let msg: VoteMessage = value
            .vote_msg
            .ok_or(Self::Error::MissingRequiredField(
                "Unverified<VoteMessage>.obj".to_owned(),
            ))?
            .try_into()?;
        let signature: SecpSignature = value
            .author_signature
            .ok_or(Self::Error::MissingRequiredField(
                "Unverified<VoteMessage>.signature".to_owned(),
            ))?
            .try_into()?;
        Ok(Unverified::new(msg, signature))
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

impl From<&VerifiedTimeoutMessage> for ProtoUnverifiedTimeoutMessage {
    fn from(value: &VerifiedTimeoutMessage) -> Self {
        Self {
            tmo_msg: Some((&(**value)).into()),
            author_signature: Some(value.author_signature().into()),
        }
    }
}

impl TryFrom<ProtoUnverifiedTimeoutMessage> for UnverifiedTimeoutMessage {
    type Error = ProtoError;
    fn try_from(value: ProtoUnverifiedTimeoutMessage) -> Result<Self, Self::Error> {
        Ok(Self::new(
            value
                .tmo_msg
                .ok_or(Self::Error::MissingRequiredField(
                    "Unverified<TimeoutMessage<AggregateSignatures>>.obj".to_owned(),
                ))?
                .try_into()?,
            value
                .author_signature
                .ok_or(Self::Error::MissingRequiredField(
                    "Unverified<TimeoutMessage<AggregateSignatures>>.author_signature".to_owned(),
                ))?
                .try_into()?,
        ))
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

impl From<&VerifiedProposalMessage> for ProtoUnverifiedProposalMessageAggSig {
    fn from(value: &VerifiedProposalMessage) -> Self {
        Self {
            proposal: Some((&(**value)).into()),
            author_signature: Some(value.author_signature().into()),
        }
    }
}

impl TryFrom<ProtoUnverifiedProposalMessageAggSig> for UnverifiedProposalMessage {
    type Error = ProtoError;
    fn try_from(value: ProtoUnverifiedProposalMessageAggSig) -> Result<Self, Self::Error> {
        Ok(Self::new(
            value
                .proposal
                .ok_or(Self::Error::MissingRequiredField(
                    "Unverified<ProposalMessage<AggregateSignatures>>.proposal".to_owned(),
                ))?
                .try_into()?,
            value
                .author_signature
                .ok_or(Self::Error::MissingRequiredField(
                    "Unverified<ProposalMessage<AggregateSignatures>>.author_signature".to_owned(),
                ))?
                .try_into()?,
        ))
    }
}

impl From<&VerifiedConsensusMessage> for ProtoUnverifiedConsensusMessage {
    fn from(value: &VerifiedConsensusMessage) -> Self {
        match value {
            ConsensusTypeVerifiedMessage::Proposal(msg) => Self {
                oneof_message: Some(proto_unverified_consensus_message::OneofMessage::Proposal(
                    msg.into(),
                )),
            },
            ConsensusTypeVerifiedMessage::Timeout(msg) => Self {
                oneof_message: Some(proto_unverified_consensus_message::OneofMessage::Timeout(
                    msg.into(),
                )),
            },
            ConsensusTypeVerifiedMessage::Vote(msg) => Self {
                oneof_message: Some(proto_unverified_consensus_message::OneofMessage::Vote(
                    msg.into(),
                )),
            },
        }
    }
}

impl TryFrom<ProtoUnverifiedConsensusMessage> for UnverifiedConsensusMessage {
    type Error = ProtoError;

    fn try_from(value: ProtoUnverifiedConsensusMessage) -> Result<Self, Self::Error> {
        Ok(match value.oneof_message {
            Some(proto_unverified_consensus_message::OneofMessage::Proposal(msg)) => {
                Self::Proposal(msg.try_into()?)
            }
            Some(proto_unverified_consensus_message::OneofMessage::Timeout(msg)) => {
                Self::Timeout(msg.try_into()?)
            }
            Some(proto_unverified_consensus_message::OneofMessage::Vote(msg)) => {
                Self::Vote(msg.try_into()?)
            }
            None => Err(ProtoError::MissingRequiredField(
                "UnverifiedConsensusMessage".to_owned(),
            ))?,
        })
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
