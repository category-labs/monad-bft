use std::ops::Deref;

use monad_crypto::convert::{proto_to_signature, signature_to_proto};
use monad_crypto::secp256k1::{SecpError, SecpPubKey};
use monad_crypto::Signature;
use monad_proto::error::ProtoError;
use monad_proto::proto::message::*;

use crate::signatures::aggregate_signature::AggregateSignatures;
use crate::types::consensus_message::ConsensusMessage;
use crate::types::message::{
    ProposalMessage as ConsensusTypePropMsg, TimeoutMessage as ConsensusTypeTmoMsg, VoteMessage,
};
use crate::validation::signing::{Unverified, Verified};

type TimeoutMessage<S> = ConsensusTypeTmoMsg<S, AggregateSignatures<S>>;
type ProposalMessage<S> = ConsensusTypePropMsg<S, AggregateSignatures<S>>;
pub(crate) type VerifiedConsensusMessage<S> =
    Verified<S, ConsensusMessage<S, AggregateSignatures<S>>>;
pub(crate) type UnverifiedConsensusMessage<S> =
    Unverified<S, ConsensusMessage<S, AggregateSignatures<S>>>;

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

impl<S: Signature> From<&TimeoutMessage<S>> for ProtoTimeoutMessage {
    fn from(value: &TimeoutMessage<S>) -> Self {
        ProtoTimeoutMessage {
            tminfo: Some((&value.tminfo).into()),
            last_round_tc: (value.last_round_tc.as_ref().map(|v| v.into())),
        }
    }
}

impl<S: Signature<PubKey = SecpPubKey, Error = SecpError>> TryFrom<ProtoTimeoutMessage>
    for TimeoutMessage<S>
{
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

impl<S: Signature> From<&ProposalMessage<S>> for ProtoProposalMessageAggSig {
    fn from(value: &ProposalMessage<S>) -> Self {
        Self {
            block: Some((&value.block).into()),
            last_round_tc: value.last_round_tc.as_ref().map(|v| v.into()),
        }
    }
}

impl<S: Signature<PubKey = SecpPubKey, Error = SecpError>> TryFrom<ProtoProposalMessageAggSig>
    for ProposalMessage<S>
{
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

impl<S: Signature> From<&VerifiedConsensusMessage<S>> for ProtoUnverifiedConsensusMessage {
    fn from(value: &VerifiedConsensusMessage<S>) -> Self {
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
            author_signature: Some(signature_to_proto(value.author_signature())),
        }
    }
}

impl<S: Signature<PubKey = SecpPubKey, Error = SecpError>> TryFrom<ProtoUnverifiedConsensusMessage>
    for UnverifiedConsensusMessage<S>
{
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
        let signature = proto_to_signature(value.author_signature.ok_or(
            Self::Error::MissingRequiredField("Unverified<ConsensusMessage>.signature".to_owned()),
        )?)?;
        Ok(Unverified::new(message, signature))
    }
}
