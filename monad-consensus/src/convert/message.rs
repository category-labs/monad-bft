use std::ops::Deref;

use monad_consensus_types::{multi_sig::MultiSig, validation::Hashable};
use monad_crypto::{
    convert::{proto_to_signature, signature_to_proto},
    GenericSignature, Signature,
};
use monad_proto::{error::ProtoError, proto::message::*};

use crate::{
    messages::{
        consensus_message::ConsensusMessage,
        message::{
            ProposalMessage as ConsensusTypePropMsg, TimeoutMessage as ConsensusTypeTmoMsg,
            VoteMessage as ConsensusTypeVoteMessage,
        },
    },
    validation::signing::{Unverified, Verified},
};

type VoteMessage<S> = ConsensusTypeVoteMessage<MultiSig<S>>;
type TimeoutMessage<S> = ConsensusTypeTmoMsg<S, MultiSig<S>>;
type ProposalMessage<S> = ConsensusTypePropMsg<S, MultiSig<S>>;
pub(crate) type VerifiedConsensusMessage<S> = Verified<S, ConsensusMessage<S, MultiSig<S>>>;
pub(crate) type UnverifiedConsensusMessage<S> = Unverified<S, ConsensusMessage<S, MultiSig<S>>>;

impl<S: Signature + GenericSignature + Hashable> From<&VoteMessage<S>> for ProtoVoteMessage {
    fn from(value: &VoteMessage<S>) -> Self {
        ProtoVoteMessage {
            vote: Some((&value.vote).into()),
            sig: Some(signature_to_proto(&value.sig)),
        }
    }
}

impl<S: Signature + GenericSignature + Hashable> TryFrom<ProtoVoteMessage> for VoteMessage<S> {
    type Error = ProtoError;

    fn try_from(value: ProtoVoteMessage) -> Result<Self, Self::Error> {
        Ok(Self {
            vote: value
                .vote
                .ok_or(Self::Error::MissingRequiredField(
                    "VoteMessageSigned.vote".to_owned(),
                ))?
                .try_into()?,
            sig: proto_to_signature(value.sig.ok_or(Self::Error::MissingRequiredField(
                "VoteMessageSigned.vote".to_owned(),
            ))?)?,
        })
    }
}

impl<S: Signature + GenericSignature + Hashable> From<&TimeoutMessage<S>> for ProtoTimeoutMessage {
    fn from(value: &TimeoutMessage<S>) -> Self {
        ProtoTimeoutMessage {
            tminfo: Some((&value.tminfo).into()),
            last_round_tc: (value.last_round_tc.as_ref().map(|v| v.into())),
        }
    }
}

impl<S: Signature + GenericSignature + Hashable> TryFrom<ProtoTimeoutMessage>
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

impl<S: Signature + GenericSignature + Hashable> From<&ProposalMessage<S>>
    for ProtoProposalMessageAggSig
{
    fn from(value: &ProposalMessage<S>) -> Self {
        Self {
            block: Some((&value.block).into()),
            last_round_tc: value.last_round_tc.as_ref().map(|v| v.into()),
        }
    }
}

impl<S: Signature + GenericSignature + Hashable> TryFrom<ProtoProposalMessageAggSig>
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

impl<S: Signature + GenericSignature + Hashable> From<&VerifiedConsensusMessage<S>>
    for ProtoUnverifiedConsensusMessage
{
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

impl<S: Signature + GenericSignature + Hashable> TryFrom<ProtoUnverifiedConsensusMessage>
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
