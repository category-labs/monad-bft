use std::ops::Deref;

use monad_consensus_types::{
    certificate_signature::CertificateSignatureRecoverable,
    convert::signing::{
        certificate_signature_to_proto, message_signature_to_proto, proto_to_certificate_signature,
        proto_to_message_signature,
    },
    message_signature::MessageSignature,
    multi_sig::MultiSig,
};
use monad_proto::{error::ProtoError, proto::message::*};

use crate::{
    messages::{
        consensus_message::ConsensusMessage,
        message::{
            BlockSyncMessage, ProposalMessage as ConsensusTypePropMsg, RequestBlockSyncMessage,
            TimeoutMessage as ConsensusTypeTmoMsg, VoteMessage as ConsensusTypeVoteMsg,
        },
    },
    validation::signing::{Unverified, Verified},
};

type TimeoutMessage<MS, CS> = ConsensusTypeTmoMsg<MS, MultiSig<CS>>;
type ProposalMessage<MS, CS> = ConsensusTypePropMsg<MS, MultiSig<CS>>;
type VoteMessage<CS> = ConsensusTypeVoteMsg<MultiSig<CS>>;

pub(crate) type VerifiedConsensusMessage<MS, CS> = Verified<MS, ConsensusMessage<MS, MultiSig<CS>>>;
pub(crate) type UnverifiedConsensusMessage<MS, CS> =
    Unverified<MS, ConsensusMessage<MS, MultiSig<CS>>>;

impl<CS: CertificateSignatureRecoverable> From<&VoteMessage<CS>> for ProtoVoteMessage {
    fn from(value: &VoteMessage<CS>) -> Self {
        ProtoVoteMessage {
            vote: Some((&value.vote).into()),
            sig: Some(certificate_signature_to_proto(&value.sig)),
        }
    }
}

impl<CS: CertificateSignatureRecoverable> TryFrom<ProtoVoteMessage> for VoteMessage<CS> {
    type Error = ProtoError;

    fn try_from(value: ProtoVoteMessage) -> Result<Self, Self::Error> {
        Ok(Self {
            vote: value
                .vote
                .ok_or(ProtoError::MissingRequiredField("VoteMsg.vote".to_owned()))?
                .try_into()?,
            sig: proto_to_certificate_signature(
                value
                    .sig
                    .ok_or(ProtoError::MissingRequiredField("VoteMsg.sig".to_owned()))?,
            )?,
        })
    }
}

impl<MS: MessageSignature, CS: CertificateSignatureRecoverable> From<&TimeoutMessage<MS, CS>>
    for ProtoTimeoutMessage
{
    fn from(value: &TimeoutMessage<MS, CS>) -> Self {
        ProtoTimeoutMessage {
            tminfo: Some((&value.tminfo).into()),
            last_round_tc: (value.last_round_tc.as_ref().map(|v| v.into())),
        }
    }
}

impl<MS: MessageSignature, CS: CertificateSignatureRecoverable> TryFrom<ProtoTimeoutMessage>
    for TimeoutMessage<MS, CS>
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

impl<MS: MessageSignature, CS: CertificateSignatureRecoverable> From<&ProposalMessage<MS, CS>>
    for ProtoProposalMessageAggSig
{
    fn from(value: &ProposalMessage<MS, CS>) -> Self {
        Self {
            block: Some((&value.block).into()),
            last_round_tc: value.last_round_tc.as_ref().map(|v| v.into()),
        }
    }
}

impl<MS: MessageSignature, CS: CertificateSignatureRecoverable> TryFrom<ProtoProposalMessageAggSig>
    for ProposalMessage<MS, CS>
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

impl From<&RequestBlockSyncMessage> for ProtoRequestBlockSyncMessage {
    fn from(value: &RequestBlockSyncMessage) -> Self {
        ProtoRequestBlockSyncMessage {
            block_id: Some((&value.block_id).into()),
        }
    }
}

impl TryFrom<ProtoRequestBlockSyncMessage> for RequestBlockSyncMessage {
    type Error = ProtoError;

    fn try_from(value: ProtoRequestBlockSyncMessage) -> Result<Self, Self::Error> {
        Ok(Self {
            block_id: value
                .block_id
                .ok_or(Self::Error::MissingRequiredField(
                    "RequestBlockSyncMessage.block_id".to_owned(),
                ))?
                .try_into()?,
        })
    }
}

impl<CS: CertificateSignatureRecoverable> From<&BlockSyncMessage<MultiSig<CS>>>
    for ProtoBlockSyncMessage
{
    fn from(value: &BlockSyncMessage<MultiSig<CS>>) -> Self {
        ProtoBlockSyncMessage {
            block: Some((&value.block).into()),
        }
    }
}

impl<CS: CertificateSignatureRecoverable> TryFrom<ProtoBlockSyncMessage>
    for BlockSyncMessage<MultiSig<CS>>
{
    type Error = ProtoError;

    fn try_from(value: ProtoBlockSyncMessage) -> Result<Self, Self::Error> {
        Ok(Self {
            block: value
                .block
                .ok_or(Self::Error::MissingRequiredField(
                    "BlockSyncMessage.block_id".to_owned(),
                ))?
                .try_into()?,
        })
    }
}
impl<MS: MessageSignature, CS: CertificateSignatureRecoverable>
    From<&VerifiedConsensusMessage<MS, CS>> for ProtoUnverifiedConsensusMessage
{
    fn from(value: &VerifiedConsensusMessage<MS, CS>) -> Self {
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
            ConsensusMessage::RequestBlockSync(msg) => {
                proto_unverified_consensus_message::OneofMessage::RequestBlockSync(msg.into())
            }
            ConsensusMessage::BlockSync(msg) => {
                proto_unverified_consensus_message::OneofMessage::BlockSync(msg.into())
            }
        };
        Self {
            oneof_message: Some(oneof_message),
            author_signature: Some(message_signature_to_proto(value.author_signature())),
        }
    }
}

impl<MS: MessageSignature, CS: CertificateSignatureRecoverable>
    TryFrom<ProtoUnverifiedConsensusMessage> for UnverifiedConsensusMessage<MS, CS>
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
            Some(proto_unverified_consensus_message::OneofMessage::RequestBlockSync(msg)) => {
                ConsensusMessage::RequestBlockSync(msg.try_into()?)
            }
            Some(proto_unverified_consensus_message::OneofMessage::BlockSync(msg)) => {
                ConsensusMessage::BlockSync(msg.try_into()?)
            }
            None => Err(ProtoError::MissingRequiredField(
                "Unverified<ConsensusMessage>.oneofmessage".to_owned(),
            ))?,
        };
        let signature = proto_to_message_signature(value.author_signature.ok_or(
            Self::Error::MissingRequiredField("Unverified<ConsensusMessage>.signature".to_owned()),
        )?)?;
        Ok(Unverified::new(message, signature))
    }
}
