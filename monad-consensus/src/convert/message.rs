use std::ops::Deref;

use monad_consensus_types::{
    convert::signing::{
        certificate_signature_to_proto, message_signature_to_proto, proto_to_certificate_signature,
        proto_to_message_signature,
    },
    message_signature::MessageSignature,
    signature_collection::SignatureCollection,
};
use monad_proto::{error::ProtoError, proto::message::*};

use crate::{
    messages::{
        consensus_message::ConsensusMessage,
        message::{
            BlockSyncMessage, ProposalMessage, RequestBlockSyncMessage, TimeoutMessage, VoteMessage,
        },
    },
    validation::signing::{Unverified, Verified},
};

pub(crate) type VerifiedConsensusMessage<MS, SCT> = Verified<MS, ConsensusMessage<SCT>>;
pub(crate) type UnverifiedConsensusMessage<MS, SCT> = Unverified<MS, ConsensusMessage<SCT>>;

impl<SCT: SignatureCollection> From<&VoteMessage<SCT>> for ProtoVoteMessage {
    fn from(value: &VoteMessage<SCT>) -> Self {
        ProtoVoteMessage {
            vote: Some((&value.vote).into()),
            sig: Some(certificate_signature_to_proto(&value.sig)),
        }
    }
}

impl<SCT: SignatureCollection> TryFrom<ProtoVoteMessage> for VoteMessage<SCT> {
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

impl<SCT: SignatureCollection> From<&TimeoutMessage<SCT>> for ProtoTimeoutMessage {
    fn from(value: &TimeoutMessage<SCT>) -> Self {
        ProtoTimeoutMessage {
            timeout: Some((&value.timeout).into()),
            sig: Some(certificate_signature_to_proto(&value.sig)),
        }
    }
}

impl<SCT: SignatureCollection> TryFrom<ProtoTimeoutMessage> for TimeoutMessage<SCT> {
    type Error = ProtoError;
    fn try_from(value: ProtoTimeoutMessage) -> Result<Self, Self::Error> {
        Ok(Self {
            timeout: value
                .timeout
                .ok_or(ProtoError::MissingRequiredField(
                    "TimeoutMessage.timeout".to_owned(),
                ))?
                .try_into()?,
            sig: proto_to_certificate_signature(value.sig.ok_or(
                ProtoError::MissingRequiredField("TimeoutMessage.sig".to_owned()),
            )?)?,
        })
    }
}

impl<SCT: SignatureCollection> From<&ProposalMessage<SCT>> for ProtoProposalMessage {
    fn from(value: &ProposalMessage<SCT>) -> Self {
        Self {
            block: Some((&value.block).into()),
            last_round_tc: value.last_round_tc.as_ref().map(|v| v.into()),
        }
    }
}

impl<SCT: SignatureCollection> TryFrom<ProtoProposalMessage> for ProposalMessage<SCT> {
    type Error = ProtoError;

    fn try_from(value: ProtoProposalMessage) -> Result<Self, Self::Error> {
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

impl<SCT: SignatureCollection> From<&BlockSyncMessage<SCT>> for ProtoBlockSyncMessage {
    fn from(value: &BlockSyncMessage<SCT>) -> Self {
        return Self {
            oneof_message: Some(match value.deref() {
                BlockSyncMessage::BlockFound(b) => {
                    proto_block_sync_message::OneofMessage::BlockFound(b.into())
                }
                BlockSyncMessage::NotAvailable(bid) => {
                    proto_block_sync_message::OneofMessage::NotAvailable(bid.into())
                }
            }),
        };
    }
}

impl<SCT: SignatureCollection> TryFrom<ProtoBlockSyncMessage> for BlockSyncMessage<SCT> {
    type Error = ProtoError;

    fn try_from(value: ProtoBlockSyncMessage) -> Result<Self, Self::Error> {
        Ok(match value.oneof_message {
            Some(proto_block_sync_message::OneofMessage::BlockFound(b)) => {
                BlockSyncMessage::BlockFound(b.try_into()?)
            }
            Some(proto_block_sync_message::OneofMessage::NotAvailable(bid)) => {
                BlockSyncMessage::NotAvailable(bid.try_into()?)
            }
            None => Err(ProtoError::MissingRequiredField(
                "BlockSyncMessage.oneofmessage".to_owned(),
            ))?,
        })
    }
}
impl<MS: MessageSignature, SCT: SignatureCollection> From<&VerifiedConsensusMessage<MS, SCT>>
    for ProtoUnverifiedConsensusMessage
{
    fn from(value: &VerifiedConsensusMessage<MS, SCT>) -> Self {
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

impl<MS: MessageSignature, SCT: SignatureCollection> TryFrom<ProtoUnverifiedConsensusMessage>
    for UnverifiedConsensusMessage<MS, SCT>
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
