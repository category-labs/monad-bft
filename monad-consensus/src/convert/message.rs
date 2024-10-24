use std::ops::Deref;

use monad_consensus_types::{
    convert::signing::{certificate_signature_to_proto, proto_to_certificate_signature},
    signature_collection::SignatureCollection,
};
use monad_crypto::certificate_signature::CertificateSignatureRecoverable;
use monad_proto::{error::ProtoError, proto::message::*};

use crate::{
    messages::{
        consensus_message::{ConsensusMessage, ProtocolMessage},
        message::{PeerStateRootMessage, ProposalMessage, TimeoutMessage, VoteMessage},
    },
    validation::signing::{Unvalidated, Unverified, Validated, Verified},
};

pub(crate) type VerifiedConsensusMessage<MS, SCT> = Verified<MS, Validated<ConsensusMessage<SCT>>>;
pub(crate) type UnverifiedConsensusMessage<MS, SCT> =
    Unverified<MS, Unvalidated<ConsensusMessage<SCT>>>;

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
            payload: Some((&value.payload).into()),
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
            payload: value
                .payload
                .ok_or(Self::Error::MissingRequiredField(
                    "ProposalMessage<AggregateSignatures>.payload".to_owned(),
                ))?
                .try_into()?,
            last_round_tc: value.last_round_tc.map(|v| v.try_into()).transpose()?,
        })
    }
}

impl<MS: CertificateSignatureRecoverable, SCT: SignatureCollection>
    From<&VerifiedConsensusMessage<MS, SCT>> for ProtoUnverifiedConsensusMessage
{
    fn from(value: &VerifiedConsensusMessage<MS, SCT>) -> Self {
        let oneof_message = match &value.deref().deref().message {
            ProtocolMessage::Proposal(msg) => {
                proto_unverified_consensus_message::OneofMessage::Proposal(msg.into())
            }
            ProtocolMessage::Vote(msg) => {
                proto_unverified_consensus_message::OneofMessage::Vote(msg.into())
            }
            ProtocolMessage::Timeout(msg) => {
                proto_unverified_consensus_message::OneofMessage::Timeout(msg.into())
            }
        };
        Self {
            version: value.version.clone(),
            oneof_message: Some(oneof_message),
            author_signature: Some(certificate_signature_to_proto(value.author_signature())),
        }
    }
}

impl<MS: CertificateSignatureRecoverable, SCT: SignatureCollection>
    TryFrom<ProtoUnverifiedConsensusMessage> for UnverifiedConsensusMessage<MS, SCT>
{
    type Error = ProtoError;

    fn try_from(value: ProtoUnverifiedConsensusMessage) -> Result<Self, Self::Error> {
        let message = match value.oneof_message {
            Some(proto_unverified_consensus_message::OneofMessage::Proposal(msg)) => {
                ProtocolMessage::Proposal(msg.try_into()?)
            }
            Some(proto_unverified_consensus_message::OneofMessage::Timeout(msg)) => {
                ProtocolMessage::Timeout(msg.try_into()?)
            }
            Some(proto_unverified_consensus_message::OneofMessage::Vote(msg)) => {
                ProtocolMessage::Vote(msg.try_into()?)
            }
            None => Err(ProtoError::MissingRequiredField(
                "Unverified<ConsensusMessage>.oneofmessage".to_owned(),
            ))?,
        };
        let signature = proto_to_certificate_signature(value.author_signature.ok_or(
            Self::Error::MissingRequiredField("Unverified<ConsensusMessage>.signature".to_owned()),
        )?)?;
        let version = value.version;
        let consensus_msg = ConsensusMessage { version, message };
        Ok(Unverified::new(Unvalidated::new(consensus_msg), signature))
    }
}

// TODO-2: PeerStateRootMessage doesn't belong to monad-consensus. Create a new
// crate for it?
impl<SCT: SignatureCollection> From<&Validated<PeerStateRootMessage<SCT>>>
    for ProtoPeerStateRootMessage
{
    fn from(value: &Validated<PeerStateRootMessage<SCT>>) -> Self {
        let msg = value.deref();
        Self {
            peer: Some((&msg.peer).into()),
            info: Some((&msg.info).into()),
            sig: Some(certificate_signature_to_proto(&msg.sig)),
        }
    }
}

impl<SCT: SignatureCollection> TryFrom<ProtoPeerStateRootMessage>
    for Unvalidated<PeerStateRootMessage<SCT>>
{
    type Error = ProtoError;

    fn try_from(value: ProtoPeerStateRootMessage) -> Result<Self, Self::Error> {
        let msg = PeerStateRootMessage {
            peer: value
                .peer
                .ok_or(ProtoError::MissingRequiredField(
                    "PeerStateRootMessage.peer".to_owned(),
                ))?
                .try_into()?,
            info: value
                .info
                .ok_or(ProtoError::MissingRequiredField(
                    "PeerStateRootMessage.info".to_owned(),
                ))?
                .try_into()?,
            sig: proto_to_certificate_signature(value.sig.ok_or(
                ProtoError::MissingRequiredField("PeerStateRootMessage.sig".to_owned()),
            )?)?,
        };

        Ok(Unvalidated::new(msg))
    }
}
