use std::ops::Deref;

use monad_consensus_types::{
    convert::signing::{certificate_signature_to_proto, proto_to_certificate_signature},
    signature_collection::SignatureCollection,
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_proto::{
    error::ProtoError,
    proto::{
        event::{proto_unverified_consensus_message_type, ProtoUnverifiedConsensusMessageType},
        message::*,
    },
};
use monad_types::ExecutionProtocol;

use crate::{
    messages::{
        consensus_message::{
            CompressedConsensusMessage, CompressedProtocolMessage, ConsensusMessage,
            ProtocolMessage, UnverifiedCompressedConsensusMessage, UnverifiedConsensusMessage,
            UnverifiedConsensusMessageType, VerifiedCompressedConsensusMessage,
            VerifiedConsensusMessage,
        },
        message::{CompressedProposalMessage, ProposalMessage, TimeoutMessage, VoteMessage},
    },
    validation::signing::{Unvalidated, Unverified},
};

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

impl<ST, SCT, EPT> From<&ProposalMessage<ST, SCT, EPT>> for ProtoProposalMessage
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn from(value: &ProposalMessage<ST, SCT, EPT>) -> Self {
        Self {
            block_header: Some((&value.block_header).into()),
            block_body: Some((&value.block_body).into()),
            last_round_tc: value.last_round_tc.as_ref().map(|v| v.into()),
        }
    }
}

impl<ST, SCT, EPT> From<&CompressedProposalMessage<ST, SCT, EPT>> for ProtoCompressedProposalMessage
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn from(value: &CompressedProposalMessage<ST, SCT, EPT>) -> Self {
        Self {
            block_header: Some((&value.block_header).into()),
            compressed_block_body: Some((&value.compressed_block_body).into()),
            last_round_tc: value.last_round_tc.as_ref().map(|v| v.into()),
        }
    }
}

impl<ST, SCT, EPT> TryFrom<ProtoProposalMessage> for ProposalMessage<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    type Error = ProtoError;

    fn try_from(value: ProtoProposalMessage) -> Result<Self, Self::Error> {
        Ok(Self {
            block_header: value
                .block_header
                .ok_or(Self::Error::MissingRequiredField(
                    "ProposalMessage<AggregateSignatures>.block_header".to_owned(),
                ))?
                .try_into()?,
            block_body: value
                .block_body
                .ok_or(Self::Error::MissingRequiredField(
                    "ProposalMessage<AggregateSignatures>.block_body".to_owned(),
                ))?
                .try_into()?,
            last_round_tc: value.last_round_tc.map(|v| v.try_into()).transpose()?,
        })
    }
}

impl<ST, SCT, EPT> TryFrom<ProtoCompressedProposalMessage>
    for CompressedProposalMessage<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    type Error = ProtoError;

    fn try_from(value: ProtoCompressedProposalMessage) -> Result<Self, Self::Error> {
        Ok(Self {
            block_header: value
                .block_header
                .ok_or(Self::Error::MissingRequiredField(
                    "CompressedProposalMessage<AggregateSignatures>.block_header".to_owned(),
                ))?
                .try_into()?,
            compressed_block_body: value
                .compressed_block_body
                .ok_or(Self::Error::MissingRequiredField(
                    "CompressedProposalMessage<AggregateSignatures>.compressed_block_body"
                        .to_owned(),
                ))?
                .try_into()?,
            last_round_tc: value.last_round_tc.map(|v| v.try_into()).transpose()?,
        })
    }
}

impl<ST, SCT, EPT> From<&VerifiedConsensusMessage<ST, SCT, EPT>> for ProtoUnverifiedConsensusMessage
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn from(value: &VerifiedConsensusMessage<ST, SCT, EPT>) -> Self {
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
            version: value.version,
            oneof_message: Some(oneof_message),
            author_signature: Some(certificate_signature_to_proto(value.author_signature())),
        }
    }
}

impl<ST, SCT, EPT> From<&VerifiedCompressedConsensusMessage<ST, SCT, EPT>>
    for ProtoUnverifiedCompressedConsensusMessage
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn from(value: &VerifiedCompressedConsensusMessage<ST, SCT, EPT>) -> Self {
        let oneof_message = match &value.deref().deref().message {
            CompressedProtocolMessage::CompressedProposal(msg) => {
                proto_unverified_compressed_consensus_message::OneofMessage::CompressedProposal(
                    msg.into(),
                )
            }
            CompressedProtocolMessage::Vote(msg) => {
                proto_unverified_compressed_consensus_message::OneofMessage::Vote(msg.into())
            }
            CompressedProtocolMessage::Timeout(msg) => {
                proto_unverified_compressed_consensus_message::OneofMessage::Timeout(msg.into())
            }
        };
        Self {
            version: value.version,
            oneof_message: Some(oneof_message),
            author_signature: Some(certificate_signature_to_proto(value.author_signature())),
        }
    }
}

impl<ST, SCT, EPT> TryFrom<ProtoUnverifiedConsensusMessage>
    for UnverifiedConsensusMessage<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
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

impl<ST, SCT, EPT> TryFrom<ProtoUnverifiedCompressedConsensusMessage>
    for UnverifiedCompressedConsensusMessage<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    type Error = ProtoError;

    fn try_from(value: ProtoUnverifiedCompressedConsensusMessage) -> Result<Self, Self::Error> {
        let message = match value.oneof_message {
            Some(
                proto_unverified_compressed_consensus_message::OneofMessage::CompressedProposal(
                    msg,
                ),
            ) => CompressedProtocolMessage::CompressedProposal(msg.try_into()?),
            Some(proto_unverified_compressed_consensus_message::OneofMessage::Timeout(msg)) => {
                CompressedProtocolMessage::Timeout(msg.try_into()?)
            }
            Some(proto_unverified_compressed_consensus_message::OneofMessage::Vote(msg)) => {
                CompressedProtocolMessage::Vote(msg.try_into()?)
            }
            None => Err(ProtoError::MissingRequiredField(
                "Unverified<CompressedConsensusMessage>.oneofmessage".to_owned(),
            ))?,
        };
        let signature = proto_to_certificate_signature(value.author_signature.ok_or(
            Self::Error::MissingRequiredField(
                "Unverified<CompressedConsensusMessage>.signature".to_owned(),
            ),
        )?)?;
        let version = value.version;
        let compressed_consensus_msg = CompressedConsensusMessage { version, message };
        Ok(Unverified::new(
            Unvalidated::new(compressed_consensus_msg),
            signature,
        ))
    }
}

impl<ST, SCT, EPT> TryFrom<ProtoUnverifiedConsensusMessageType>
    for UnverifiedConsensusMessageType<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    type Error = ProtoError;

    fn try_from(value: ProtoUnverifiedConsensusMessageType) -> Result<Self, Self::Error> {
        match value.oneof_message {
            Some(proto_unverified_consensus_message_type::OneofMessage::UncompressedMessage(
                msg,
            )) => Ok(Self::Uncompressed(msg.try_into()?)),
            Some(proto_unverified_consensus_message_type::OneofMessage::CompressedMessage(msg)) => {
                Ok(Self::Compressed(msg.try_into()?))
            }
            None => Err(ProtoError::MissingRequiredField(
                "UnverifiedConsensusMessageType.oneofmessage".to_owned(),
            ))?,
        }
    }
}
