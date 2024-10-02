use std::ops::Deref;

use monad_consensus_types::{
    convert::signing::{certificate_signature_to_proto, proto_to_certificate_signature},
    signature_collection::SignatureCollection,
};
use monad_crypto::certificate_signature::CertificateSignatureRecoverable;
use monad_proto::{
    error::ProtoError,
    proto::{blocksync::*, message::*},
};

use crate::{
    messages::{
        consensus_message::{ConsensusMessage, ProtocolMessage},
        message::{
            BlockSyncHeadersResponse, BlockSyncPayloadResponse, BlockSyncRequestMessage,
            BlockSyncResponseMessage, PeerStateRootMessage, ProposalMessage, TimeoutMessage,
            VoteMessage,
        },
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

impl From<&BlockSyncRequestMessage> for ProtoBlockSyncRequestMessage {
    fn from(value: &BlockSyncRequestMessage) -> Self {
        let request_type = match value {
            BlockSyncRequestMessage::Headers(block_id_range) => {
                proto_block_sync_request_message::RequestType::BlockIdRange(block_id_range.into())
            }
            BlockSyncRequestMessage::Payload(payload_id) => {
                proto_block_sync_request_message::RequestType::PayloadId(payload_id.into())
            }
        };

        Self {
            request_type: Some(request_type),
        }
    }
}

impl TryFrom<ProtoBlockSyncRequestMessage> for BlockSyncRequestMessage {
    type Error = ProtoError;

    fn try_from(value: ProtoBlockSyncRequestMessage) -> Result<Self, Self::Error> {
        let request_message = match value.request_type {
            Some(proto_block_sync_request_message::RequestType::BlockIdRange(block_id_range)) => {
                BlockSyncRequestMessage::Headers(block_id_range.try_into()?)
            }
            Some(proto_block_sync_request_message::RequestType::PayloadId(payload_id)) => {
                BlockSyncRequestMessage::Payload(payload_id.try_into()?)
            }
            None => Err(ProtoError::MissingRequiredField(
                "BlockSyncRequestMessage.request_type".to_owned(),
            ))?,
        };

        Ok(request_message)
    }
}

impl<SCT: SignatureCollection> From<&BlockSyncHeadersResponse<SCT>>
    for ProtoBlockSyncHeadersResponse
{
    fn from(value: &BlockSyncHeadersResponse<SCT>) -> Self {
        Self {
            one_of_message: Some(match value {
                BlockSyncHeadersResponse::Found((block_id_range, blocksync_headers)) => {
                    proto_block_sync_headers_response::OneOfMessage::HeadersFound(
                        ProtoBlockSyncHeaders {
                            block_id_range: Some(block_id_range.into()),
                            headers: blocksync_headers
                                .iter()
                                .map(|b| b.into())
                                .collect::<Vec<_>>(),
                        },
                    )
                }
                BlockSyncHeadersResponse::NotAvailable(block_id_range) => {
                    proto_block_sync_headers_response::OneOfMessage::NotAvailable(
                        block_id_range.into(),
                    )
                }
            }),
        }
    }
}

impl<SCT: SignatureCollection> TryFrom<ProtoBlockSyncHeadersResponse>
    for BlockSyncHeadersResponse<SCT>
{
    type Error = ProtoError;

    fn try_from(value: ProtoBlockSyncHeadersResponse) -> Result<Self, Self::Error> {
        let blocksync_header_response = match value.one_of_message {
            Some(proto_block_sync_headers_response::OneOfMessage::HeadersFound(
                blocksync_headers,
            )) => BlockSyncHeadersResponse::Found((
                blocksync_headers
                    .block_id_range
                    .ok_or(ProtoError::MissingRequiredField(
                        "BlockSyncHeaders.block_id_range".to_owned(),
                    ))?
                    .try_into()?,
                blocksync_headers
                    .headers
                    .into_iter()
                    .map(|b| b.try_into())
                    .collect::<Result<Vec<_>, _>>()?,
            )),
            Some(proto_block_sync_headers_response::OneOfMessage::NotAvailable(block_id_range)) => {
                BlockSyncHeadersResponse::NotAvailable(block_id_range.try_into()?)
            }
            None => Err(ProtoError::MissingRequiredField(
                "BlockSyncHeadersResponse.one_of_message".to_owned(),
            ))?,
        };

        Ok(blocksync_header_response)
    }
}

impl From<&BlockSyncPayloadResponse> for ProtoBlockSyncPayloadResponse {
    fn from(value: &BlockSyncPayloadResponse) -> Self {
        Self {
            one_of_message: Some(match value {
                BlockSyncPayloadResponse::Found((payload_id, payload)) => {
                    proto_block_sync_payload_response::OneOfMessage::PayloadFound(
                        ProtoBlockSyncPayload {
                            payload_id: Some(payload_id.into()),
                            payload: Some(payload.into()),
                        },
                    )
                }
                BlockSyncPayloadResponse::NotAvailable(payload_id) => {
                    proto_block_sync_payload_response::OneOfMessage::NotAvailable(payload_id.into())
                }
            }),
        }
    }
}

impl TryFrom<ProtoBlockSyncPayloadResponse> for BlockSyncPayloadResponse {
    type Error = ProtoError;

    fn try_from(value: ProtoBlockSyncPayloadResponse) -> Result<Self, Self::Error> {
        let blocksync_header_response = match value.one_of_message {
            Some(proto_block_sync_payload_response::OneOfMessage::PayloadFound(
                blocksync_payload,
            )) => BlockSyncPayloadResponse::Found((
                blocksync_payload
                    .payload_id
                    .ok_or(ProtoError::MissingRequiredField(
                        "BlockSyncPayload.payload_id".to_owned(),
                    ))?
                    .try_into()?,
                blocksync_payload
                    .payload
                    .ok_or(ProtoError::MissingRequiredField(
                        "BlockSyncPayload.payload".to_owned(),
                    ))?
                    .try_into()?,
            )),
            Some(proto_block_sync_payload_response::OneOfMessage::NotAvailable(payload_id)) => {
                BlockSyncPayloadResponse::NotAvailable(payload_id.try_into()?)
            }
            None => Err(ProtoError::MissingRequiredField(
                "BlockSyncPayloadResponse.one_of_message".to_owned(),
            ))?,
        };

        Ok(blocksync_header_response)
    }
}

impl<SCT: SignatureCollection> From<&BlockSyncResponseMessage<SCT>>
    for ProtoBlockSyncResponseMessage
{
    fn from(response: &BlockSyncResponseMessage<SCT>) -> Self {
        Self {
            blocksync_response: Some(match response {
                BlockSyncResponseMessage::HeadersResponse(headers_response) => {
                    proto_block_sync_response_message::BlocksyncResponse::HeadersResponse(
                        headers_response.into(),
                    )
                }
                BlockSyncResponseMessage::PayloadResponse(payload_response) => {
                    proto_block_sync_response_message::BlocksyncResponse::PayloadResponse(
                        payload_response.into(),
                    )
                }
            }),
        }
    }
}

impl<SCT: SignatureCollection> TryFrom<ProtoBlockSyncResponseMessage>
    for BlockSyncResponseMessage<SCT>
{
    type Error = ProtoError;

    fn try_from(value: ProtoBlockSyncResponseMessage) -> Result<Self, Self::Error> {
        let blocksync_response_message = match value.blocksync_response {
            Some(proto_block_sync_response_message::BlocksyncResponse::HeadersResponse(
                headers_response,
            )) => BlockSyncResponseMessage::HeadersResponse(headers_response.try_into()?),
            Some(proto_block_sync_response_message::BlocksyncResponse::PayloadResponse(
                payload_response,
            )) => BlockSyncResponseMessage::PayloadResponse(payload_response.try_into()?),
            None => Err(ProtoError::MissingRequiredField(
                "BlockSyncResponseMessage.blocksync_response".to_owned(),
            ))?,
        };

        Ok(blocksync_response_message)
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
