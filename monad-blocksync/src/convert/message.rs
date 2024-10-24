use monad_consensus_types::signature_collection::SignatureCollection;
use monad_proto::{
    error::ProtoError,
    proto::{
        blocksync::{
            proto_block_sync_headers_response, proto_block_sync_payload_response,
            ProtoBlockSyncHeaders, ProtoBlockSyncHeadersResponse, ProtoBlockSyncPayloadResponse,
        },
        message::{
            proto_block_sync_request_message, proto_block_sync_response_message,
            ProtoBlockSyncRequestMessage, ProtoBlockSyncResponseMessage,
        },
    },
};

use crate::{
    blocksync::BlockSyncSelfRequester,
    messages::message::{
        BlockSyncHeadersResponse, BlockSyncPayloadResponse, BlockSyncRequestMessage,
        BlockSyncResponseMessage,
    },
};

impl From<&BlockSyncSelfRequester> for i32 {
    fn from(requester: &BlockSyncSelfRequester) -> Self {
        match requester {
            BlockSyncSelfRequester::Consensus => 0,
            BlockSyncSelfRequester::StateSync => 1,
        }
    }
}

impl TryFrom<i32> for BlockSyncSelfRequester {
    type Error = ProtoError;
    fn try_from(requester: i32) -> Result<Self, Self::Error> {
        match requester {
            0 => Ok(BlockSyncSelfRequester::Consensus),
            1 => Ok(BlockSyncSelfRequester::StateSync),
            _ => Err(ProtoError::DeserializeError(
                "unknown blocksync requester".to_owned(),
            )),
        }
    }
}

impl From<&BlockSyncRequestMessage> for ProtoBlockSyncRequestMessage {
    fn from(value: &BlockSyncRequestMessage) -> Self {
        let request_type = match value {
            BlockSyncRequestMessage::Headers(block_range) => {
                proto_block_sync_request_message::RequestType::BlockRange(block_range.into())
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
            Some(proto_block_sync_request_message::RequestType::BlockRange(block_range)) => {
                BlockSyncRequestMessage::Headers(block_range.try_into()?)
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
            headers_response: Some(match value {
                BlockSyncHeadersResponse::Found((block_range, blocksync_headers)) => {
                    proto_block_sync_headers_response::HeadersResponse::HeadersFound(
                        ProtoBlockSyncHeaders {
                            block_range: Some(block_range.into()),
                            headers: blocksync_headers
                                .iter()
                                .map(|b| b.into())
                                .collect::<Vec<_>>(),
                        },
                    )
                }
                BlockSyncHeadersResponse::NotAvailable(block_range) => {
                    proto_block_sync_headers_response::HeadersResponse::NotAvailable(
                        block_range.into(),
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
        let blocksync_header_response = match value.headers_response {
            Some(proto_block_sync_headers_response::HeadersResponse::HeadersFound(
                blocksync_headers,
            )) => BlockSyncHeadersResponse::Found((
                blocksync_headers
                    .block_range
                    .ok_or(ProtoError::MissingRequiredField(
                        "BlockSyncHeaders.block_range".to_owned(),
                    ))?
                    .try_into()?,
                blocksync_headers
                    .headers
                    .into_iter()
                    .map(|b| b.try_into())
                    .collect::<Result<Vec<_>, _>>()?,
            )),
            Some(proto_block_sync_headers_response::HeadersResponse::NotAvailable(block_range)) => {
                BlockSyncHeadersResponse::NotAvailable(block_range.try_into()?)
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
            payload_response: Some(match value {
                BlockSyncPayloadResponse::Found(payload) => {
                    proto_block_sync_payload_response::PayloadResponse::PayloadFound(payload.into())
                }
                BlockSyncPayloadResponse::NotAvailable(payload_id) => {
                    proto_block_sync_payload_response::PayloadResponse::NotAvailable(
                        payload_id.into(),
                    )
                }
            }),
        }
    }
}

impl TryFrom<ProtoBlockSyncPayloadResponse> for BlockSyncPayloadResponse {
    type Error = ProtoError;

    fn try_from(value: ProtoBlockSyncPayloadResponse) -> Result<Self, Self::Error> {
        let blocksync_payload_response = match value.payload_response {
            Some(proto_block_sync_payload_response::PayloadResponse::PayloadFound(payload)) => {
                BlockSyncPayloadResponse::Found(payload.try_into()?)
            }
            Some(proto_block_sync_payload_response::PayloadResponse::NotAvailable(payload_id)) => {
                BlockSyncPayloadResponse::NotAvailable(payload_id.try_into()?)
            }
            None => Err(ProtoError::MissingRequiredField(
                "BlockSyncPayloadResponse.one_of_message".to_owned(),
            ))?,
        };

        Ok(blocksync_payload_response)
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
