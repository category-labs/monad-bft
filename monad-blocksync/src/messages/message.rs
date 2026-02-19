// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use alloy_rlp::{bytes, encode_list, Decodable, Encodable, Header};
use monad_consensus_types::{
    block::{BlockRange, ConsensusBlockHeader},
    payload::{ConsensusBlockBody, ConsensusBlockBodyId},
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_types::{ExecutionProtocol, LimitedVec};
use monad_validator::signature_collection::SignatureCollection;
use serde::Serialize;

/// Maximum number of headers allowed in a BlockSyncHeadersResponse message.
/// This bounds RLP deserialization memory usage for incoming messages.
pub const BLOCKSYNC_MAX_NUM_HEADERS: usize = 800;

const BLOCK_SYNC_REQUEST_MESSAGE_NAME: &str = "BlockSyncRequestMessage";
const BLOCK_SYNC_RESPONSE_MESSAGE_NAME: &str = "BlockSyncResponseMessage";
const BLOCK_SYNC_HEADERS_RESPONSE_NAME: &str = "BlockSyncHeadersResponse";
const BLOCK_SYNC_PAYLOAD_RESPONSE_NAME: &str = "BlockSyncBodyResponse";

/// Request block sync message sent to a peer
///
/// The node sends the block sync request either missing blocks headers or
/// a single payload
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, Serialize)]
pub enum BlockSyncRequestMessage {
    Headers(BlockRange),
    Payload(ConsensusBlockBodyId),
}

impl Encodable for BlockSyncRequestMessage {
    fn encode(&self, out: &mut dyn bytes::BufMut) {
        let name = BLOCK_SYNC_REQUEST_MESSAGE_NAME;
        match self {
            Self::Headers(b) => {
                let enc: [&dyn Encodable; 3] = [&name, &1u8, &b];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
            Self::Payload(id) => {
                let enc: [&dyn Encodable; 3] = [&name, &2u8, &id];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
        }
    }
}

impl Decodable for BlockSyncRequestMessage {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let mut payload = Header::decode_bytes(buf, true)?;
        let name = String::decode(&mut payload)?;
        if name != BLOCK_SYNC_REQUEST_MESSAGE_NAME {
            return Err(alloy_rlp::Error::Custom(
                "expected to decode type BlockSyncRequestMessage",
            ));
        }
        let result = match u8::decode(&mut payload)? {
            1 => Self::Headers(BlockRange::decode(&mut payload)?),
            2 => Self::Payload(ConsensusBlockBodyId::decode(&mut payload)?),
            _ => {
                return Err(alloy_rlp::Error::Custom(
                    "failed to decode unknown BlockSyncRequestMessage",
                ))
            }
        };
        if !payload.is_empty() {
            return Err(alloy_rlp::Error::UnexpectedLength);
        }
        Ok(result)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub enum BlockSyncHeadersResponse<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    Found((BlockRange, Vec<ConsensusBlockHeader<ST, SCT, EPT>>)),
    NotAvailable(BlockRange),
}

impl<ST, SCT, EPT> BlockSyncHeadersResponse<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    pub fn get_block_range(&self) -> BlockRange {
        match self {
            BlockSyncHeadersResponse::Found((block_range, _)) => *block_range,
            BlockSyncHeadersResponse::NotAvailable(block_range) => *block_range,
        }
    }
}

impl<ST, SCT, EPT> Encodable for BlockSyncHeadersResponse<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn encode(&self, out: &mut dyn bytes::BufMut) {
        let name = BLOCK_SYNC_HEADERS_RESPONSE_NAME;
        match self {
            Self::Found((block_range, blocks)) => {
                let enc: [&dyn Encodable; 4] = [&name, &1u8, &block_range, &blocks];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
            Self::NotAvailable(block_range) => {
                let enc: [&dyn Encodable; 3] = [&name, &2u8, &block_range];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
        }
    }
}

impl<ST, SCT, EPT> Decodable for BlockSyncHeadersResponse<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let mut payload = Header::decode_bytes(buf, true)?;
        let name = String::decode(&mut payload)?;
        if name != BLOCK_SYNC_HEADERS_RESPONSE_NAME {
            return Err(alloy_rlp::Error::Custom(
                "expected to decode type BlockSyncHeaderResponse",
            ));
        }
        let result = match u8::decode(&mut payload)? {
            1 => {
                let block_range = BlockRange::decode(&mut payload)?;
                let headers = LimitedVec::<
                    ConsensusBlockHeader<ST, SCT, EPT>,
                    BLOCKSYNC_MAX_NUM_HEADERS,
                >::decode(&mut payload)?
                .into_inner();
                Self::Found((block_range, headers))
            }
            2 => Self::NotAvailable(BlockRange::decode(&mut payload)?),
            _ => {
                return Err(alloy_rlp::Error::Custom(
                    "failed to decode unknown BlockSyncHeadersResponse",
                ))
            }
        };
        if !payload.is_empty() {
            return Err(alloy_rlp::Error::UnexpectedLength);
        }
        Ok(result)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub enum BlockSyncBodyResponse<EPT>
where
    EPT: ExecutionProtocol,
{
    Found(ConsensusBlockBody<EPT>),
    NotAvailable(ConsensusBlockBodyId),
}

impl<EPT> BlockSyncBodyResponse<EPT>
where
    EPT: ExecutionProtocol,
{
    pub fn get_payload_id(&self) -> ConsensusBlockBodyId {
        match self {
            BlockSyncBodyResponse::Found(payload) => payload.get_id(),
            BlockSyncBodyResponse::NotAvailable(payload_id) => *payload_id,
        }
    }
}

impl<EPT> Encodable for BlockSyncBodyResponse<EPT>
where
    EPT: ExecutionProtocol,
{
    fn encode(&self, out: &mut dyn bytes::BufMut) {
        let name = BLOCK_SYNC_PAYLOAD_RESPONSE_NAME;
        match self {
            Self::Found(payload) => {
                let enc: [&dyn Encodable; 3] = [&name, &1u8, &payload];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
            Self::NotAvailable(payload_id) => {
                let enc: [&dyn Encodable; 3] = [&name, &2u8, &payload_id];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
        }
    }
}

impl<EPT> Decodable for BlockSyncBodyResponse<EPT>
where
    EPT: ExecutionProtocol,
{
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let mut payload = Header::decode_bytes(buf, true)?;
        let name = String::decode(&mut payload)?;
        if name != BLOCK_SYNC_PAYLOAD_RESPONSE_NAME {
            return Err(alloy_rlp::Error::Custom(
                "expected to decode type BlockSyncBodyResponse",
            ));
        }
        let result = match u8::decode(&mut payload)? {
            1 => Self::Found(ConsensusBlockBody::decode(&mut payload)?),
            2 => Self::NotAvailable(ConsensusBlockBodyId::decode(&mut payload)?),
            _ => {
                return Err(alloy_rlp::Error::Custom(
                    "failed to decode unknown BlockSyncBodyResponse",
                ))
            }
        };
        if !payload.is_empty() {
            return Err(alloy_rlp::Error::UnexpectedLength);
        }
        Ok(result)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub enum BlockSyncResponseMessage<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    HeadersResponse(BlockSyncHeadersResponse<ST, SCT, EPT>),
    PayloadResponse(BlockSyncBodyResponse<EPT>),
}

impl<ST, SCT, EPT> BlockSyncResponseMessage<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    pub fn found_headers(
        block_range: BlockRange,
        headers: Vec<ConsensusBlockHeader<ST, SCT, EPT>>,
    ) -> BlockSyncResponseMessage<ST, SCT, EPT> {
        BlockSyncResponseMessage::HeadersResponse(BlockSyncHeadersResponse::Found((
            block_range,
            headers,
        )))
    }

    pub fn headers_not_available(
        block_range: BlockRange,
    ) -> BlockSyncResponseMessage<ST, SCT, EPT> {
        BlockSyncResponseMessage::HeadersResponse(BlockSyncHeadersResponse::NotAvailable(
            block_range,
        ))
    }

    pub fn found_payload(body: ConsensusBlockBody<EPT>) -> BlockSyncResponseMessage<ST, SCT, EPT> {
        BlockSyncResponseMessage::PayloadResponse(BlockSyncBodyResponse::Found(body))
    }

    pub fn payload_not_available(
        block_body_id: ConsensusBlockBodyId,
    ) -> BlockSyncResponseMessage<ST, SCT, EPT> {
        BlockSyncResponseMessage::PayloadResponse(BlockSyncBodyResponse::NotAvailable(
            block_body_id,
        ))
    }
}

impl<ST, SCT, EPT> Encodable for BlockSyncResponseMessage<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn encode(&self, out: &mut dyn bytes::BufMut) {
        let name = BLOCK_SYNC_RESPONSE_MESSAGE_NAME;
        match self {
            Self::HeadersResponse(resp) => {
                let enc: [&dyn Encodable; 3] = [&name, &1u8, &resp];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
            Self::PayloadResponse(resp) => {
                let enc: [&dyn Encodable; 3] = [&name, &2u8, &resp];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
        }
    }
}

impl<ST, SCT, EPT> Decodable for BlockSyncResponseMessage<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let mut payload = Header::decode_bytes(buf, true)?;
        let name = String::decode(&mut payload)?;
        if name != BLOCK_SYNC_RESPONSE_MESSAGE_NAME {
            return Err(alloy_rlp::Error::Custom(
                "expected to decode type BlockSyncResponseMessage",
            ));
        }
        let result = match u8::decode(&mut payload)? {
            1 => Self::HeadersResponse(BlockSyncHeadersResponse::decode(&mut payload)?),
            2 => Self::PayloadResponse(BlockSyncBodyResponse::decode(&mut payload)?),
            _ => {
                return Err(alloy_rlp::Error::Custom(
                    "failed to decode unknown BlockSyncResponseMessage",
                ))
            }
        };
        if !payload.is_empty() {
            return Err(alloy_rlp::Error::UnexpectedLength);
        }
        Ok(result)
    }
}
