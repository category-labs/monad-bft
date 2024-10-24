use monad_consensus_types::{
    block::{Block, BlockRange},
    payload::{Payload, PayloadId},
    signature_collection::SignatureCollection,
};
use monad_crypto::hasher::{Hashable, Hasher};
use monad_types::EnumDiscriminant;
use zerocopy::AsBytes;

/// Request block sync message sent to a peer
///
/// The node sends the block sync request either missing blocks headers or
/// a single payload
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum BlockSyncRequestMessage {
    Headers(BlockRange),
    Payload(PayloadId),
}

impl Hashable for BlockSyncRequestMessage {
    fn hash(&self, state: &mut impl Hasher) {
        state.update(std::any::type_name::<Self>().as_bytes());
        match self {
            BlockSyncRequestMessage::Headers(block_range) => {
                EnumDiscriminant(1).hash(state);
                block_range.hash(state);
            }
            BlockSyncRequestMessage::Payload(payload_id) => {
                EnumDiscriminant(2).hash(state);
                state.update(payload_id.0.as_bytes());
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BlockSyncHeadersResponse<SCT: SignatureCollection> {
    Found((BlockRange, Vec<Block<SCT>>)),
    NotAvailable(BlockRange),
}

impl<SCT: SignatureCollection> BlockSyncHeadersResponse<SCT> {
    pub fn get_block_range(&self) -> BlockRange {
        match self {
            BlockSyncHeadersResponse::Found((block_range, _)) => *block_range,
            BlockSyncHeadersResponse::NotAvailable(block_range) => *block_range,
        }
    }
}

impl<SCT: SignatureCollection> Hashable for BlockSyncHeadersResponse<SCT> {
    fn hash(&self, state: &mut impl Hasher) {
        state.update(std::any::type_name::<Self>().as_bytes());
        match self {
            BlockSyncHeadersResponse::Found((block_range, blocks)) => {
                EnumDiscriminant(1).hash(state);
                block_range.hash(state);
                for block in blocks {
                    block.hash(state);
                }
            }
            BlockSyncHeadersResponse::NotAvailable(block_range) => {
                EnumDiscriminant(2).hash(state);
                block_range.hash(state);
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BlockSyncPayloadResponse {
    Found(Payload),
    NotAvailable(PayloadId),
}

impl BlockSyncPayloadResponse {
    pub fn get_payload_id(&self) -> PayloadId {
        match self {
            BlockSyncPayloadResponse::Found(payload) => payload.get_id(),
            BlockSyncPayloadResponse::NotAvailable(payload_id) => *payload_id,
        }
    }
}

impl Hashable for BlockSyncPayloadResponse {
    fn hash(&self, state: &mut impl Hasher) {
        state.update(std::any::type_name::<Self>().as_bytes());
        match self {
            BlockSyncPayloadResponse::Found(payload) => {
                EnumDiscriminant(1).hash(state);
                payload.hash(state);
            }
            BlockSyncPayloadResponse::NotAvailable(payload_id) => {
                EnumDiscriminant(2).hash(state);
                state.update(payload_id.0.as_bytes());
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BlockSyncResponseMessage<SCT: SignatureCollection> {
    HeadersResponse(BlockSyncHeadersResponse<SCT>),
    PayloadResponse(BlockSyncPayloadResponse),
}

impl<SCT: SignatureCollection> BlockSyncResponseMessage<SCT> {
    pub fn found_headers(
        block_range: BlockRange,
        headers: Vec<Block<SCT>>,
    ) -> BlockSyncResponseMessage<SCT> {
        BlockSyncResponseMessage::HeadersResponse(BlockSyncHeadersResponse::Found((
            block_range,
            headers,
        )))
    }

    pub fn headers_not_available(block_range: BlockRange) -> BlockSyncResponseMessage<SCT> {
        BlockSyncResponseMessage::HeadersResponse(BlockSyncHeadersResponse::NotAvailable(
            block_range,
        ))
    }

    pub fn found_payload(payload: Payload) -> BlockSyncResponseMessage<SCT> {
        BlockSyncResponseMessage::PayloadResponse(BlockSyncPayloadResponse::Found(payload))
    }

    pub fn payload_not_available(payload_id: PayloadId) -> BlockSyncResponseMessage<SCT> {
        BlockSyncResponseMessage::PayloadResponse(BlockSyncPayloadResponse::NotAvailable(
            payload_id,
        ))
    }
}

impl<SCT: SignatureCollection> Hashable for BlockSyncResponseMessage<SCT> {
    fn hash(&self, state: &mut impl Hasher) {
        state.update(std::any::type_name::<Self>().as_bytes());
        match self {
            BlockSyncResponseMessage::HeadersResponse(headers_response) => {
                EnumDiscriminant(1).hash(state);
                headers_response.hash(state);
            }
            BlockSyncResponseMessage::PayloadResponse(payload_response) => {
                EnumDiscriminant(2).hash(state);
                payload_response.hash(state)
            }
        }
    }
}
