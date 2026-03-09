use bytes::BytesMut;
use monad_crypto::certificate_signature::PubKey;

use crate::util::{Recipient, UdpMessage};

pub(crate) struct Chunk<PT: PubKey> {
    chunk_id: usize,
    recipient: Recipient<PT>,
    payload: BytesMut,
}

impl<PT: PubKey> From<Chunk<PT>> for UdpMessage<PT> {
    fn from(chunk: Chunk<PT>) -> Self {
        Self {
            recipient: chunk.recipient,
            stride: chunk.payload.len(),
            payload: chunk.payload.freeze(),
        }
    }
}

impl<PT: PubKey> Chunk<PT> {
    pub fn new(chunk_id: usize, recipient: Recipient<PT>, payload: BytesMut) -> Self {
        debug_assert!(chunk_id < u16::MAX as usize);
        Self {
            chunk_id,
            recipient,
            payload,
        }
    }

    pub fn recipient(&self) -> &Recipient<PT> {
        &self.recipient
    }

    pub fn chunk_id(&self) -> usize {
        self.chunk_id
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    pub fn payload_mut(&mut self) -> &mut [u8] {
        &mut self.payload
    }
}

// used in gso grouping
pub(super) struct AggregatedChunk<PT: PubKey> {
    pub recipient: Recipient<PT>,
    pub payload: bytes::BytesMut,
    pub stride: usize,
}

impl<PT: PubKey> AggregatedChunk<PT> {
    #[must_use]
    pub fn aggregate(&mut self, chunk: Chunk<PT>) -> Option<Self> {
        if self.recipient == chunk.recipient && chunk.payload.len() == self.stride {
            // same recipient, merge the payload. BytesMut::unsplit is
            // O(1) when the chunk payloads are consecutive.
            self.payload.unsplit(chunk.payload);
            return None;
        }

        let new_agg = chunk.into();
        Some(std::mem::replace(self, new_agg))
    }
}

impl<PT: PubKey> From<Chunk<PT>> for AggregatedChunk<PT> {
    fn from(chunk: Chunk<PT>) -> Self {
        Self {
            recipient: chunk.recipient,
            stride: chunk.payload.len(),
            payload: chunk.payload,
        }
    }
}

impl<PT: PubKey> From<AggregatedChunk<PT>> for UdpMessage<PT> {
    fn from(agg_chunk: AggregatedChunk<PT>) -> Self {
        UdpMessage {
            recipient: agg_chunk.recipient,
            stride: agg_chunk.stride,
            payload: agg_chunk.payload.freeze(),
        }
    }
}
