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
