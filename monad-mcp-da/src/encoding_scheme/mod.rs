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

mod d25;
mod stub;

use bytes::Bytes;

use super::{
    assignment::{ChunkAssignment, ChunkId},
    chorus::env::EncodingScheme,
    chunk_tree::ChunkTree,
    layout::PacketLayout,
    types::{NodeId, ValidatorData},
};

// the DA-side capabilities of an encoding scheme: it fixes the packet
// layout, the chunk assignment and the symbol code.
//
// Note: packet_layout & chunk_assignment can be expensive to
// calculate. Avoid recomputing them where possible.
pub(crate) trait DAEncodingScheme {
    type Encoder: SymbolEncoder;
    type Decoder: SymbolDecoder;

    // None if the scheme fits no packet layout (e.g. an oversized
    // msg_len)
    fn packet_layout(&self, num_validators: usize) -> Option<PacketLayout>;

    fn chunk_assignment(
        &self,
        layout: &PacketLayout,
        author: &NodeId,
        validator_data: &ValidatorData,
    ) -> ChunkAssignment;

    fn encoder(&self, layout: PacketLayout, num_chunks: usize) -> Self::Encoder;

    fn decoder(&self, layout: PacketLayout, num_chunks: usize) -> Self::Decoder;

    // encode a proposal into its chunk tree. None if the message does
    // not fit the layout.
    fn encode(&self, message: &[u8], layout: PacketLayout, num_chunks: usize) -> Option<ChunkTree> {
        if message.is_empty() {
            return None;
        }
        let symbols = self.encoder(layout, num_chunks).encode(message);
        ChunkTree::complete(&layout, symbols)
    }
}

pub(crate) trait SymbolEncoder {
    // one symbol per chunk, in chunk id order
    fn encode(&self, message: &[u8]) -> Vec<Bytes>;
}

// the decoding state of one proposal.
pub(crate) trait SymbolDecoder {
    fn ingest(&mut self, chunk_id: ChunkId, symbol: &Bytes);

    // the message, once enough symbols arrived
    fn try_decode(&mut self) -> Option<Bytes>;
}

impl DAEncodingScheme for EncodingScheme {
    type Encoder = Box<dyn SymbolEncoder>;
    type Decoder = Box<dyn SymbolDecoder>;

    fn packet_layout(&self, num_validators: usize) -> Option<PacketLayout> {
        match self {
            EncodingScheme::D25(d25) => d25.packet_layout(num_validators),
        }
    }

    fn chunk_assignment(
        &self,
        layout: &PacketLayout,
        author: &NodeId,
        validator_data: &ValidatorData,
    ) -> ChunkAssignment {
        match self {
            EncodingScheme::D25(d25) => d25.chunk_assignment(layout, author, validator_data),
        }
    }

    fn encoder(&self, layout: PacketLayout, num_chunks: usize) -> Self::Encoder {
        match self {
            EncodingScheme::D25(d25) => Box::new(d25.encoder(layout, num_chunks)),
        }
    }

    fn decoder(&self, layout: PacketLayout, num_chunks: usize) -> Self::Decoder {
        match self {
            EncodingScheme::D25(d25) => Box::new(d25.decoder(layout, num_chunks)),
        }
    }
}

impl SymbolEncoder for Box<dyn SymbolEncoder> {
    fn encode(&self, message: &[u8]) -> Vec<Bytes> {
        (**self).encode(message)
    }
}

impl SymbolDecoder for Box<dyn SymbolDecoder> {
    fn ingest(&mut self, chunk_id: ChunkId, symbol: &Bytes) {
        (**self).ingest(chunk_id, symbol)
    }

    fn try_decode(&mut self) -> Option<Bytes> {
        (**self).try_decode()
    }
}
