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

pub mod d25;
mod stub;

use bytes::Bytes;

use super::{
    assignment::{ChunkAssignment, ChunkId},
    chunk_tree::ChunkTree,
    types::{EncodingScheme, NodeId, ValidatorData},
};

// The encoding scheme specifies: the symbol code, the chunk
// assignment, and the merkle depth its chunks need.
pub(crate) trait DAEncodingScheme {
    type Encoder: SymbolEncoder;
    type Decoder: SymbolDecoder;

    fn depth(&self) -> u8;
    fn msg_len(&self) -> usize;
    fn num_source_chunks(&self) -> usize;

    // whether a proposer among num_validators must have chosen exactly
    // this scheme for its message
    fn is_canonical(&self, num_validators: usize) -> bool;

    // Note: can be expensive to calculate. Avoid recomputing when possible.
    fn chunk_assignment(&self, author: &NodeId, validator_data: &ValidatorData) -> ChunkAssignment;

    fn encoder(&self, num_chunks: usize) -> Self::Encoder;
    fn decoder(&self, num_chunks: usize) -> Self::Decoder;

    // the chunk tree of a message of the scheme's length. None for any
    // other length.
    fn encode(&self, message: &[u8], num_chunks: usize) -> Option<ChunkTree> {
        if message.is_empty() || message.len() != self.msg_len() {
            return None;
        }
        let symbols = self.encoder(num_chunks).encode(message);
        ChunkTree::complete(self.depth(), symbols)
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

    fn depth(&self) -> u8 {
        match self {
            EncodingScheme::D25(d25) => d25.depth(),
        }
    }

    fn msg_len(&self) -> usize {
        match self {
            EncodingScheme::D25(d25) => d25.msg_len(),
        }
    }

    fn num_source_chunks(&self) -> usize {
        match self {
            EncodingScheme::D25(d25) => d25.num_source_chunks(),
        }
    }

    fn is_canonical(&self, num_validators: usize) -> bool {
        match self {
            EncodingScheme::D25(d25) => d25.is_canonical(num_validators),
        }
    }

    fn chunk_assignment(&self, author: &NodeId, validator_data: &ValidatorData) -> ChunkAssignment {
        match self {
            EncodingScheme::D25(d25) => d25.chunk_assignment(author, validator_data),
        }
    }

    fn encoder(&self, num_chunks: usize) -> Self::Encoder {
        match self {
            EncodingScheme::D25(d25) => Box::new(d25.encoder(num_chunks)),
        }
    }

    fn decoder(&self, num_chunks: usize) -> Self::Decoder {
        match self {
            EncodingScheme::D25(d25) => Box::new(d25.decoder(num_chunks)),
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
