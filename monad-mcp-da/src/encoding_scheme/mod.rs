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

pub mod d33;

mod merkle;
mod reed_solomon;

use bytes::Bytes;

use super::{
    assignment::{ChunkAssignment, ChunkId},
    chunk::ChunkData,
    types::{EncodingScheme, MerkleRoot, NodeId, ValidatorData},
};

// The encoding scheme specifies the symbol code and the chunk
// assignment. It sizes its symbols by the layout its header travels
// in.
pub(crate) trait DAEncodingScheme {
    type Chunks: ProposalChunks;

    fn msg_len(&self) -> usize;
    fn num_source_chunks(&self) -> usize;

    // whether a proposer among num_validators must have chosen exactly
    // this scheme for its message
    fn is_canonical(&self, num_validators: usize) -> bool;

    // Note: can be expensive to calculate. Avoid recomputing when possible.
    fn chunk_assignment(&self, author: &NodeId, validator_data: &ValidatorData) -> ChunkAssignment;

    // every chunk of a message of the scheme's length. None for any
    // other length.
    fn encode(&self, message: &[u8], num_chunks: usize) -> Option<Self::Chunks>;

    // the chunks of a proposal under root, before any arrives
    fn empty_chunks(&self, root: MerkleRoot, num_chunks: usize) -> Self::Chunks;
}

// decoded proposal does not reencode back to root
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvalidEncoding;

pub type Resolved = Result<Bytes, InvalidEncoding>;

// the chunk does not bind to the root
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct BadProof;

// the chunks of one proposal under its root: complete once encoded,
// received one by one until they decode
pub(crate) trait ProposalChunks: Send {
    fn root(&self) -> MerkleRoot;

    // received, or derivable once decoded
    fn contains(&self, chunk_id: ChunkId) -> bool;

    // true when the chunk is new. nothing is new once resolved.
    fn ingest(&mut self, chunk_id: ChunkId, data: ChunkData) -> Result<bool, BadProof>;

    // once: the message when enough chunks arrived, or invalid when it
    // does not re-encode to the root
    fn try_decode(&mut self) -> Option<Resolved>;
    fn is_resolved(&self) -> bool;

    // None unless contained
    fn chunk_data(&self, chunk_id: ChunkId) -> Option<ChunkData>;
}

pub(crate) trait SymbolEncoder: Send {
    // one symbol per chunk, in chunk id order
    fn encode(&self, message: &[u8]) -> Vec<Bytes>;
}

// the decoding state of one proposal.
pub(crate) trait SymbolDecoder: Send {
    fn ingest(&mut self, chunk_id: ChunkId, symbol: &Bytes);

    // the message, once enough symbols arrived
    fn try_decode(&mut self) -> Option<Bytes>;
}

impl DAEncodingScheme for EncodingScheme {
    type Chunks = Box<dyn ProposalChunks>;

    fn msg_len(&self) -> usize {
        match self {
            EncodingScheme::D33(d33) => d33.msg_len(),
        }
    }

    fn num_source_chunks(&self) -> usize {
        match self {
            EncodingScheme::D33(d33) => d33.num_source_chunks(),
        }
    }

    fn is_canonical(&self, num_validators: usize) -> bool {
        match self {
            EncodingScheme::D33(d33) => d33.is_canonical(num_validators),
        }
    }

    fn chunk_assignment(&self, author: &NodeId, validator_data: &ValidatorData) -> ChunkAssignment {
        match self {
            EncodingScheme::D33(d33) => d33.chunk_assignment(author, validator_data),
        }
    }

    fn encode(&self, message: &[u8], num_chunks: usize) -> Option<Self::Chunks> {
        match self {
            EncodingScheme::D33(d33) => {
                let chunks = d33.encode(message, num_chunks)?;
                Some(Box::new(chunks))
            }
        }
    }

    fn empty_chunks(&self, root: MerkleRoot, num_chunks: usize) -> Self::Chunks {
        match self {
            EncodingScheme::D33(d33) => {
                let chunks = d33.empty_chunks(root, num_chunks);
                Box::new(chunks)
            }
        }
    }
}

impl ProposalChunks for Box<dyn ProposalChunks> {
    fn root(&self) -> MerkleRoot {
        (**self).root()
    }

    fn contains(&self, chunk_id: ChunkId) -> bool {
        (**self).contains(chunk_id)
    }

    fn ingest(&mut self, chunk_id: ChunkId, data: ChunkData) -> Result<bool, BadProof> {
        (**self).ingest(chunk_id, data)
    }

    fn try_decode(&mut self) -> Option<Resolved> {
        (**self).try_decode()
    }

    fn is_resolved(&self) -> bool {
        (**self).is_resolved()
    }

    fn chunk_data(&self, chunk_id: ChunkId) -> Option<ChunkData> {
        (**self).chunk_data(chunk_id)
    }
}
