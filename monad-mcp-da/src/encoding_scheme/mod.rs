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

mod hollow;
mod merkle;
mod proposal_chunks;
mod symbol_code;

use bytes::Bytes;

pub use self::proposal_chunks::{InvalidEncoding, Resolved};
pub(crate) use self::{
    hollow::Hollow,
    proposal_chunks::{BadProof, ProposalChunks},
};
use super::{
    assignment::ChunkAssignment,
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
    fn encode(&self, message: &Bytes, num_chunks: usize) -> Option<Self::Chunks>;

    // the chunks of a proposal under root, before any arrives
    fn empty_chunks(&self, root: MerkleRoot, num_chunks: usize) -> Self::Chunks;
}

impl DAEncodingScheme for EncodingScheme {
    type Chunks = Box<dyn ProposalChunks>;

    fn msg_len(&self) -> usize {
        match self {
            EncodingScheme::D33(d33) => d33.msg_len(),
            EncodingScheme::D33Hollow(d33) => Hollow::new(*d33).msg_len(),
        }
    }

    fn num_source_chunks(&self) -> usize {
        match self {
            EncodingScheme::D33(d33) => d33.num_source_chunks(),
            EncodingScheme::D33Hollow(d33) => Hollow::new(*d33).num_source_chunks(),
        }
    }

    fn is_canonical(&self, num_validators: usize) -> bool {
        match self {
            EncodingScheme::D33(d33) => d33.is_canonical(num_validators),
            EncodingScheme::D33Hollow(d33) => Hollow::new(*d33).is_canonical(num_validators),
        }
    }

    fn chunk_assignment(&self, author: &NodeId, validator_data: &ValidatorData) -> ChunkAssignment {
        match self {
            EncodingScheme::D33(d33) => d33.chunk_assignment(author, validator_data),
            EncodingScheme::D33Hollow(d33) => {
                Hollow::new(*d33).chunk_assignment(author, validator_data)
            }
        }
    }

    fn encode(&self, message: &Bytes, num_chunks: usize) -> Option<Self::Chunks> {
        match self {
            EncodingScheme::D33(d33) => {
                let chunks = d33.encode(message, num_chunks)?;
                Some(Box::new(chunks))
            }
            EncodingScheme::D33Hollow(d33) => {
                let chunks = Hollow::new(*d33).encode(message, num_chunks)?;
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
            EncodingScheme::D33Hollow(d33) => {
                let chunks = Hollow::new(*d33).empty_chunks(root, num_chunks);
                Box::new(chunks)
            }
        }
    }
}
