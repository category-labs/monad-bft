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

#[cfg(feature = "enable_hollow")]
mod hollow;
mod merkle;
mod proposal_chunks;
mod reed_solomon;

use bytes::Bytes;

#[cfg(all(test, feature = "enable_hollow"))]
pub(crate) use self::hollow::{HORIZON, SLACK, is_published, root_of_message};
#[cfg(feature = "enable_hollow")]
pub(crate) use self::hollow::{Hollow, Retained};
pub(crate) use self::proposal_chunks::{Chunks, Decoder, Encoded, ProposalChunks};
pub use self::proposal_chunks::{InvalidEncoding, Resolved};
use super::{
    assignment::ChunkAssignment,
    types::{EncodingScheme, MerkleRoot, NodeId, ProposalHeader, Timestamp, ValidatorData},
};

// The encoding scheme specifies the symbol code and the chunk
// assignment. It sizes its symbols by the layout its header travels
// in.
pub(crate) trait DAEncodingScheme {
    // takes the chunks of a proposal back to its encoding
    type Decoder: Decoder;

    fn msg_len(&self) -> usize;
    fn num_source_chunks(&self) -> usize;

    // whether a proposer among num_validators must have chosen exactly
    // this scheme for its message
    fn is_canonical(&self, num_validators: usize) -> bool;

    // Note: can be expensive to calculate. Avoid recomputing when possible.
    fn chunk_assignment(&self, author: &NodeId, validator_data: &ValidatorData) -> ChunkAssignment;

    // every chunk of a message of the scheme's length. None for any
    // other length.
    fn encode(
        &self,
        message: &Bytes,
        num_chunks: usize,
    ) -> Option<<Self::Decoder as Decoder>::Encoded>;

    // for the chunks of the proposal under root, before any arrives
    fn decoder(&self, root: MerkleRoot, num_chunks: usize) -> Self::Decoder;
}

// whether a proposer among num_validators must have chosen exactly the
// scheme a header names
pub(crate) fn is_canonical(scheme: &EncodingScheme, num_validators: usize) -> bool {
    match scheme {
        EncodingScheme::D33(d33) => d33.is_canonical(num_validators),
        #[cfg(feature = "enable_hollow")]
        EncodingScheme::D33Hollow(d33) => Hollow::new(*d33).is_canonical(num_validators),
    }
}

// the scheme a header names, applied to its proposal: who is assigned
// which chunk, and the chunks, of which none is received yet. `now` is
// when this node takes the proposal up; a hollow decoder depends on it.
pub(crate) fn instantiate(
    header: &ProposalHeader,
    author: &NodeId,
    validator_data: &ValidatorData,
    now: Timestamp,
) -> (ChunkAssignment, Box<dyn ProposalChunks>) {
    let root = header.root;
    match &header.scheme {
        EncodingScheme::D33(d33) => {
            // real chunks decode whenever they arrive
            let _ = now;
            let assignment = d33.chunk_assignment(author, validator_data);
            let decoder = d33.decoder(root, assignment.num_chunks());
            (assignment, boxed(decoder))
        }
        #[cfg(feature = "enable_hollow")]
        EncodingScheme::D33Hollow(d33) => {
            let hollow = Hollow::new(*d33);
            let assignment = hollow.chunk_assignment(author, validator_data);
            let decoder = hollow.decoder_at(root, assignment.num_chunks(), now);
            (assignment, boxed(decoder))
        }
    }
}

// chunks of which none is received yet
fn boxed<D>(decoder: D) -> Box<dyn ProposalChunks>
where
    D: Decoder + Send + 'static,
    D::Encoded: Send,
{
    Box::new(Chunks::Decoding(decoder))
}
