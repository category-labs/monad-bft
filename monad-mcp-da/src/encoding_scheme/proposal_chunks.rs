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

use bytes::Bytes;

use super::super::{assignment::ChunkId, chunk::ChunkData, types::MerkleRoot};

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
