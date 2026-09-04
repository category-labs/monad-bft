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

use std::collections::{BTreeMap, HashMap};

use bytes::Bytes;

use super::types::{MerkleHash, ProposalHeader};

// a chunk id as carried on the wire, not yet checked against an
// assignment.
pub type WireChunkId = u16;

// the chunk-specific half of a chunk: what remains after the shared
// proposal header and the chunk id are factored out.
#[derive(Clone)]
pub struct ChunkData {
    pub(crate) symbol: Bytes,
    pub(crate) proof: Box<[MerkleHash]>,
}

// a chunk is exactly a well-formed (proposal header, chunk id, merkle
// proof, symbol). todo: enforce proof validity at parse time, so a
// Chunk always holds a valid merkle proof.
#[derive(Clone)]
pub struct Chunk {
    header: ProposalHeader,
    chunk_id: WireChunkId,
    data: ChunkData,
}

impl Chunk {
    pub(crate) fn new(header: ProposalHeader, chunk_id: WireChunkId, data: ChunkData) -> Self {
        Self {
            header,
            chunk_id,
            data,
        }
    }

    pub fn proposal_header(&self) -> &ProposalHeader {
        &self.header
    }

    pub fn chunk_id(&self) -> WireChunkId {
        self.chunk_id
    }

    pub fn proof(&self) -> &[MerkleHash] {
        &self.data.proof
    }

    pub fn symbol(&self) -> &Bytes {
        &self.data.symbol
    }

    pub(crate) fn into_parts(self) -> (ProposalHeader, WireChunkId, ChunkData) {
        (self.header, self.chunk_id, self.data)
    }
}

// a partial view of one proposal: its header plus any subset of its
// chunks. the unit of both dissemination and ingestion.
#[derive(Clone)]
pub struct ProposalEnvelope {
    header: ProposalHeader,
    chunks: BTreeMap<WireChunkId, ChunkData>,
}

impl ProposalEnvelope {
    pub(crate) fn new(header: ProposalHeader, chunks: BTreeMap<WireChunkId, ChunkData>) -> Self {
        Self { header, chunks }
    }

    pub fn from_chunk(chunk: Chunk) -> Self {
        let (header, chunk_id, data) = chunk.into_parts();
        let mut chunks = BTreeMap::new();
        chunks.insert(chunk_id, data);
        Self::new(header, chunks)
    }

    pub fn from_header(header: ProposalHeader) -> Self {
        Self::new(header, BTreeMap::new())
    }

    // group chunks by header. each header appears once.
    pub fn group(
        chunks: impl IntoIterator<Item = Chunk>,
    ) -> impl Iterator<Item = (ProposalHeader, BTreeMap<WireChunkId, ChunkData>)> {
        let mut groups: HashMap<ProposalHeader, BTreeMap<WireChunkId, ChunkData>> = HashMap::new();
        for chunk in chunks {
            let (header, chunk_id, data) = chunk.into_parts();
            groups.entry(header).or_default().insert(chunk_id, data);
        }
        groups.into_iter()
    }

    pub fn header(&self) -> &ProposalHeader {
        &self.header
    }

    pub fn chunks(&self) -> &BTreeMap<WireChunkId, ChunkData> {
        &self.chunks
    }

    pub(crate) fn insert(&mut self, chunk_id: WireChunkId, data: ChunkData) {
        self.chunks.insert(chunk_id, data);
    }

    pub(crate) fn into_parts(self) -> (ProposalHeader, BTreeMap<WireChunkId, ChunkData>) {
        (self.header, self.chunks)
    }
}
