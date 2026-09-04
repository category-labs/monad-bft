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

use std::collections::{BTreeMap, BTreeSet, HashMap};

use bytes::Bytes;

use super::{
    assignment::ChunkId,
    types::{ChunkRequestType, MerkleHash, ProposalHeader},
};

// a chunk id as carried on the wire, not yet checked against an
// assignment.
pub type WireChunkId = u16;

// the chunks of the type (your-chunks/my-chunks)
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum ChunksSubset {
    All,
    // non-empty. todo: allow empty to request the header alone.
    Narrowed(BTreeSet<WireChunkId>),
}

impl ChunksSubset {
    // the caller must ensure chunk_ids is not empty.
    pub fn narrowed(chunk_ids: impl IntoIterator<Item = ChunkId>) -> Self {
        let chunk_ids: BTreeSet<_> = chunk_ids.into_iter().map(ChunkId::to_wire).collect();
        assert!(!chunk_ids.is_empty());
        Self::Narrowed(chunk_ids)
    }

    pub fn restrict<'a>(
        &'a self,
        chunk_ids: impl IntoIterator<Item = ChunkId> + 'a,
    ) -> impl Iterator<Item = ChunkId> + 'a {
        chunk_ids.into_iter().filter(move |id| match self {
            Self::All => true,
            Self::Narrowed(named) => named.contains(&id.to_wire()),
        })
    }
}

// the chunks of one owner, the requester's or the peer's
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ChunkRequest {
    pub(crate) kind: ChunkRequestType,
    pub(crate) subset: ChunksSubset,
}

impl ChunkRequest {
    // the full set of chunks of the kind
    pub(crate) fn all(kind: ChunkRequestType) -> Self {
        Self {
            kind,
            subset: ChunksSubset::All,
        }
    }
}

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

#[cfg(test)]
mod tests {
    use super::{
        super::test_util::{epoch_handle, proposal_chunks},
        *,
    };

    #[test]
    fn grouping_collects_each_proposal_once() {
        let epoch_handle = epoch_handle();
        let (header_a, a) = proposal_chunks(&epoch_handle, 1);
        let (header_b, b) = proposal_chunks(&epoch_handle, 2);

        let mixed = [a[0].clone(), b[0].clone(), a[1].clone()];
        let groups: HashMap<_, _> = ProposalEnvelope::group(mixed).collect();
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[&header_a].len(), 2);
        assert_eq!(groups[&header_b].len(), 1);
    }

    #[test]
    fn restrict_keeps_the_named_ids() {
        let ids = [
            ChunkId::unchecked(1),
            ChunkId::unchecked(2),
            ChunkId::unchecked(3),
        ];

        let all: Vec<_> = ChunksSubset::All.restrict(ids).collect();
        assert_eq!(all, ids);

        let narrowed = ChunksSubset::narrowed([ChunkId::unchecked(2), ChunkId::unchecked(9)]);
        let kept: Vec<_> = narrowed.restrict(ids).collect();
        assert_eq!(kept, [ChunkId::unchecked(2)]);
    }
}
