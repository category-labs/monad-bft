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

use std::{
    borrow::Cow,
    collections::{BTreeMap, BTreeSet, HashMap},
};

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

// the chunk-specific half of a chunk
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ChunkData {
    pub(crate) symbol: Bytes,
    pub(crate) proof: Box<[MerkleHash]>,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Chunk<'a> {
    header: Cow<'a, ProposalHeader>,
    chunk_id: WireChunkId,
    data: Cow<'a, ChunkData>,
}

impl Chunk<'static> {
    pub fn new(header: ProposalHeader, chunk_id: WireChunkId, data: ChunkData) -> Self {
        Self {
            header: Cow::Owned(header),
            chunk_id,
            data: Cow::Owned(data),
        }
    }
}

impl<'a> Chunk<'a> {
    fn view(header: &'a ProposalHeader, chunk_id: WireChunkId, data: &'a ChunkData) -> Self {
        Self {
            header: Cow::Borrowed(header),
            chunk_id,
            data: Cow::Borrowed(data),
        }
    }

    pub fn header(&self) -> &ProposalHeader {
        &self.header
    }

    pub fn chunk_id(&self) -> WireChunkId {
        self.chunk_id
    }

    pub fn data(&self) -> &ChunkData {
        &self.data
    }

    pub fn into_parts(self) -> (ProposalHeader, WireChunkId, ChunkData) {
        (
            self.header.into_owned(),
            self.chunk_id,
            self.data.into_owned(),
        )
    }
}

// a partial view of one proposal: its header plus any subset of its
// chunks. the unit of both dissemination and ingestion.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ProposalEnvelope {
    header: ProposalHeader,
    chunks: BTreeMap<WireChunkId, ChunkData>,
}

impl ProposalEnvelope {
    pub(crate) fn new(header: ProposalHeader, chunks: BTreeMap<WireChunkId, ChunkData>) -> Self {
        Self { header, chunks }
    }

    pub fn from_chunk(chunk: Chunk<'_>) -> Self {
        let (header, chunk_id, data) = chunk.into_parts();
        Self::new(header, BTreeMap::from([(chunk_id, data)]))
    }

    pub fn from_header(header: ProposalHeader) -> Self {
        Self::new(header, BTreeMap::new())
    }

    // group chunks by header. each header appears once.
    pub fn group<'a>(chunks: impl IntoIterator<Item = Chunk<'a>>) -> impl Iterator<Item = Self> {
        let mut groups: HashMap<ProposalHeader, BTreeMap<WireChunkId, ChunkData>> = HashMap::new();
        for chunk in chunks {
            let (header, chunk_id, data) = chunk.into_parts();
            groups.entry(header).or_default().insert(chunk_id, data);
        }

        let mut envelopes = Vec::with_capacity(groups.len());
        for (header, chunks) in groups {
            envelopes.push(Self::new(header, chunks));
        }
        envelopes.into_iter()
    }

    pub fn header(&self) -> &ProposalHeader {
        &self.header
    }

    pub fn chunks(&self) -> impl Iterator<Item = Chunk<'_>> {
        self.chunks
            .iter()
            .map(|(chunk_id, data)| Chunk::view(&self.header, *chunk_id, data))
    }

    pub fn chunk_data(&self) -> &BTreeMap<WireChunkId, ChunkData> {
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
        super::test_util::{epoch_handle, group, proposal_chunks},
        *,
    };

    #[test]
    fn grouping_collects_each_proposal_once() {
        let epoch_handle = epoch_handle();
        let (header_a, a) = proposal_chunks(&epoch_handle, 1);
        let (header_b, b) = proposal_chunks(&epoch_handle, 2);

        let mixed = [a[0].clone(), b[0].clone(), a[1].clone()];
        let mut envelopes: Vec<_> = ProposalEnvelope::group(mixed).collect();
        envelopes.sort_by_key(|envelope| envelope.chunk_data().len());
        assert_eq!(envelopes.len(), 2);
        assert_eq!(envelopes[0].header(), &header_b);
        assert_eq!(envelopes[0].chunk_data().len(), 1);
        assert_eq!(envelopes[1].header(), &header_a);
        assert_eq!(envelopes[1].chunk_data().len(), 2);
    }

    #[test]
    fn an_envelope_views_its_chunks_under_one_header() {
        let epoch_handle = epoch_handle();
        let (_, chunks) = proposal_chunks(&epoch_handle, 1);
        let envelope = group(&chunks[1..4]);

        let views: Vec<_> = envelope.chunks().collect();
        assert_eq!(views.len(), 3);
        for (view, owned) in views.iter().zip(&chunks[1..4]) {
            assert!(std::ptr::eq(view.header(), envelope.header()));
            assert_eq!(view, owned);
        }

        let regrouped: Vec<_> = ProposalEnvelope::group(views).collect();
        assert_eq!(regrouped, vec![envelope.clone()]);
        assert_eq!(
            ProposalEnvelope::from_chunk(chunks[2].clone()),
            group(&chunks[2..3])
        );
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
