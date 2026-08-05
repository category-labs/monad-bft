use std::collections::BTreeMap;

use bytes::Bytes;

use super::{assignment::ChunkId, chunk::Chunk, types::MerkleHash};

struct ChunkData {
    symbol: Bytes,
    proof: Box<[MerkleHash]>,
}

type MerkleProof = [MerkleHash];

pub(crate) struct ChunkStore {
    num_chunks: usize,
    // todo: use a merkle tree data structure to keep track of merkle
    // proof, which structurally maintains invariant merkle proof
    // validity.
    chunks: BTreeMap<ChunkId, ChunkData>,
}

impl ChunkStore {
    pub(crate) fn new(num_chunks: usize) -> Self {
        Self {
            num_chunks,
            chunks: BTreeMap::new(),
        }
    }

    pub(crate) fn insert(&mut self, chunk: &impl Chunk) {
        let chunk_id = chunk.chunk_id();
        let proof = chunk.proof();
        let symbol = chunk.symbol().clone();
        self.insert_raw(chunk_id, proof, symbol)
    }

    pub(crate) fn insert_raw(&mut self, chunk_id: ChunkId, proof: &MerkleProof, symbol: Bytes) {
        assert!(usize::from(chunk_id) < self.num_chunks);
        if self.chunks.contains_key(&chunk_id) {
            return;
        }

        let chunk_data = ChunkData {
            symbol,
            proof: proof.into(),
        };
        self.chunks.insert(chunk_id, chunk_data);
    }

    pub(crate) fn contains(&self, chunk_id: ChunkId) -> bool {
        assert!(usize::from(chunk_id) < self.num_chunks);
        self.chunks.contains_key(&chunk_id)
    }

    pub(crate) fn len(&self) -> usize {
        self.chunks.len()
    }

    pub(crate) fn symbols(&self) -> impl Iterator<Item = &Bytes> {
        self.chunks.values().map(|chunk_data| &chunk_data.symbol)
    }

    pub(crate) fn get_symbol(&self, chunk_id: ChunkId) -> Option<&Bytes> {
        assert!(usize::from(chunk_id) < self.num_chunks);
        let chunk_data = self.chunks.get(&chunk_id)?;
        Some(&chunk_data.symbol)
    }

    pub(crate) fn get_proof(&self, chunk_id: ChunkId) -> Option<&MerkleProof> {
        assert!(usize::from(chunk_id) < self.num_chunks);
        let chunk_data = self.chunks.get(&chunk_id)?;
        Some(&chunk_data.proof)
    }
}
