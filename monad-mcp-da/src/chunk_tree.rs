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

use std::collections::BTreeMap;

use bytes::Bytes;
use monad_merkle::{MerkleProof, MerkleTree};

use super::{
    assignment::ChunkId,
    chunk::{ChunkData, WireChunkId},
    layout::PacketLayout,
    types::{MerkleHash, MerkleRoot},
};

// the chunks of one proposal under its merkle root. Partial while
// chunks arrive with their wire proofs. Complete once every symbol is
// known, when any chunk is derivable.
pub(crate) enum ChunkTree {
    Partial {
        root: MerkleRoot,
        chunks: BTreeMap<ChunkId, ChunkData>,
    },
    Complete {
        root: MerkleRoot,
        // in chunk id order
        symbols: Vec<Bytes>,
        tree: MerkleTree,
    },
}

impl ChunkTree {
    pub(crate) fn partial(root: MerkleRoot) -> Self {
        Self::Partial {
            root,
            chunks: BTreeMap::new(),
        }
    }

    // commit to the symbols in chunk id order. None if they do not fit
    // the layout's tree depth.
    pub(crate) fn complete(layout: &PacketLayout, symbols: Vec<Bytes>) -> Option<Self> {
        let depth = layout.merkle_tree_depth();
        if symbols.is_empty() || depth == 0 || depth > MerkleTree::MAX_DEPTH {
            return None;
        }
        if symbols.len() > 1usize << (depth - 1) {
            return None;
        }

        let mut leaves = Vec::with_capacity(symbols.len());
        for (leaf_idx, symbol) in symbols.iter().enumerate() {
            let hash = layout.merkle_leaf_hash(leaf_idx as WireChunkId, symbol);
            leaves.push(hash);
        }
        let tree = MerkleTree::new_with_depth(&leaves, depth);
        let root = MerkleRoot(MerkleHash(*tree.root()));

        Some(Self::Complete {
            root,
            symbols,
            tree,
        })
    }

    pub(crate) fn root(&self) -> MerkleRoot {
        match self {
            Self::Partial { root, .. } | Self::Complete { root, .. } => *root,
        }
    }

    // whether the chunk's proof binds it to our root
    pub(crate) fn verify(
        &self,
        layout: &PacketLayout,
        chunk_id: ChunkId,
        data: &ChunkData,
    ) -> bool {
        let leaf_idx = chunk_id.to_wire();
        let leaf = layout.merkle_leaf_hash(leaf_idx, &data.symbol);
        let siblings = data.proof.iter().map(|hash| hash.0).collect();
        let Some(proof) = MerkleProof::new_from_leaf_idx(siblings, leaf_idx) else {
            return false;
        };
        let Some(computed) = proof.compute_root(&leaf) else {
            return false;
        };
        MerkleRoot(MerkleHash(computed)) == self.root()
    }

    // record a received chunk. nothing to record once complete.
    pub(crate) fn insert(&mut self, chunk_id: ChunkId, data: ChunkData) {
        let Self::Partial { chunks, .. } = self else {
            return;
        };
        chunks.entry(chunk_id).or_insert(data);
    }

    pub(crate) fn contains(&self, chunk_id: ChunkId) -> bool {
        match self {
            Self::Partial { chunks, .. } => chunks.contains_key(&chunk_id),
            Self::Complete { .. } => true,
        }
    }

    // None if the chunk has not arrived
    pub(crate) fn chunk_data(&self, chunk_id: ChunkId) -> Option<ChunkData> {
        match self {
            Self::Partial { chunks, .. } => chunks.get(&chunk_id).cloned(),
            Self::Complete { symbols, tree, .. } => {
                let symbol = symbols[usize::from(chunk_id)].clone();
                let proof = tree.proof(chunk_id.to_wire());
                let siblings = proof.siblings().iter().map(|hash| MerkleHash(*hash));
                Some(ChunkData {
                    symbol,
                    proof: siblings.collect(),
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn symbols(n: usize) -> Vec<Bytes> {
        (0..n).map(|i| Bytes::from(vec![i as u8; 8])).collect()
    }

    fn id(wire: WireChunkId) -> ChunkId {
        ChunkId::unchecked(wire)
    }

    // depth 3: four leaves
    fn layout() -> PacketLayout {
        PacketLayout::new(32, 3)
    }

    #[test]
    fn derived_chunks_verify_under_the_root() {
        let complete = ChunkTree::complete(&layout(), symbols(4)).expect("fits depth 3");
        let partial = ChunkTree::partial(complete.root());

        for wire in 0..4 {
            let data = complete.chunk_data(id(wire)).expect("derivable");
            assert!(partial.verify(&layout(), id(wire), &data));
            assert!(complete.contains(id(wire)));
        }
    }

    #[test]
    fn verification_binds_symbol_index_and_root() {
        let complete = ChunkTree::complete(&layout(), symbols(4)).unwrap();
        let data = complete.chunk_data(id(0)).unwrap();
        let tree = ChunkTree::partial(complete.root());

        // wrong index
        assert!(!tree.verify(&layout(), id(1), &data));

        // tampered symbol
        let tampered = ChunkData {
            symbol: Bytes::from_static(b"tampered"),
            proof: data.proof.clone(),
        };
        assert!(!tree.verify(&layout(), id(0), &tampered));

        // another proposal's root
        let other = ChunkTree::complete(&layout(), symbols(3)).unwrap();
        assert!(!ChunkTree::partial(other.root()).verify(&layout(), id(0), &data));
    }

    #[test]
    fn completion_needs_symbols_that_fit_the_depth() {
        assert!(ChunkTree::complete(&layout(), vec![]).is_none());
        assert!(ChunkTree::complete(&layout(), symbols(5)).is_none());
        assert!(ChunkTree::complete(&PacketLayout::new(32, 0), symbols(1)).is_none());
        let too_deep = PacketLayout::new(32, MerkleTree::MAX_DEPTH + 1);
        assert!(ChunkTree::complete(&too_deep, symbols(1)).is_none());
        assert!(ChunkTree::complete(&layout(), symbols(4)).is_some());
    }

    #[test]
    fn a_partial_tree_records_chunks_and_a_complete_one_ignores_them() {
        let mut complete = ChunkTree::complete(&layout(), symbols(4)).unwrap();
        let data = complete.chunk_data(id(0)).unwrap();

        let mut partial = ChunkTree::partial(complete.root());
        assert!(!partial.contains(id(0)));
        assert!(partial.chunk_data(id(0)).is_none());
        partial.insert(id(0), data.clone());
        assert!(partial.contains(id(0)));
        assert!(partial.chunk_data(id(0)).is_some());
        assert!(!partial.contains(id(1)));

        complete.insert(id(0), data);
        assert!(complete.contains(id(3)));
    }
}
