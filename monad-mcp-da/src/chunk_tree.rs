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

use super::{
    assignment::ChunkId,
    chunk::{ChunkData, WireChunkId},
    types::MerkleRoot,
    wire,
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
        tree: wire::Tree,
    },
}

impl ChunkTree {
    pub(crate) fn partial(root: MerkleRoot) -> Self {
        Self::Partial {
            root,
            chunks: BTreeMap::new(),
        }
    }

    // commit to wire-sized symbols in chunk id order. None if they do
    // not fit a tree of the depth.
    pub(crate) fn complete(depth: u8, symbols: Vec<Bytes>) -> Option<Self> {
        if symbols.is_empty() || !(wire::MIN_DEPTH..=wire::MAX_DEPTH).contains(&depth) {
            return None;
        }
        if symbols.len() > 1usize << (depth - 1) {
            return None;
        }

        let symbol_len = wire::symbol_len(depth);
        let mut leaves = Vec::with_capacity(symbols.len());
        for (leaf_idx, symbol) in symbols.iter().enumerate() {
            if symbol.len() != symbol_len {
                return None;
            }
            leaves.push(wire::leaf_hash(leaf_idx as WireChunkId, symbol));
        }
        let tree = wire::Tree::new(depth, &leaves);

        Some(Self::Complete {
            root: tree.root(),
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
    pub(crate) fn verify(&self, chunk_id: ChunkId, data: &ChunkData) -> bool {
        let leaf_idx = chunk_id.to_wire();
        let leaf = wire::leaf_hash(leaf_idx, &data.symbol);
        wire::verify_proof(&self.root(), leaf_idx, &leaf, &data.proof)
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
            Self::Complete { symbols, tree, .. } => Some(ChunkData {
                symbol: symbols[usize::from(chunk_id)].clone(),
                proof: tree.proof(chunk_id.to_wire()),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // four leaves
    const DEPTH: u8 = 3;

    fn symbols(n: usize) -> Vec<Bytes> {
        let mut symbols = Vec::with_capacity(n);
        for i in 0..n {
            symbols.push(Bytes::from(vec![i as u8; wire::symbol_len(DEPTH)]));
        }
        symbols
    }

    fn id(wire: WireChunkId) -> ChunkId {
        ChunkId::unchecked(wire)
    }

    #[test]
    fn derived_chunks_verify_under_the_root() {
        let complete = ChunkTree::complete(DEPTH, symbols(4)).expect("fits depth 3");
        let partial = ChunkTree::partial(complete.root());

        for wire in 0..4 {
            let data = complete.chunk_data(id(wire)).expect("derivable");
            assert!(partial.verify(id(wire), &data));
            assert!(complete.contains(id(wire)));
        }
    }

    #[test]
    fn verification_binds_symbol_index_and_root() {
        let complete = ChunkTree::complete(DEPTH, symbols(4)).unwrap();
        let data = complete.chunk_data(id(0)).unwrap();
        let tree = ChunkTree::partial(complete.root());

        // wrong index
        assert!(!tree.verify(id(1), &data));

        // tampered symbol
        let tampered = ChunkData {
            symbol: Bytes::from_static(b"tampered"),
            proof: data.proof.clone(),
        };
        assert!(!tree.verify(id(0), &tampered));

        // another proposal's root
        let other = ChunkTree::complete(DEPTH, symbols(3)).unwrap();
        assert!(!ChunkTree::partial(other.root()).verify(id(0), &data));
    }

    #[test]
    fn completion_needs_wire_sized_symbols_that_fit_the_depth() {
        assert!(ChunkTree::complete(DEPTH, vec![]).is_none());
        assert!(ChunkTree::complete(DEPTH, symbols(5)).is_none());
        assert!(ChunkTree::complete(wire::MIN_DEPTH - 1, symbols(1)).is_none());
        assert!(ChunkTree::complete(wire::MAX_DEPTH + 1, symbols(1)).is_none());
        // sized for depth 3, not 4
        assert!(ChunkTree::complete(DEPTH + 1, symbols(4)).is_none());
        assert!(ChunkTree::complete(DEPTH, symbols(4)).is_some());
    }

    #[test]
    fn a_partial_tree_records_chunks_and_a_complete_one_ignores_them() {
        let mut complete = ChunkTree::complete(DEPTH, symbols(4)).unwrap();
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
