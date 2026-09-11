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

use super::{assignment::ChunkId, chunk::ChunkData, types::MerkleRoot, util::Tree};

// the chunks of one proposal under its merkle root. Partial while
// chunks arrive with their wire proofs. Complete once every symbol is
// known, when any chunk is derivable.
pub(crate) enum ChunkTree {
    Partial {
        root: MerkleRoot,
        chunks: BTreeMap<ChunkId, ChunkData>,
    },
    Complete {
        // in chunk id order
        symbols: Vec<Bytes>,
        tree: Tree,
    },
}

impl ChunkTree {
    pub(crate) fn partial(root: MerkleRoot) -> Self {
        Self::Partial {
            root,
            chunks: BTreeMap::new(),
        }
    }

    // the caller must ensure the tree is over the symbols' leaves in
    // chunk id order
    pub(crate) fn complete(symbols: Vec<Bytes>, tree: Tree) -> Self {
        Self::Complete { symbols, tree }
    }

    pub(crate) fn root(&self) -> MerkleRoot {
        match self {
            Self::Partial { root, .. } => *root,
            Self::Complete { tree, .. } => tree.root(),
        }
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
    use monad_crypto::hasher::{Hash, Hasher as _, HasherType};

    use super::{super::chunk::WireChunkId, *};

    // four leaves
    const DEPTH: u8 = 3;

    fn leaf(leaf_idx: usize, symbol: &[u8]) -> Hash {
        let mut hasher = HasherType::new();
        hasher.update([leaf_idx as u8]);
        hasher.update(symbol);
        hasher.hash()
    }

    fn complete(n: usize) -> ChunkTree {
        let mut symbols = Vec::with_capacity(n);
        for i in 0..n {
            symbols.push(Bytes::from(vec![i as u8; 8]));
        }
        let mut leaves = Vec::with_capacity(n);
        for (leaf_idx, symbol) in symbols.iter().enumerate() {
            leaves.push(leaf(leaf_idx, symbol));
        }
        ChunkTree::complete(symbols, Tree::from_leaves(DEPTH, leaves))
    }

    fn id(wire: WireChunkId) -> ChunkId {
        ChunkId::unchecked(wire)
    }

    #[test]
    fn a_complete_tree_derives_every_chunk_with_its_proof() {
        let complete = complete(4);
        for wire in 0..4 {
            let data = complete.chunk_data(id(wire)).expect("derivable");
            assert_eq!(data.symbol, Bytes::from(vec![wire as u8; 8]));
            let leaf = leaf(wire as usize, &data.symbol);
            assert!(Tree::verify_proof(
                &complete.root(),
                wire,
                &leaf,
                &data.proof
            ));
            assert!(complete.contains(id(wire)));
        }
    }

    #[test]
    fn a_partial_tree_records_chunks_and_a_complete_one_ignores_them() {
        let mut complete = complete(4);
        let data = complete.chunk_data(id(0)).unwrap();

        let mut partial = ChunkTree::partial(complete.root());
        assert_eq!(partial.root(), complete.root());
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
