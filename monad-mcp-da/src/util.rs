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

use std::collections::BTreeSet;

use monad_crypto::hasher::Hash;
use monad_merkle::{MerkleProof, MerkleTree};

use super::types::{MerkleHash, MerkleRoot, Slot};

// todo: share with conductor's CompletionTracker
pub struct SlotCompletion {
    cap: Slot,
    completed_slots: BTreeSet<Slot>,
}

impl SlotCompletion {
    pub fn new() -> Self {
        Self {
            cap: Slot::MIN,
            completed_slots: BTreeSet::new(),
        }
    }

    pub fn mark_completed(&mut self, slot: Slot) {
        if slot < self.cap {
            return;
        }

        self.completed_slots.insert(slot);

        while self.completed_slots.contains(&self.cap) {
            self.completed_slots.remove(&self.cap);
            self.cap = self.cap.checked_next().expect("slot cap overflow");
        }
    }

    pub fn cap(&self) -> Slot {
        self.cap
    }
}

// a wrapper of MerkleTree type adapted for interfacing with da types.
pub struct Tree(MerkleTree);

impl Tree {
    pub const MAX_DEPTH: u8 = MerkleTree::MAX_DEPTH;

    // the caller must ensure the depth is in range and the leaves fit it
    pub fn from_leaves(depth: u8, leaves: impl IntoIterator<Item = Hash>) -> Self {
        let leaves: Vec<Hash> = leaves.into_iter().collect();
        Self(MerkleTree::new_with_depth(&leaves, depth))
    }

    pub fn root(&self) -> MerkleRoot {
        MerkleRoot(MerkleHash(*self.0.root()))
    }

    // the caller must ensure the leaf exists
    pub fn proof(&self, leaf_idx: u16) -> Box<[MerkleHash]> {
        let proof = self.0.proof(leaf_idx);
        let mut siblings = Vec::with_capacity(proof.siblings().len());
        for hash in proof.siblings() {
            siblings.push(MerkleHash(*hash));
        }
        siblings.into_boxed_slice()
    }

    pub fn verify_proof(
        root: &MerkleRoot,
        leaf_idx: u16,
        leaf: &Hash,
        proof: &[MerkleHash],
    ) -> bool {
        let mut siblings = Vec::with_capacity(proof.len());
        for hash in proof {
            siblings.push(hash.0);
        }
        let Some(proof) = MerkleProof::new_from_leaf_idx(siblings, leaf_idx) else {
            return false;
        };
        let Some(computed) = proof.compute_root(leaf) else {
            return false;
        };
        MerkleRoot(MerkleHash(computed)) == *root
    }
}

#[cfg(test)]
mod tests {
    use monad_crypto::hasher::{Hasher as _, HasherType};

    use super::*;

    fn leaf(byte: u8) -> Hash {
        let mut hasher = HasherType::new();
        hasher.update([byte]);
        hasher.hash()
    }

    #[test]
    fn proofs_bind_the_leaf_its_index_and_the_root() {
        let leaves = [leaf(0), leaf(1), leaf(2)];
        let tree = Tree::from_leaves(3, leaves);
        for (idx, leaf) in leaves.iter().enumerate() {
            let idx = idx as u16;
            let proof = tree.proof(idx);
            assert_eq!(proof.len(), 2);
            assert!(Tree::verify_proof(&tree.root(), idx, leaf, &proof));
            assert!(!Tree::verify_proof(
                &tree.root(),
                (idx + 1) % 3,
                leaf,
                &proof
            ));
        }

        let other = Tree::from_leaves(3, [leaf(0), leaf(1)]);
        assert!(!Tree::verify_proof(
            &other.root(),
            0,
            &leaves[0],
            &tree.proof(0)
        ));
        assert!(!Tree::verify_proof(
            &tree.root(),
            0,
            &leaf(9),
            &tree.proof(0)
        ));
    }

    #[test]
    fn the_cap_advances_through_contiguous_completions_only() {
        let mut completion = SlotCompletion::new();

        completion.mark_completed(Slot(2));
        assert_eq!(completion.cap(), Slot(0));
        completion.mark_completed(Slot(0));
        assert_eq!(completion.cap(), Slot(1));
        completion.mark_completed(Slot(1));
        assert_eq!(completion.cap(), Slot(3));

        // below the cap is already accounted for
        completion.mark_completed(Slot(0));
        assert_eq!(completion.cap(), Slot(3));
    }
}
