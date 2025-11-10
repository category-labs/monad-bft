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

use monad_crypto::hasher::{Hash, Hasher, HasherType};
use serde::{Deserialize, Serialize};

pub type MerkleHash = [u8; 20];

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MerkleProof {
    // index into the canonical merkle tree of the leaf that this proof is over
    tree_leaf_idx: u16,
    // the actual "proof" section - includes siblings going up the tree
    siblings: Vec<MerkleHash>,
}

impl MerkleProof {
    pub fn new_from_leaf_idx(siblings: Vec<MerkleHash>, leaf_idx: u8) -> Option<Self> {
        let num_leaves = 2_u16.checked_pow(siblings.len() as u32)?;
        let tree_len = 2_u16.checked_mul(num_leaves)?.checked_sub(1)?;
        let tree_leaf_start_idx = tree_len - num_leaves;
        let leaf_idx = u16::from(leaf_idx);
        if leaf_idx >= num_leaves {
            return None;
        }

        Some(Self {
            tree_leaf_idx: tree_leaf_start_idx + leaf_idx,
            siblings,
        })
    }

    pub fn compute_root(&self, leaf: &Hash) -> Option<MerkleHash> {
        let mut merkle_hash = hash_to_merkle(leaf);
        let mut current_idx = Some(self.tree_leaf_idx as usize);

        for sibling in self.siblings.iter().rev() {
            let mut h = HasherType::new();
            if current_idx? % 2 == 1 {
                h.update(merkle_hash);
                h.update(sibling);
            } else {
                h.update(sibling);
                h.update(merkle_hash);
            }
            merkle_hash = hash_to_merkle(&h.hash());
            current_idx = parent_idx(current_idx?);
        }

        Some(merkle_hash)
    }

    /// Returns siblings from top to bottom
    pub fn siblings(&self) -> &[MerkleHash] {
        &self.siblings
    }
}

pub struct MerkleTree {
    tree_leaf_start_idx: u16,
    tree: Vec<MerkleHash>,
}

impl MerkleTree {
    pub fn new_with_depth(leaves: &[Hash], depth: u8) -> Self {
        let num_leaves = 2_usize.pow(depth.checked_sub(1).unwrap().into());
        let tree_len = 2 * num_leaves - 1;
        assert!(tree_len <= u16::MAX.into()); // make sure all tree idx fit in u16

        // root is at tree[0]
        // children of tree[i] are at tree[2i+1], tree[2i+2]
        // first leaf is at tree_len - num_leaves
        let mut tree = vec![MerkleHash::default(); tree_len];
        let tree_leaf_start_idx = tree_len - num_leaves;
        for (leaf, raw_leaf) in tree[tree_leaf_start_idx..].iter_mut().zip(leaves.iter()) {
            *leaf = hash_to_merkle(raw_leaf);
        }

        for idx in (0..tree_leaf_start_idx).rev() {
            let mut h = HasherType::new();
            h.update(tree[2 * idx + 1]);
            h.update(tree[2 * idx + 2]);
            tree[idx] = hash_to_merkle(&h.hash());
        }

        Self {
            tree_leaf_start_idx: tree_leaf_start_idx
                .try_into()
                .expect("asserted tree_len already"),
            tree,
        }
    }

    pub fn new(leaves: &[Hash]) -> Self {
        let depth: u8 = (leaves.len().next_power_of_two().ilog2() + 1)
            .try_into()
            .expect("too many leaves");
        Self::new_with_depth(leaves, depth)
    }

    pub fn root(&self) -> &MerkleHash {
        &self.tree[0]
    }

    pub fn proof(&self, leaf_idx: u8) -> MerkleProof {
        let tree_leaf_idx: u16 = self.tree_leaf_start_idx + u16::from(leaf_idx);
        let siblings = {
            // merkle proof consists of siblings going up the tree
            let mut siblings = Vec::new();
            let mut current_idx = tree_leaf_idx.into();
            while let Some(sibling_idx) = sibling_idx(current_idx) {
                current_idx = parent_idx(current_idx).expect("if has sibling, must have parent");
                siblings.push(self.tree[sibling_idx]);
            }
            siblings.reverse();
            siblings
        };

        MerkleProof {
            tree_leaf_idx,
            siblings,
        }
    }
}

fn hash_to_merkle(hash: &Hash) -> MerkleHash {
    hash.0[..std::mem::size_of::<MerkleHash>()]
        .try_into()
        .unwrap()
}

fn parent_idx(idx: usize) -> Option<usize> {
    Some(idx.checked_sub(1)? / 2)
}

fn sibling_idx(idx: usize) -> Option<usize> {
    let parent_idx = parent_idx(idx)?;
    Some(2 * parent_idx + (1 + idx % 2))
}

#[cfg(test)]
mod tests {
    use monad_crypto::hasher::{Hasher, HasherType};

    use crate::{MerkleProof, MerkleTree};

    #[test]
    fn test_proof() {
        for num_leaves in 1..=32 {
            let leaves = (0_u8..num_leaves)
                .map(|i| {
                    let mut h = HasherType::new();
                    h.update([i]);
                    h.hash()
                })
                .collect::<Vec<_>>();
            let tree = MerkleTree::new(&leaves);

            let fake_leaf = {
                let mut h = HasherType::new();
                h.update([1, 2, 3]);
                h.hash()
            };
            for (leaf_idx, leaf) in leaves.iter().enumerate() {
                let leaf_idx = leaf_idx as u8;
                let proof = tree.proof(leaf_idx);
                let computed_root = proof.compute_root(leaf).unwrap();
                // test that computed root matches
                assert_eq!(tree.root(), &computed_root);

                // test that forged leaf does not yield matching root
                let computed_root_fake = proof.compute_root(&fake_leaf).unwrap();
                assert_ne!(tree.root(), &computed_root_fake);

                let siblings = proof.siblings();
                // test proof reconstruction
                let reconstructed_proof =
                    MerkleProof::new_from_leaf_idx(siblings.into(), leaf_idx).unwrap();
                assert_eq!(
                    &reconstructed_proof.compute_root(leaf).unwrap(),
                    &computed_root
                );

                // test proof reconstruction with overflowed leaf_idx
                let max_leaf_idx = num_leaves.next_power_of_two();
                assert!(MerkleProof::new_from_leaf_idx(siblings.into(), max_leaf_idx).is_none());
                assert!(
                    MerkleProof::new_from_leaf_idx(siblings.into(), max_leaf_idx - 1).is_some()
                );

                // test proof reconstruction with incorrect leaf_idx
                let bad_leaf_idx = if num_leaves == 1 {
                    continue;
                } else {
                    // shuffle leaf idx
                    leaf_idx.wrapping_add(1) % num_leaves
                };
                let bad_reconstructed_proof =
                    MerkleProof::new_from_leaf_idx(siblings.into(), bad_leaf_idx).unwrap();
                assert_ne!(
                    &bad_reconstructed_proof.compute_root(leaf).unwrap(),
                    &computed_root
                );
            }
        }
    }
}
