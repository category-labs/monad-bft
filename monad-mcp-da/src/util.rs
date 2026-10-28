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

use monad_crypto::hasher::Hash;
use monad_merkle::{MerkleProof, MerkleTree};

use super::types::{MerkleHash, MerkleRoot, NodeId, ProposalId};

// shuffle a sender's messages about one proposal into a pseudo-random order:
// uniform over proposals and senders, so no node is systematically served or
// asked first, and a function of (pid, sender) alone, so it is the same in
// every run, process and release.
//
// todo: replace with the send strategy once sends are prioritized.
pub(crate) fn shuffle_sends<T>(items: &mut [T], pid: &ProposalId, sender: &NodeId) {
    use monad_crypto::hasher::{Hasher as _, HasherType};
    use rand::{SeedableRng as _, seq::SliceRandom as _};
    use rand_chacha::ChaCha20Rng;

    let mut hasher = HasherType::new();
    hasher.update(b"monad-mcp-da send order");
    hasher.update(alloy_rlp::encode(pid));
    hasher.update(alloy_rlp::encode(sender));
    items.shuffle(&mut ChaCha20Rng::from_seed(hasher.hash().0));
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
    fn a_send_order_is_a_permutation_fixed_by_proposal_and_sender() {
        use super::super::test_util::{epoch_handle, proposal_chunks};

        let pid = |payload| -> ProposalId {
            use monad_mcp_chorus::spec::ProposalHeader as _;
            proposal_chunks(&epoch_handle(), payload).0.pid()
        };
        let order = |pid: &ProposalId, sender: u64| {
            let mut nodes: Vec<u64> = (0..16).collect();
            shuffle_sends(&mut nodes, pid, &NodeId::dummy(sender));
            nodes
        };

        let (a, b) = (pid(1), pid(2));
        let mut sorted = order(&a, 0);
        sorted.sort();
        assert_eq!(sorted, (0..16).collect::<Vec<_>>());
        assert_eq!(order(&a, 0), order(&a, 0));
        assert_ne!(order(&a, 0), order(&a, 1));
        assert_ne!(order(&a, 0), order(&b, 0));
        // pinned: blake3, rlp and ChaCha20 are stable, so the order is the
        // same across processes and releases
        assert_eq!(order(&a, 0), PINNED);
    }

    const PINNED: [u64; 16] = [8, 7, 0, 5, 6, 13, 15, 2, 9, 10, 14, 11, 12, 3, 4, 1];
}
