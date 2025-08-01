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
    collections::{HashMap, HashSet},
    fmt::Debug,
    ops::Deref,
};

use monad_consensus_types::{
    block::BlockPolicy,
    payload::{ConsensusBlockBody, ConsensusBlockBodyId},
    signature_collection::SignatureCollection,
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_state_backend::StateBackend;
use monad_types::{BlockId, ExecutionProtocol};

pub struct Tree<ST, SCT, EPT, BPT, SBT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT>,
    SBT: StateBackend,
{
    tree: HashMap<BlockId, BlockTreeEntry<ST, SCT, EPT, BPT, SBT>>,
    payloads: HashMap<ConsensusBlockBodyId, BlockBodyIndex<EPT>>,
}

#[derive(Debug, PartialEq, Eq)]
struct BlockBodyIndex<EPT>
where
    EPT: ExecutionProtocol,
{
    body: ConsensusBlockBody<EPT>,
    /// the set of blocks in tree that point to this payload
    active_blocks: HashSet<BlockId>,
}

impl<ST, SCT, EPT, BPT, SBT> Tree<ST, SCT, EPT, BPT, SBT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT>,
    SBT: StateBackend,
{
    pub(crate) fn set_coherent(&mut self, block_id: &BlockId, coherent: bool) -> Result<(), ()> {
        self.tree.get_mut(block_id).ok_or(())?.is_coherent = coherent;
        Ok(())
    }

    pub(crate) fn remove(
        &mut self,
        block_id: &BlockId,
    ) -> Option<BlockTreeEntry<ST, SCT, EPT, BPT, SBT>> {
        let maybe_removed = self.tree.remove(block_id);
        if let Some(removed) = &maybe_removed {
            let payload_id = removed.validated_block.get_body_id();
            let payload_index = self
                .payloads
                .get_mut(&payload_id)
                .expect("payload_index must exist for removed block");
            // maintain active_blocks invariant
            let removed = payload_index.active_blocks.remove(block_id);
            assert!(removed);
            if payload_index.active_blocks.is_empty() {
                // garbage collect payload
                let removed = self.payloads.remove(&payload_id);
                assert!(removed.is_some());
            }
        }
        maybe_removed
    }

    /// inserts block with is_coherent set to false
    /// caller is responsible for updating coherency via set_coherent
    pub(crate) fn insert(&mut self, block: BPT::ValidatedBlock) {
        let new_block_id = block.get_id();
        let parent_id = block.get_parent_id();
        let body_id = block.get_body_id();
        let body = block.body().clone();

        // Get all the children blocks in the blocktree
        let mut children_blocks = Vec::new();
        for (block_id, blocktree_entry) in self.tree.iter() {
            if blocktree_entry.validated_block.get_parent_id() == new_block_id {
                children_blocks.push(*block_id);
            }
        }

        // Create the new blocktree entry
        let is_coherent = false;
        let new_block_entry = BlockTreeEntry {
            validated_block: block,
            is_coherent,
            children_blocks,
        };

        let replaced = self.tree.insert(new_block_id, new_block_entry);
        assert!(replaced.is_none());

        if let Some(parent_entry) = self.tree.get_mut(&parent_id) {
            parent_entry.children_blocks.push(new_block_id);
        }

        let newly_inserted = self
            .payloads
            .entry(body_id)
            .or_insert(BlockBodyIndex {
                body,
                active_blocks: Default::default(),
            })
            .active_blocks
            .insert(new_block_id);
        assert!(newly_inserted);
    }

    pub fn get_payload(
        &self,
        block_body_id: &ConsensusBlockBodyId,
    ) -> Option<&ConsensusBlockBody<EPT>> {
        let payload_index = self.payloads.get(block_body_id)?;
        Some(&payload_index.body)
    }
}

impl<ST, SCT, EPT, BPT, SBT> Deref for Tree<ST, SCT, EPT, BPT, SBT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT>,
    SBT: StateBackend,
{
    type Target = HashMap<BlockId, BlockTreeEntry<ST, SCT, EPT, BPT, SBT>>;

    fn deref(&self) -> &Self::Target {
        &self.tree
    }
}

impl<ST, SCT, EPT, BPT, SBT> Default for Tree<ST, SCT, EPT, BPT, SBT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT>,
    SBT: StateBackend,
{
    fn default() -> Self {
        Self {
            tree: Default::default(),
            payloads: Default::default(),
        }
    }
}

impl<ST, SCT, EPT, BPT, SBT> Debug for Tree<ST, SCT, EPT, BPT, SBT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT>,
    SBT: StateBackend,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Tree")
            .field("tree", &self.tree)
            .field("payloads", &self.payloads)
            .finish_non_exhaustive()
    }
}

impl<ST, SCT, EPT, BPT, SBT> PartialEq<Self> for Tree<ST, SCT, EPT, BPT, SBT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT>,
    SBT: StateBackend,
{
    fn eq(&self, other: &Self) -> bool {
        self.tree == other.tree && self.payloads == other.payloads
    }
}

impl<ST, SCT, EPT, BPT, SBT> Eq for Tree<ST, SCT, EPT, BPT, SBT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT>,
    SBT: StateBackend,
{
}

pub struct BlockTreeEntry<ST, SCT, EPT, BPT, SBT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT>,
    SBT: StateBackend,
{
    pub validated_block: BPT::ValidatedBlock,
    /// A blocktree entry is coherent if there is a path to root from the entry and it
    /// is a valid extension of the chain
    pub is_coherent: bool,
    /// A vector of all the block ids that extend this validated block in the blocktree
    pub children_blocks: Vec<BlockId>,
}

impl<ST, SCT, EPT, BPT, SBT> Clone for BlockTreeEntry<ST, SCT, EPT, BPT, SBT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT>,
    SBT: StateBackend,
{
    fn clone(&self) -> Self {
        Self {
            validated_block: self.validated_block.clone(),
            is_coherent: self.is_coherent,
            children_blocks: self.children_blocks.clone(),
        }
    }
}

impl<ST, SCT, EPT, BPT, SBT> Debug for BlockTreeEntry<ST, SCT, EPT, BPT, SBT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT>,
    SBT: StateBackend,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlockTreeEntry")
            .field("validated_block", &self.validated_block)
            .field("is_coherent", &self.is_coherent)
            .field("children_blocks", &self.children_blocks)
            .finish()
    }
}

impl<ST, SCT, EPT, BPT, SBT> PartialEq<Self> for BlockTreeEntry<ST, SCT, EPT, BPT, SBT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT>,
    SBT: StateBackend,
{
    fn eq(&self, other: &Self) -> bool {
        self.validated_block == other.validated_block
            && self.is_coherent == other.is_coherent
            && self.children_blocks == other.children_blocks
    }
}

impl<ST, SCT, EPT, BPT, SBT> Eq for BlockTreeEntry<ST, SCT, EPT, BPT, SBT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT>,
    SBT: StateBackend,
{
}
