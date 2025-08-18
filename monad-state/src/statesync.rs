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

use std::collections::{HashMap, VecDeque};

use monad_consensus::messages::message::ProposalMessage;
use monad_consensus_types::{
    block::{BlockRange, ConsensusBlockHeader, ConsensusFullBlock, GENESIS_TIMESTAMP},
    checkpoint::RootInfo,
    payload::{ConsensusBlockBody, ConsensusBlockBodyId},
    quorum_certificate::QuorumCertificate,
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_types::{
    BlockId, Epoch, ExecutionProtocol, NodeId, SeqNum, GENESIS_BLOCK_ID, GENESIS_ROUND,
    GENESIS_SEQ_NUM,
};
use monad_validator::signature_collection::SignatureCollection;

/// BlockBuffer is responsible for tracking pending blocks mid-statesync
/// It performs a function very similar to the blocktree, but specifically for statesync purposes
/// This could likely be unified with the blocktree, but will be a larger implementation lift
#[derive(Clone)]
pub(crate) struct BlockBuffer<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    max_buffered_proposals: usize,

    /// trigger resync once passively observe new_root > current_root + resync_threshold
    resync_threshold: SeqNum,
    state_root_delay: SeqNum,

    root: BlockId,
    // blocks <= root
    full_blocks: HashMap<BlockId, ConsensusFullBlock<ST, SCT, EPT>>,
    payload_cache: HashMap<ConsensusBlockBodyId, ConsensusBlockBody<EPT>>,
    // block headers >= root
    block_headers: HashMap<BlockId, ConsensusBlockHeader<ST, SCT, EPT>>,

    // cache of last max_buffered_proposals proposals
    proposal_buffer: VecDeque<(NodeId<SCT::NodeIdPubKey>, ProposalMessage<ST, SCT, EPT>)>,
}

impl<ST, SCT, EPT> BlockBuffer<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    pub fn new(state_root_delay: SeqNum, root: BlockId, resync_threshold: SeqNum) -> Self {
        Self {
            max_buffered_proposals: resync_threshold.0 as usize,
            resync_threshold,
            state_root_delay,

            root,

            full_blocks: Default::default(),
            payload_cache: Default::default(),
            block_headers: Default::default(),
            proposal_buffer: Default::default(),
        }
    }

    pub fn get_payload_cache(&self) -> &HashMap<ConsensusBlockBodyId, ConsensusBlockBody<EPT>> {
        &self.payload_cache
    }

    pub fn root_seq_num(&self) -> Option<SeqNum> {
        Some(self.root_info()?.seq_num)
    }

    pub fn root_info(&self) -> Option<RootInfo> {
        if self.root == GENESIS_BLOCK_ID {
            return Some(RootInfo {
                seq_num: GENESIS_SEQ_NUM,
                round: GENESIS_ROUND,
                epoch: Epoch(1),
                block_id: GENESIS_BLOCK_ID,
                timestamp_ns: GENESIS_TIMESTAMP,
            });
        }

        let root = self.full_blocks.get(&self.root)?;
        Some(RootInfo {
            round: root.get_block_round(),
            seq_num: root.get_seq_num(),
            epoch: root.get_epoch(),
            block_id: root.get_id(),
            timestamp_ns: root.get_timestamp(),
        })
    }

    pub fn root_delayed_execution_result(&self) -> Option<&Vec<EPT::FinalizedHeader>> {
        let root = self.full_blocks.get(&self.root)?;

        Some(root.get_execution_results())
    }

    /// returns a new sync_target if applicable.
    ///
    /// concretely, if new_root > current_root + resync_threshold
    pub fn handle_proposal(
        &mut self,
        author: NodeId<SCT::NodeIdPubKey>,
        proposal: ProposalMessage<ST, SCT, EPT>,
    ) -> Option<(ConsensusBlockHeader<ST, SCT, EPT>, QuorumCertificate<SCT>)> {
        // TODO more validation? leader checking? more sophisticated eviction?

        let root_seq_num = self.root_seq_num()?;
        if proposal.tip.block_header.seq_num < root_seq_num {
            return None;
        }
        let proposal_qc = proposal.tip.block_header.qc.clone();
        self.block_headers.insert(
            proposal.tip.block_header.get_id(),
            proposal.tip.block_header.clone(),
        );

        self.proposal_buffer.push_back((author, proposal));
        if self.proposal_buffer.len() > self.max_buffered_proposals {
            self.proposal_buffer.pop_front();
        }

        let qc_parent_block = self.block_headers.get(&proposal_qc.get_block_id())?;
        let finalized_block_id = proposal_qc.get_committable_id(qc_parent_block)?;
        let finalized_block = self.block_headers.get(&finalized_block_id)?;

        if finalized_block.seq_num <= root_seq_num + self.resync_threshold {
            return None;
        }

        Some((finalized_block.clone(), proposal_qc))
    }

    pub fn handle_blocksync(&mut self, block: ConsensusFullBlock<ST, SCT, EPT>) {
        if self
            .root_seq_num()
            .is_some_and(|root_seq_num| block.get_seq_num() > root_seq_num)
        {
            // this should never happen, but here for clarity
            return;
        }

        self.payload_cache
            .insert(block.get_body_id(), block.body().clone());
        self.full_blocks.insert(block.get_id(), block);
    }

    pub fn re_root(&mut self, new_root: ConsensusBlockHeader<ST, SCT, EPT>) {
        // remove obsolete full_blocks
        self.full_blocks
            .retain(|_id, block| block.get_seq_num() + self.state_root_delay >= new_root.seq_num);
        for (_sender, proposal) in &self.proposal_buffer {
            if proposal.tip.block_header.seq_num <= new_root.seq_num {
                if let Ok(full_block) = ConsensusFullBlock::new(
                    proposal.tip.block_header.clone(),
                    proposal.block_body.clone(),
                ) {
                    self.full_blocks
                        .insert(proposal.tip.block_header.get_id(), full_block);
                }
            }
            // we could also evict from proposal_buffer here, but unnecessary
        }

        // remove obsolete block headers
        self.block_headers
            .retain(|_id, block| block.seq_num >= new_root.seq_num);

        self.root = new_root.get_id();
        self.payload_cache = self
            .full_blocks
            .values()
            .map(|block| (block.get_body_id(), block.body().clone()))
            .collect();
    }

    pub fn proposals(
        &self,
    ) -> impl Iterator<Item = &(NodeId<SCT::NodeIdPubKey>, ProposalMessage<ST, SCT, EPT>)> {
        self.proposal_buffer.iter()
    }

    /// chain of blocks starting with root (highest to lowest seq_num)
    pub fn root_parent_chain(&self) -> Vec<&ConsensusFullBlock<ST, SCT, EPT>> {
        let mut next_block_id = self.root;
        let mut root_parent_chain = Vec::new();
        while let Some(block) = self.full_blocks.get(&next_block_id) {
            root_parent_chain.push(block);
            next_block_id = block.get_parent_id();
        }
        root_parent_chain
    }

    pub fn needs_blocksync(&self) -> Option<BlockRange> {
        if self.root == GENESIS_BLOCK_ID {
            return None;
        }

        let chain = self.root_parent_chain();
        let Some(last) = chain.last() else {
            let request_range = BlockRange {
                last_block_id: self.root,
                num_blocks: self.state_root_delay,
            };
            tracing::debug!(?request_range, "statesync blocksyncing blocks");
            return Some(request_range);
        };

        let block_parent_id = last.get_parent_id();

        if chain.len() < self.state_root_delay.0 as usize {
            let request_range = BlockRange {
                last_block_id: block_parent_id,
                num_blocks: self.state_root_delay - SeqNum(chain.len() as u64),
            };
            tracing::debug!(?request_range, "statesync blocksyncing blocks");
            return Some(request_range);
        }

        None
    }
}
