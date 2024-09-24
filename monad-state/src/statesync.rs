use std::collections::{HashMap, VecDeque};

use monad_consensus::messages::message::ProposalMessage;
use monad_consensus_types::{
    block::{Block, BlockType, FullBlock},
    checkpoint::RootInfo,
    quorum_certificate::{QuorumCertificate, GENESIS_BLOCK_ID},
    signature_collection::SignatureCollection,
};
use monad_types::{BlockId, NodeId, SeqNum, GENESIS_SEQ_NUM};

use crate::NUM_BLOCK_HASH;

/// BlockBuffer is responsible for tracking pending blocks mid-statesync
/// It performs a function very similar to the blocktree, but specifically for statesync purposes
/// This could likely be unified with the blocktree, but will be a larger implementation lift
#[derive(Clone)]
pub(crate) struct BlockBuffer<SCT: SignatureCollection> {
    max_buffered_proposals: usize,

    /// trigger resync once passively observe new_root > current_root + resync_threshold
    resync_threshold: SeqNum,
    state_root_delay: SeqNum,

    root: SeqNum,
    // blocks <= root
    full_blocks: HashMap<BlockId, FullBlock<SCT>>,
    // block headers >= root
    block_headers: HashMap<BlockId, Block<SCT>>,

    // cache of last max_buffered_proposals proposals
    proposal_buffer: VecDeque<(NodeId<SCT::NodeIdPubKey>, ProposalMessage<SCT>)>,
}

impl<SCT: SignatureCollection> BlockBuffer<SCT> {
    pub fn new(state_root_delay: SeqNum, root: SeqNum) -> Self {
        Self {
            max_buffered_proposals: (NUM_BLOCK_HASH + state_root_delay).0 as usize,
            resync_threshold: SeqNum((NUM_BLOCK_HASH + state_root_delay).0 / 2),
            state_root_delay,

            root,

            full_blocks: Default::default(),
            block_headers: Default::default(),
            proposal_buffer: Default::default(),
        }
    }

    /// returns a new sync_target is applicable.
    ///
    /// concretely, if new_root > current_root + resync_threshold
    pub fn handle_proposal(
        &mut self,
        author: NodeId<SCT::NodeIdPubKey>,
        proposal: ProposalMessage<SCT>,
    ) -> Option<(RootInfo, QuorumCertificate<SCT>)> {
        // TODO more validation? leader checking? more sophisticated eviction?

        if proposal.block.get_seq_num() < self.root {
            return None;
        }
        let block_header = proposal.block.clone();
        self.block_headers
            .insert(proposal.block.get_id(), block_header.clone());

        self.proposal_buffer.push_back((author, proposal));
        if self.proposal_buffer.len() > self.max_buffered_proposals {
            self.proposal_buffer.pop_front();
        }

        let start_seq_num = block_header.get_seq_num();
        let mut next_block_id = block_header.get_parent_id();
        while let Some(block) = self.block_headers.get(&next_block_id) {
            next_block_id = block.get_parent_id();
            let end_seq_num = block.get_seq_num();

            if !block.is_empty_block() && end_seq_num + self.state_root_delay <= start_seq_num {
                if end_seq_num > self.root + self.resync_threshold {
                    return Some((
                        RootInfo {
                            round: block.round,
                            seq_num: block.get_seq_num(),
                            epoch: block.epoch,
                            block_id: block.get_id(),
                            state_root: block.get_state_root(),
                        },
                        block_header.get_qc().clone(),
                    ));
                }
                break;
            }
        }

        None
    }

    pub fn handle_blocksync(&mut self, block: FullBlock<SCT>) {
        if block.get_seq_num() > self.root {
            // this should never happen, but here for clarity
            return;
        }

        self.full_blocks.insert(block.get_id(), block);
    }

    pub fn re_root(mut self, root: SeqNum) -> Self {
        // check that new root is increasing
        assert!(self.root <= root);

        // remove obsolete full_blocks
        self.full_blocks.retain(|_id, block| {
            block.get_seq_num() + NUM_BLOCK_HASH + self.state_root_delay >= root
        });
        for (_sender, proposal) in &self.proposal_buffer {
            if proposal.block.get_seq_num() < self.root {
                self.full_blocks.insert(
                    proposal.block.get_id(),
                    FullBlock {
                        block: proposal.block.clone(),
                        payload: proposal.payload.clone(),
                    },
                );
            }
            // we could also evict from proposal_buffer here, but unnecessary
        }

        // remove obsolete block headers
        self.block_headers
            .retain(|_id, block| block.get_seq_num() >= root);

        Self {
            max_buffered_proposals: self.max_buffered_proposals,

            resync_threshold: self.resync_threshold,
            state_root_delay: self.state_root_delay,

            root,

            full_blocks: self.full_blocks,
            block_headers: self.block_headers,
            proposal_buffer: self.proposal_buffer,
        }
    }

    pub fn proposals(
        &self,
    ) -> impl Iterator<Item = &(NodeId<SCT::NodeIdPubKey>, ProposalMessage<SCT>)> {
        self.proposal_buffer.iter()
    }

    /// chain of blocks starting with root (highest to lowest seq_num)
    pub fn root_parent_chain(&self, root: &RootInfo) -> Vec<&FullBlock<SCT>> {
        assert_eq!(self.root, root.seq_num);
        let mut next_block_id = root.block_id;
        let mut root_parent_chain = Vec::new();
        while let Some(block) = self.full_blocks.get(&next_block_id) {
            root_parent_chain.push(block);
            next_block_id = block.get_parent_id();
        }
        root_parent_chain
    }

    pub fn needs_blocksync(&self, root: &RootInfo) -> Option<BlockId> {
        if root.seq_num == GENESIS_SEQ_NUM {
            return None;
        }

        let chain = self.root_parent_chain(root);

        let Some(last) = chain.last() else {
            return Some(root.block_id);
        };

        let block_seq_num = last.get_seq_num();
        let block_is_empty = last.is_empty_block();
        let block_parent_id = last.get_parent_id();

        if block_parent_id != GENESIS_BLOCK_ID
            && (block_is_empty || // if block is empty, keep requesting until we hit non-empty
            block_seq_num
                > root.seq_num.max(NUM_BLOCK_HASH + self.state_root_delay) - NUM_BLOCK_HASH - self.state_root_delay)
        {
            Some(block_parent_id)
        } else {
            None
        }
    }
}
