use std::collections::{HashMap, VecDeque};

use monad_consensus::messages::message::ProposalMessage;
use monad_consensus_types::{
    block::{Block, BlockRange, BlockType, FullBlock},
    checkpoint::RootInfo,
    payload::{Payload, PayloadId},
    quorum_certificate::QuorumCertificate,
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
    payload_cache: HashMap<PayloadId, Payload>,
    // block headers >= root
    block_headers: HashMap<BlockId, Block<SCT>>,

    // cache of last max_buffered_proposals proposals
    proposal_buffer: VecDeque<(NodeId<SCT::NodeIdPubKey>, ProposalMessage<SCT>)>,
}

impl<SCT: SignatureCollection> BlockBuffer<SCT> {
    pub fn new(state_root_delay: SeqNum, root: SeqNum, resync_threshold: SeqNum) -> Self {
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

    pub fn get_payload_cache(&self) -> &HashMap<PayloadId, Payload> {
        &self.payload_cache
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
        let proposal_qc = proposal.block.qc.clone();
        self.block_headers
            .insert(proposal.block.get_id(), proposal.block.clone());

        self.proposal_buffer.push_back((author, proposal));
        if self.proposal_buffer.len() > self.max_buffered_proposals {
            self.proposal_buffer.pop_front();
        }

        let finalized_block_id = proposal_qc.get_committable_id()?;
        let finalized_block = self.block_headers.get(&finalized_block_id)?;

        if finalized_block.is_empty_block() {
            return None;
        }

        if finalized_block.get_seq_num() <= self.root + self.resync_threshold {
            return None;
        }

        Some((
            RootInfo {
                round: finalized_block.round,
                seq_num: finalized_block.get_seq_num(),
                epoch: finalized_block.epoch,
                block_id: finalized_block.get_id(),
                state_root: finalized_block.get_state_root(),
                timestamp_ns: finalized_block.get_timestamp().try_into().unwrap(),
            },
            proposal_qc,
        ))
    }

    pub fn handle_blocksync(&mut self, block: FullBlock<SCT>) {
        if block.get_seq_num() > self.root {
            // this should never happen, but here for clarity
            return;
        }

        self.payload_cache
            .insert(block.get_payload_id(), block.get_payload());
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

            payload_cache: self
                .full_blocks
                .values()
                .map(|block| (block.get_payload_id(), block.get_payload()))
                .collect(),
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

    pub fn needs_blocksync(&self, root: &RootInfo) -> Option<BlockRange> {
        if root.seq_num == GENESIS_SEQ_NUM {
            return None;
        }

        let mut blocksync_to = root.seq_num.max(NUM_BLOCK_HASH + self.state_root_delay)
            - NUM_BLOCK_HASH
            - self.state_root_delay;
        if blocksync_to == GENESIS_SEQ_NUM {
            // don't request genesis block
            // NOTE: this only works because there will be no NULL blocks with SeqNum(0)
            blocksync_to = SeqNum(1);
        };

        let chain = self.root_parent_chain(root);

        let Some(last) = chain.last() else {
            let request_range = BlockRange {
                last_block_id: root.block_id,
                root_seq_num: blocksync_to,
            };
            tracing::debug!(
                ?request_range,
                min_requested_blocks =? root.seq_num - blocksync_to,
                "statesync blocksyncing blocks"
            );
            return Some(request_range);
        };

        let block_seq_num = last.get_seq_num();
        let block_is_empty = last.is_empty_block();
        let block_parent_id = last.get_parent_id();

        if block_seq_num > blocksync_to || (block_seq_num == blocksync_to && block_is_empty) {
            let request_range = BlockRange {
                last_block_id: block_parent_id,
                root_seq_num: blocksync_to,
            };
            tracing::debug!(
                ?request_range,
                min_requested_blocks =? block_seq_num - blocksync_to,
                "statesync blocksyncing blocks"
            );
            Some(request_range)
        } else {
            None
        }
    }
}
