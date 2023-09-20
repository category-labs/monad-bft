use std::collections::{hash_map::Entry, HashMap};

use monad_consensus::messages::message::BlockSyncMessage;
use monad_consensus_types::{
    block::{Block, BlockType},
    message_signature::MessageSignature,
    quorum_certificate::QuorumCertificate,
    signature_collection::SignatureCollection,
};
use monad_tracing_counter::inc_count;
use monad_types::{BlockId, NodeId};
use monad_validator::validator_set::ValidatorSetType;

use crate::command::ConsensusCommand;

const DEFAULT_AUTHOR_INDEX: usize = 0;
const DEFAULT_RETRY: usize = 0;

#[derive(Debug, Clone)]
pub struct InFlightBlockSync<SCT> {
    pub req_target: NodeId,
    pub retry_cnt: usize,
    pub qc: QuorumCertificate<SCT>, // qc responsible for this event
}
pub enum BlockSyncResult<ST, SCT: SignatureCollection> {
    Success(Block<SCT>),               // retrieved
    Failed(ConsensusCommand<ST, SCT>), // unable to retrieve
    IllegalResponse,                   // never requested from this peer or never requested
}

impl<SCT: SignatureCollection> InFlightBlockSync<SCT> {
    pub fn new(req_target: NodeId, qc: QuorumCertificate<SCT>) -> Self {
        Self {
            req_target,
            retry_cnt: DEFAULT_RETRY,
            qc,
        }
    }
}

#[derive(Debug, Clone)]
pub struct BlockSyncManager<SCT> {
    requests: HashMap<BlockId, InFlightBlockSync<SCT>>,
}

impl<SCT> Default for BlockSyncManager<SCT>
where
    SCT: SignatureCollection,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<SCT> BlockSyncManager<SCT>
where
    SCT: SignatureCollection,
{
    pub fn new() -> Self {
        Self {
            requests: HashMap::new(),
        }
    }

    pub fn request<ST: MessageSignature, VT: ValidatorSetType>(
        &mut self,
        qc: &QuorumCertificate<SCT>,
        validator_set: &VT,
    ) -> Vec<ConsensusCommand<ST, SCT>> {
        assert!(validator_set.len() > 0);
        let id = &qc.info.vote.id;
        match self.requests.entry(*id) {
            Entry::Occupied(_) => vec![],
            Entry::Vacant(entry) => {
                inc_count!(block_sync_request);
                let peer = validator_set.get_list()[DEFAULT_AUTHOR_INDEX % validator_set.len()];
                let req = InFlightBlockSync::new(peer, qc.clone());
                let req_cmd = vec![(&req).into()];
                entry.insert(req);
                req_cmd
            }
        }
    }

    pub fn handle_retrieval<ST: MessageSignature, VT: ValidatorSetType>(
        &mut self,
        author: &NodeId,
        msg: BlockSyncMessage<SCT>,
        validator_set: &VT,
    ) -> BlockSyncResult<ST, SCT> {
        let bid = match &msg {
            BlockSyncMessage::BlockFound(b) => b.get_id(),
            BlockSyncMessage::NotAvailable(bid) => *bid,
        };
        if let Entry::Occupied(mut entry) = self.requests.entry(bid) {
            let InFlightBlockSync {
                req_target,
                retry_cnt,
                qc: _,
            } = entry.get_mut();

            // TODO: remove this check and check it at router level
            if author != req_target {
                return BlockSyncResult::IllegalResponse;
            }

            match msg {
                BlockSyncMessage::BlockFound(block) => {
                    entry.remove_entry();
                    // block retrieve successful
                    BlockSyncResult::Success(block)
                }
                BlockSyncMessage::NotAvailable(_) => {
                    // block retrieve failed, re-request
                    *retry_cnt += 1;

                    *req_target = validator_set.get_list()[(*retry_cnt) % validator_set.len()];
                    BlockSyncResult::Failed(ConsensusCommand::RequestSync {
                        peer: *req_target,
                        block_id: bid,
                    })
                }
            }
        } else {
            BlockSyncResult::IllegalResponse
        }
    }
}
