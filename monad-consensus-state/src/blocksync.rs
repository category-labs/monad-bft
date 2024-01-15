//! BlockSync is a mechanism for nodes to request the block that is pointed to
//! by a QC that it has received.
//! Receiving a QC which does not point to any block in the pending blocktree implies
//! blocks are missing because an honest majority nodes in the network formed a QC and
//! created a proposal including it.
//! It is possible to miss blocks because of faults or messages arriving delayed or
//! out-of-order
//! BlockSync is achieved by sending a request with the blockid to an arbitrary node.
//! If a request times out or fails, another node is chosen.

use core::fmt;
use std::{collections::HashMap, time::Duration};

use monad_consensus::messages::message::BlockSyncResponseMessage;
use monad_consensus_types::{
    block::Block, block_validator::BlockValidator, quorum_certificate::QuorumCertificate,
    signature_collection::SignatureCollection,
};
use monad_tracing_counter::inc_count;
use monad_types::{BlockId, NodeId, TimeoutVariant};
use monad_validator::validator_set::ValidatorSetType;
use tracing::debug;

use crate::command::ConsensusCommand;

const DEFAULT_NODE_INDEX: usize = 0;

/// Represents a blocksync request that has been made and
/// that we are waiting for
#[derive(PartialEq, Eq, Debug, Clone)]
struct InFlightRequest<SCT: SignatureCollection> {
    /// The node we are requesting the block from
    req_target: NodeId<SCT::NodeIdPubKey>,

    /// The number of times we have tried making this request
    retry_cnt: usize,

    /// The QC which triggers this request for a missing block
    qc: QuorumCertificate<SCT>,
}

/// Possible results from handling a BlockSyncMessage, which is
/// the reply to a request
pub enum BlockSyncResult<SCT: SignatureCollection> {
    /// retrieved and validated
    Success(Block<SCT>),

    /// unable to retrieve
    Failed(Vec<ConsensusCommand<SCT>>),

    /// never requested from this node or never requested
    UnexpectedResponse,
}

impl<SCT: SignatureCollection> fmt::Debug for BlockSyncResult<SCT> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            BlockSyncResult::Success(_) => write!(f, "successful"),
            BlockSyncResult::Failed(_) => write!(f, "failed"),
            BlockSyncResult::UnexpectedResponse => write!(f, "unexpected"),
        }
    }
}

impl<SCT: SignatureCollection> BlockSyncResult<SCT> {
    pub fn log(&self, bid: BlockId) {
        match self {
            BlockSyncResult::Success(_) => {
                inc_count!(block_sync_response_successful);
            }
            BlockSyncResult::Failed(_) => {
                inc_count!(block_sync_response_failed);
            }
            BlockSyncResult::UnexpectedResponse => {
                inc_count!(block_sync_response_unexpected);
            }
        };

        debug!("Block sync response: bid={:?}, result={:?}", bid, self);
    }
}

impl<SCT: SignatureCollection> InFlightRequest<SCT> {
    pub fn new(
        req_target: NodeId<SCT::NodeIdPubKey>,
        retry_cnt: usize,
        qc: QuorumCertificate<SCT>,
    ) -> Self {
        Self {
            req_target,
            retry_cnt,
            qc,
        }
    }
}

/// Tracks inflight requests
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockSyncRequester<SCT: SignatureCollection> {
    /// current inflight requests
    requests: HashMap<BlockId, InFlightRequest<SCT>>,

    /// this node
    my_id: NodeId<SCT::NodeIdPubKey>,

    /// amount of time to wait for response to a request
    /// before giving up on that specific request
    tmo_duration: Duration,
}

impl<SCT> BlockSyncRequester<SCT>
where
    SCT: SignatureCollection,
{
    pub fn new(my_id: NodeId<SCT::NodeIdPubKey>, tmo_duration: Duration) -> Self {
        Self {
            requests: HashMap::new(),
            my_id,
            tmo_duration,
        }
    }

    pub fn create_timeout_command(&self, id: BlockId) -> ConsensusCommand<SCT> {
        ConsensusCommand::Schedule {
            duration: self.tmo_duration,
            on_timeout: TimeoutVariant::BlockSync(id),
        }
    }

    fn create_request_command(&self, sync: &InFlightRequest<SCT>) -> ConsensusCommand<SCT> {
        ConsensusCommand::RequestSync {
            peer: sync.req_target,
            block_id: sync.qc.get_block_id(),
        }
    }

    /// create a command to request the block for the given QC
    /// does nothing if there is already a pending request for the QC
    pub fn request<VT>(
        &mut self,
        qc: &QuorumCertificate<SCT>,
        validator_set: &VT,
    ) -> Vec<ConsensusCommand<SCT>>
    where
        VT: ValidatorSetType<NodeIdPubKey = SCT::NodeIdPubKey>,
    {
        self.request_helper(qc, validator_set, DEFAULT_NODE_INDEX)
    }

    // this function creates a request and creates the appropriate commands
    // to execute the request and creates/stores an InFlightRequest to track
    // the request
    fn request_helper<VT>(
        &mut self,
        qc: &QuorumCertificate<SCT>,
        validator_set: &VT,
        req_cnt: usize,
    ) -> Vec<ConsensusCommand<SCT>>
    where
        VT: ValidatorSetType<NodeIdPubKey = SCT::NodeIdPubKey>,
    {
        let id = qc.get_block_id();

        if self.requests.contains_key(&id) {
            return vec![];
        }

        debug!("Block sync request: bid={:?}, qc={:?}", id, qc);
        inc_count!(block_sync_request);

        let (req_peer, cnt) = self.choose_peer(validator_set.get_list(), req_cnt);
        let req = InFlightRequest::new(req_peer, cnt, qc.clone());
        let req_cmd = self.create_request_command(&req);
        self.requests.insert(id, req);

        vec![req_cmd, self.create_timeout_command(id)]
    }

    /// Handle the response to a BlockSync request
    /// If the request was not fulfilled, the request is tried again with
    /// a different node
    pub fn handle_response<VT, TV>(
        &mut self,
        author: &NodeId<SCT::NodeIdPubKey>,
        msg: BlockSyncResponseMessage<SCT>,
        validator_set: &VT,
        transaction_validator: &TV,
    ) -> BlockSyncResult<SCT>
    where
        VT: ValidatorSetType<NodeIdPubKey = SCT::NodeIdPubKey>,
        TV: BlockValidator,
    {
        let bid = msg.get_block_id();

        if self
            .requests
            .get(&bid)
            .is_some_and(|r| r.req_target != *author)
        {
            return BlockSyncResult::UnexpectedResponse;
        }

        if let Some(pending_req) = self.requests.remove(&bid) {
            match msg {
                BlockSyncResponseMessage::BlockFound(unverified_full_block) => {
                    if let Some(block) =
                        Block::try_from_unverified(unverified_full_block, transaction_validator)
                    {
                        return BlockSyncResult::Success(block);
                    }
                }
                BlockSyncResponseMessage::NotAvailable(_) => {}
            };

            BlockSyncResult::Failed(self.request_helper(
                &pending_req.qc,
                validator_set,
                pending_req.retry_cnt + 1,
            ))
        } else {
            BlockSyncResult::UnexpectedResponse
        }
    }

    /// If we receive a BlockSync timeout for a blockid, retry with a new
    /// target node
    pub fn handle_timeout<VT>(
        &mut self,
        bid: BlockId,
        validator_set: &VT,
    ) -> Vec<ConsensusCommand<SCT>>
    where
        VT: ValidatorSetType<NodeIdPubKey = SCT::NodeIdPubKey>,
    {
        // avoid duplicate logging
        let mut cmds = vec![ConsensusCommand::ScheduleReset(TimeoutVariant::BlockSync(
            bid,
        ))];

        if let Some(pending_req) = self.requests.remove(&bid) {
            cmds.extend(self.request_helper(
                &pending_req.qc,
                validator_set,
                pending_req.retry_cnt + 1,
            ));
        }
        cmds
    }

    // choose a node for a request that is not self.
    fn choose_peer(
        &self,
        peers: &[NodeId<SCT::NodeIdPubKey>],
        mut cnt: usize,
    ) -> (NodeId<SCT::NodeIdPubKey>, usize) {
        debug_assert!(peers.len() > 1);

        let mut peer = peers[(cnt) % peers.len()];
        while peer == self.my_id {
            cnt += 1;
            peer = peers[(cnt) % peers.len()];
        }

        (peer, cnt)
    }
}

#[cfg(test)]
mod test {
    use core::panic;
    use std::time::Duration;

    use monad_consensus_types::{
        block::{Block, BlockType, UnverifiedBlock},
        block_validator::MockValidator,
        ledger::CommitResult,
        payload::{ExecutionArtifacts, FullTransactionList, Payload, RandaoReveal},
        quorum_certificate::{QcInfo, QuorumCertificate},
        signature_collection::SignatureCollection,
        voting::{Vote, VoteInfo},
    };
    use monad_crypto::{
        certificate_signature::{CertificateKeyPair, CertificateSignaturePubKey},
        hasher::Hash,
        NopSignature,
    };
    use monad_eth_types::EthAddress;
    use monad_testutil::{
        signing::{get_key, MockSignatures},
        validators::create_keys_w_validators,
    };
    use monad_types::{BlockId, NodeId, Round, SeqNum, TimeoutVariant};
    use monad_validator::validator_set::{ValidatorSet, ValidatorSetType};

    use super::BlockSyncRequester;
    use crate::{command::ConsensusCommand, BlockSyncResponseMessage, BlockSyncResult};
    type ST = NopSignature;
    type SC = MockSignatures<ST>;
    type VT = ValidatorSet<CertificateSignaturePubKey<ST>>;
    type QC = QuorumCertificate<SC>;
    type TV = MockValidator;

    fn extract_request_sync<SCT: SignatureCollection>(
        cmds: &[ConsensusCommand<SCT>],
    ) -> &ConsensusCommand<SCT> {
        let res = cmds
            .iter()
            .find(|c| matches!(c, ConsensusCommand::RequestSync { .. },));
        res.expect("request sync not found")
    }

    #[test]
    fn test_handle_request_block_sync_message_basic_functionality() {
        let keypair = get_key::<ST>(6);
        let mut manager =
            BlockSyncRequester::<SC>::new(NodeId::new(keypair.pubkey()), Duration::MAX);
        let (_, _, valset, _) = create_keys_w_validators::<ST, SC>(4);

        let qc = &QC::new(
            QcInfo {
                vote: Vote {
                    vote_info: VoteInfo {
                        id: BlockId(Hash([0x01_u8; 32])),
                        round: Round(0),
                        parent_id: BlockId(Hash([0x02_u8; 32])),
                        parent_round: Round(0),
                        seq_num: SeqNum(0),
                    },
                    ledger_commit_info: CommitResult::NoCommit,
                },
            },
            SC::with_pubkeys(&[]),
        );

        let cmds = manager.request::<VT>(qc, &valset);

        assert!(cmds.len() == 2);
        let (peer, bid) = match cmds[0] {
            ConsensusCommand::RequestSync { peer, block_id } => (peer, block_id),
            _ => panic!("manager didn't request a block when no inflight block is observed"),
        };

        assert!(peer == valset.get_list()[0]);
        assert!(bid == qc.get_block_id());

        // repeated request would yield no result
        for _ in 0..1000 {
            let cmds = manager.request::<VT>(qc, &valset);
            assert!(cmds.is_empty());
        }

        let qc = &QC::new(
            QcInfo {
                vote: Vote {
                    vote_info: VoteInfo {
                        id: BlockId(Hash([0x02_u8; 32])),
                        round: Round(0),
                        parent_id: BlockId(Hash([0x02_u8; 32])),
                        parent_round: Round(0),
                        seq_num: SeqNum(0),
                    },
                    ledger_commit_info: CommitResult::NoCommit,
                },
            },
            SC::with_pubkeys(&[]),
        );
        let cmds = manager.request::<VT>(qc, &valset);

        assert!(cmds.len() == 2);
        let (peer, bid) = match cmds[0] {
            ConsensusCommand::RequestSync { peer, block_id } => (peer, block_id),
            _ => panic!("manager didn't request a block when no inflight block is observed"),
        };

        assert!(peer == valset.get_list()[0]);
        assert!(bid == qc.get_block_id());
    }

    #[test]
    fn test_handle_request() {
        let keypair = get_key::<ST>(6);
        let mut manager =
            BlockSyncRequester::<SC>::new(NodeId::new(keypair.pubkey()), Duration::MAX);
        let (_, _, valset, _) = create_keys_w_validators::<ST, SC>(4);
        let transaction_validator = TV::default();

        let payload = Payload {
            txns: FullTransactionList::empty(),
            header: ExecutionArtifacts::zero(),
            seq_num: SeqNum(0),
            beneficiary: EthAddress::default(),
            randao_reveal: RandaoReveal::default(),
        };

        let block_1 = Block::new(
            NodeId::new(keypair.pubkey()),
            Round(0),
            &payload,
            &QC::new(
                QcInfo {
                    vote: Vote {
                        vote_info: VoteInfo {
                            id: BlockId(Hash([0x01_u8; 32])),
                            round: Round(0),
                            parent_id: BlockId(Hash([0x02_u8; 32])),
                            parent_round: Round(0),
                            seq_num: SeqNum(0),
                        },
                        ledger_commit_info: CommitResult::NoCommit,
                    },
                },
                SC::with_pubkeys(&[]),
            ),
        );

        let block_2 = Block::new(
            NodeId::new(keypair.pubkey()),
            Round(1),
            &payload,
            &QC::new(
                QcInfo {
                    vote: Vote {
                        vote_info: VoteInfo {
                            id: BlockId(Hash([0x01_u8; 32])),
                            round: Round(0),
                            parent_id: BlockId(Hash([0x02_u8; 32])),
                            parent_round: Round(0),
                            seq_num: SeqNum(0),
                        },
                        ledger_commit_info: CommitResult::NoCommit,
                    },
                },
                SC::with_pubkeys(&[]),
            ),
        );

        let block_3 = Block::new(
            NodeId::new(keypair.pubkey()),
            Round(2),
            &payload,
            &QC::new(
                QcInfo {
                    vote: Vote {
                        vote_info: VoteInfo {
                            id: BlockId(Hash([0x01_u8; 32])),
                            round: Round(0),
                            parent_id: BlockId(Hash([0x02_u8; 32])),
                            parent_round: Round(0),
                            seq_num: SeqNum(0),
                        },
                        ledger_commit_info: CommitResult::NoCommit,
                    },
                },
                SC::with_pubkeys(&[]),
            ),
        );

        // first qc
        let qc_1 = &QC::new(
            QcInfo {
                vote: Vote {
                    vote_info: VoteInfo {
                        id: block_1.get_id(),
                        round: Round(0),
                        parent_id: BlockId(Hash([0x02_u8; 32])),
                        parent_round: Round(0),
                        seq_num: SeqNum(0),
                    },
                    ledger_commit_info: CommitResult::NoCommit,
                },
            },
            SC::with_pubkeys(&[]),
        );

        let cmds = manager.request::<VT>(qc_1, &valset);

        assert!(cmds.len() == 2);
        let (peer_1, bid) = match cmds[0] {
            ConsensusCommand::RequestSync { peer, block_id } => (peer, block_id),
            _ => panic!("manager didn't request a block when no inflight block is observed"),
        };

        assert!(peer_1 == valset.get_list()[0]);
        assert!(bid == qc_1.get_block_id());

        // second qc
        let qc_2 = &QC::new(
            QcInfo {
                vote: Vote {
                    vote_info: VoteInfo {
                        id: block_2.get_id(),
                        round: Round(0),
                        parent_id: BlockId(Hash([0x02_u8; 32])),
                        parent_round: Round(0),
                        seq_num: SeqNum(0),
                    },
                    ledger_commit_info: CommitResult::NoCommit,
                },
            },
            SC::with_pubkeys(&[]),
        );

        let cmds = manager.request::<VT>(qc_2, &valset);

        assert!(cmds.len() == 2);
        let (peer_2, bid) = match cmds[0] {
            ConsensusCommand::RequestSync { peer, block_id } => (peer, block_id),
            _ => panic!("manager didn't request a block when no inflight block is observed"),
        };

        assert!(peer_2 == valset.get_list()[0]);
        assert!(bid == qc_2.get_block_id());

        // third request
        let qc_3 = &QC::new(
            QcInfo {
                vote: Vote {
                    vote_info: VoteInfo {
                        id: block_3.get_id(),
                        round: Round(0),
                        parent_id: BlockId(Hash([0x02_u8; 32])),
                        parent_round: Round(0),
                        seq_num: SeqNum(0),
                    },
                    ledger_commit_info: CommitResult::NoCommit,
                },
            },
            SC::with_pubkeys(&[]),
        );

        let cmds = manager.request::<VT>(qc_3, &valset);

        assert!(cmds.len() == 2);
        let (peer_3, bid) = match cmds[0] {
            ConsensusCommand::RequestSync { peer, block_id } => (peer, block_id),
            _ => panic!("manager didn't request a block when no inflight block is observed"),
        };

        assert!(peer_3 == valset.get_list()[0]);
        assert!(bid == qc_3.get_block_id());

        let msg_no_block_1 = BlockSyncResponseMessage::<SC>::NotAvailable(block_1.get_id());

        let msg_with_block_1 =
            BlockSyncResponseMessage::<SC>::BlockFound(UnverifiedBlock(block_1.clone()));

        let msg_no_block_2 = BlockSyncResponseMessage::<SC>::NotAvailable(block_2.get_id());

        let msg_with_block_2 =
            BlockSyncResponseMessage::<SC>::BlockFound(UnverifiedBlock(block_2.clone()));

        let msg_no_block_3 = BlockSyncResponseMessage::<SC>::NotAvailable(block_3.get_id());

        let msg_with_block_3 =
            BlockSyncResponseMessage::<SC>::BlockFound(UnverifiedBlock(block_3.clone()));

        // arbitrary response should be rejected
        let BlockSyncResult::<SC>::UnexpectedResponse = manager.handle_response(
            &NodeId::new(keypair.pubkey()),
            msg_no_block_1,
            &valset,
            &transaction_validator,
        ) else {
            panic!("illegal response is processed");
        };

        // valid message from invalid individual should still get dropped

        let BlockSyncResult::<SC>::UnexpectedResponse = manager.handle_response(
            &NodeId::new(keypair.pubkey()),
            msg_with_block_2.clone(),
            &valset,
            &transaction_validator,
        ) else {
            panic!("illegal response is processed");
        };

        let BlockSyncResult::<SC>::Failed(retry_command) = manager.handle_response(
            &peer_2,
            msg_no_block_2.clone(),
            &valset,
            &transaction_validator,
        ) else {
            panic!("illegal response is processed");
        };

        let ConsensusCommand::RequestSync {
            peer: peer_2,
            block_id: _,
        } = extract_request_sync(&retry_command)
        else {
            panic!("request sync not found")
        };

        let BlockSyncResult::<SC>::Success(b) =
            manager.handle_response(&peer_1, msg_with_block_1, &valset, &transaction_validator)
        else {
            panic!("illegal response is processed");
        };

        let BlockSyncResult::<SC>::Failed(retry_command) =
            manager.handle_response(&peer_3, msg_no_block_3, &valset, &transaction_validator)
        else {
            panic!("illegal response is processed");
        };

        let ConsensusCommand::RequestSync {
            peer: peer_3,
            block_id: _,
        } = extract_request_sync(&retry_command)
        else {
            panic!("request sync not found")
        };

        assert!(b == block_1);

        let BlockSyncResult::<SC>::Failed(retry_command) =
            manager.handle_response(peer_2, msg_no_block_2, &valset, &transaction_validator)
        else {
            panic!("illegal response is processed");
        };

        let ConsensusCommand::RequestSync {
            peer: peer_2,
            block_id: _,
        } = extract_request_sync(&retry_command)
        else {
            panic!("request sync not found")
        };

        let BlockSyncResult::<SC>::Success(b) =
            manager.handle_response(peer_3, msg_with_block_3, &valset, &transaction_validator)
        else {
            panic!("illegal response is processed");
        };

        assert!(b == block_3);

        let BlockSyncResult::<SC>::Success(b) =
            manager.handle_response(peer_2, msg_with_block_2, &valset, &transaction_validator)
        else {
            panic!("illegal response is processed");
        };

        assert!(b == block_2);
    }

    #[test]
    fn test_never_request_to_self() {
        let (_, _, valset, _) = create_keys_w_validators::<ST, SC>(30);
        let my_id = valset.get_list()[0];
        let mut manager = BlockSyncRequester::<SC>::new(my_id, Duration::MAX);

        let qc = &QC::new(
            QcInfo {
                vote: Vote {
                    vote_info: VoteInfo {
                        id: BlockId(Hash([0x01_u8; 32])),
                        round: Round(0),
                        parent_id: BlockId(Hash([0x02_u8; 32])),
                        parent_round: Round(0),
                        seq_num: SeqNum(0),
                    },
                    ledger_commit_info: CommitResult::NoCommit,
                },
            },
            SC::with_pubkeys(&[]),
        );

        let cmds = manager.request::<VT>(qc, &valset);

        assert!(cmds.len() == 2);
        let (mut peer, bid) = match cmds[0] {
            ConsensusCommand::RequestSync { peer, block_id } => (peer, block_id),
            _ => panic!("manager didn't request a block when no inflight block is observed"),
        };
        // should have skipped self
        assert!(peer == valset.get_list()[1]);
        assert!(bid == qc.get_block_id());
        let transaction_validator = TV::default();
        let msg_failed = BlockSyncResponseMessage::<SC>::NotAvailable(bid);
        for _ in 0..10 {
            for i in 2..31 {
                let BlockSyncResult::<SC>::Failed(retry_command) = manager.handle_response(
                    &peer,
                    msg_failed.clone(),
                    &valset,
                    &transaction_validator,
                ) else {
                    panic!("illegal response is processed");
                };

                let ConsensusCommand::RequestSync {
                    peer: p,
                    block_id: bid,
                } = retry_command[0]
                else {
                    panic!("RequestSync is not produced")
                };

                peer = p;

                if i % valset.len() == 0 {
                    assert_eq!(peer, valset.get_list()[1]);
                    assert_eq!(bid, qc.get_block_id());
                } else {
                    assert_eq!(peer, valset.get_list()[i % valset.len()]);
                    assert_eq!(bid, qc.get_block_id());
                }
            }
        }
    }

    #[test]
    fn test_proper_emit_timeout() {
        // Total of 3 cases of timeout
        // Request, Failed, natural timeout
        let (_, _, valset, _) = create_keys_w_validators::<ST, SC>(30);
        let my_id = valset.get_list()[0];
        let mut manager = BlockSyncRequester::<SC>::new(my_id, Duration::MAX);

        let block = {
            let payload = Payload {
                txns: FullTransactionList::empty(),
                header: ExecutionArtifacts::zero(),
                seq_num: SeqNum(0),
                beneficiary: EthAddress::default(),
                randao_reveal: RandaoReveal::default(),
            };

            let qc = &QC::new(
                QcInfo {
                    vote: Vote {
                        vote_info: VoteInfo {
                            id: BlockId(Hash([0x01_u8; 32])),
                            round: Round(0),
                            parent_id: BlockId(Hash([0x02_u8; 32])),
                            parent_round: Round(0),
                            seq_num: SeqNum(0),
                        },
                        ledger_commit_info: CommitResult::NoCommit,
                    },
                },
                SC::with_pubkeys(&[]),
            );

            Block::new(valset.get_list()[0], Round(0), &payload, qc)
        };

        let qc = &QC::new(
            QcInfo {
                vote: Vote {
                    vote_info: VoteInfo {
                        id: block.get_id(),
                        round: Round(0),
                        parent_id: BlockId(Hash([0x02_u8; 32])),
                        parent_round: Round(0),
                        seq_num: SeqNum(0),
                    },
                    ledger_commit_info: CommitResult::NoCommit,
                },
            },
            SC::with_pubkeys(&[]),
        );

        let cmds = manager.request::<VT>(qc, &valset);

        assert!(cmds.len() == 2);

        let (peer, bid) = match cmds[0] {
            ConsensusCommand::RequestSync { peer, block_id } => (peer, block_id),
            _ => panic!("manager didn't request a block when no inflight block is observed"),
        };
        assert!(peer == valset.get_list()[1]);
        assert!(bid == qc.get_block_id());

        let (duration, TimeoutVariant::BlockSync(bid)) = (match cmds[1] {
            ConsensusCommand::Schedule {
                duration,
                on_timeout,
            } => (duration, on_timeout),
            _ => panic!("manager didn't request a block when no inflight block is observed"),
        }) else {
            panic!("timeout event is not block sync")
        };
        assert!(duration == Duration::MAX);
        assert!(bid == qc.get_block_id());

        // similarly, failure of message should have also triggered a timeout
        let msg_failed = BlockSyncResponseMessage::<SC>::NotAvailable(bid);

        let transaction_validator = TV::default();

        let BlockSyncResult::<SC>::Failed(retry_command) =
            manager.handle_response(&peer, msg_failed, &valset, &transaction_validator)
        else {
            panic!("illegal response is processed");
        };

        assert_eq!(retry_command.len(), 2);

        let (peer, bid) = match retry_command[0] {
            ConsensusCommand::RequestSync { peer, block_id } => (peer, block_id),
            _ => panic!("manager didn't request a block when no inflight block is observed"),
        };
        assert!(peer == valset.get_list()[2]);
        assert!(bid == qc.get_block_id());

        let (duration, TimeoutVariant::BlockSync(bid)) = (match retry_command[1] {
            ConsensusCommand::Schedule {
                duration,
                on_timeout,
            } => (duration, on_timeout),
            _ => panic!("manager didn't request a block when no inflight block is observed"),
        }) else {
            panic!("timeout event is not block sync")
        };
        assert!(duration == Duration::MAX);
        assert!(bid == qc.get_block_id());

        // lastly, natural timeout should trigger it too.

        let retry_command = manager.handle_timeout(bid, &valset);

        assert_eq!(retry_command.len(), 3);

        let ConsensusCommand::ScheduleReset(TimeoutVariant::BlockSync(bid)) = retry_command[0]
        else {
            panic!("timeout didn't emit reset first")
        };

        assert!(bid == qc.get_block_id());

        let (peer, bid) = match retry_command[1] {
            ConsensusCommand::RequestSync { peer, block_id } => (peer, block_id),
            _ => panic!("manager didn't request a block when no inflight block is observed"),
        };
        assert!(peer == valset.get_list()[3]);
        assert!(bid == qc.get_block_id());

        let (duration, TimeoutVariant::BlockSync(bid)) = (match retry_command[2] {
            ConsensusCommand::Schedule {
                duration,
                on_timeout,
            } => (duration, on_timeout),
            _ => panic!("manager didn't request a block when no inflight block is observed"),
        }) else {
            panic!("timeout event is not block sync")
        };
        assert!(duration == Duration::MAX);
        assert!(bid == qc.get_block_id());

        // if somehow we sync up on the block, timeout should be ignored

        let msg_with_block =
            BlockSyncResponseMessage::<SC>::BlockFound(UnverifiedBlock(block.clone()));

        let BlockSyncResult::<SC>::Success(b) =
            manager.handle_response(&peer, msg_with_block, &valset, &transaction_validator)
        else {
            panic!("illegal response is processed");
        };

        assert_eq!(b, block);

        // this should return nothing, except the regular reset
        let retry_command = manager.handle_timeout(bid, &valset);

        assert_eq!(retry_command.len(), 1);

        let ConsensusCommand::ScheduleReset(TimeoutVariant::BlockSync(bid)) = retry_command[0]
        else {
            panic!("timeout didn't emit reset first")
        };

        assert!(bid == qc.get_block_id());
    }
}
