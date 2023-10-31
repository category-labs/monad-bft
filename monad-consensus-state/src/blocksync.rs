use core::fmt;
use std::{
    collections::{hash_map::Entry, HashMap},
    time::Duration,
};

use log::debug;
use monad_consensus::messages::message::BlockSyncMessage;
use monad_consensus_types::{
    block::{BlockType, FullBlock},
    quorum_certificate::QuorumCertificate,
    signature_collection::SignatureCollection,
    transaction_validator::TransactionValidator,
};
use monad_tracing_counter::inc_count;
use monad_types::{BlockId, NodeId, TimeoutVariant};
use monad_validator::validator_set::ValidatorSetType;

use crate::command::ConsensusCommand;

const DEFAULT_PEER_INDEX: usize = 0;

#[cfg_attr(feature = "monad_test", derive(PartialEq, Eq))]
#[derive(Debug, Clone)]
pub struct InFlightBlockSync<SCT> {
    pub req_target: NodeId,
    pub retry_cnt: usize,
    pub qc: QuorumCertificate<SCT>, // qc responsible for this event
}

pub enum BlockSyncResult<SCT: SignatureCollection> {
    Success(FullBlock<SCT>),            // retrieved and validated
    Retry(Vec<ConsensusCommand<SCT>>),  // unable to retrieve, retry
    IllegalResponse,                    // never requested from this peer or never requested
    GiveUp(Vec<ConsensusCommand<SCT>>), // no more retry
}

impl<SCT: SignatureCollection> fmt::Debug for BlockSyncResult<SCT> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            BlockSyncResult::Success(_) => write!(f, "successful"),
            BlockSyncResult::Retry(_) => write!(f, "retry"),
            BlockSyncResult::IllegalResponse => write!(f, "illegal"),
            BlockSyncResult::GiveUp(_) => write!(f, "give up"),
        }
    }
}

impl<SCT: SignatureCollection> BlockSyncResult<SCT> {
    fn log(&self, bid: BlockId) {
        match self {
            BlockSyncResult::Success(_) => {
                inc_count!(block_sync_response_successful);
            }
            BlockSyncResult::Retry(_) => {
                inc_count!(block_sync_response_retry);
            }
            BlockSyncResult::IllegalResponse => {
                inc_count!(block_sync_response_illegal);
            }
            BlockSyncResult::GiveUp(_) => {
                inc_count!(block_sync_response_give_up);
            }
        };

        debug!("Block sync response: bid={:?}, result={:?}", bid, self);
    }
}

impl<SCT: SignatureCollection> InFlightBlockSync<SCT> {
    pub fn new(req_target: NodeId, retry_cnt: usize, qc: QuorumCertificate<SCT>) -> Self {
        Self {
            req_target,
            retry_cnt,
            qc,
        }
    }
}
#[cfg_attr(feature = "monad_test", derive(PartialEq, Eq))]
#[derive(Debug, Clone)]
pub struct BlockSyncManager<SCT> {
    requests: HashMap<BlockId, InFlightBlockSync<SCT>>,
    id: NodeId,
    // how long it take before giving up on current block sync request
    tmo_duration: Duration,
    // how long it takes before a block is given up
    retry_limit: usize,
}

fn choose_peer(my_id: NodeId, peers: &[NodeId], mut cnt: usize) -> Option<(NodeId, usize)> {
    if peers.is_empty() || (peers.len() == 1 && peers[0] == my_id) {
        None
    } else {
        let mut peer;
        loop {
            peer = peers[(cnt) % peers.len()];
            if peer != my_id {
                break;
            }
            cnt += 1;
        }
        Some((peer, cnt))
    }
}

impl<SCT> BlockSyncManager<SCT>
where
    SCT: SignatureCollection,
{
    pub fn new(id: NodeId, tmo_duration: Duration, retry_limit: usize) -> Self {
        Self {
            requests: HashMap::new(),
            id,
            tmo_duration,
            retry_limit,
        }
    }

    //TODO: reset only existed because replay, if reset is no longer needed all, this function should be removed
    fn get_timeout_reset(&self, id: BlockId) -> ConsensusCommand<SCT> {
        ConsensusCommand::ScheduleReset(TimeoutVariant::BlockSync(id))
    }

    fn get_timeout(&self, id: BlockId) -> ConsensusCommand<SCT> {
        ConsensusCommand::Schedule {
            duration: self.tmo_duration,
            on_timeout: TimeoutVariant::BlockSync(id),
        }
    }

    pub fn request<VT: ValidatorSetType>(
        &mut self,
        qc: &QuorumCertificate<SCT>,
        validator_set: &VT,
    ) -> Vec<ConsensusCommand<SCT>> {
        let id = &qc.info.vote.id;
        let req = match self.requests.entry(*id) {
            Entry::Occupied(_) => None,
            Entry::Vacant(entry) => {
                debug!("Block sync request: bid={:?}, qc={:?}", id, qc);
                inc_count!(block_sync_request);
                if let Some((peer, cnt)) =
                    choose_peer(self.id, validator_set.get_list(), DEFAULT_PEER_INDEX)
                {
                    let tracker = InFlightBlockSync::new(peer, cnt, qc.clone());
                    let command = (&tracker).into();
                    entry.insert(tracker);
                    Some(command)
                } else {
                    // no peer possible given the validator_set
                    None
                }
            }
        };
        if let Some(command) = req {
            vec![command, self.get_timeout(*id)]
        } else {
            vec![]
        }
    }

    pub fn handle_retrieval<VT: ValidatorSetType, TV: TransactionValidator>(
        &mut self,
        author: &NodeId,
        msg: BlockSyncMessage<SCT>,
        validator_set: &VT,
        transaction_validator: &TV,
    ) -> BlockSyncResult<SCT> {
        let bid = match &msg {
            BlockSyncMessage::BlockFound(b) => b.block.get_id(),
            BlockSyncMessage::NotAvailable(bid) => *bid,
        };
        let result = if let Entry::Occupied(mut entry) = self.requests.entry(bid) {
            let InFlightBlockSync {
                req_target,
                retry_cnt,
                qc: _,
            } = entry.get_mut();

            if author != req_target {
                return BlockSyncResult::IllegalResponse;
            }

            match msg {
                BlockSyncMessage::BlockFound(unverified_full_block) => {
                    if let Some(full_block) =
                        FullBlock::try_from_unverified(unverified_full_block, transaction_validator)
                    {
                        // block retrieve and validate successful
                        entry.remove_entry();
                        return BlockSyncResult::Success(full_block);
                    }
                }
                BlockSyncMessage::NotAvailable(_) => {}
            };

            if let Some((peer, cnt)) =
                choose_peer(self.id, validator_set.get_list(), *retry_cnt + 1)
            {
                if cnt <= self.retry_limit {
                    *req_target = peer;
                    *retry_cnt = cnt;
                    return BlockSyncResult::Retry(vec![entry.get().into(), self.get_timeout(bid)]);
                }
            }
            // give up on block
            entry.remove();
            BlockSyncResult::GiveUp(vec![])
        } else {
            BlockSyncResult::IllegalResponse
        };
        result.log(bid);
        result
    }

    pub fn handle_timeout<VT: ValidatorSetType>(
        &mut self,
        bid: BlockId,
        validator_set: &VT,
    ) -> BlockSyncResult<SCT> {
        let result = match self.requests.entry(bid) {
            // if block_sync manager still care about the block
            Entry::Occupied(mut entry) => {
                let InFlightBlockSync {
                    req_target,
                    retry_cnt,
                    qc: _,
                } = entry.get_mut();
                if let Some((peer, cnt)) =
                    choose_peer(self.id, validator_set.get_list(), *retry_cnt + 1)
                {
                    if cnt <= self.retry_limit {
                        *req_target = peer;
                        *retry_cnt = cnt;
                        let resync_cmd = entry.get().into();
                        return BlockSyncResult::Retry(vec![
                            self.get_timeout_reset(bid),
                            resync_cmd,
                            self.get_timeout(bid),
                        ]);
                    }
                }
                // give up on this block
                entry.remove();
                BlockSyncResult::GiveUp(vec![self.get_timeout_reset(bid)])
            }
            Entry::Vacant(_) => BlockSyncResult::GiveUp(vec![self.get_timeout_reset(bid)]),
        };
        result.log(bid);
        result
    }
}

#[cfg(test)]
mod test {
    use core::panic;
    use std::time::Duration;

    use monad_consensus_types::{
        block::{Block, UnverifiedFullBlock},
        ledger::LedgerCommitInfo,
        payload::{
            ExecutionArtifacts, FullTransactionList, Payload, RandaoReveal, TransactionHashList,
        },
        quorum_certificate::{QcInfo, QuorumCertificate},
        signature_collection::SignatureCollection,
        transaction_validator::MockValidator,
        voting::VoteInfo,
    };
    use monad_crypto::hasher::{Hash, Hasher, HasherType};
    use monad_eth_types::EthAddress;
    use monad_testutil::{
        signing::{get_key, MockSignatures},
        validators::create_keys_w_validators,
    };
    use monad_types::{BlockId, NodeId, Round, TimeoutVariant};
    use monad_validator::validator_set::{ValidatorSet, ValidatorSetType};

    use super::BlockSyncManager;
    use crate::{command::ConsensusCommand, BlockSyncMessage, BlockSyncResult};
    type SC = MockSignatures;
    type VT = ValidatorSet;
    type QC = QuorumCertificate<SC>;
    type TV = MockValidator;

    struct FakeHasher1();

    impl Hasher for FakeHasher1 {
        fn new() -> Self {
            Self()
        }
        fn update(&mut self, _data: impl AsRef<[u8]>) {}
        fn hash(self) -> Hash {
            Hash([0x01_u8; 32])
        }
    }

    struct FakeHasher2();

    impl Hasher for FakeHasher2 {
        fn new() -> Self {
            Self()
        }
        fn update(&mut self, _data: impl AsRef<[u8]>) {}
        fn hash(self) -> Hash {
            Hash([0x02_u8; 32])
        }
    }

    struct FakeHasher3();

    impl Hasher for FakeHasher3 {
        fn new() -> Self {
            Self()
        }
        fn update(&mut self, _data: impl AsRef<[u8]>) {}
        fn hash(self) -> Hash {
            Hash([0x03_u8; 32])
        }
    }

    fn extract_request_sync<SCT: SignatureCollection>(
        cmds: &[ConsensusCommand<SCT>],
    ) -> &ConsensusCommand<SCT> {
        let res = cmds.iter().find(|c| {
            matches!(
                c,
                ConsensusCommand::RequestSync {
                    peer: NodeId(_),
                    block_id: BlockId(_),
                },
            )
        });
        res.expect("request sync not found")
    }

    #[test]
    fn test_handle_request_block_sync_message_basic_functionality() {
        let keypair = get_key(6);
        let mut manager =
            BlockSyncManager::<SC>::new(NodeId(keypair.pubkey()), Duration::MAX, usize::MAX);
        let (_, _, valset, _) = create_keys_w_validators::<SC>(4);

        let qc = &QC::new::<HasherType>(
            QcInfo {
                vote: VoteInfo {
                    id: BlockId(Hash([0x01_u8; 32])),
                    round: Round(0),
                    parent_id: BlockId(Hash([0x02_u8; 32])),
                    parent_round: Round(0),
                    seq_num: 0,
                },
                ledger_commit: LedgerCommitInfo::default(),
            },
            MockSignatures::with_pubkeys(&[]),
        );

        let cmds = manager.request::<VT>(qc, &valset);

        assert!(cmds.len() == 2);
        let (peer, bid) = match cmds[0] {
            ConsensusCommand::RequestSync { peer, block_id } => (peer, block_id),
            _ => panic!("manager didn't request a block when no inflight block is observed"),
        };

        assert!(peer == valset.get_list()[0]);
        assert!(bid == qc.info.vote.id);

        // repeated request would yield no result
        for _ in 0..1000 {
            let cmds = manager.request::<VT>(qc, &valset);
            assert!(cmds.is_empty());
        }

        let qc = &QC::new::<HasherType>(
            QcInfo {
                vote: VoteInfo {
                    id: BlockId(Hash([0x02_u8; 32])),
                    round: Round(0),
                    parent_id: BlockId(Hash([0x02_u8; 32])),
                    parent_round: Round(0),
                    seq_num: 0,
                },
                ledger_commit: LedgerCommitInfo::default(),
            },
            MockSignatures::with_pubkeys(&[]),
        );
        let cmds = manager.request::<VT>(qc, &valset);

        assert!(cmds.len() == 2);
        let (peer, bid) = match cmds[0] {
            ConsensusCommand::RequestSync { peer, block_id } => (peer, block_id),
            _ => panic!("manager didn't request a block when no inflight block is observed"),
        };

        assert!(peer == valset.get_list()[0]);
        assert!(bid == qc.info.vote.id);
    }

    #[test]
    fn test_handle_retrieval() {
        let keypair = get_key(6);
        let mut manager =
            BlockSyncManager::<SC>::new(NodeId(keypair.pubkey()), Duration::MAX, usize::MAX);
        let (_, _, valset, _) = create_keys_w_validators::<SC>(4);
        let transaction_validator = TV::default();

        // first qc
        let qc_1 = &QC::new::<HasherType>(
            QcInfo {
                vote: VoteInfo {
                    id: BlockId(Hash([0x01_u8; 32])),
                    round: Round(0),
                    parent_id: BlockId(Hash([0x02_u8; 32])),
                    parent_round: Round(0),
                    seq_num: 0,
                },
                ledger_commit: LedgerCommitInfo::default(),
            },
            MockSignatures::with_pubkeys(&[]),
        );

        let cmds = manager.request::<VT>(qc_1, &valset);

        assert!(cmds.len() == 2);
        let (peer_1, bid) = match cmds[0] {
            ConsensusCommand::RequestSync { peer, block_id } => (peer, block_id),
            _ => panic!("manager didn't request a block when no inflight block is observed"),
        };

        assert!(peer_1 == valset.get_list()[0]);
        assert!(bid == qc_1.info.vote.id);

        // second qc
        let qc_2 = &QC::new::<HasherType>(
            QcInfo {
                vote: VoteInfo {
                    id: BlockId(Hash([0x02_u8; 32])),
                    round: Round(0),
                    parent_id: BlockId(Hash([0x02_u8; 32])),
                    parent_round: Round(0),
                    seq_num: 0,
                },
                ledger_commit: LedgerCommitInfo::default(),
            },
            MockSignatures::with_pubkeys(&[]),
        );

        let cmds = manager.request::<VT>(qc_2, &valset);

        assert!(cmds.len() == 2);
        let (peer_2, bid) = match cmds[0] {
            ConsensusCommand::RequestSync { peer, block_id } => (peer, block_id),
            _ => panic!("manager didn't request a block when no inflight block is observed"),
        };

        assert!(peer_2 == valset.get_list()[0]);
        assert!(bid == qc_2.info.vote.id);

        // third request
        let qc_3 = &QC::new::<HasherType>(
            QcInfo {
                vote: VoteInfo {
                    id: BlockId(Hash([0x03_u8; 32])),
                    round: Round(0),
                    parent_id: BlockId(Hash([0x02_u8; 32])),
                    parent_round: Round(0),
                    seq_num: 0,
                },
                ledger_commit: LedgerCommitInfo::default(),
            },
            MockSignatures::with_pubkeys(&[]),
        );

        let cmds = manager.request::<VT>(qc_3, &valset);

        assert!(cmds.len() == 2);
        let (peer_3, bid) = match cmds[0] {
            ConsensusCommand::RequestSync { peer, block_id } => (peer, block_id),
            _ => panic!("manager didn't request a block when no inflight block is observed"),
        };

        assert!(peer_3 == valset.get_list()[0]);
        assert!(bid == qc_3.info.vote.id);

        let payload = Payload {
            txns: TransactionHashList::default(),
            header: ExecutionArtifacts::zero(),
            seq_num: 0,
            beneficiary: EthAddress::default(),
            randao_reveal: RandaoReveal::default(),
        };

        let block_1 = Block::new::<FakeHasher1>(
            NodeId(keypair.pubkey()),
            Round(3),
            &payload,
            &QC::new::<HasherType>(
                QcInfo {
                    vote: VoteInfo {
                        id: BlockId(Hash([0x01_u8; 32])),
                        round: Round(0),
                        parent_id: BlockId(Hash([0x02_u8; 32])),
                        parent_round: Round(0),
                        seq_num: 0,
                    },
                    ledger_commit: LedgerCommitInfo::default(),
                },
                MockSignatures::with_pubkeys(&[]),
            ),
        );

        let block_2 = Block::new::<FakeHasher2>(
            NodeId(keypair.pubkey()),
            Round(3),
            &payload,
            &QC::new::<HasherType>(
                QcInfo {
                    vote: VoteInfo {
                        id: BlockId(Hash([0x01_u8; 32])),
                        round: Round(0),
                        parent_id: BlockId(Hash([0x02_u8; 32])),
                        parent_round: Round(0),
                        seq_num: 0,
                    },
                    ledger_commit: LedgerCommitInfo::default(),
                },
                MockSignatures::with_pubkeys(&[]),
            ),
        );

        let block_3 = Block::new::<FakeHasher3>(
            NodeId(keypair.pubkey()),
            Round(3),
            &payload,
            &QC::new::<HasherType>(
                QcInfo {
                    vote: VoteInfo {
                        id: BlockId(Hash([0x01_u8; 32])),
                        round: Round(0),
                        parent_id: BlockId(Hash([0x02_u8; 32])),
                        parent_round: Round(0),
                        seq_num: 0,
                    },
                    ledger_commit: LedgerCommitInfo::default(),
                },
                MockSignatures::with_pubkeys(&[]),
            ),
        );

        let msg_no_block_1 = BlockSyncMessage::<SC>::NotAvailable(BlockId(Hash([0x01_u8; 32])));

        let msg_with_block_1 = BlockSyncMessage::<SC>::BlockFound(UnverifiedFullBlock {
            block: block_1.clone(),
            full_txs: FullTransactionList::default(),
        });

        let msg_no_block_2 = BlockSyncMessage::<SC>::NotAvailable(BlockId(Hash([0x02_u8; 32])));

        let msg_with_block_2 = BlockSyncMessage::<SC>::BlockFound(UnverifiedFullBlock {
            block: block_2.clone(),
            full_txs: FullTransactionList::default(),
        });

        let msg_no_block_3 = BlockSyncMessage::<SC>::NotAvailable(BlockId(Hash([0x03_u8; 32])));

        let msg_with_block_3 = BlockSyncMessage::<SC>::BlockFound(UnverifiedFullBlock {
            block: block_3.clone(),
            full_txs: FullTransactionList::default(),
        });

        // arbitrary response should be rejected
        let BlockSyncResult::<SC>::IllegalResponse = manager.handle_retrieval(
            &NodeId(keypair.pubkey()),
            msg_no_block_1,
            &valset,
            &transaction_validator,
        ) else {
            panic!("illegal response is processed");
        };

        // valid message from invalid individual should still get dropped

        let BlockSyncResult::<SC>::IllegalResponse = manager.handle_retrieval(
            &NodeId(keypair.pubkey()),
            msg_with_block_2.clone(),
            &valset,
            &transaction_validator,
        ) else {
            panic!("illegal response is processed");
        };

        let BlockSyncResult::<SC>::Retry(retry_command) = manager.handle_retrieval(
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
            manager.handle_retrieval(&peer_1, msg_with_block_1, &valset, &transaction_validator)
        else {
            panic!("illegal response is processed");
        };

        let BlockSyncResult::<SC>::Retry(retry_command) =
            manager.handle_retrieval(&peer_3, msg_no_block_3, &valset, &transaction_validator)
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

        assert!(b.get_block() == &block_1);

        let BlockSyncResult::<SC>::Retry(retry_command) =
            manager.handle_retrieval(peer_2, msg_no_block_2, &valset, &transaction_validator)
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
            manager.handle_retrieval(peer_3, msg_with_block_3, &valset, &transaction_validator)
        else {
            panic!("illegal response is processed");
        };

        assert!(b.get_block() == &block_3);

        let BlockSyncResult::<SC>::Success(b) =
            manager.handle_retrieval(peer_2, msg_with_block_2, &valset, &transaction_validator)
        else {
            panic!("illegal response is processed");
        };

        assert!(b.get_block() == &block_2);
    }

    #[test]
    fn test_never_request_to_self() {
        let (_, _, valset, _) = create_keys_w_validators::<SC>(30);
        let my_id = valset.get_list()[0];
        let mut manager = BlockSyncManager::<SC>::new(my_id, Duration::MAX, usize::MAX);

        let qc = &QC::new::<HasherType>(
            QcInfo {
                vote: VoteInfo {
                    id: BlockId(Hash([0x01_u8; 32])),
                    round: Round(0),
                    parent_id: BlockId(Hash([0x02_u8; 32])),
                    parent_round: Round(0),
                    seq_num: 0,
                },
                ledger_commit: LedgerCommitInfo::default(),
            },
            MockSignatures::with_pubkeys(&[]),
        );

        let cmds = manager.request::<VT>(qc, &valset);

        assert!(cmds.len() == 2);
        let (mut peer, bid) = match cmds[0] {
            ConsensusCommand::RequestSync { peer, block_id } => (peer, block_id),
            _ => panic!("manager didn't request a block when no inflight block is observed"),
        };
        // should have skipped self
        assert!(peer == valset.get_list()[1]);
        assert!(bid == qc.info.vote.id);
        let transaction_validator = TV::default();
        let msg_failed = BlockSyncMessage::<SC>::NotAvailable(bid);
        for _ in 0..10 {
            for i in 2..31 {
                let BlockSyncResult::<SC>::Retry(retry_command) = manager.handle_retrieval(
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
                    assert_eq!(bid, qc.info.vote.id);
                } else {
                    assert_eq!(peer, valset.get_list()[i % valset.len()]);
                    assert_eq!(bid, qc.info.vote.id);
                }
            }
        }
    }

    #[test]
    fn test_proper_emit_timeout() {
        // Total of 3 cases of timeout
        // Request, Retry, natural timeout
        let (_, _, valset, _) = create_keys_w_validators::<SC>(30);
        let my_id = valset.get_list()[0];
        let mut manager = BlockSyncManager::<SC>::new(my_id, Duration::MAX, usize::MAX);

        let qc = &QC::new::<HasherType>(
            QcInfo {
                vote: VoteInfo {
                    id: BlockId(Hash([0x01_u8; 32])),
                    round: Round(0),
                    parent_id: BlockId(Hash([0x02_u8; 32])),
                    parent_round: Round(0),
                    seq_num: 0,
                },
                ledger_commit: LedgerCommitInfo::default(),
            },
            MockSignatures::with_pubkeys(&[]),
        );

        let cmds = manager.request::<VT>(qc, &valset);

        assert!(cmds.len() == 2);

        let (peer, bid) = match cmds[0] {
            ConsensusCommand::RequestSync { peer, block_id } => (peer, block_id),
            _ => panic!("manager didn't request a block when no inflight block is observed"),
        };
        assert!(peer == valset.get_list()[1]);
        assert!(bid == qc.info.vote.id);

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
        assert!(bid == qc.info.vote.id);

        // similarly, failure of message should have also triggered a timeout
        let msg_failed = BlockSyncMessage::<SC>::NotAvailable(bid);

        let transaction_validator = TV::default();

        let BlockSyncResult::<SC>::Retry(retry_command) =
            manager.handle_retrieval(&peer, msg_failed, &valset, &transaction_validator)
        else {
            panic!("illegal response is processed");
        };

        assert_eq!(retry_command.len(), 2);

        let (peer, bid) = match retry_command[0] {
            ConsensusCommand::RequestSync { peer, block_id } => (peer, block_id),
            _ => panic!("manager didn't request a block when no inflight block is observed"),
        };
        assert!(peer == valset.get_list()[2]);
        assert!(bid == qc.info.vote.id);

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
        assert!(bid == qc.info.vote.id);

        // lastly, natural timeout should trigger it too.

        let BlockSyncResult::Retry(retry_command) = manager.handle_timeout(bid, &valset) else {
            panic!("wrong type emitted")
        };

        assert_eq!(retry_command.len(), 3);

        let ConsensusCommand::ScheduleReset(TimeoutVariant::BlockSync(bid)) = retry_command[0]
        else {
            panic!("timeout didn't emit reset first")
        };

        assert!(bid == qc.info.vote.id);

        let (peer, bid) = match retry_command[1] {
            ConsensusCommand::RequestSync { peer, block_id } => (peer, block_id),
            _ => panic!("manager didn't request a block when no inflight block is observed"),
        };
        assert!(peer == valset.get_list()[3]);
        assert!(bid == qc.info.vote.id);

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
        assert!(bid == qc.info.vote.id);

        // if somehow we sync up on the block, timeout should be ignored
        let payload = Payload {
            txns: TransactionHashList::default(),
            header: ExecutionArtifacts::zero(),
            seq_num: 0,
            beneficiary: EthAddress::default(),
            randao_reveal: RandaoReveal::default(),
        };
        let block = Block::new::<FakeHasher1>(peer, Round(3), &payload, qc);

        let msg_with_block = BlockSyncMessage::<SC>::BlockFound(UnverifiedFullBlock {
            block: block.clone(),
            full_txs: FullTransactionList::default(),
        });

        let BlockSyncResult::<SC>::Success(b) =
            manager.handle_retrieval(&peer, msg_with_block, &valset, &transaction_validator)
        else {
            panic!("illegal response is processed");
        };

        assert_eq!(b.get_block(), &block);

        // this should return nothing, except the regular reset
        let BlockSyncResult::GiveUp(give_up_cmd) = manager.handle_timeout(bid, &valset) else {
            panic!("wrong type emitted")
        };

        assert_eq!(give_up_cmd.len(), 1);

        let ConsensusCommand::ScheduleReset(TimeoutVariant::BlockSync(bid)) = give_up_cmd[0] else {
            panic!("timeout didn't emit reset first")
        };

        assert!(bid == qc.info.vote.id);
    }

    #[test]
    fn test_give_up() {
        let (_, _, valset, _) = create_keys_w_validators::<SC>(30);
        let my_id = valset.get_list()[0];
        let mut manager = BlockSyncManager::<SC>::new(my_id, Duration::MAX, 4);

        let qc = &QC::new::<HasherType>(
            QcInfo {
                vote: VoteInfo {
                    id: BlockId(Hash([0x01_u8; 32])),
                    round: Round(0),
                    parent_id: BlockId(Hash([0x02_u8; 32])),
                    parent_round: Round(0),
                    seq_num: 0,
                },
                ledger_commit: LedgerCommitInfo::default(),
            },
            MockSignatures::with_pubkeys(&[]),
        );

        let cmds = manager.request::<VT>(qc, &valset);

        assert!(cmds.len() == 2);
        let (mut peer, bid) = match cmds[0] {
            ConsensusCommand::RequestSync { peer, block_id } => (peer, block_id),
            _ => panic!("manager didn't request a block when no inflight block is observed"),
        };

        let msg_failed = BlockSyncMessage::<SC>::NotAvailable(bid);

        let transaction_validator = TV::default();

        for _ in 0..3 {
            let BlockSyncResult::<SC>::Retry(retry_command) = manager.handle_retrieval(
                &peer,
                msg_failed.clone(),
                &valset,
                &transaction_validator,
            ) else {
                panic!("illegal response is processed");
            };

            let ConsensusCommand::RequestSync {
                peer: p,
                block_id: _,
            } = retry_command[0]
            else {
                panic!("RequestSync is not produced")
            };

            let ConsensusCommand::Schedule {
                duration,
                on_timeout: TimeoutVariant::BlockSync(id),
            } = retry_command[1]
            else {
                panic!("Schedule is not produced")
            };
            assert_eq!(duration, Duration::MAX);
            assert_eq!(bid, id);

            peer = p;
        }
        // last request exceeded the request limit, should give up
        let BlockSyncResult::<SC>::GiveUp(_) =
            manager.handle_retrieval(&peer, msg_failed, &valset, &transaction_validator)
        else {
            panic!("illegal response is processed");
        };

        assert!(!manager.requests.contains_key(&bid));
        // if you re-request, its still fine
        let cmds = manager.request::<VT>(qc, &valset);
        assert!(manager.requests.contains_key(&bid));
    }
    #[test]
    fn test_give_up_from_timeout() {
        let (_, _, valset, _) = create_keys_w_validators::<SC>(30);
        let my_id = valset.get_list()[0];
        let mut manager = BlockSyncManager::<SC>::new(my_id, Duration::MAX, 4);

        let qc = &QC::new::<HasherType>(
            QcInfo {
                vote: VoteInfo {
                    id: BlockId(Hash([0x01_u8; 32])),
                    round: Round(0),
                    parent_id: BlockId(Hash([0x02_u8; 32])),
                    parent_round: Round(0),
                    seq_num: 0,
                },
                ledger_commit: LedgerCommitInfo::default(),
            },
            MockSignatures::with_pubkeys(&[]),
        );

        let cmds = manager.request::<VT>(qc, &valset);

        let ConsensusCommand::RequestSync {
            peer: _,
            block_id: bid,
        } = cmds[0]
        else {
            panic!("manager didn't request a block when no inflight block is observed");
        };

        for _ in 0..3 {
            let BlockSyncResult::Retry(retry_command) = manager.handle_timeout(bid, &valset) else {
                panic!("wrong type emitted")
            };

            let ConsensusCommand::ScheduleReset(TimeoutVariant::BlockSync(id)) = retry_command[0]
            else {
                panic!("ScheduleReset is not produced")
            };
            assert_eq!(bid, id);

            let ConsensusCommand::RequestSync {
                peer: _,
                block_id: id,
            } = retry_command[1]
            else {
                panic!("requestSync is not produced")
            };
            assert_eq!(bid, id);

            let ConsensusCommand::Schedule {
                duration,
                on_timeout: TimeoutVariant::BlockSync(id),
            } = retry_command[2]
            else {
                panic!("Schedule is not produced")
            };
            assert_eq!(duration, Duration::MAX);
            assert_eq!(bid, id);
        }

        let BlockSyncResult::GiveUp(cmds) = manager.handle_timeout(bid, &valset) else {
            panic!("wrong type emitted")
        };
        assert_eq!(cmds.len(), 1);
        let ConsensusCommand::ScheduleReset(TimeoutVariant::BlockSync(id)) = cmds[0] else {
            panic!("ScheduleReset is not produced")
        };
        assert_eq!(id, bid);
        assert!(manager.requests.is_empty());
    }
}
