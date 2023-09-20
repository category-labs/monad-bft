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
                // TODO: Avoid requesting to yourself
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

                    // TODO: Avoid requesting to yourself
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

#[cfg(test)]
mod test {
    use core::panic;

    use monad_consensus_types::{
        block::Block,
        ledger::LedgerCommitInfo,
        payload::{ExecutionArtifacts, Payload, TransactionList},
        quorum_certificate::{QcInfo, QuorumCertificate},
        validation::{Hasher, Sha256Hash},
        voting::VoteInfo,
    };
    use monad_crypto::NopSignature;
    use monad_testutil::{
        signing::{get_key, MockSignatures},
        validators::create_keys_w_validators,
    };
    use monad_types::{BlockId, Hash, NodeId, Round};
    use monad_validator::validator_set::{ValidatorSet, ValidatorSetType};

    use super::BlockSyncManager;
    use crate::{command::ConsensusCommand, BlockSyncMessage, BlockSyncResult};
    type ST = NopSignature;
    type SC = MockSignatures;
    type VT = ValidatorSet;
    type QC = QuorumCertificate<SC>;

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

    #[test]
    fn test_handle_request_block_sync_message_basic_functionality() {
        let mut manager = BlockSyncManager::<SC>::default();
        let (_, _, valset, _) = create_keys_w_validators::<SC>(4);

        let qc = &QC::new::<Sha256Hash>(
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

        let cmds = manager.request::<ST, VT>(qc, &valset);

        assert!(cmds.len() == 1);
        let (peer, bid) = match cmds[0] {
            ConsensusCommand::RequestSync { peer, block_id } => (peer, block_id),
            _ => panic!("manager didn't request a block when no inflight block is observed"),
        };

        assert!(peer == valset.get_list()[0]);
        assert!(bid == qc.info.vote.id);

        // repeated request would yield no result
        for _ in 0..1000 {
            let cmds = manager.request::<ST, VT>(qc, &valset);
            assert!(cmds.is_empty());
        }

        let qc = &QC::new::<Sha256Hash>(
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
        let cmds = manager.request::<ST, VT>(qc, &valset);

        assert!(cmds.len() == 1);
        let (peer, bid) = match cmds[0] {
            ConsensusCommand::RequestSync { peer, block_id } => (peer, block_id),
            _ => panic!("manager didn't request a block when no inflight block is observed"),
        };

        assert!(peer == valset.get_list()[0]);
        assert!(bid == qc.info.vote.id);
    }

    #[test]
    fn test_handle_retrieval() {
        let mut manager = BlockSyncManager::<SC>::default();
        let (_, _, valset, _) = create_keys_w_validators::<SC>(4);

        // first qc
        let qc_1 = &QC::new::<Sha256Hash>(
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

        let cmds = manager.request::<ST, VT>(qc_1, &valset);

        assert!(cmds.len() == 1);
        let (peer_1, bid) = match cmds[0] {
            ConsensusCommand::RequestSync { peer, block_id } => (peer, block_id),
            _ => panic!("manager didn't request a block when no inflight block is observed"),
        };

        assert!(peer_1 == valset.get_list()[0]);
        assert!(bid == qc_1.info.vote.id);

        // second qc
        let qc_2 = &QC::new::<Sha256Hash>(
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

        let cmds = manager.request::<ST, VT>(qc_2, &valset);

        assert!(cmds.len() == 1);
        let (peer_2, bid) = match cmds[0] {
            ConsensusCommand::RequestSync { peer, block_id } => (peer, block_id),
            _ => panic!("manager didn't request a block when no inflight block is observed"),
        };

        assert!(peer_2 == valset.get_list()[0]);
        assert!(bid == qc_2.info.vote.id);

        // third request
        let qc_3 = &QC::new::<Sha256Hash>(
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

        let cmds = manager.request::<ST, VT>(qc_3, &valset);

        assert!(cmds.len() == 1);
        let (peer_3, bid) = match cmds[0] {
            ConsensusCommand::RequestSync { peer, block_id } => (peer, block_id),
            _ => panic!("manager didn't request a block when no inflight block is observed"),
        };

        assert!(peer_3 == valset.get_list()[0]);
        assert!(bid == qc_3.info.vote.id);

        let keypair = get_key(6);

        let payload = Payload {
            txns: TransactionList(vec![]),
            header: ExecutionArtifacts::zero(),
            seq_num: 0,
        };

        let block_1 = Block::new::<FakeHasher1>(
            NodeId(keypair.pubkey()),
            Round(3),
            &payload,
            &QC::new::<Sha256Hash>(
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
            &QC::new::<Sha256Hash>(
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
            &QC::new::<Sha256Hash>(
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

        let msg_with_block_1 = BlockSyncMessage::<SC>::BlockFound(block_1.clone());

        let msg_no_block_2 = BlockSyncMessage::<SC>::NotAvailable(BlockId(Hash([0x02_u8; 32])));

        let msg_with_block_2 = BlockSyncMessage::<SC>::BlockFound(block_2.clone());

        let msg_no_block_3 = BlockSyncMessage::<SC>::NotAvailable(BlockId(Hash([0x03_u8; 32])));

        let msg_with_block_3 = BlockSyncMessage::<SC>::BlockFound(block_3.clone());

        // arbitrary response should be rejected
        let BlockSyncResult::<ST, SC>::IllegalResponse =
            manager.handle_retrieval(&NodeId(keypair.pubkey()), msg_no_block_1, &valset)
        else {
            panic!("illegal response is processed");
        };

        // valid message from invalid individual should still get dropped

        let BlockSyncResult::<ST, SC>::IllegalResponse =
            manager.handle_retrieval(&NodeId(keypair.pubkey()), msg_with_block_2.clone(), &valset)
        else {
            panic!("illegal response is processed");
        };

        let BlockSyncResult::<ST, SC>::Failed(retry_command) =
            manager.handle_retrieval(&peer_2, msg_no_block_2.clone(), &valset)
        else {
            panic!("illegal response is processed");
        };

        let ConsensusCommand::RequestSync {
            peer: peer_2,
            block_id: _,
        } = retry_command
        else {
            panic!("retry didn't create a publish command");
        };

        let BlockSyncResult::<ST, SC>::Success(b) =
            manager.handle_retrieval(&peer_1, msg_with_block_1, &valset)
        else {
            panic!("illegal response is processed");
        };

        let BlockSyncResult::<ST, SC>::Failed(retry_command) =
            manager.handle_retrieval(&peer_3, msg_no_block_3, &valset)
        else {
            panic!("illegal response is processed");
        };

        let ConsensusCommand::RequestSync {
            peer: peer_3,
            block_id: _,
        } = retry_command
        else {
            panic!("retry didn't create a publish command");
        };

        assert!(b == block_1);

        let BlockSyncResult::<ST, SC>::Failed(retry_command) =
            manager.handle_retrieval(&peer_2, msg_no_block_2, &valset)
        else {
            panic!("illegal response is processed");
        };

        let ConsensusCommand::RequestSync {
            peer: peer_2,
            block_id: _,
        } = retry_command
        else {
            panic!("retry didn't create a publish command");
        };

        let BlockSyncResult::<ST, SC>::Success(b) =
            manager.handle_retrieval(&peer_3, msg_with_block_3, &valset)
        else {
            panic!("illegal response is processed");
        };

        assert!(b == block_3);

        let BlockSyncResult::<ST, SC>::Success(b) =
            manager.handle_retrieval(&peer_2, msg_with_block_2, &valset)
        else {
            panic!("illegal response is processed");
        };

        assert!(b == block_2);
    }
}
