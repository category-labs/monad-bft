use std::collections::{hash_map::Entry, HashMap};

use monad_consensus::messages::{
    consensus_message::ConsensusMessage,
    message::{BlockSyncMessage, RequestBlockSyncMessage},
};
use monad_consensus_state::command::{ConsensusCommand, FetchedBlock};
use monad_consensus_types::{
    block::Block, message_signature::MessageSignature, signature_collection::SignatureCollection,
};
use monad_executor::{PeerId, RouterTarget};
use monad_types::{BlockId, NodeId};
use monad_validator::validator_set::ValidatorSetType;
const DEFAULT_AUTHOR_INDEX: usize = 0;
const DEFAULT_RETRY_INIT: usize = 0;
#[derive(Debug)]
pub struct BlockSyncState {
    request_mapping: HashMap<(PeerId, BlockId), usize>,
}
pub enum BlockRetrievalResult<ST, SC: SignatureCollection> {
    Success(Block<SC>),               // retrieved
    Failed(ConsensusCommand<ST, SC>), // unable to retrieve
    IllegalResponse(PeerId),          // never requested from this peer
}
pub trait BlockSyncProcess<ST, SC, VT>
where
    ST: MessageSignature,
    SC: SignatureCollection,
    VT: ValidatorSetType,
{
    fn new() -> Self;

    fn request_block_sync(
        &mut self,
        bid: BlockId,
        validators: &VT,
    ) -> Option<(PeerId, ConsensusMessage<ST, SC>)>;

    fn handle_request_block_sync_message(
        &mut self,
        author: NodeId,
        s: RequestBlockSyncMessage,
    ) -> Vec<ConsensusCommand<ST, SC>>;

    fn handle_block_sync_message(
        &mut self,
        author: NodeId,
        s: BlockSyncMessage<SC>,
        validators: &VT,
    ) -> BlockRetrievalResult<ST, SC>;
}

impl<ST, SC, VT> BlockSyncProcess<ST, SC, VT> for BlockSyncState
where
    ST: MessageSignature,
    SC: SignatureCollection,
    VT: ValidatorSetType,
{
    fn new() -> Self {
        BlockSyncState {
            request_mapping: HashMap::new(),
        }
    }

    fn request_block_sync(
        &mut self,
        bid: BlockId,
        validators: &VT,
    ) -> Option<(PeerId, ConsensusMessage<ST, SC>)> {
        assert!(validators.len() > 0);
        let peer = PeerId(validators.get_list()[DEFAULT_AUTHOR_INDEX].0);
        let message = ConsensusMessage::RequestBlockSync(RequestBlockSyncMessage { block_id: bid });
        match self.request_mapping.entry((peer, bid)) {
            Entry::Occupied(_entry) => None,
            Entry::Vacant(entry) => {
                entry.insert(DEFAULT_RETRY_INIT);
                Some((peer, message))
            }
        }
    }

    fn handle_request_block_sync_message(
        &mut self,
        author: NodeId,
        s: RequestBlockSyncMessage,
    ) -> Vec<ConsensusCommand<ST, SC>> {
        vec![ConsensusCommand::LedgerFetch(
            s.block_id,
            Box::new(move |block| FetchedBlock {
                requester: author,
                block_id: s.block_id,
                block,
            }),
        )]
    }

    fn handle_block_sync_message(
        &mut self,
        author: NodeId,
        s: BlockSyncMessage<SC>,
        validators: &VT,
    ) -> BlockRetrievalResult<ST, SC> {
        assert!(validators.len() > 0);

        let peer = PeerId(author.0);
        let bid = s.block_id;

        match self.request_mapping.entry((peer, bid)) {
            Entry::Occupied(entry) => {
                match s.block {
                    Some(block) => {
                        // block retrieve successful
                        entry.remove_entry();
                        BlockRetrievalResult::Success(block)
                    }
                    None => {
                        // block retrieve failed, re-request
                        let (_, mut retry) = entry.remove_entry();
                        retry += 1;

                        let peer = PeerId(validators.get_list()[retry % validators.len()].0);
                        let message = ConsensusMessage::RequestBlockSync(RequestBlockSyncMessage {
                            block_id: bid,
                        });
                        self.request_mapping.insert((peer, bid), retry);
                        BlockRetrievalResult::Failed(ConsensusCommand::Publish {
                            target: RouterTarget::PointToPoint(peer),
                            message,
                        })
                    }
                }
            }
            _ => BlockRetrievalResult::IllegalResponse(peer),
        }
    }
}

#[cfg(test)]
mod test {
    use core::panic;

    use monad_consensus::messages::{
        consensus_message::ConsensusMessage, message::RequestBlockSyncMessage,
    };
    use monad_consensus_state::command::ConsensusCommand;
    use monad_consensus_types::{
        block::Block,
        ledger::LedgerCommitInfo,
        payload::{ExecutionArtifacts, Payload, TransactionList},
        quorum_certificate::{QcInfo, QuorumCertificate},
        validation::Sha256Hash,
        voting::VoteInfo,
    };
    use monad_crypto::{secp256k1::PubKey, NopSignature};
    use monad_executor::{PeerId, RouterTarget};
    use monad_testutil::{
        signing::{get_key, MockSignatures},
        validators::create_keys_w_validators,
    };
    use monad_types::{BlockId, Hash, NodeId, Round};
    use monad_validator::validator_set::{ValidatorSet, ValidatorSetType};

    use crate::{BlockRetrievalResult, BlockSyncMessage, BlockSyncProcess, BlockSyncState};
    type ST = NopSignature;
    type SC = MockSignatures;
    type VT = ValidatorSet;
    type QC = QuorumCertificate<SC>;

    fn verify_target_and_message(
        target: &PeerId,
        message: &ConsensusMessage<ST, SC>,
        desired_pubkey: PubKey,
        desired_block_id: BlockId,
    ) {
        match message {
            ConsensusMessage::RequestBlockSync(rbsm) => {
                let block_id = rbsm.block_id;
                assert_eq!(block_id, desired_block_id);
            }
            _ => panic!("request_block_sync didn't produce a valid ConsensusMessage type"),
        }

        match target {
            PeerId(pubkey) => {
                assert_eq!(*pubkey, desired_pubkey);
            }
            _ => panic!("request_block_sync didn't produce a valid routing target"),
        }
    }

    #[test]
    fn test_request_block_sync_basic_functionality() {
        let mut process: BlockSyncState = BlockSyncProcess::<ST, SC, VT>::new();
        let (_, _, valset, _) = create_keys_w_validators::<SC>(4);

        if let Some((target, message)) =
            process.request_block_sync(BlockId(Hash([0x00_u8; 32])), &valset)
        {
            verify_target_and_message(
                &target,
                &message,
                valset.get_list()[0].0,
                BlockId(Hash([0x00_u8; 32])),
            );
        } else {
            panic!("request_block_sync no return")
        }
    }

    #[test]
    fn test_request_block_sync_no_duplicate() {
        let mut process: BlockSyncState = BlockSyncProcess::<ST, SC, VT>::new();
        let (_, _, valset, _) = create_keys_w_validators::<SC>(4);

        if let Some((target, message)) =
            process.request_block_sync(BlockId(Hash([0x00_u8; 32])), &valset)
        {
            verify_target_and_message(
                &target,
                &message,
                valset.get_list()[0].0,
                BlockId(Hash([0x00_u8; 32])),
            );
            for _ in 0..15 {
                assert!(
                    <BlockSyncState as BlockSyncProcess<ST, SC, ValidatorSet>>::request_block_sync(
                        &mut process,
                        BlockId(Hash([0x00_u8; 32])),
                        &valset
                    )
                    .is_none()
                );
            }
        } else {
            panic!("request_block_sync no return")
        }
    }

    #[test]
    fn test_handle_request_block_sync_message_basic_functionality() {
        let mut process: BlockSyncState = BlockSyncProcess::<ST, SC, VT>::new();
        let keypair = get_key(6);
        let command: Vec<ConsensusCommand<ST, SC>> =
            <BlockSyncState as BlockSyncProcess<ST, SC, VT>>::handle_request_block_sync_message(
                &mut process,
                NodeId(keypair.pubkey()),
                RequestBlockSyncMessage {
                    block_id: BlockId(Hash([0x00_u8; 32])),
                },
            );
        assert_eq!(command.len(), 1);
        let res = command
            .iter()
            .find(|c| matches!(c, ConsensusCommand::LedgerFetch(_, _)));
        assert!(res.is_some());
    }

    #[test]
    fn test_handle_block_sync_message_basic_functionality() {
        let mut process: BlockSyncState = BlockSyncProcess::<ST, SC, VT>::new();
        let (_, _, valset, _) = create_keys_w_validators::<SC>(4);

        let Some((mut target, original_request)) =
            process.request_block_sync(BlockId(Hash([0x00_u8; 32])), &valset)
        else {
            panic!("request_block_sync no return")
        };

        verify_target_and_message(
            &target,
            &original_request,
            valset.get_list()[0].0,
            BlockId(Hash([0x00_u8; 32])),
        );

        let keypair = get_key(6);

        let payload = Payload {
            txns: TransactionList(vec![]),
            header: ExecutionArtifacts::zero(),
            seq_num: 0,
        };

        let block = Block::new::<Sha256Hash>(
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

        let msg_no_block = BlockSyncMessage::<SC> {
            block_id: BlockId(Hash([0x00_u8; 32])),
            block: None,
        };

        let msg_with_block = BlockSyncMessage::<SC> {
            block_id: BlockId(Hash([0x00_u8; 32])),
            block: Some(block.clone()),
        };

        // arbitrary response should be rejected
        let BlockRetrievalResult::<ST, SC>::IllegalResponse(_) = process.handle_block_sync_message(
            NodeId(keypair.pubkey()),
            msg_no_block.clone(),
            &valset,
        ) else {
            panic!("illegal response is processed");
        };

        // valid message from invalid individual should still get dropped
        let BlockRetrievalResult::<ST, SC>::IllegalResponse(_) = process.handle_block_sync_message(
            NodeId(keypair.pubkey()),
            msg_with_block.clone(),
            &valset,
        ) else {
            panic!("illegal response is processed");
        };

        // no block response should cause round robin behaviour
        for i in 1..1000 {
            assert!(NodeId(target.0) == valset.get_list()[(i - 1) % 4]);
            if let BlockRetrievalResult::<ST, SC>::Failed(retry_command) =
                process.handle_block_sync_message(NodeId(target.0), msg_no_block.clone(), &valset)
            {
                let ConsensusCommand::Publish {
                    target: router_target,
                    message: consensus_message,
                } = retry_command
                else {
                    panic!("retry didn't create a publish command");
                };
                let RouterTarget::PointToPoint(peer) = router_target else {
                    panic!("router target is not p2p");
                };
                target = peer;
                assert!(NodeId(target.0) == valset.get_list()[i % 4]);
                if let ConsensusMessage::RequestBlockSync(m) = consensus_message {
                    assert!(m.block_id == BlockId(Hash([0x00_u8; 32])));
                } else {
                    panic!("re-request is not a request block sync");
                }
            } else {
                panic!("request didn't process as failed");
            };
        }

        // now feeding the actual block sync should complete it
        if let BlockRetrievalResult::<ST, SC>::Success(result_block) =
            process.handle_block_sync_message(NodeId(target.0), msg_with_block, &valset)
        {
            assert!(block == result_block);
        } else {
            panic!("request didn't process as successful");
        };
    }

    /**
     * Multiple block is being requested (representing different)
     */
    #[test]
    fn test_handle_multiple_block_sync() {
        let mut process: BlockSyncState = BlockSyncProcess::<ST, SC, VT>::new();
        let (_, _, valset, _) = create_keys_w_validators::<SC>(4);

        // first request
        let Some((target_1, original_request)) =
            process.request_block_sync(BlockId(Hash([0x11_u8; 32])), &valset)
        else {
            panic!("request_block_sync no return")
        };

        verify_target_and_message(
            &target_1,
            &original_request,
            valset.get_list()[0].0,
            BlockId(Hash([0x11_u8; 32])),
        );

        // second request
        let Some((target_2, original_request)) =
            process.request_block_sync(BlockId(Hash([0x22_u8; 32])), &valset)
        else {
            panic!("request_block_sync no return")
        };

        verify_target_and_message(
            &target_2,
            &original_request,
            valset.get_list()[0].0,
            BlockId(Hash([0x22_u8; 32])),
        );

        // third request
        let Some((target_3, original_request)) =
            process.request_block_sync(BlockId(Hash([0x33_u8; 32])), &valset)
        else {
            panic!("request_block_sync no return")
        };

        verify_target_and_message(
            &target_3,
            &original_request,
            valset.get_list()[0].0,
            BlockId(Hash([0x33_u8; 32])),
        );

        let keypair = get_key(6);

        let payload = Payload {
            txns: TransactionList(vec![]),
            header: ExecutionArtifacts::zero(),
            seq_num: 0,
        };

        let block = Block::new::<Sha256Hash>(
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

        let msg_no_block_1 = BlockSyncMessage::<SC> {
            block_id: BlockId(Hash([0x11_u8; 32])),
            block: None,
        };

        let msg_with_block_1 = BlockSyncMessage::<SC> {
            block_id: BlockId(Hash([0x11_u8; 32])),
            block: Some(block.clone()),
        };

        let msg_no_block_2 = BlockSyncMessage::<SC> {
            block_id: BlockId(Hash([0x22_u8; 32])),
            block: None,
        };

        let msg_with_block_2 = BlockSyncMessage::<SC> {
            block_id: BlockId(Hash([0x22_u8; 32])),
            block: Some(block.clone()),
        };

        let msg_no_block_3 = BlockSyncMessage::<SC> {
            block_id: BlockId(Hash([0x33_u8; 32])),
            block: None,
        };

        let msg_with_block_3 = BlockSyncMessage::<SC> {
            block_id: BlockId(Hash([0x33_u8; 32])),
            block: Some(block.clone()),
        };

        // arbitrary response should be rejected
        let BlockRetrievalResult::<ST, SC>::IllegalResponse(_) =
            process.handle_block_sync_message(NodeId(keypair.pubkey()), msg_no_block_1, &valset)
        else {
            panic!("illegal response is processed");
        };

        // valid message from invalid individual should still get dropped
        let BlockRetrievalResult::<ST, SC>::IllegalResponse(_) = process.handle_block_sync_message(
            NodeId(keypair.pubkey()),
            msg_with_block_2.clone(),
            &valset,
        ) else {
            panic!("illegal response is processed");
        };

        // message can arrive in any order,
        let BlockRetrievalResult::<ST, SC>::Failed(retry_command) =
            process.handle_block_sync_message(NodeId(target_2.0), msg_no_block_2.clone(), &valset)
        else {
            panic!("illegal response is processed");
        };

        let ConsensusCommand::Publish {
            target: router_target,
            message: _,
        } = retry_command
        else {
            panic!("retry didn't create a publish command");
        };
        let RouterTarget::PointToPoint(target_2) = router_target else {
            panic!("router target is not p2p");
        };

        let BlockRetrievalResult::<ST, SC>::Success(b) =
            process.handle_block_sync_message(NodeId(target_1.0), msg_with_block_1, &valset)
        else {
            panic!("illegal response is processed");
        };

        let BlockRetrievalResult::<ST, SC>::Failed(retry_command) =
            process.handle_block_sync_message(NodeId(target_3.0), msg_no_block_3, &valset)
        else {
            panic!("illegal response is processed");
        };

        let ConsensusCommand::Publish {
            target: router_target,
            message: _,
        } = retry_command
        else {
            panic!("retry didn't create a publish command");
        };
        let RouterTarget::PointToPoint(target_3) = router_target else {
            panic!("router target is not p2p");
        };

        assert!(b == block);

        let BlockRetrievalResult::<ST, SC>::Failed(retry_command) =
            process.handle_block_sync_message(NodeId(target_2.0), msg_no_block_2, &valset)
        else {
            panic!("illegal response is processed");
        };

        let ConsensusCommand::Publish {
            target: router_target,
            message: _,
        } = retry_command
        else {
            panic!("retry didn't create a publish command");
        };
        let RouterTarget::PointToPoint(target_2) = router_target else {
            panic!("router target is not p2p");
        };

        let BlockRetrievalResult::<ST, SC>::Success(b) =
            process.handle_block_sync_message(NodeId(target_3.0), msg_with_block_3, &valset)
        else {
            panic!("illegal response is processed");
        };

        assert!(b == block);

        let BlockRetrievalResult::<ST, SC>::Success(b) =
            process.handle_block_sync_message(NodeId(target_2.0), msg_with_block_2, &valset)
        else {
            panic!("illegal response is processed");
        };

        assert!(b == block);
    }
}
