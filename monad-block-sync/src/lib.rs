use std::collections::{hash_map::Entry, HashMap};

use monad_consensus::messages::{
    consensus_message::ConsensusMessage,
    message::{BlockSyncMessage, RequestBlockSyncMessage},
};
use monad_consensus_state::command::{ConsensusCommand, FetchedBlock};
use monad_consensus_types::{
    block::Block, message_signature::MessageSignature, signature_collection::SignatureCollection,
};
use monad_executor::PeerId;
use monad_types::{BlockId, NodeId};
use monad_validator::validator_set::ValidatorSetType;
const DEFAULT_AUTHOR_INDEX: usize = 0;
const DEFAULT_RETRY_INIT: usize = 0;
#[derive(Debug)]
pub struct BlockSyncState {
    request_mapping: HashMap<(PeerId, BlockId), usize>,
}
pub enum BlockRetrievalResult<ST, SC: SignatureCollection> {
    Success(Block<SC>),                         // retrieved
    Failed((PeerId, ConsensusMessage<ST, SC>)), // unable to retrieve
    IllegalResponse(PeerId),                    // never requested from this peer (slash)
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
                        BlockRetrievalResult::Failed((peer, message))
                    }
                }
            }
            _ => BlockRetrievalResult::IllegalResponse(peer),
        }
    }
}

#[cfg(test)]
mod test {
    use monad_consensus::messages::{
        consensus_message::ConsensusMessage, message::RequestBlockSyncMessage,
    };
    use monad_consensus_state::command::ConsensusCommand;
    use monad_consensus_types::multi_sig::MultiSig;
    use monad_crypto::{secp256k1::PubKey, NopSignature};
    use monad_executor::PeerId;
    use monad_testutil::{signing::get_key, validators::create_keys_w_validators};
    use monad_types::{BlockId, Hash, NodeId};
    use monad_validator::validator_set::{ValidatorSet, ValidatorSetType};

    use crate::{BlockSyncProcess, BlockSyncState};
    type ST = NopSignature;
    type SC = MultiSig<ST>;
    type VT = ValidatorSet;

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
}
