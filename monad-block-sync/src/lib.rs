use monad_consensus::messages::{
    consensus_message::ConsensusMessage, message::RequestBlockSyncMessage,
};
use monad_consensus_state::command::{ConsensusCommand, FetchedBlock};
use monad_consensus_types::{
    message_signature::MessageSignature, signature_collection::SignatureCollection,
};
use monad_executor::{PeerId, RouterTarget};
use monad_types::{BlockId, NodeId};
use monad_validator::validator_set::ValidatorSetType;

#[derive(Debug)]
pub struct BlockSyncState {
    round_robin_validator_selector: usize,
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
        blockid: BlockId,
        validators: &VT,
    ) -> (RouterTarget, ConsensusMessage<ST, SC>);

    fn handle_request_block_sync_message(
        &mut self,
        author: NodeId,
        s: RequestBlockSyncMessage,
    ) -> Vec<ConsensusCommand<ST, SC>>;
}

impl<ST, SC, VT> BlockSyncProcess<ST, SC, VT> for BlockSyncState
where
    ST: MessageSignature,
    SC: SignatureCollection,
    VT: ValidatorSetType,
{
    fn new() -> Self {
        BlockSyncState {
            round_robin_validator_selector: 0,
        }
    }

    fn request_block_sync(
        &mut self,
        blockid: BlockId,
        validators: &VT,
    ) -> (RouterTarget, ConsensusMessage<ST, SC>) {
        let target = RouterTarget::PointToPoint(PeerId(
            validators.get_list()[self.round_robin_validator_selector % validators.len()].0,
        ));
        self.round_robin_validator_selector += 1;
        let message =
            ConsensusMessage::RequestBlockSync(RequestBlockSyncMessage { block_id: blockid });
        (target, message)
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
                block,
            }),
        )]
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
    use monad_executor::RouterTarget;
    use monad_testutil::{signing::get_key, validators::create_keys_w_validators};
    use monad_types::{BlockId, Hash, NodeId};
    use monad_validator::validator_set::{ValidatorSet, ValidatorSetType};

    use crate::{BlockSyncProcess, BlockSyncState};
    type ST = NopSignature;
    type SC = MultiSig<ST>;
    type VT = ValidatorSet;

    fn verify_target_and_message(
        target: &RouterTarget,
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
            RouterTarget::PointToPoint(node) => {
                assert_eq!(node.0, desired_pubkey);
            }
            _ => panic!("request_block_sync didn't produce a valid routing target"),
        }
    }

    #[test]
    fn test_request_block_sync_basic_functionality() {
        let mut process: BlockSyncState = BlockSyncProcess::<ST, SC, VT>::new();
        let (_, _, valset, _) = create_keys_w_validators::<SC>(4);

        let (target, message): (RouterTarget, ConsensusMessage<ST, SC>) =
            process.request_block_sync(BlockId(Hash([0x00_u8; 32])), &valset);

        verify_target_and_message(
            &target,
            &message,
            valset.get_list()[0].0,
            BlockId(Hash([0x00_u8; 32])),
        );
    }

    #[test]
    fn test_request_block_sync_round_robin() {
        let mut process: BlockSyncState = BlockSyncProcess::<ST, SC, VT>::new();
        let (_, _, valset, _) = create_keys_w_validators::<SC>(4);

        let (mut target, mut message): (RouterTarget, ConsensusMessage<ST, SC>) =
            process.request_block_sync(BlockId(Hash([0x00_u8; 32])), &valset);
        for i in 0..15 {
            verify_target_and_message(
                &target,
                &message,
                valset.get_list()[i % 4].0,
                BlockId(Hash([0x00_u8; 32])),
            );
            (target, message) = process.request_block_sync(BlockId(Hash([0x00_u8; 32])), &valset);
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
