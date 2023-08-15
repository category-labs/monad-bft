use monad_consensus::messages::{
    consensus_message::ConsensusMessage, message::RequestBlockSyncMessage,
};
use monad_consensus_state::command::ConsensusCommand;
use monad_consensus_types::{
    message_signature::MessageSignature, signature_collection::SignatureCollection,
};
use monad_executor::{PeerId, RouterTarget};
use monad_types::{BlockId, NodeId};
use monad_validator::validator_set::ValidatorSetType;

pub struct BlockSyncState {}

pub trait BlockSyncProcess<ST, SCT, VT>
where
    ST: MessageSignature,
    SCT: SignatureCollection,
    VT: ValidatorSetType,
{
    fn new() -> Self;

    fn request_block_sync(
        &mut self,
        blockid: BlockId,
        validators: &VT,
    ) -> (RouterTarget, ConsensusMessage<ST, SCT>);

    fn handle_request_block_sync_message(
        &mut self,
        author: NodeId,
        s: RequestBlockSyncMessage,
        validators: &VT,
    ) -> Vec<ConsensusCommand<ST, SCT>>;
}

impl<ST, SCT, VT> BlockSyncProcess<ST, SCT, VT> for BlockSyncState
where
    ST: MessageSignature,
    SCT: SignatureCollection,
    VT: ValidatorSetType,
{
    fn new() -> Self {
        BlockSyncState {}
    }
    fn request_block_sync(
        &mut self,
        blockid: BlockId,
        validators: &VT,
    ) -> (RouterTarget, ConsensusMessage<ST, SCT>) {
        let target = RouterTarget::PointToPoint(PeerId(validators.get_list()[0].0));
        let message =
            ConsensusMessage::RequestBlockSync(RequestBlockSyncMessage { block_id: blockid });
        (target, message)
    }

    fn handle_request_block_sync_message(
        &mut self,
        author: NodeId,
        s: RequestBlockSyncMessage,
        validators: &VT,
    ) -> Vec<ConsensusCommand<ST, SCT>> {
        vec![]
    }
}
