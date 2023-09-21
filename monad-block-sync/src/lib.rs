use monad_consensus::messages::message::RequestBlockSyncMessage;
use monad_consensus_state::command::{ConsensusCommand, FetchedBlock};
use monad_consensus_types::signature_collection::SignatureCollection;
use monad_types::NodeId;
use monad_validator::validator_set::ValidatorSetType;
#[derive(Debug)]
pub struct BlockSyncState {}

pub trait BlockSyncProcess<SCT, VT>
where
    SCT: SignatureCollection,
    VT: ValidatorSetType,
{
    fn new() -> Self;

    fn handle_request_block_sync_message(
        &mut self,
        author: NodeId,
        s: RequestBlockSyncMessage,
    ) -> Vec<ConsensusCommand<SCT>>;
}

impl<SCT, VT> BlockSyncProcess<SCT, VT> for BlockSyncState
where
    SCT: SignatureCollection,
    VT: ValidatorSetType,
{
    fn new() -> Self {
        BlockSyncState {}
    }

    fn handle_request_block_sync_message(
        &mut self,
        author: NodeId,
        s: RequestBlockSyncMessage,
    ) -> Vec<ConsensusCommand<SCT>> {
        vec![ConsensusCommand::LedgerFetch(
            author,
            s.block_id,
            Box::new(move |unverified_full_block| FetchedBlock {
                requester: author,
                block_id: s.block_id,
                unverified_full_block,
            }),
        )]
    }
}

#[cfg(test)]
mod test {
    use monad_consensus::messages::message::RequestBlockSyncMessage;
    use monad_consensus_state::command::ConsensusCommand;
    use monad_consensus_types::multi_sig::MultiSig;
    use monad_crypto::NopSignature;
    use monad_testutil::signing::get_key;
    use monad_types::{BlockId, Hash, NodeId};
    use monad_validator::validator_set::ValidatorSet;

    use crate::{BlockSyncProcess, BlockSyncState};
    type SignatureType = NopSignature;
    type SignatureCollectionType = MultiSig<SignatureType>;
    type ValidatorSetT = ValidatorSet;

    #[test]
    fn test_handle_request_block_sync_message_basic_functionality() {
        let mut process: BlockSyncState =
            BlockSyncProcess::<SignatureCollectionType, ValidatorSetT>::new();
        let keypair = get_key(6);
        let command: Vec<ConsensusCommand<SignatureCollectionType>> = <BlockSyncState as BlockSyncProcess<
            SignatureCollectionType,
            ValidatorSetT,
        >>::handle_request_block_sync_message(
            &mut process,
            NodeId(keypair.pubkey()),
            RequestBlockSyncMessage {
                block_id: BlockId(Hash([0x00_u8; 32])),
            },
        );
        assert_eq!(command.len(), 1);
        let res = command
            .iter()
            .find(|c| matches!(c, ConsensusCommand::LedgerFetch(_, _, _)));
        assert!(res.is_some());
    }
}
