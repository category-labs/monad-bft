use monad_consensus::messages::message::RequestBlockSyncMessage;
use monad_consensus_state::command::ConsensusCommand;
use monad_consensus_types::{command::FetchedBlock, signature_collection::SignatureCollection};
use monad_types::NodeId;

/// Responds to BlockSync requests from other nodes
#[derive(Debug)]
pub(crate) struct BlockSyncResponder {}

impl BlockSyncResponder {
    /// Send a command to Ledger to fetch the block to respond with
    pub(crate) fn handle_request_block_sync_message<SCT: SignatureCollection>(
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
    use monad_crypto::{hasher::Hash, NopSignature};
    use monad_testutil::signing::get_key;
    use monad_types::{BlockId, NodeId};

    use crate::BlockSyncResponder;
    type SignatureType = NopSignature;
    type SignatureCollectionType = MultiSig<SignatureType>;

    #[test]
    fn test_handle_request_block_sync_message_basic_functionality() {
        let mut responder = BlockSyncResponder {};
        let keypair = get_key(6);
        let command: Vec<ConsensusCommand<SignatureCollectionType>> =
            <BlockSyncResponder>::handle_request_block_sync_message(
                &mut responder,
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
