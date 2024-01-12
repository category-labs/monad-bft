use monad_consensus::{
    messages::message::{BlockSyncResponseMessage, RequestBlockSyncMessage},
    validation::signing::Validated,
};
use monad_consensus_types::{block::Block, signature_collection::SignatureCollection};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_executor_glue::{
    BlockSyncEvent, Command, FetchedBlock, LedgerCommand, MonadEvent, RouterCommand,
};
use monad_types::{NodeId, RouterTarget};

use crate::VerifiedMonadMessage;

/// Responds to BlockSync requests from other nodes
#[derive(Debug)]
pub(crate) struct BlockSyncResponder {}

impl BlockSyncResponder {
    /// Send a command to Ledger to fetch the block to respond with
    pub(crate) fn handle_request_block_sync_message<ST, SCT, C>(
        &mut self,
        author: NodeId<SCT::NodeIdPubKey>,
        s: RequestBlockSyncMessage,
        consensus_cached_block: Option<&Block<SCT>>,
    ) -> Vec<Command<MonadEvent<ST, SCT>, VerifiedMonadMessage<ST, SCT>, Block<SCT>, C, SCT>>
    where
        ST: CertificateSignatureRecoverable,
        SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    {
        if let Some(block) = consensus_cached_block {
            // use retrieved block if currently cached in pending block tree
            vec![Command::RouterCommand(RouterCommand::Publish {
                target: RouterTarget::PointToPoint(author),
                message: VerifiedMonadMessage::BlockSyncResponse(Validated::new(
                    BlockSyncResponseMessage::BlockFound(block.clone().into()),
                )),
            })]
        } else {
            // else ask ledger
            vec![Command::LedgerCommand(LedgerCommand::LedgerFetch(
                author,
                s.block_id,
                Box::new(move |block: Option<Block<SCT>>| {
                    let requester = author;
                    let block_id = s.block_id;

                    MonadEvent::BlockSyncEvent(BlockSyncEvent::FetchedBlock(FetchedBlock {
                        requester,
                        block_id,
                        unverified_block: block.map(|b| b.into()),
                    }))
                }),
            ))]
        }
    }
}
