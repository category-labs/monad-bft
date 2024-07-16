use std::marker::PhantomData;

use monad_consensus::{
    messages::message::{BlockSyncResponseMessage, RequestBlockSyncMessage},
    validation::signing::Validated,
};
use monad_consensus_state::ConsensusState;
use monad_consensus_types::{
    block::{Block, BlockPolicy},
    block_validator::BlockValidator,
    metrics::Metrics,
    payload::StateRootValidator,
    signature_collection::SignatureCollection,
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_eth_reserve_balance::ReserveBalanceCacheTrait;
use monad_executor_glue::{
    BlockSyncEvent, Command, FetchedBlock, LedgerCommand, MonadEvent, RouterCommand,
};
use monad_types::{BlockId, NodeId, RouterTarget};
use monad_validator::validator_set::ValidatorSetTypeFactory;

use crate::{handle_validation_error, MonadState, VerifiedMonadMessage};

/// Responds to BlockSync requests from other nodes
#[derive(Debug)]
pub(crate) struct BlockSyncResponder {}

pub(crate) enum BlockSyncCommand<SCT: SignatureCollection> {
    /// Fetch the block from consensus ledger
    FetchBlock {
        requester: NodeId<SCT::NodeIdPubKey>,
        block_id: BlockId,
    },
    /// Respond to the block sync request
    BlockSyncResponse {
        requester: NodeId<SCT::NodeIdPubKey>,
        block_id: BlockId,
        response: Validated<BlockSyncResponseMessage<SCT>>,
    },
}

impl BlockSyncResponder {
    /// Send a command to Ledger to fetch the block to respond with
    pub(crate) fn handle_request_block_sync_message<SCT: SignatureCollection>(
        &self,
        requester: NodeId<SCT::NodeIdPubKey>,
        s: RequestBlockSyncMessage,
        consensus_cached_block: Option<&Block<SCT>>,
    ) -> Vec<BlockSyncCommand<SCT>> {
        if let Some(block) = consensus_cached_block {
            // use retrieved block if currently cached in pending block tree
            vec![BlockSyncCommand::BlockSyncResponse {
                requester,
                block_id: s.block_id,
                response: Validated::new(BlockSyncResponseMessage::BlockFound(block.clone())),
            }]
        } else {
            // else ask ledger
            vec![BlockSyncCommand::FetchBlock {
                requester,
                block_id: s.block_id,
            }]
        }
    }
}

pub(super) struct BlockSyncChildState<'a, ST, SCT, BPT, RBCT, VTF, LT, TT, BVT, SVT, ASVT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    BPT: BlockPolicy<SCT, RBCT>,
    RBCT: ReserveBalanceCacheTrait,
    BVT: BlockValidator<SCT, RBCT, BPT>,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    block_sync_responder: &'a BlockSyncResponder,

    /// BlockSyncResponder queries consensus first when receiving
    /// BlockSyncRequest
    consensus: &'a ConsensusState<ST, SCT, RBCT, BPT>,

    metrics: &'a mut Metrics,

    _phantom: PhantomData<(ST, SCT, BPT, RBCT, VTF, LT, TT, BVT, SVT, ASVT)>,
}

impl<'a, ST, SCT, BPT, RBCT, VTF, LT, TT, BVT, SVT, ASVT>
    BlockSyncChildState<'a, ST, SCT, BPT, RBCT, VTF, LT, TT, BVT, SVT, ASVT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    BPT: BlockPolicy<SCT, RBCT>,
    RBCT: ReserveBalanceCacheTrait,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    BVT: BlockValidator<SCT, RBCT, BPT>,
    SVT: StateRootValidator,
{
    pub(super) fn new(
        monad_state: &'a mut MonadState<ST, SCT, BPT, RBCT, VTF, LT, TT, BVT, SVT, ASVT>,
    ) -> Self {
        Self {
            block_sync_responder: &monad_state.block_sync_responder,
            consensus: &monad_state.consensus,
            metrics: &mut monad_state.metrics,
            _phantom: PhantomData,
        }
    }

    pub(super) fn update(&mut self, event: BlockSyncEvent<SCT>) -> Vec<BlockSyncCommand<SCT>> {
        let cmds = match event {
            BlockSyncEvent::BlockSyncRequest {
                sender,
                unvalidated_request,
            } => {
                let validated_request = match unvalidated_request.validate() {
                    Ok(req) => req,
                    Err(e) => {
                        handle_validation_error(e, self.metrics);
                        // TODO-2: collect evidence
                        let evidence_cmds = vec![];
                        return evidence_cmds;
                    }
                }
                .into_inner();
                let block_id = validated_request.block_id;
                self.block_sync_responder.handle_request_block_sync_message(
                    sender,
                    validated_request,
                    self.consensus.fetch_uncommitted_block(&block_id),
                )
            }
            BlockSyncEvent::FetchedBlock(fetched_block) => {
                vec![BlockSyncCommand::BlockSyncResponse {
                    requester: fetched_block.requester,
                    block_id: fetched_block.block_id,
                    response: Validated::new(match fetched_block.unverified_block {
                        Some(b) => BlockSyncResponseMessage::BlockFound(b),
                        None => BlockSyncResponseMessage::NotAvailable(fetched_block.block_id),
                    }),
                }]
            }
        };
        cmds
    }
}

impl<ST, SCT> From<BlockSyncCommand<SCT>>
    for Vec<Command<MonadEvent<ST, SCT>, VerifiedMonadMessage<ST, SCT>, Block<SCT>, SCT>>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    fn from(cmd: BlockSyncCommand<SCT>) -> Self {
        match cmd {
            BlockSyncCommand::BlockSyncResponse {
                requester: author,
                block_id,
                response,
            } => {
                vec![Command::RouterCommand(RouterCommand::Publish {
                    target: RouterTarget::PointToPoint(author),
                    message: VerifiedMonadMessage::BlockSyncResponse(response),
                })]
            }
            BlockSyncCommand::FetchBlock {
                requester,
                block_id,
            } => vec![Command::LedgerCommand(LedgerCommand::LedgerFetch(
                requester,
                block_id,
                Box::new(move |block: Option<Block<_>>| {
                    MonadEvent::BlockSyncEvent(BlockSyncEvent::FetchedBlock(FetchedBlock {
                        requester,
                        block_id,
                        unverified_block: block,
                    }))
                }),
            ))],
        }
    }
}
