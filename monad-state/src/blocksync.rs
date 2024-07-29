use std::{
    collections::{hash_map::Entry, BTreeSet, HashMap},
    marker::PhantomData,
    time::Duration,
};

use itertools::Itertools;
use monad_consensus::messages::message::{BlockSyncResponseMessage, RequestBlockSyncMessage};
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
use monad_eth_reserve_balance::{state_backend::StateBackend, ReserveBalanceCacheTrait};
use monad_executor_glue::{
    BlockSyncEvent, Command, ConsensusEvent, LedgerCommand, LoopbackCommand, MonadEvent,
    RouterCommand, TimerCommand,
};
use monad_types::{BlockId, NodeId, RouterTarget, TimeoutVariant};
use monad_validator::{
    validator_set::{ValidatorSetType, ValidatorSetTypeFactory},
    validators_epoch_mapping::ValidatorsEpochMapping,
};
use rand::{prelude::SliceRandom, SeedableRng};
use rand_chacha::ChaCha8Rng;

use crate::{MonadState, VerifiedMonadMessage};

/// Responds to BlockSync requests from other nodes
#[derive(Debug)]
pub(crate) struct BlockSync<ST: CertificateSignatureRecoverable> {
    requests: HashMap<BlockId, BTreeSet<NodeId<CertificateSignaturePubKey<ST>>>>,
    self_requests: HashMap<BlockId, Option<NodeId<CertificateSignaturePubKey<ST>>>>,
    rng: ChaCha8Rng,
}

impl<ST: CertificateSignatureRecoverable> Default for BlockSync<ST> {
    fn default() -> Self {
        Self {
            requests: Default::default(),
            self_requests: Default::default(),
            rng: ChaCha8Rng::seed_from_u64(123456),
        }
    }
}

pub(crate) enum BlockSyncCommand<SCT: SignatureCollection> {
    SendRequest {
        to: NodeId<SCT::NodeIdPubKey>,
        request: RequestBlockSyncMessage,
    },
    ScheduleTimeout(BlockId),
    ResetTimeout(BlockId),
    /// Respond to an external block sync request
    SendResponse {
        to: NodeId<SCT::NodeIdPubKey>,
        response: BlockSyncResponseMessage<SCT>,
    },
    /// Fetch the block from consensus ledger
    FetchBlock(BlockId),
    /// Response to a BlockSyncEvent::SelfRequest
    Emit(Block<SCT>),
}

pub(super) struct BlockSyncChildState<'a, ST, SCT, BPT, SBT, RBCT, VTF, LT, TT, BVT, SVT, ASVT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    BPT: BlockPolicy<SCT, SBT, RBCT>,
    SBT: StateBackend,
    RBCT: ReserveBalanceCacheTrait<SBT>,
    BVT: BlockValidator<SCT, BPT, SBT, RBCT>,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    block_sync: &'a mut BlockSync<ST>,

    /// BlockSync queries consensus first when receiving
    /// BlockSyncRequest
    consensus: &'a ConsensusState<SCT, BPT, SBT, RBCT>,
    val_epoch_map: &'a ValidatorsEpochMapping<VTF, SCT>,
    delta: &'a Duration,
    nodeid: &'a NodeId<CertificateSignaturePubKey<ST>>,

    metrics: &'a mut Metrics,

    _phantom: PhantomData<(ST, SCT, BPT, RBCT, VTF, LT, TT, BVT, SVT, ASVT)>,
}

impl<'a, ST, SCT, BPT, SBT, RBCT, VTF, LT, TT, BVT, SVT, ASVT>
    BlockSyncChildState<'a, ST, SCT, BPT, SBT, RBCT, VTF, LT, TT, BVT, SVT, ASVT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    BPT: BlockPolicy<SCT, SBT, RBCT>,
    SBT: StateBackend,
    RBCT: ReserveBalanceCacheTrait<SBT>,
    BVT: BlockValidator<SCT, BPT, SBT, RBCT>,
    SVT: StateRootValidator,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    pub(super) fn new(
        monad_state: &'a mut MonadState<ST, SCT, BPT, SBT, RBCT, VTF, LT, TT, BVT, SVT, ASVT>,
    ) -> Self {
        Self {
            block_sync: &mut monad_state.block_sync,
            consensus: &monad_state.consensus,
            val_epoch_map: &monad_state.val_epoch_map,
            delta: &monad_state.consensus_config.delta,
            nodeid: &monad_state.nodeid,
            metrics: &mut monad_state.metrics,
            _phantom: PhantomData,
        }
    }

    fn pick_peer(&mut self) -> NodeId<CertificateSignaturePubKey<ST>> {
        let epoch = self.consensus.get_current_epoch();
        let validators = self
            .val_epoch_map
            .get_val_set(&epoch)
            .expect("current epoch exists");
        let members = validators.get_members();
        let members = members
            .iter()
            .filter(|(peer, _)| peer != &self.nodeid)
            .collect_vec();
        assert!(!members.is_empty(), "no nodes to blocksync from");
        *members
            .choose_weighted(&mut self.block_sync.rng, |(_peer, weight)| weight.0)
            .expect("nonempty")
            .0
    }

    pub(super) fn update(
        &mut self,
        event: BlockSyncEvent<SCT>,
    ) -> Vec<WrappedBlockSyncCommand<SCT>> {
        let mut cmds = Vec::new();
        match event {
            BlockSyncEvent::Request {
                sender,
                request: RequestBlockSyncMessage { block_id },
            } => {
                let consensus_cached_block = self.consensus.fetch_uncommitted_block(&block_id);

                if let Some(block) = consensus_cached_block {
                    // use retrieved block if currently cached in pending block tree
                    cmds.push(BlockSyncCommand::SendResponse {
                        to: sender,
                        response: BlockSyncResponseMessage::BlockFound(block.clone()),
                    })
                } else if !self.block_sync.self_requests.contains_key(&block_id) {
                    // ask ledger
                    let entry = self.block_sync.requests.entry(block_id).or_default();
                    entry.insert(sender);
                    cmds.push(BlockSyncCommand::FetchBlock(block_id))
                } else {
                    cmds.push(BlockSyncCommand::SendResponse {
                        to: sender,
                        response: BlockSyncResponseMessage::NotAvailable(block_id),
                    })
                }
            }
            BlockSyncEvent::SelfRequest {
                request: RequestBlockSyncMessage { block_id },
            } => {
                if let Entry::Vacant(entry) = self.block_sync.self_requests.entry(block_id) {
                    entry.insert(None);
                    cmds.push(BlockSyncCommand::FetchBlock(block_id));
                } else {
                    // already have outstanding request, don't need to do anything
                }
            }
            BlockSyncEvent::SelfCancelRequest {
                request: RequestBlockSyncMessage { block_id },
            } => {
                let _removed = self.block_sync.self_requests.remove(&block_id);
            }

            BlockSyncEvent::SelfResponse { response } => {
                let block_id = response.get_block_id();
                let requesters = self
                    .block_sync
                    .requests
                    .remove(&block_id)
                    .unwrap_or_default();
                cmds.extend(requesters.into_iter().map(|requester| {
                    BlockSyncCommand::SendResponse {
                        to: requester,
                        response: response.clone(),
                    }
                }));
                if self
                    .block_sync
                    .self_requests
                    .get(&block_id)
                    .is_some_and(|to| to.is_none())
                {
                    match response {
                        BlockSyncResponseMessage::BlockFound(block) => {
                            cmds.push(BlockSyncCommand::Emit(block));
                            let removed = self.block_sync.self_requests.remove(&block_id).is_some();
                            assert!(removed);
                        }
                        BlockSyncResponseMessage::NotAvailable(_) => {
                            let to = self.pick_peer();
                            self.block_sync.self_requests.insert(block_id, Some(to));
                            self.metrics.blocksync_events.blocksync_request += 1;
                            cmds.push(BlockSyncCommand::SendRequest {
                                to,
                                request: RequestBlockSyncMessage { block_id },
                            });
                            cmds.push(BlockSyncCommand::ScheduleTimeout(block_id));
                        }
                    };
                }
            }
            BlockSyncEvent::Response { sender, response } => {
                let block_id = response.get_block_id();
                if self
                    .block_sync
                    .self_requests
                    .get(&block_id)
                    .is_some_and(|to| to == &Some(sender))
                {
                    cmds.push(BlockSyncCommand::ResetTimeout(block_id));
                    match response {
                        BlockSyncResponseMessage::BlockFound(block) => {
                            self.metrics.blocksync_events.blocksync_response_successful += 1;
                            cmds.push(BlockSyncCommand::Emit(block));
                            let removed = self.block_sync.self_requests.remove(&block_id).is_some();
                            assert!(removed);
                        }
                        BlockSyncResponseMessage::NotAvailable(_) => {
                            self.metrics.blocksync_events.blocksync_response_failed += 1;
                            let to = self.pick_peer();
                            self.block_sync.self_requests.insert(block_id, Some(to));
                            self.metrics.blocksync_events.blocksync_request += 1;
                            cmds.push(BlockSyncCommand::SendRequest {
                                to,
                                request: RequestBlockSyncMessage { block_id },
                            });
                            cmds.push(BlockSyncCommand::ScheduleTimeout(block_id));
                        }
                    };
                } else {
                    self.metrics.blocksync_events.blocksync_response_unexpected += 1;
                }
            }
            BlockSyncEvent::Timeout(request) => {
                let block_id = request.block_id;
                if self
                    .block_sync
                    .self_requests
                    .get(&block_id)
                    .is_some_and(|to| to.is_some())
                {
                    let to = self.pick_peer();
                    self.block_sync.self_requests.insert(block_id, Some(to));
                    self.metrics.blocksync_events.blocksync_request += 1;
                    cmds.push(BlockSyncCommand::SendRequest {
                        to,
                        request: RequestBlockSyncMessage { block_id },
                    });
                    cmds.push(BlockSyncCommand::ScheduleTimeout(block_id));
                }
            }
        };
        cmds.into_iter()
            .map(|command| WrappedBlockSyncCommand {
                request_timeout: *self.delta * 3,
                command,
            })
            .collect()
    }
}

pub(crate) struct WrappedBlockSyncCommand<SCT: SignatureCollection> {
    request_timeout: Duration,
    command: BlockSyncCommand<SCT>,
}

impl<ST, SCT> From<WrappedBlockSyncCommand<SCT>>
    for Vec<Command<MonadEvent<ST, SCT>, VerifiedMonadMessage<ST, SCT>, SCT>>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    fn from(wrapped: WrappedBlockSyncCommand<SCT>) -> Self {
        match wrapped.command {
            BlockSyncCommand::SendRequest { to, request } => {
                vec![Command::RouterCommand(RouterCommand::Publish {
                    target: RouterTarget::PointToPoint(to),
                    message: VerifiedMonadMessage::BlockSyncRequest(request),
                })]
            }
            BlockSyncCommand::ScheduleTimeout(block_id) => {
                vec![Command::TimerCommand(TimerCommand::Schedule {
                    duration: wrapped.request_timeout,
                    variant: TimeoutVariant::BlockSync(block_id),
                    on_timeout: MonadEvent::BlockSyncEvent(BlockSyncEvent::Timeout(
                        RequestBlockSyncMessage { block_id },
                    )),
                })]
            }
            BlockSyncCommand::ResetTimeout(block_id) => {
                vec![Command::TimerCommand(TimerCommand::ScheduleReset(
                    TimeoutVariant::BlockSync(block_id),
                ))]
            }
            BlockSyncCommand::SendResponse { to, response } => {
                vec![Command::RouterCommand(RouterCommand::Publish {
                    target: RouterTarget::PointToPoint(to),
                    message: VerifiedMonadMessage::BlockSyncResponse(response),
                })]
            }
            BlockSyncCommand::FetchBlock(block_id) => {
                vec![Command::LedgerCommand(LedgerCommand::LedgerFetch(block_id))]
            }
            BlockSyncCommand::Emit(block) => {
                vec![Command::LoopbackCommand(LoopbackCommand::Forward(
                    MonadEvent::ConsensusEvent(ConsensusEvent::BlockSync(block)),
                ))]
            }
        }
    }
}
