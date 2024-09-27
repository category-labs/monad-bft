use std::{
    collections::{hash_map::Entry, BTreeSet, HashMap},
    marker::PhantomData,
    time::Duration,
};

use itertools::Itertools;
use monad_consensus::{
    messages::message::{
        BlockSyncHeadersResponse, BlockSyncPayloadResponse, BlockSyncRequestMessage,
        BlockSyncResponseMessage,
    },
    validation::signing::verify_certificates,
};
use monad_consensus_types::{
    block::{Block, BlockIdRange, BlockPolicy, BlockType, FullBlock},
    block_validator::BlockValidator,
    metrics::Metrics,
    payload::{self, Payload, PayloadId, StateRootValidator},
    signature_collection::SignatureCollection,
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable, PubKey,
};
use monad_executor_glue::{
    BlockSyncEvent, BlockSyncSelfRequester, Command, ConsensusEvent, LedgerCommand,
    LoopbackCommand, MonadEvent, RouterCommand, StateSyncEvent, TimeoutVariant, TimerCommand,
};
use monad_state_backend::StateBackend;
use monad_types::{BlockId, NodeId, RouterTarget};
use monad_validator::{
    epoch_manager::EpochManager,
    validator_set::{ValidatorSetType, ValidatorSetTypeFactory},
    validators_epoch_mapping::ValidatorsEpochMapping,
};
use rand::{prelude::SliceRandom, SeedableRng};
use rand_chacha::ChaCha8Rng;

use crate::{ConsensusMode, MonadState, VerifiedMonadMessage};

/// Responds to BlockSync requests from other nodes
#[derive(Debug)]
pub(crate) struct BlockSync<ST: CertificateSignatureRecoverable, SCT: SignatureCollection> {
    // requests: HashMap<BlockId, BTreeSet<NodeId<CertificateSignaturePubKey<ST>>>>,

    // self_requests: HashMap<BlockId, SelfRequest<CertificateSignaturePubKey<ST>>>,

    // Requests from peers
    header_requests: HashMap<BlockIdRange, BTreeSet<NodeId<CertificateSignaturePubKey<ST>>>>,
    payload_requests: HashMap<PayloadId, BTreeSet<NodeId<CertificateSignaturePubKey<ST>>>>,

    // Requests from self
    self_header_requests: HashMap<BlockIdRange, SelfRequest<CertificateSignaturePubKey<ST>>>,
    self_payload_requests: HashMap<PayloadId, SelfRequest<CertificateSignaturePubKey<ST>>>,
    // Parallel payload requests from self after receiving headers
    // If payload is None, the payload request is still in flight
    self_completed_header_requests: HashMap<BlockIdRange, Vec<(Block<SCT>, Option<Payload>)>>,

    self_request_mode: BlockSyncSelfRequester,

    rng: ChaCha8Rng,
}

#[derive(Debug, PartialEq, Eq)]
struct SelfRequest<PT: PubKey> {
    // this will ALWAYS match BlockSync::self_request_mode
    // we keep this here to be defensive
    // we assert these are the same when emitting blocks
    requester: BlockSyncSelfRequester,
    /// None == current outstanding request is to self
    to: Option<NodeId<PT>>,
}

impl<ST: CertificateSignatureRecoverable, SCT: SignatureCollection> Default for BlockSync<ST, SCT> {
    fn default() -> Self {
        Self {
            // requests: Default::default(),
            // self_requests: Default::default(),
            header_requests: Default::default(),
            payload_requests: Default::default(),

            self_header_requests: Default::default(),
            self_payload_requests: Default::default(),
            self_completed_header_requests: Default::default(),

            self_request_mode: BlockSyncSelfRequester::StateSync,

            rng: ChaCha8Rng::seed_from_u64(123456),
        }
    }
}

pub(crate) enum BlockSyncCommand<SCT: SignatureCollection> {
    SendRequest {
        to: NodeId<SCT::NodeIdPubKey>,
        request: BlockSyncRequestMessage,
    },
    ScheduleTimeout(BlockSyncRequestMessage),
    ResetTimeout(BlockSyncRequestMessage),
    /// Respond to an external block sync request
    SendResponse {
        to: NodeId<SCT::NodeIdPubKey>,
        response: BlockSyncResponseMessage<SCT>,
    },
    /// Fetch block headers from consensus ledger
    FetchHeaders(BlockIdRange),
    /// Fetch a payload from consensus ledger
    FetchPayload(PayloadId),
    /// Response to a BlockSyncEvent::SelfRequest
    Emit(BlockSyncSelfRequester, (BlockIdRange, Vec<FullBlock<SCT>>)),
}

pub(super) struct BlockSyncChildState<'a, ST, SCT, BPT, SBT, VTF, LT, TT, BVT, SVT, ASVT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    BPT: BlockPolicy<SCT, SBT>,
    SBT: StateBackend,
    BVT: BlockValidator<SCT, BPT, SBT>,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    block_sync: &'a mut BlockSync<ST, SCT>,

    /// BlockSync queries consensus first when receiving BlockSyncRequest
    consensus: &'a ConsensusMode<SCT, BPT, SBT>,
    epoch_manager: &'a EpochManager,
    val_epoch_map: &'a ValidatorsEpochMapping<VTF, SCT>,
    delta: &'a Duration,
    nodeid: &'a NodeId<CertificateSignaturePubKey<ST>>,

    metrics: &'a mut Metrics,

    _phantom: PhantomData<(ST, SCT, BPT, SBT, VTF, LT, TT, BVT, SVT, ASVT)>,
}

impl<'a, ST, SCT, BPT, SBT, VTF, LT, TT, BVT, SVT, ASVT>
    BlockSyncChildState<'a, ST, SCT, BPT, SBT, VTF, LT, TT, BVT, SVT, ASVT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    BPT: BlockPolicy<SCT, SBT>,
    SBT: StateBackend,
    BVT: BlockValidator<SCT, BPT, SBT>,
    SVT: StateRootValidator,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    pub(super) fn new(
        monad_state: &'a mut MonadState<ST, SCT, BPT, SBT, VTF, LT, TT, BVT, SVT, ASVT>,
    ) -> Self {
        Self {
            block_sync: &mut monad_state.block_sync,
            consensus: &monad_state.consensus,
            epoch_manager: &monad_state.epoch_manager,
            val_epoch_map: &monad_state.val_epoch_map,
            delta: &monad_state.consensus_config.delta,
            nodeid: &monad_state.nodeid,
            metrics: &mut monad_state.metrics,
            _phantom: PhantomData,
        }
    }

    fn self_headers_request_exists(&self, block_id_range: BlockIdRange) -> bool {
        self.block_sync
            .self_header_requests
            .contains_key(&block_id_range)
            || self
                .block_sync
                .self_completed_header_requests
                .contains_key(&block_id_range)
    }

    fn verify_block_headers(
        &self,
        block_id_range: BlockIdRange,
        headers: &Vec<Block<SCT>>,
    ) -> bool {
        let num_headers = headers.len();

        // atleast one block header was requested and received
        // TODO: doesn't have to be assert
        assert_ne!(block_id_range.from, block_id_range.to);
        assert!(num_headers > 0);

        // The id of the last header must be block_id_range.to
        if block_id_range.to != headers.last().unwrap().get_id() {
            return false;
        }

        // verify that the headers form a chain between block_id_range.from and block_id_range.to
        // by verifying the QCs and their parent block ids
        for (index, block_header) in headers.iter().enumerate() {
            let qc = &block_header.qc;
            // verify the QC
            if let Err(_err) =
                verify_certificates(self.epoch_manager, self.val_epoch_map, &None, qc)
            {
                return false;
            }

            // verify the QC points to correct block id
            let previous_block_id = if index == 0 {
                // the QC first header should point to block_id_range.from
                block_id_range.from
            } else {
                // the QC should point to the previous block
                headers[index - 1].get_id()
            };

            if qc.get_block_id() != previous_block_id {
                return false;
            }

            // TODO verify block id
        }

        true
    }

    pub(super) fn update(
        &mut self,
        event: BlockSyncEvent<SCT>,
    ) -> Vec<WrappedBlockSyncCommand<SCT>> {
        // pick_peer is a closure instead of a function to help the borrow checker
        let mut pick_peer = || {
            let epoch = self.consensus.current_epoch();
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
        };

        let mut cmds = Vec::new();
        match event {
            BlockSyncEvent::Request { sender, request } => {
                // let consensus_cached_block = match &self.consensus {
                //     ConsensusMode::Sync { .. } => None,
                //     ConsensusMode::Live(consensus) => consensus.fetch_uncommitted_block(&block_id),
                // };

                // if let Some(block) = consensus_cached_block {
                //     // use retrieved block if currently cached in pending block tree
                //     cmds.push(BlockSyncCommand::SendResponse {
                //         to: sender,
                //         response: BlockSyncResponseMessage::BlockFound(block),
                //     })
                // } else if !self.block_sync.self_requests.contains_key(&block_id) {
                //     // ask ledger
                //     let entry = self.block_sync.requests.entry(block_id).or_default();
                //     entry.insert(sender);
                //     cmds.push(BlockSyncCommand::FetchBlock(block_id))
                // } else {
                //     cmds.push(BlockSyncCommand::SendResponse {
                //         to: sender,
                //         response: BlockSyncResponseMessage::NotAvailable(block_id),
                //     })
                // }
                match request {
                    BlockSyncRequestMessage::Headers(block_id_range) => {
                        let entry = self
                            .block_sync
                            .header_requests
                            .entry(block_id_range)
                            .or_default();
                        entry.insert(sender);
                        // TODO: retrieve some of the headers from consensus first and request rest from ledger
                        cmds.push(BlockSyncCommand::FetchHeaders(block_id_range))
                    }
                    BlockSyncRequestMessage::Payload(payload_id) => {
                        let consensus_cached_payload = match &self.consensus {
                            ConsensusMode::Sync { .. } => None,
                            ConsensusMode::Live(consensus) => {
                                consensus.fetch_uncommitted_payload(payload_id)
                            }
                        };

                        if let Some(payload) = consensus_cached_payload {
                            cmds.push(BlockSyncCommand::SendResponse {
                                to: sender,
                                response: BlockSyncResponseMessage::PayloadResponse(
                                    BlockSyncPayloadResponse::Found((payload_id, payload)),
                                ),
                            });
                        } else {
                            let entry = self
                                .block_sync
                                .payload_requests
                                .entry(payload_id)
                                .or_default();
                            entry.insert(sender);

                            cmds.push(BlockSyncCommand::FetchPayload(payload_id))
                        }
                    }
                };
            }
            BlockSyncEvent::SelfRequest {
                requester,
                block_id_range,
            } => {
                if requester != self.block_sync.self_request_mode {
                    self.block_sync.self_header_requests.clear();
                    self.block_sync.self_payload_requests.clear();
                    self.block_sync.self_request_mode = requester;
                }

                if !self.self_headers_request_exists(block_id_range) {
                    let existing_entry = self.block_sync.self_header_requests.insert(
                        block_id_range,
                        SelfRequest {
                            requester,
                            to: None,
                        },
                    );
                    assert!(existing_entry.is_none(), "asserted above");
                }
            }
            BlockSyncEvent::SelfCancelRequest {
                requester,
                block_id_range,
            } => {
                if let Entry::Occupied(entry) =
                    self.block_sync.self_header_requests.entry(block_id_range)
                {
                    if entry.get().requester == requester {
                        entry.remove();
                    }
                }
                // TODO also check self_completed_header_requests
            }

            BlockSyncEvent::SelfResponse { response } => {
                // let block_id = response.get_block_id();
                // let requesters = self
                //     .block_sync
                //     .requests
                //     .remove(&block_id)
                //     .unwrap_or_default();
                // cmds.extend(requesters.into_iter().map(|requester| {
                //     BlockSyncCommand::SendResponse {
                //         to: requester,
                //         response: response.clone(),
                //     }
                // }));

                // if let Entry::Occupied(mut entry) = self.block_sync.self_requests.entry(block_id) {
                //     let self_request = entry.get_mut();
                //     if self_request.to.is_none() {
                //         match response {
                //             BlockSyncResponseMessage::BlockFound(block) => {
                //                 assert_eq!(
                //                     self_request.requester,
                //                     self.block_sync.self_request_mode
                //                 );
                //                 cmds.push(BlockSyncCommand::Emit(self_request.requester, block));
                //                 entry.remove();
                //             }
                //             BlockSyncResponseMessage::NotAvailable(_) => {
                //                 let to = pick_peer();
                //                 self_request.to = Some(to);
                //                 self.metrics.blocksync_events.blocksync_request += 1;
                //                 cmds.push(BlockSyncCommand::SendRequest {
                //                     to,
                //                     request: RequestBlockSyncMessage { block_id },
                //                 });
                //                 cmds.push(BlockSyncCommand::ScheduleTimeout(block_id));
                //             }
                //         };
                //     }
                // }
                match response {
                    BlockSyncResponseMessage::HeadersResponse(headers_response) => {
                        let block_id_range = match headers_response {
                            BlockSyncHeadersResponse::Found((bid_range, _)) => bid_range,
                            BlockSyncHeadersResponse::NotAvailable(bid_range) => bid_range,
                        };

                        let requesters = self
                            .block_sync
                            .header_requests
                            .remove(&block_id_range)
                            .unwrap_or_default();
                        cmds.extend(requesters.into_iter().map(|requester| {
                            BlockSyncCommand::SendResponse {
                                to: requester,
                                response: BlockSyncResponseMessage::HeadersResponse(
                                    headers_response.clone(),
                                ),
                            }
                        }));

                        if let Entry::Occupied(mut entry) =
                            self.block_sync.self_header_requests.entry(block_id_range)
                        {
                            let self_request = entry.get_mut();
                            if self_request.to.is_none() {
                                match headers_response {
                                    BlockSyncHeadersResponse::Found((block_id_range, headers)) => {
                                        assert_eq!(
                                            self_request.requester,
                                            self.block_sync.self_request_mode
                                        );
                                        entry.remove();

                                        // verify headers
                                        if !self.verify_block_headers(block_id_range, &headers) {
                                            panic!("self response should never fail block headers verification");
                                        }

                                        if !self
                                            .block_sync
                                            .self_completed_header_requests
                                            .contains_key(&block_id_range)
                                        {
                                            for block_header in headers.iter() {
                                                let payload_id = block_header.get_payload_id();

                                                // fetch payload only if we haven't already requested
                                                if !self
                                                    .block_sync
                                                    .self_payload_requests
                                                    .contains_key(&payload_id)
                                                {
                                                    cmds.push(BlockSyncCommand::FetchPayload(
                                                        block_header.get_payload_id(),
                                                    ));
                                                }
                                            }

                                            let payload_requests = headers
                                                .into_iter()
                                                .map(|block| (block, None))
                                                .collect();
                                            self.block_sync
                                                .self_completed_header_requests
                                                .insert(block_id_range, payload_requests);
                                        }
                                    }
                                    BlockSyncHeadersResponse::NotAvailable(block_id_range) => {}
                                }
                            }
                        }
                    }
                    BlockSyncResponseMessage::PayloadResponse(payload_response) => {
                        let payload_id = match payload_response {
                            BlockSyncPayloadResponse::Found((payload_id, _)) => payload_id,
                            BlockSyncPayloadResponse::NotAvailable(payload_id) => payload_id,
                        };

                        let requesters = self
                            .block_sync
                            .payload_requests
                            .remove(&payload_id)
                            .unwrap_or_default();
                        cmds.extend(requesters.into_iter().map(|requester| {
                            BlockSyncCommand::SendResponse {
                                to: requester,
                                response: BlockSyncResponseMessage::PayloadResponse(
                                    payload_response.clone(),
                                ),
                            }
                        }));

                        match payload_response {
                            BlockSyncPayloadResponse::Found((payload_id, payload)) => {}
                            BlockSyncPayloadResponse::NotAvailable(payload_id) => {}
                        }
                    }
                }
            }
            BlockSyncEvent::Response { sender, response } => {
                // let block_id = response.get_block_id();
                // if let Entry::Occupied(mut entry) = self.block_sync.self_requests.entry(block_id) {
                //     let self_request = entry.get_mut();
                //     if self_request.to == Some(sender) {
                //         cmds.push(BlockSyncCommand::ResetTimeout(block_id));
                //         match response {
                //             BlockSyncResponseMessage::BlockFound(block) => {
                //                 self.metrics.blocksync_events.blocksync_response_successful += 1;
                //                 assert_eq!(
                //                     self_request.requester,
                //                     self.block_sync.self_request_mode
                //                 );
                //                 cmds.push(BlockSyncCommand::Emit(self_request.requester, block));
                //                 entry.remove();
                //             }
                //             BlockSyncResponseMessage::NotAvailable(_) => {
                //                 self.metrics.blocksync_events.blocksync_response_failed += 1;
                //                 let to = pick_peer();
                //                 self_request.to = Some(to);
                //                 self.metrics.blocksync_events.blocksync_request += 1;
                //                 cmds.push(BlockSyncCommand::SendRequest {
                //                     to,
                //                     request: RequestBlockSyncMessage { block_id },
                //                 });
                //                 cmds.push(BlockSyncCommand::ScheduleTimeout(block_id));
                //             }
                //         };
                //     } else {
                //         self.metrics.blocksync_events.blocksync_response_unexpected += 1;
                //     }
                // } else {
                //     self.metrics.blocksync_events.blocksync_response_unexpected += 1;
                // }
            }
            BlockSyncEvent::Timeout(request) => {
                // let block_id = request.block_id;
                // if let Entry::Occupied(mut entry) = self.block_sync.self_requests.entry(block_id) {
                //     let self_request = entry.get_mut();
                //     if self_request.to.is_some() {
                //         let to = pick_peer();
                //         self_request.to = Some(to);
                //         self.metrics.blocksync_events.blocksync_request += 1;
                //         cmds.push(BlockSyncCommand::SendRequest {
                //             to,
                //             request: RequestBlockSyncMessage { block_id },
                //         });
                //         cmds.push(BlockSyncCommand::ScheduleTimeout(block_id));
                //     }
                // }
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
            BlockSyncCommand::ScheduleTimeout(blocksync_request) => {
                vec![Command::TimerCommand(TimerCommand::Schedule {
                    duration: wrapped.request_timeout,
                    variant: TimeoutVariant::BlockSync(blocksync_request),
                    on_timeout: MonadEvent::BlockSyncEvent(BlockSyncEvent::Timeout(
                        blocksync_request,
                    )),
                })]
            }
            BlockSyncCommand::ResetTimeout(blocksync_request) => {
                vec![Command::TimerCommand(TimerCommand::ScheduleReset(
                    TimeoutVariant::BlockSync(blocksync_request),
                ))]
            }
            BlockSyncCommand::SendResponse { to, response } => {
                vec![Command::RouterCommand(RouterCommand::Publish {
                    target: RouterTarget::PointToPoint(to),
                    message: VerifiedMonadMessage::BlockSyncResponse(response),
                })]
            }
            // BlockSyncCommand::FetchBlock(block_id) => {
            //     vec![Command::LedgerCommand(LedgerCommand::LedgerFetch(block_id))]
            // }
            BlockSyncCommand::FetchHeaders(block_id_range) => {
                vec![Command::LedgerCommand(LedgerCommand::LedgerFetchHeaders(
                    block_id_range,
                ))]
            }
            BlockSyncCommand::FetchPayload(payload_id) => {
                vec![Command::LedgerCommand(LedgerCommand::LedgerFetchPayload(
                    payload_id,
                ))]
            }
            BlockSyncCommand::Emit(requester, (block_id_range, full_blocks)) => {
                vec![Command::LoopbackCommand(LoopbackCommand::Forward(
                    match requester {
                        BlockSyncSelfRequester::StateSync => {
                            MonadEvent::StateSyncEvent(StateSyncEvent::BlockSync {
                                block_id_range,
                                full_blocks,
                            })
                        }
                        BlockSyncSelfRequester::Consensus => {
                            MonadEvent::ConsensusEvent(ConsensusEvent::BlockSync {
                                block_id_range,
                                full_blocks,
                            })
                        }
                    },
                ))]
            }
        }
    }
}
