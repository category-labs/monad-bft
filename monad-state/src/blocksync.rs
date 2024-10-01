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
    payload::{Payload, PayloadId, StateRootValidator},
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
use monad_types::{Epoch, NodeId, RouterTarget};
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
    // Requests from peers
    header_requests: HashMap<BlockIdRange, BTreeSet<NodeId<CertificateSignaturePubKey<ST>>>>,
    payload_requests: HashMap<PayloadId, BTreeSet<NodeId<CertificateSignaturePubKey<ST>>>>,

    // Requests from self
    self_header_requests: HashMap<BlockIdRange, SelfRequest<CertificateSignaturePubKey<ST>>>,
    self_payload_requests: HashMap<PayloadId, SelfRequest<CertificateSignaturePubKey<ST>>>,
    // Parallel payload requests from self after receiving headers
    // If payload is None, the payload request is still in flight
    self_completed_header_requests:
        HashMap<BlockIdRange, (BlockSyncSelfRequester, Vec<(Block<SCT>, Option<Payload>)>)>,

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

impl<ST: CertificateSignatureRecoverable, SCT: SignatureCollection> BlockSync<ST, SCT> {
    pub fn clear_self_requests(&mut self) {
        self.self_header_requests.clear();
        self.self_payload_requests.clear();
        self.self_completed_header_requests.clear();
    }

    pub fn self_headers_request_exists(&self, block_id_range: BlockIdRange) -> bool {
        self.self_header_requests.contains_key(&block_id_range)
            || self
                .self_completed_header_requests
                .contains_key(&block_id_range)
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

    fn pick_peer(
        self_node_id: &NodeId<CertificateSignaturePubKey<ST>>,
        current_epoch: Epoch,
        val_epoch_map: &ValidatorsEpochMapping<VTF, SCT>,
        rng: &mut ChaCha8Rng,
    ) -> NodeId<CertificateSignaturePubKey<ST>> {
        // let epoch = self.consensus.current_epoch();
        let validators = val_epoch_map
            .get_val_set(&current_epoch)
            .expect("current epoch exists");
        let members = validators.get_members();
        let members = members
            .iter()
            .filter(|(peer, _)| peer != &self_node_id)
            .collect_vec();
        assert!(!members.is_empty(), "no nodes to blocksync from");
        *members
            .choose_weighted(rng, |(_peer, weight)| weight.0)
            .expect("nonempty")
            .0
    }

    // TODO return actual errors instead of bool
    fn verify_block_headers(
        epoch_manager: &EpochManager,
        val_epoch_map: &ValidatorsEpochMapping<VTF, SCT>,
        block_id_range: BlockIdRange,
        block_headers: &Vec<Block<SCT>>,
    ) -> bool {
        let num_headers = block_headers.len();

        // atleast one block header was requested and received
        // TODO: doesn't have to be assert ?
        assert_ne!(block_id_range.from, block_id_range.to);
        assert!(num_headers > 0);

        // The id of the last header must be block_id_range.to
        if block_id_range.to != block_headers.last().unwrap().get_id() {
            return false;
        }

        // verify that the headers form a chain between block_id_range.from and block_id_range.to
        // by verifying the QCs and their parent block ids
        for (index, block_header) in block_headers.iter().enumerate() {
            let qc = &block_header.qc;
            // verify the QC
            if let Err(_err) = verify_certificates(epoch_manager, val_epoch_map, &None, qc) {
                return false;
            }

            // verify the QC points to correct block id
            let previous_block_id = if index == 0 {
                // the QC of first header should point to block_id_range.from
                block_id_range.from
            } else {
                // the QC should point to the previous header
                block_headers[index - 1].get_id()
            };

            if qc.get_block_id() != previous_block_id {
                return false;
            }

            // TODO verify block id
        }

        true
    }

    // If sender is None, response is from self ledger
    fn handle_headers_response_for_self(
        &mut self,
        sender: Option<NodeId<SCT::NodeIdPubKey>>,
        headers_response: BlockSyncHeadersResponse<SCT>,
    ) -> Vec<BlockSyncCommand<SCT>> {
        let mut cmds = Vec::new();

        let block_id_range = headers_response.get_block_id_range();
        if let Entry::Occupied(mut entry) =
            self.block_sync.self_header_requests.entry(block_id_range)
        {
            // self requested blocksync headers
            let self_request = entry.get_mut();
            if self_request.to != sender {
                // unexpected sender
                return cmds;
            }

            // reset timeout
            if sender.is_some() {
                cmds.push(BlockSyncCommand::ResetTimeout(
                    BlockSyncRequestMessage::Headers(block_id_range),
                ));
            }

            let self_requester = self_request.requester;
            match headers_response {
                BlockSyncHeadersResponse::Found((block_id_range, headers)) => {
                    assert_eq!(self_requester, self.block_sync.self_request_mode);

                    // verify headers. includes QC verification
                    if Self::verify_block_headers(
                        self.epoch_manager,
                        self.val_epoch_map,
                        block_id_range,
                        &headers,
                    ) {
                        // valid headers received, remove entry for its request
                        entry.remove();

                        if self
                            .block_sync
                            .self_completed_header_requests
                            .contains_key(&block_id_range)
                        {
                            // headers request already completed, nothing to do
                            return cmds;
                        }

                        // valid headers received. request its payloads
                        for block_header in headers.iter() {
                            let payload_id = block_header.get_payload_id();

                            // TODO: check blocktree and not request existing payloads
                            // fetch payload only if we haven't already requested
                            if !self
                                .block_sync
                                .self_payload_requests
                                .contains_key(&payload_id)
                            {
                                self.block_sync.self_payload_requests.insert(
                                    payload_id,
                                    SelfRequest {
                                        requester: self_requester,
                                        to: None,
                                    },
                                );
                                cmds.push(BlockSyncCommand::FetchPayload(
                                    block_header.get_payload_id(),
                                ));
                                cmds.push(BlockSyncCommand::ScheduleTimeout(
                                    BlockSyncRequestMessage::Payload(payload_id),
                                ));
                            }
                        }

                        let payload_requests =
                            headers.into_iter().map(|block| (block, None)).collect();
                        self.block_sync
                            .self_completed_header_requests
                            .insert(block_id_range, (self_requester, payload_requests));
                    } else {
                        assert!(
                            sender != None,
                            "self response shouldn't fail headers verification"
                        );

                        // invalid headers, request from different peer
                        let to = Self::pick_peer(
                            &self.nodeid,
                            self.consensus.current_epoch(),
                            &self.val_epoch_map,
                            &mut self.block_sync.rng,
                        );
                        self_request.to = Some(to);
                        cmds.push(BlockSyncCommand::SendRequest {
                            to,
                            request: BlockSyncRequestMessage::Headers(block_id_range),
                        });
                        cmds.push(BlockSyncCommand::ScheduleTimeout(
                            BlockSyncRequestMessage::Headers(block_id_range),
                        ));
                    }
                }
                BlockSyncHeadersResponse::NotAvailable(block_id_range) => {
                    // request from peer
                    let to = Self::pick_peer(
                        &self.nodeid,
                        self.consensus.current_epoch(),
                        &self.val_epoch_map,
                        &mut self.block_sync.rng,
                    );
                    self_request.to = Some(to);
                    cmds.push(BlockSyncCommand::SendRequest {
                        to,
                        request: BlockSyncRequestMessage::Headers(block_id_range),
                    });
                    cmds.push(BlockSyncCommand::ScheduleTimeout(
                        BlockSyncRequestMessage::Headers(block_id_range),
                    ));
                }
            }
        }

        cmds
    }

    // If sender is None, response is from self ledger
    fn handle_payload_response_for_self(
        &mut self,
        sender: Option<NodeId<SCT::NodeIdPubKey>>,
        payload_response: BlockSyncPayloadResponse,
    ) -> Vec<BlockSyncCommand<SCT>> {
        let payload_id = payload_response.get_payload_id();
        let mut cmds = Vec::new();

        if let Entry::Occupied(mut entry) = self.block_sync.self_payload_requests.entry(payload_id)
        {
            // self requested payload
            let self_request = entry.get_mut();
            if self_request.to != sender {
                // unexpected sender
                return cmds;
            }

            // reset timeout
            if sender.is_some() {
                cmds.push(BlockSyncCommand::ResetTimeout(
                    BlockSyncRequestMessage::Payload(payload_id),
                ));
            }

            let self_requester = self_request.requester;
            match payload_response {
                BlockSyncPayloadResponse::Found((payload_id, payload)) => {
                    assert_eq!(self_requester, self.block_sync.self_request_mode);
                    // TODO: payload validation
                    entry.remove();

                    let requested_ranges: Vec<_> = self
                        .block_sync
                        .self_completed_header_requests
                        .keys()
                        .cloned()
                        .collect();
                    for block_id_range in requested_ranges {
                        let Entry::Occupied(mut entry) = self
                            .block_sync
                            .self_completed_header_requests
                            .entry(block_id_range)
                        else {
                            panic!("should be in tree");
                        };

                        // add the payload completed header requests
                        let (_, payload_requests) = entry.get_mut();
                        if let Some((_, maybe_payload)) = payload_requests
                            .iter_mut()
                            .find(|(header, _)| header.payload_id == payload_id)
                        {
                            if maybe_payload.is_none() {
                                // clone incase there are multiple requests that require the same payload
                                *maybe_payload = Some(payload.clone());
                            }
                        }

                        // if all payloads are received for this range, create the full blocks and emit
                        if payload_requests
                            .iter()
                            .all(|(_, maybe_payload)| maybe_payload.is_some())
                        {
                            let (self_requester, requested_blocks) = entry.remove();
                            let full_blocks = requested_blocks
                                .into_iter()
                                .map(|(block, payload)| FullBlock {
                                    block,
                                    payload: payload.expect("asserted"),
                                })
                                .collect();
                            cmds.push(BlockSyncCommand::Emit(
                                self_requester,
                                (block_id_range, full_blocks),
                            ));
                        }
                    }
                }
                BlockSyncPayloadResponse::NotAvailable(payload_id) => {
                    // self requested payload not found, request from peer.
                    let to = Self::pick_peer(
                        &self.nodeid,
                        self.consensus.current_epoch(),
                        &self.val_epoch_map,
                        &mut self.block_sync.rng,
                    );
                    self_request.to = Some(to);
                    cmds.push(BlockSyncCommand::SendRequest {
                        to,
                        request: BlockSyncRequestMessage::Payload(payload_id),
                    });
                    cmds.push(BlockSyncCommand::ScheduleTimeout(
                        BlockSyncRequestMessage::Payload(payload_id),
                    ));
                }
            }
        }

        cmds
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
                    self.block_sync.clear_self_requests();
                    self.block_sync.self_request_mode = requester;
                }

                if !self.block_sync.self_headers_request_exists(block_id_range) {
                    self.block_sync.self_header_requests.insert(
                        block_id_range,
                        SelfRequest {
                            requester,
                            to: None,
                        },
                    );

                    cmds.push(BlockSyncCommand::FetchHeaders(block_id_range));
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

                if let Entry::Occupied(entry) = self
                    .block_sync
                    .self_completed_header_requests
                    .entry(block_id_range)
                {
                    if entry.get().0 == requester {
                        entry.remove();
                    }
                }
            }
            BlockSyncEvent::SelfResponse { response } => match response {
                BlockSyncResponseMessage::HeadersResponse(headers_response) => {
                    let block_id_range = headers_response.get_block_id_range();

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

                    cmds.extend(self.handle_headers_response_for_self(None, headers_response));
                }
                BlockSyncResponseMessage::PayloadResponse(payload_response) => {
                    let payload_id = payload_response.get_payload_id();

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

                    cmds.extend(self.handle_payload_response_for_self(None, payload_response));
                }
            },
            BlockSyncEvent::Response { sender, response } => match response {
                BlockSyncResponseMessage::HeadersResponse(headers_response) => {
                    cmds.extend(
                        self.handle_headers_response_for_self(Some(sender), headers_response),
                    );
                }
                BlockSyncResponseMessage::PayloadResponse(payload_response) => {
                    cmds.extend(
                        self.handle_payload_response_for_self(Some(sender), payload_response),
                    );
                }
            },
            BlockSyncEvent::Timeout(request) => match request {
                BlockSyncRequestMessage::Headers(block_id_range) => {
                    if let Entry::Occupied(mut entry) =
                        self.block_sync.self_header_requests.entry(block_id_range)
                    {
                        let self_request = entry.get_mut();
                        if self_request.to.is_some() {
                            let to = pick_peer();
                            self_request.to = Some(to);
                            cmds.push(BlockSyncCommand::SendRequest {
                                to,
                                request: BlockSyncRequestMessage::Headers(block_id_range),
                            });
                            cmds.push(BlockSyncCommand::ScheduleTimeout(
                                BlockSyncRequestMessage::Headers(block_id_range),
                            ));
                        }
                    }
                }
                BlockSyncRequestMessage::Payload(payload_id) => {
                    if let Entry::Occupied(mut entry) =
                        self.block_sync.self_payload_requests.entry(payload_id)
                    {
                        let self_request = entry.get_mut();
                        if self_request.to.is_some() {
                            let to = pick_peer();
                            self_request.to = Some(to);
                            cmds.push(BlockSyncCommand::SendRequest {
                                to,
                                request: BlockSyncRequestMessage::Payload(payload_id),
                            });
                            cmds.push(BlockSyncCommand::ScheduleTimeout(
                                BlockSyncRequestMessage::Payload(payload_id),
                            ));
                        }
                    }
                }
            },
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
