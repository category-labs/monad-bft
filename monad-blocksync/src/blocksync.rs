use std::collections::{hash_map::Entry, BTreeMap, BTreeSet, HashMap};

use itertools::Itertools;
use monad_blocktree::blocktree::BlockTree;
use monad_consensus_types::{
    block::{Block, BlockPolicy, BlockRange, BlockType, FullBlock},
    metrics::Metrics,
    payload::{Payload, PayloadId},
    signature_collection::SignatureCollection,
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable, PubKey,
};
use monad_state_backend::StateBackend;
use monad_types::{Epoch, NodeId};
use monad_validator::{
    epoch_manager::EpochManager,
    validator_set::{ValidatorSetType, ValidatorSetTypeFactory},
    validators_epoch_mapping::ValidatorsEpochMapping,
};
use rand::{seq::SliceRandom, SeedableRng};
use rand_chacha::ChaCha8Rng;
use tracing::trace;

use crate::messages::message::{
    BlockSyncHeadersResponse, BlockSyncPayloadResponse, BlockSyncRequestMessage,
    BlockSyncResponseMessage,
};

// TODO configurable
// determines the max number of parallel payload requests self can make
const BLOCKSYNC_MAX_PAYLOAD_REQUESTS: usize = 10;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockSyncSelfRequester {
    /// Consensus requested this blocksync request
    Consensus,
    /// Statesync requested this blocksync request
    StateSync,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BlockSyncCommand<SCT: SignatureCollection> {
    /// Request sent to a peer
    SendRequest {
        to: NodeId<SCT::NodeIdPubKey>,
        request: BlockSyncRequestMessage,
    },
    /// Schedule a timeout for a request sent to a peer
    ScheduleTimeout(BlockSyncRequestMessage),
    /// Reset timeout once peer responds
    ResetTimeout(BlockSyncRequestMessage),
    /// Respond to an external block sync request
    SendResponse {
        to: NodeId<SCT::NodeIdPubKey>,
        response: BlockSyncResponseMessage<SCT>,
    },
    /// Fetch a range of headers from consensus ledger
    FetchHeaders(BlockRange),
    /// Fetch a single payload from consensus ledger
    FetchPayload(PayloadId),
    /// Response to a BlockSyncEvent::SelfRequest
    Emit(BlockSyncSelfRequester, (BlockRange, Vec<FullBlock<SCT>>)),
}

#[derive(Debug, PartialEq, Eq)]
pub struct SelfRequest<PT: PubKey> {
    /// this will ALWAYS match BlockSync::self_request_mode
    /// we keep this here to be defensive
    /// we assert these are the same when emitting blocks
    requester: BlockSyncSelfRequester,
    /// None == current outstanding request is to self
    to: Option<NodeId<PT>>,
}

/// State to keep track of self requests and requests from peers
#[derive(Debug)]
pub struct BlockSync<ST: CertificateSignatureRecoverable, SCT: SignatureCollection> {
    /// Requests from peers
    /// The map stores some of the blocks fetched from consensus blocktree while the
    /// rest of the blocks from the requested range are fetched from ledger
    /// e.g. If NodeX requests a -> c and blocks b -> c are fetched from blocktree,
    /// stored in the map is [a -> b, (NodeX, b -> c)] while a -> b is fetched from ledger
    headers_requests:
        HashMap<BlockRange, BTreeMap<NodeId<CertificateSignaturePubKey<ST>>, Vec<Block<SCT>>>>,
    payload_requests: HashMap<PayloadId, BTreeSet<NodeId<CertificateSignaturePubKey<ST>>>>,

    /// Headers requests for self
    self_headers_requests: HashMap<BlockRange, SelfRequest<CertificateSignaturePubKey<ST>>>,
    /// Payload requests for self
    self_payload_requests: HashMap<PayloadId, Option<SelfRequest<CertificateSignaturePubKey<ST>>>>,
    /// Should be <= BLOCKSYNC_MAX_PAYLOAD_REQUESTS
    self_payload_requests_in_flight: usize,
    /// Parallel payload requests from self after receiving headers
    /// If payload is None, the payload request is still in flight and should be
    /// in self_payload_requests
    self_completed_headers_requests: HashMap<BlockRange, SelfCompletedHeader<SCT>>,

    self_request_mode: BlockSyncSelfRequester,

    rng: ChaCha8Rng,
}

// TODO move this to a separate file, restrict mutators to maintain invariants
#[derive(Debug)]
struct SelfCompletedHeader<SCT: SignatureCollection> {
    requester: BlockSyncSelfRequester,
    blocks: Vec<(Block<SCT>, Option<Payload>)>,
    payload_cache: HashMap<PayloadId, Payload>,
}

impl<ST: CertificateSignatureRecoverable, SCT: SignatureCollection> Default for BlockSync<ST, SCT> {
    fn default() -> Self {
        Self {
            headers_requests: Default::default(),
            payload_requests: Default::default(),
            self_headers_requests: Default::default(),
            self_payload_requests: Default::default(),
            self_payload_requests_in_flight: 0,
            self_completed_headers_requests: Default::default(),
            self_request_mode: BlockSyncSelfRequester::StateSync,
            rng: ChaCha8Rng::seed_from_u64(123456),
        }
    }
}

impl<ST: CertificateSignatureRecoverable, SCT: SignatureCollection> BlockSync<ST, SCT>
where
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    fn clear_self_requests(&mut self) {
        self.self_headers_requests.clear();
        self.self_payload_requests.clear();
        self.self_payload_requests_in_flight = 0;
        self.self_completed_headers_requests.clear();
    }

    fn self_request_exists(&self, block_range: BlockRange) -> bool {
        self.self_headers_requests.contains_key(&block_range)
            || self
                .self_completed_headers_requests
                .contains_key(&block_range)
    }
}

pub enum BlockCache<'a, SCT, BPT, SBT>
where
    SCT: SignatureCollection,
    BPT: BlockPolicy<SCT, SBT>,
    SBT: StateBackend,
{
    BlockTree(&'a BlockTree<SCT, BPT, SBT>),
    BlockBuffer(&'a HashMap<PayloadId, Payload>),
}

pub struct BlockSyncWrapper<'a, ST, SCT, BPT, SBT, VTF>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    BPT: BlockPolicy<SCT, SBT>,
    SBT: StateBackend,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    pub block_sync: &'a mut BlockSync<ST, SCT>,

    pub block_cache: BlockCache<'a, SCT, BPT, SBT>,
    pub metrics: &'a mut Metrics,
    pub nodeid: &'a NodeId<SCT::NodeIdPubKey>,
    pub current_epoch: Epoch,
    pub epoch_manager: &'a EpochManager,
    pub val_epoch_map: &'a ValidatorsEpochMapping<VTF, SCT>,
}

impl<ST, SCT, BPT, SBT, VTF> BlockSyncWrapper<'_, ST, SCT, BPT, SBT, VTF>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    BPT: BlockPolicy<SCT, SBT>,
    SBT: StateBackend,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    #[must_use]
    pub fn handle_self_request(
        &mut self,
        requester: BlockSyncSelfRequester,
        block_range: BlockRange,
    ) -> Vec<BlockSyncCommand<SCT>> {
        trace!(?requester, ?block_range, "blocksync: self request");
        if requester != self.block_sync.self_request_mode {
            self.block_sync.clear_self_requests();
            self.block_sync.self_request_mode = requester;
        }

        let mut cmds = Vec::new();

        if self.block_sync.self_request_exists(block_range) {
            return cmds;
        }

        self.block_sync.self_headers_requests.insert(
            block_range,
            SelfRequest {
                requester,
                to: None,
            },
        );

        cmds.push(BlockSyncCommand::FetchHeaders(block_range));

        cmds
    }

    pub fn handle_self_cancel_request(
        &mut self,
        requester: BlockSyncSelfRequester,
        block_range: BlockRange,
    ) {
        trace!(?requester, ?block_range, "blocksync: self cancel request");
        if let Entry::Occupied(entry) = self.block_sync.self_headers_requests.entry(block_range) {
            if entry.get().requester == requester {
                entry.remove();
            }
        }
        if let Entry::Occupied(entry) = self
            .block_sync
            .self_completed_headers_requests
            .entry(block_range)
        {
            if entry.get().requester == requester {
                entry.remove();
            }
        }
    }

    #[must_use]
    pub fn handle_peer_request(
        &mut self,
        sender: NodeId<SCT::NodeIdPubKey>,
        request: BlockSyncRequestMessage,
    ) -> Vec<BlockSyncCommand<SCT>> {
        let mut cmds = Vec::new();

        match request {
            BlockSyncRequestMessage::Headers(block_range) => {
                self.metrics.blocksync_events.peer_headers_request += 1;
                trace!(?sender, ?block_range, "blocksync: peer headers request");

                let cached_blocks = match self.block_cache {
                    BlockCache::BlockTree(blocktree) => blocktree
                        .get_parent_block_chain(&block_range.last_block_id)
                        .into_iter()
                        .filter_map(|validated_block| {
                            if validated_block.get_seq_num() >= block_range.root_seq_num {
                                Some(validated_block.get_unvalidated_block_ref().clone())
                            } else {
                                None
                            }
                        })
                        .collect_vec(),
                    BlockCache::BlockBuffer(_) => Vec::new(), // TODO
                };

                // all blocks are cached if the first block is the non-empty block of requested
                // root_seq_num.
                if !cached_blocks.is_empty()
                    && cached_blocks.first().unwrap().get_seq_num() == block_range.root_seq_num
                    && !cached_blocks.first().unwrap().is_empty_block()
                {
                    // reply with the cached blocks
                    cmds.push(BlockSyncCommand::SendResponse {
                        to: sender,
                        response: BlockSyncResponseMessage::HeadersResponse(
                            BlockSyncHeadersResponse::Found((block_range, cached_blocks)),
                        ),
                    });
                } else {
                    // requested range is more than cached blocks, fetch rest from ledger
                    let last_block_id_to_fetch = cached_blocks
                        .first()
                        .map(|block| block.get_qc().get_block_id())
                        .unwrap_or(block_range.last_block_id);
                    let ledger_fetch_range = BlockRange {
                        last_block_id: last_block_id_to_fetch,
                        root_seq_num: block_range.root_seq_num,
                    };

                    let entry = self
                        .block_sync
                        .headers_requests
                        .entry(ledger_fetch_range)
                        .or_default();
                    entry.insert(sender, cached_blocks);
                    cmds.push(BlockSyncCommand::FetchHeaders(ledger_fetch_range));
                }
            }
            BlockSyncRequestMessage::Payload(payload_id) => {
                self.metrics.blocksync_events.peer_payload_request += 1;
                trace!(?sender, ?payload_id, "blocksync: peer payload request");

                if let Some(cached_payload) = self.get_cached_payload(payload_id) {
                    cmds.push(BlockSyncCommand::SendResponse {
                        to: sender,
                        response: BlockSyncResponseMessage::PayloadResponse(
                            BlockSyncPayloadResponse::Found(cached_payload),
                        ),
                    });

                    return cmds;
                }

                let entry = self
                    .block_sync
                    .payload_requests
                    .entry(payload_id)
                    .or_default();
                entry.insert(sender);
                cmds.push(BlockSyncCommand::FetchPayload(payload_id));
            }
        }

        cmds
    }

    fn pick_peer(
        self_node_id: &NodeId<CertificateSignaturePubKey<ST>>,
        current_epoch: Epoch,
        val_epoch_map: &ValidatorsEpochMapping<VTF, SCT>,
        rng: &mut ChaCha8Rng,
    ) -> NodeId<CertificateSignaturePubKey<ST>> {
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

    // TODO return more informative errors instead of bool
    fn verify_block_headers(block_range: BlockRange, block_headers: &[Block<SCT>]) -> bool {
        let num_blocks = block_headers.len() as u32;

        // atleast one block header was received
        if num_blocks == 0 {
            return false;
        }

        // The seq num of the first header must be equal to the requested root seq num
        if block_range.root_seq_num != block_headers.first().unwrap().get_seq_num() {
            return false;
        }

        // The first header should not a be NULL block
        if block_headers.first().unwrap().is_empty_block() {
            return false;
        }

        // The id of the last header must be block_range.last_block_id
        if block_range.last_block_id != block_headers.last().unwrap().get_id() {
            return false;
        }

        // verify that the headers form a chain by verifying the QCs point to
        // their parent block ids
        for (parent_block_header, block_header) in
            block_headers.iter().zip(block_headers.iter().skip(1))
        {
            if parent_block_header.get_id() != block_header.get_parent_id() {
                return false;
            }
        }

        true
    }

    fn get_cached_payload(&self, payload_id: PayloadId) -> Option<Payload> {
        if let Some(payload) = match self.block_cache {
            BlockCache::BlockBuffer(full_blocks) => full_blocks.get(&payload_id).cloned(),
            BlockCache::BlockTree(blocktree) => blocktree.get_payload(&payload_id),
        } {
            return Some(payload);
        }

        for (_, completed_header) in self.block_sync.self_completed_headers_requests.iter() {
            if let Some(payload) = completed_header.payload_cache.get(&payload_id) {
                return Some(payload.clone());
            }
        }

        None
    }

    #[must_use]
    fn try_initiate_payload_requests_for_self(&mut self) -> Vec<BlockSyncCommand<SCT>> {
        let mut cmds = Vec::new();

        let payload_ids_to_request = self
            .block_sync
            .self_payload_requests
            .keys()
            .cloned()
            .collect_vec();
        for payload_id in payload_ids_to_request {
            if let Some(payload) = self.get_cached_payload(payload_id) {
                // remove request and clone payload to existing headers
                self.block_sync.self_payload_requests.remove(&payload_id);

                for (_, completed_header) in
                    self.block_sync.self_completed_headers_requests.iter_mut()
                {
                    for (block, maybe_payload) in &mut completed_header.blocks {
                        if block.payload_id == payload_id && maybe_payload.is_none() {
                            // clone incase there are multiple requests that require the same payload
                            *maybe_payload = Some(payload.clone());
                            completed_header
                                .payload_cache
                                .insert(payload_id, payload.clone());

                            self.metrics
                                .blocksync_events
                                .self_payload_response_successful += 1;
                        }
                    }
                }
            }
        }

        while self.block_sync.self_payload_requests_in_flight < BLOCKSYNC_MAX_PAYLOAD_REQUESTS {
            if let Some((payload_id, req)) = self
                .block_sync
                .self_payload_requests
                .iter_mut()
                .find(|(_, req)| req.is_none())
            {
                trace!(?payload_id, "blocksync: self initiating payload request");

                cmds.push(BlockSyncCommand::FetchPayload(*payload_id));
                *req = Some(SelfRequest {
                    requester: self.block_sync.self_request_mode,
                    to: None,
                });

                self.block_sync.self_payload_requests_in_flight += 1;
                self.metrics
                    .blocksync_events
                    .self_payload_requests_in_flight += 1;
            } else {
                // all payload requests initiated
                break;
            }
        }

        cmds.extend(self.handle_completed_ranges());

        cmds
    }

    #[must_use]
    fn handle_headers_response_for_self(
        &mut self,
        sender: Option<NodeId<CertificateSignaturePubKey<ST>>>,
        headers_response: BlockSyncHeadersResponse<SCT>,
    ) -> Vec<BlockSyncCommand<SCT>> {
        let mut cmds = Vec::new();

        let block_range = headers_response.get_block_range();
        let Entry::Occupied(mut entry) = self.block_sync.self_headers_requests.entry(block_range)
        else {
            // unexpected respose. could be because the self request was cancelled
            // or from a self ledger response
            return cmds;
        };
        let self_request = entry.get_mut();

        match headers_response {
            BlockSyncHeadersResponse::Found((block_range, block_headers)) => {
                if self_request.to != sender {
                    // unexpected sender, but use the headers if valid
                    self.metrics.blocksync_events.headers_response_unexpected += 1;
                }
                assert_eq!(self_request.requester, self.block_sync.self_request_mode);

                // verify the headers
                // TODO: do we need to validate headers if response is from self ledger ?
                if Self::verify_block_headers(block_range, block_headers.as_slice()) {
                    trace!(
                        ?sender,
                        ?block_range,
                        "blocksync: headers response verifcation passed"
                    );

                    // valid headers, remove entry and reset timeout
                    entry.remove();

                    if sender.is_some() {
                        self.metrics.blocksync_events.headers_response_successful += 1;
                        cmds.push(BlockSyncCommand::ResetTimeout(
                            BlockSyncRequestMessage::Headers(block_range),
                        ));
                    } else {
                        self.metrics
                            .blocksync_events
                            .self_headers_response_successful += 1;
                    }
                    self.metrics.blocksync_events.num_headers_received +=
                        block_headers.len() as u64;

                    // add payloads to be requested
                    for payload_id in block_headers.iter().map(|block| block.payload_id) {
                        match self.block_sync.self_payload_requests.entry(payload_id) {
                            Entry::Vacant(entry) => {
                                entry.insert(None);
                            }
                            Entry::Occupied(_) => {
                                // payload request already started, do nothing
                            }
                        }
                    }

                    // insert headers as completed
                    self.block_sync.self_completed_headers_requests.insert(
                        block_range,
                        SelfCompletedHeader {
                            requester: self.block_sync.self_request_mode,
                            blocks: block_headers
                                .into_iter()
                                .map(|block| (block, None))
                                .collect(),
                            payload_cache: Default::default(),
                        },
                    );
                } else {
                    // failed header verification
                    trace!(
                        ?sender,
                        ?block_range,
                        "blocksync: headers response verifcation failed"
                    );
                    self.metrics.blocksync_events.headers_validation_failed += 1;

                    // self response shouldn't fail headers verification
                    assert!(sender.is_some());

                    // request from different peer
                    let to = Self::pick_peer(
                        self.nodeid,
                        self.current_epoch,
                        self.val_epoch_map,
                        &mut self.block_sync.rng,
                    );
                    self_request.to = Some(to);
                    self.metrics.blocksync_events.self_headers_request += 1;
                    cmds.push(BlockSyncCommand::SendRequest {
                        to,
                        request: BlockSyncRequestMessage::Headers(block_range),
                    });
                    cmds.push(BlockSyncCommand::ScheduleTimeout(
                        BlockSyncRequestMessage::Headers(block_range),
                    ));
                }
            }
            BlockSyncHeadersResponse::NotAvailable(block_range) => {
                if sender.is_some() {
                    self.metrics.blocksync_events.headers_response_failed += 1;
                } else {
                    self.metrics.blocksync_events.self_headers_response_failed += 1;
                }
                trace!(
                    ?sender,
                    ?block_range,
                    "blocksync: headers response not available"
                );

                // request from different peer
                let to = Self::pick_peer(
                    self.nodeid,
                    self.current_epoch,
                    self.val_epoch_map,
                    &mut self.block_sync.rng,
                );
                self_request.to = Some(to);
                self.metrics.blocksync_events.self_headers_request += 1;
                cmds.push(BlockSyncCommand::SendRequest {
                    to,
                    request: BlockSyncRequestMessage::Headers(block_range),
                });
                cmds.push(BlockSyncCommand::ScheduleTimeout(
                    BlockSyncRequestMessage::Headers(block_range),
                ));
            }
        }

        // try start the payload requests
        cmds.extend(self.try_initiate_payload_requests_for_self());

        cmds
    }

    #[must_use]
    // if sender is None, response is from self ledger
    fn handle_payload_response_for_self(
        &mut self,
        sender: Option<NodeId<SCT::NodeIdPubKey>>,
        payload_response: BlockSyncPayloadResponse,
    ) -> Vec<BlockSyncCommand<SCT>> {
        let mut cmds = Vec::new();
        let payload_id = payload_response.get_payload_id();

        let Entry::Occupied(mut entry) = self.block_sync.self_payload_requests.entry(payload_id)
        else {
            // unexpected respose. could be because the self request was cancelled
            // or from a self ledger response
            return cmds;
        };

        let Some(self_request) = entry.get_mut() else {
            // got payload response when the request was never initiated
            self.metrics.blocksync_events.payload_response_unexpected += 1;
            // TODO use it if valid ?
            return cmds;
        };

        if self_request.to != sender {
            // unexpected sender, but use the payload if valid
            self.metrics.blocksync_events.payload_response_unexpected += 1;
        }

        // reset timeout if requested from peer
        if sender.is_some() {
            cmds.push(BlockSyncCommand::ResetTimeout(
                BlockSyncRequestMessage::Payload(payload_id),
            ));
        }

        let self_requester = self_request.requester;
        match payload_response {
            BlockSyncPayloadResponse::Found(payload) => {
                assert_eq!(self_requester, self.block_sync.self_request_mode);

                trace!(?sender, ?payload_id, "blocksync: received payload response");
                // remove entry and update existing requests
                entry.remove();
                self.block_sync.self_payload_requests_in_flight -= 1;

                self.metrics
                    .blocksync_events
                    .self_payload_requests_in_flight -= 1;
                if sender.is_some() {
                    self.metrics.blocksync_events.payload_response_successful += 1;
                } else {
                    self.metrics
                        .blocksync_events
                        .self_payload_response_successful += 1;
                }

                for (_, completed_header) in
                    self.block_sync.self_completed_headers_requests.iter_mut()
                {
                    for (block, maybe_payload) in &mut completed_header.blocks {
                        if block.payload_id == payload_id && maybe_payload.is_none() {
                            // clone incase there are multiple requests that require the same payload
                            *maybe_payload = Some(payload.clone());
                            completed_header
                                .payload_cache
                                .insert(block.payload_id, payload.clone());
                        }
                    }
                }
            }
            BlockSyncPayloadResponse::NotAvailable(payload_id) => {
                if sender.is_some() {
                    self.metrics.blocksync_events.payload_response_failed += 1;
                } else {
                    self.metrics.blocksync_events.self_payload_response_failed += 1;
                }

                // payload not found, request from peer.
                let to = Self::pick_peer(
                    self.nodeid,
                    self.current_epoch,
                    self.val_epoch_map,
                    &mut self.block_sync.rng,
                );
                self_request.to = Some(to);
                self.metrics.blocksync_events.self_payload_request += 1;
                cmds.push(BlockSyncCommand::SendRequest {
                    to,
                    request: BlockSyncRequestMessage::Payload(payload_id),
                });
                cmds.push(BlockSyncCommand::ScheduleTimeout(
                    BlockSyncRequestMessage::Payload(payload_id),
                ));
            }
        }

        cmds.extend(self.handle_completed_ranges());

        // try initiating more payload requests
        cmds.extend(self.try_initiate_payload_requests_for_self());

        cmds
    }

    #[must_use]
    fn handle_completed_ranges(&mut self) -> Vec<BlockSyncCommand<SCT>> {
        let mut cmds = Vec::new();

        let completed_ranges = self
            .block_sync
            .self_completed_headers_requests
            .iter()
            .filter_map(|(block_range, completed_header)| {
                completed_header
                    .blocks
                    .iter()
                    .all(|(_, maybe_payload)| maybe_payload.is_some())
                    .then_some(block_range)
            })
            .cloned()
            .collect_vec();

        for completed_range in completed_ranges {
            let completed_header = self
                .block_sync
                .self_completed_headers_requests
                .remove(&completed_range)
                .unwrap();

            // create the full blocks and emit for the completed range
            let full_blocks = completed_header
                .blocks
                .into_iter()
                .map(|(block, payload)| FullBlock {
                    block,
                    payload: payload.expect("asserted"),
                })
                .collect();

            cmds.push(BlockSyncCommand::Emit(
                completed_header.requester,
                (completed_range, full_blocks),
            ));
        }

        cmds
    }

    #[must_use]
    pub fn handle_ledger_response(
        &mut self,
        response: BlockSyncResponseMessage<SCT>,
    ) -> Vec<BlockSyncCommand<SCT>> {
        trace!(?response, "blocksync: self response from ledger");
        let mut cmds = Vec::new();

        match response {
            BlockSyncResponseMessage::HeadersResponse(headers_response) => {
                let block_range = headers_response.get_block_range();

                // reply to the requested peers
                let requesters = self
                    .block_sync
                    .headers_requests
                    .remove(&block_range)
                    .unwrap_or_default();
                for (requester, cached_blocks) in requesters {
                    // extend with cached blocks respond with the requested block range
                    let requested_block_range = BlockRange {
                        last_block_id: cached_blocks
                            .last()
                            .map(|block| block.get_id())
                            .unwrap_or(block_range.last_block_id),
                        root_seq_num: block_range.root_seq_num,
                    };
                    let headers_response = match headers_response.clone() {
                        BlockSyncHeadersResponse::Found((_, mut requested_blocks)) => {
                            requested_blocks.extend(cached_blocks);
                            self.metrics
                                .blocksync_events
                                .peer_headers_request_successful += 1;
                            BlockSyncHeadersResponse::Found((
                                requested_block_range,
                                requested_blocks,
                            ))
                        }
                        BlockSyncHeadersResponse::NotAvailable(_) => {
                            self.metrics.blocksync_events.peer_headers_request_failed += 1;
                            BlockSyncHeadersResponse::NotAvailable(requested_block_range)
                        }
                    };

                    cmds.push(BlockSyncCommand::SendResponse {
                        to: requester,
                        response: BlockSyncResponseMessage::HeadersResponse(headers_response),
                    });
                }

                cmds.extend(self.handle_headers_response_for_self(None, headers_response));
            }
            BlockSyncResponseMessage::PayloadResponse(payload_response) => {
                let payload_id = payload_response.get_payload_id();

                match payload_response {
                    BlockSyncPayloadResponse::Found(_) => {
                        self.metrics
                            .blocksync_events
                            .peer_payload_request_successful += 1
                    }
                    BlockSyncPayloadResponse::NotAvailable(_) => {
                        self.metrics.blocksync_events.peer_payload_request_failed += 1
                    }
                }

                // reply to the requested peers
                let requesters = self
                    .block_sync
                    .payload_requests
                    .remove(&payload_id)
                    .unwrap_or_default();
                for requester in requesters {
                    cmds.push(BlockSyncCommand::SendResponse {
                        to: requester,
                        response: BlockSyncResponseMessage::PayloadResponse(
                            payload_response.clone(),
                        ),
                    });
                }

                cmds.extend(self.handle_payload_response_for_self(None, payload_response));
            }
        }

        cmds
    }

    #[must_use]
    pub fn handle_peer_response(
        &mut self,
        sender: NodeId<SCT::NodeIdPubKey>,
        response: BlockSyncResponseMessage<SCT>,
    ) -> Vec<BlockSyncCommand<SCT>> {
        match response {
            BlockSyncResponseMessage::HeadersResponse(headers_response) => {
                self.handle_headers_response_for_self(Some(sender), headers_response)
            }
            BlockSyncResponseMessage::PayloadResponse(payload_response) => {
                self.handle_payload_response_for_self(Some(sender), payload_response)
            }
        }
    }

    #[must_use]
    pub fn handle_timeout(
        &mut self,
        request: BlockSyncRequestMessage,
    ) -> Vec<BlockSyncCommand<SCT>> {
        trace!(?request, "blocksync: self request timeout");
        self.metrics.blocksync_events.request_timeout += 1;
        let mut cmds = Vec::new();

        match request {
            BlockSyncRequestMessage::Headers(block_range) => {
                if let Entry::Occupied(mut entry) =
                    self.block_sync.self_headers_requests.entry(block_range)
                {
                    let self_request = entry.get_mut();
                    if self_request.to.is_some() {
                        let to = Self::pick_peer(
                            self.nodeid,
                            self.current_epoch,
                            self.val_epoch_map,
                            &mut self.block_sync.rng,
                        );
                        self_request.to = Some(to);
                        cmds.push(BlockSyncCommand::SendRequest {
                            to,
                            request: BlockSyncRequestMessage::Headers(block_range),
                        });
                        cmds.push(BlockSyncCommand::ScheduleTimeout(
                            BlockSyncRequestMessage::Headers(block_range),
                        ));
                    }
                }
            }
            BlockSyncRequestMessage::Payload(payload_id) => {
                if let Entry::Occupied(mut entry) =
                    self.block_sync.self_payload_requests.entry(payload_id)
                {
                    let Some(self_request) = entry.get_mut() else {
                        // got payload timeout when the request was never initiated
                        return cmds;
                    };

                    if self_request.to.is_some() {
                        let to = Self::pick_peer(
                            self.nodeid,
                            self.current_epoch,
                            self.val_epoch_map,
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
        }

        cmds
    }
}

#[cfg(test)]
mod test {
    use std::collections::BTreeMap;

    use bytes::Bytes;
    use itertools::Itertools;
    use monad_blocktree::blocktree::BlockTree;
    use monad_consensus_types::{
        block::{
            Block, BlockKind, BlockPolicy, BlockRange, BlockType, FullBlock, PassthruBlockPolicy,
            GENESIS_TIMESTAMP,
        },
        checkpoint::RootInfo,
        metrics::Metrics,
        payload::{
            ExecutionProtocol, FullTransactionList, Payload, PayloadId, RandaoReveal,
            TransactionPayload,
        },
        quorum_certificate::QuorumCertificate,
        signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
        state_root_hash::StateRootHash,
        voting::{ValidatorMapping, Vote},
    };
    use monad_crypto::{
        certificate_signature::{
            CertificateKeyPair, CertificateSignature, CertificateSignaturePubKey,
            CertificateSignatureRecoverable, PubKey,
        },
        hasher::{Hasher, HasherType},
        NopPubKey, NopSignature,
    };
    use monad_eth_types::{Balance, EthAddress};
    use monad_multi_sig::MultiSig;
    use monad_state_backend::{InMemoryState, InMemoryStateInner, StateBackend};
    use monad_testutil::validators::create_keys_w_validators;
    use monad_types::{
        BlockId, Epoch, Hash, NodeId, Round, SeqNum, GENESIS_BLOCK_ID, GENESIS_SEQ_NUM,
    };
    use monad_validator::{
        epoch_manager::EpochManager,
        leader_election::LeaderElection,
        simple_round_robin::SimpleRoundRobin,
        validator_set::{ValidatorSetFactory, ValidatorSetType, ValidatorSetTypeFactory},
        validators_epoch_mapping::ValidatorsEpochMapping,
    };
    use test_case::test_case;
    use zerocopy::AsBytes;

    use super::{
        BlockCache, BlockSync, BlockSyncCommand, BlockSyncSelfRequester, BlockSyncWrapper,
    };
    use crate::{
        blocksync::BLOCKSYNC_MAX_PAYLOAD_REQUESTS,
        messages::message::{BlockSyncRequestMessage, BlockSyncResponseMessage},
    };

    struct BlockSyncContext<ST, SCT, BPT, SBT, VTF, LT>
    where
        ST: CertificateSignatureRecoverable,
        SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
        BPT: BlockPolicy<SCT, SBT>,
        SBT: StateBackend,
        VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
        LT: LeaderElection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    {
        block_sync: BlockSync<ST, SCT>,

        // TODO: include BlockBuffer
        blocktree: BlockTree<SCT, BPT, SBT>,
        metrics: Metrics,
        nodeid: NodeId<SCT::NodeIdPubKey>,
        current_epoch: Epoch,
        epoch_manager: EpochManager,
        val_epoch_map: ValidatorsEpochMapping<VTF, SCT>,

        keys: Vec<ST::KeyPairType>,
        cert_keys: Vec<SignatureCollectionKeyPairType<SCT>>,
        election: LT,
    }

    type PubKeyType = NopPubKey;
    type SignatureType = NopSignature;
    type SignatureCollectionType = MultiSig<NopSignature>;
    type BlockPolicyType = PassthruBlockPolicy;
    type StateBackendType = InMemoryState;
    type LeaderElectionType = SimpleRoundRobin<PubKeyType>;

    impl<ST, SCT, BPT, SBT, VTF, LT> BlockSyncContext<ST, SCT, BPT, SBT, VTF, LT>
    where
        ST: CertificateSignatureRecoverable,
        SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
        BPT: BlockPolicy<SCT, SBT>,
        SBT: StateBackend,
        VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
        LT: LeaderElection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    {
        fn wrapped_state(&mut self) -> BlockSyncWrapper<ST, SCT, BPT, SBT, VTF> {
            let block_cache = BlockCache::BlockTree(&self.blocktree);
            BlockSyncWrapper {
                block_sync: &mut self.block_sync,
                block_cache,
                metrics: &mut self.metrics,
                nodeid: &self.nodeid,
                current_epoch: self.current_epoch,
                epoch_manager: &self.epoch_manager,
                val_epoch_map: &self.val_epoch_map,
            }
        }

        fn handle_self_request(
            &mut self,
            requester: BlockSyncSelfRequester,
            block_range: BlockRange,
        ) -> Vec<BlockSyncCommand<SCT>> {
            self.wrapped_state()
                .handle_self_request(requester, block_range)
        }

        fn handle_self_cancel_request(
            &mut self,
            requester: BlockSyncSelfRequester,
            block_range: BlockRange,
        ) {
            self.wrapped_state()
                .handle_self_cancel_request(requester, block_range);
        }

        fn handle_peer_request(
            &mut self,
            sender: NodeId<SCT::NodeIdPubKey>,
            request: BlockSyncRequestMessage,
        ) -> Vec<BlockSyncCommand<SCT>> {
            self.wrapped_state().handle_peer_request(sender, request)
        }

        fn handle_ledger_response(
            &mut self,
            response: BlockSyncResponseMessage<SCT>,
        ) -> Vec<BlockSyncCommand<SCT>> {
            self.wrapped_state().handle_ledger_response(response)
        }

        fn handle_peer_response(
            &mut self,
            sender: NodeId<SCT::NodeIdPubKey>,
            response: BlockSyncResponseMessage<SCT>,
        ) -> Vec<BlockSyncCommand<SCT>> {
            self.wrapped_state().handle_peer_response(sender, response)
        }

        fn assert_empty_block_sync_state(&self) {
            assert!(self.block_sync.headers_requests.is_empty());
            assert!(self.block_sync.payload_requests.is_empty());
            assert!(self.block_sync.self_headers_requests.is_empty());
            assert!(self.block_sync.self_payload_requests.is_empty());
            assert_eq!(self.block_sync.self_payload_requests_in_flight, 0);
            assert!(self.block_sync.self_completed_headers_requests.is_empty());
        }

        // TODO: update and use ProposalGen
        fn get_blocks(&mut self, num_blocks: usize) -> Vec<FullBlock<SCT>> {
            let mut qc = QuorumCertificate::genesis_qc();
            let mut qc_seq_num = GENESIS_SEQ_NUM;
            let mut timestamp = 1;
            let mut round = Round(1);

            let mut full_blocks = Vec::new();

            for i in 0..num_blocks {
                let txns = TransactionPayload::List(FullTransactionList::new(
                    Bytes::copy_from_slice(i.as_bytes()),
                ));

                let epoch = self.epoch_manager.get_epoch(round).expect("epoch exists");
                let validators = self
                    .val_epoch_map
                    .get_val_set(&epoch)
                    .unwrap()
                    .get_members();
                let leader = self.election.get_leader(round, validators).pubkey();
                let (leader_key, leader_certkey) = self
                    .keys
                    .iter()
                    .zip(&self.cert_keys)
                    .find(|(k, _)| k.pubkey() == leader)
                    .expect("key not in valset");

                let (seq_num, block_kind) = match txns {
                    TransactionPayload::List(_) => (qc_seq_num + SeqNum(1), BlockKind::Executable),
                    TransactionPayload::Null => (qc_seq_num, BlockKind::Null),
                };
                let payload = Payload { txns };
                let block = Block::new(
                    NodeId::new(leader_key.pubkey()),
                    timestamp,
                    epoch,
                    round,
                    &ExecutionProtocol {
                        state_root: StateRootHash::default(),
                        seq_num,
                        beneficiary: EthAddress::default(),
                        randao_reveal: RandaoReveal::new::<SCT::SignatureType>(
                            round,
                            leader_certkey,
                        ),
                    },
                    payload.get_id(),
                    block_kind,
                    &qc,
                );

                let validator_cert_pubkeys = self
                    .val_epoch_map
                    .get_cert_pubkeys(&epoch)
                    .expect("should have the current validator certificate pubkeys");

                qc = self.get_qc(&self.cert_keys, &block, validator_cert_pubkeys);
                qc_seq_num = seq_num;
                timestamp += 1;
                round += Round(1);

                full_blocks.push(FullBlock { block, payload });
            }

            full_blocks
        }

        fn get_qc(
            &self,
            certkeys: &[SignatureCollectionKeyPairType<SCT>],
            block: &Block<SCT>,
            validator_mapping: &ValidatorMapping<
                CertificateSignaturePubKey<ST>,
                SignatureCollectionKeyPairType<SCT>,
            >,
        ) -> QuorumCertificate<SCT> {
            let vote = Vote {
                id: block.get_id(),
                epoch: block.epoch,
                round: block.round,
                parent_id: block.qc.get_block_id(),
                parent_round: block.qc.get_round(),
            };

            let msg = HasherType::hash_object(&vote);

            let mut sigs = Vec::new();
            for ck in certkeys {
                let sig = <SCT::SignatureType as CertificateSignature>::sign(msg.as_ref(), ck);

                for (node_id, pubkey) in validator_mapping.map.iter() {
                    if *pubkey == ck.pubkey() {
                        sigs.push((*node_id, sig));
                    }
                }
            }

            let sigcol = SCT::new(sigs, validator_mapping, msg.as_ref()).unwrap();

            QuorumCertificate::new(vote, sigcol)
        }
    }

    fn setup() -> BlockSyncContext<
        SignatureType,
        SignatureCollectionType,
        BlockPolicyType,
        StateBackendType,
        ValidatorSetFactory<PubKeyType>,
        LeaderElectionType,
    > {
        let (keys, cert_keys, valset, _valmap) = create_keys_w_validators::<
            SignatureType,
            SignatureCollectionType,
            _,
        >(2, ValidatorSetFactory::default());
        let val_stakes = Vec::from_iter(valset.get_members().clone());
        let val_cert_pubkeys = keys
            .iter()
            .map(|k| NodeId::new(k.pubkey()))
            .zip(cert_keys.iter().map(|k| k.pubkey()))
            .collect::<Vec<_>>();
        let mut val_epoch_map = ValidatorsEpochMapping::new(ValidatorSetFactory::default());
        val_epoch_map.insert(
            Epoch(1),
            val_stakes.clone(),
            ValidatorMapping::new(val_cert_pubkeys.clone()),
        );
        val_epoch_map.insert(
            Epoch(2),
            val_stakes,
            ValidatorMapping::new(val_cert_pubkeys),
        );
        let epoch_manager = EpochManager::new(SeqNum(100), Round(20), &[(Epoch(1), Round(0))]);
        let blocktree = BlockTree::new(RootInfo {
            block_id: GENESIS_BLOCK_ID,
            round: Round(0),
            seq_num: GENESIS_SEQ_NUM,
            epoch: Epoch(1),
            state_root: StateRootHash::default(),
            timestamp_ns: GENESIS_TIMESTAMP.try_into().unwrap(),
        });

        BlockSyncContext {
            block_sync: BlockSync::default(),
            blocktree,
            metrics: Metrics::default(),
            nodeid: NodeId::new(keys[0].pubkey()),
            current_epoch: Epoch(1),
            epoch_manager,
            val_epoch_map,

            keys,
            cert_keys,
            election: SimpleRoundRobin::default(),
        }
    }

    fn find_fetch_headers_commands(
        cmds: &[BlockSyncCommand<SignatureCollectionType>],
    ) -> Vec<&BlockSyncCommand<SignatureCollectionType>> {
        cmds.iter()
            .filter(|cmd| matches!(cmd, BlockSyncCommand::FetchHeaders(..)))
            .collect_vec()
    }

    fn find_fetch_payload_commands(
        cmds: &[BlockSyncCommand<SignatureCollectionType>],
    ) -> Vec<&BlockSyncCommand<SignatureCollectionType>> {
        cmds.iter()
            .filter(|cmd| matches!(cmd, BlockSyncCommand::FetchPayload(..)))
            .collect_vec()
    }

    fn find_send_request_commands(
        cmds: &[BlockSyncCommand<SignatureCollectionType>],
    ) -> Vec<&BlockSyncCommand<SignatureCollectionType>> {
        cmds.iter()
            .filter(|cmd| matches!(cmd, BlockSyncCommand::SendRequest { .. }))
            .collect_vec()
    }

    fn find_send_response_commands(
        cmds: &[BlockSyncCommand<SignatureCollectionType>],
    ) -> Vec<&BlockSyncCommand<SignatureCollectionType>> {
        cmds.iter()
            .filter(|cmd| matches!(cmd, BlockSyncCommand::SendResponse { .. }))
            .collect_vec()
    }

    fn find_schedule_timeout_commands(
        cmds: &[BlockSyncCommand<SignatureCollectionType>],
    ) -> Vec<&BlockSyncCommand<SignatureCollectionType>> {
        cmds.iter()
            .filter(|cmd| matches!(cmd, BlockSyncCommand::ScheduleTimeout(..)))
            .collect_vec()
    }

    fn find_reset_timeout_commands(
        cmds: &[BlockSyncCommand<SignatureCollectionType>],
    ) -> Vec<&BlockSyncCommand<SignatureCollectionType>> {
        cmds.iter()
            .filter(|cmd| matches!(cmd, BlockSyncCommand::ResetTimeout(..)))
            .collect_vec()
    }

    fn find_emit_commands(
        cmds: &[BlockSyncCommand<SignatureCollectionType>],
    ) -> Vec<&BlockSyncCommand<SignatureCollectionType>> {
        cmds.iter()
            .filter(|cmd| matches!(cmd, BlockSyncCommand::Emit(..)))
            .collect_vec()
    }

    #[test]
    fn initiate_headers_request() {
        let mut context = setup();

        let requester = BlockSyncSelfRequester::Consensus;
        let block_range = BlockRange {
            last_block_id: BlockId(Hash([0x00_u8; 32])),
            root_seq_num: SeqNum(1),
        };

        // requesting a block range should initiate a headers request
        let cmds = context.handle_self_request(requester, block_range);
        assert_eq!(cmds.len(), 1);

        let fetch_headers_cmds = find_fetch_headers_commands(&cmds);
        assert_eq!(fetch_headers_cmds.len(), 1);
        assert_eq!(
            fetch_headers_cmds[0],
            &BlockSyncCommand::FetchHeaders(block_range)
        );

        // headers not availabe in self ledger, should request from a peer
        let cmds = context
            .handle_ledger_response(BlockSyncResponseMessage::headers_not_available(block_range));
        assert_eq!(cmds.len(), 2);

        let request_cmds = find_send_request_commands(&cmds);
        assert_eq!(request_cmds.len(), 1);

        let expected_request = BlockSyncRequestMessage::Headers(block_range);
        match request_cmds[0] {
            BlockSyncCommand::SendRequest { request, .. } => {
                assert_eq!(*request, expected_request);
            }
            _ => unreachable!(),
        }

        let timeout_cmds = find_schedule_timeout_commands(&cmds);
        assert_eq!(timeout_cmds.len(), 1);
        match timeout_cmds[0] {
            BlockSyncCommand::ScheduleTimeout(request) => {
                assert_eq!(*request, expected_request);
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn initiate_payload_requests() {
        let mut context = setup();
        let num_blocks = 5;

        // test expects to initiate all payloads requests on headers response
        assert!(num_blocks < BLOCKSYNC_MAX_PAYLOAD_REQUESTS);

        let full_blocks = context.get_blocks(num_blocks);

        let requester = BlockSyncSelfRequester::Consensus;
        let block_range = BlockRange {
            last_block_id: full_blocks.last().unwrap().get_id(),
            root_seq_num: full_blocks.first().unwrap().get_seq_num(),
        };

        // requesting a block range should initiate a headers request
        let cmds = context.handle_self_request(requester, block_range);
        assert_eq!(cmds.len(), 1);

        // headers not availabe in self ledger, should request from a peer
        let cmds = context
            .handle_ledger_response(BlockSyncResponseMessage::headers_not_available(block_range));

        let request_cmds = find_send_request_commands(&cmds);
        assert_eq!(request_cmds.len(), 1);
        let sender = match request_cmds[0] {
            BlockSyncCommand::SendRequest { to, .. } => to,
            _ => unreachable!(),
        };

        let (headers, payloads): (Vec<_>, Vec<_>) = full_blocks
            .into_iter()
            .map(|full_block| (full_block.block, full_block.payload))
            .unzip();
        // valid headers response from a peer should initiate all its payload requests
        let cmds = context.handle_peer_response(
            *sender,
            BlockSyncResponseMessage::found_headers(block_range, headers),
        );
        // num_blocks payload fetch commands and 1 reset timeout command
        assert_eq!(cmds.len(), num_blocks + 1);

        let reset_timeout_cmds = find_reset_timeout_commands(&cmds);
        assert_eq!(reset_timeout_cmds.len(), 1);
        match reset_timeout_cmds[0] {
            BlockSyncCommand::ResetTimeout(request) => {
                assert_eq!(*request, BlockSyncRequestMessage::Headers(block_range));
            }
            _ => unreachable!(),
        }

        let fetch_payload_cmds = find_fetch_payload_commands(&cmds);
        assert_eq!(fetch_payload_cmds.len(), num_blocks);

        for payload in payloads {
            let payload_id = payload.get_id();
            assert!(fetch_payload_cmds.contains(&&BlockSyncCommand::FetchPayload(payload_id)));

            let expected_request = BlockSyncRequestMessage::Payload(payload_id);
            // payload not available in self ledger. should request from peer
            let cmds = context.handle_ledger_response(
                BlockSyncResponseMessage::payload_not_available(payload_id),
            );
            assert_eq!(cmds.len(), 2);

            let request_cmds = find_send_request_commands(&cmds);
            assert_eq!(request_cmds.len(), 1);
            match request_cmds[0] {
                BlockSyncCommand::SendRequest { request, .. } => {
                    assert_eq!(*request, expected_request);
                }
                _ => unreachable!(),
            }

            let timeout_cmds = find_schedule_timeout_commands(&cmds);
            assert_eq!(timeout_cmds.len(), 1);
            match timeout_cmds[0] {
                BlockSyncCommand::ScheduleTimeout(request) => {
                    assert_eq!(*request, expected_request);
                }
                _ => unreachable!(),
            }
        }
    }

    #[test]
    fn avoid_duplicate_payload_requests() {
        let mut context = setup();

        let num_blocks = 5;
        let num_in_blocktree = 2;
        let full_blocks = context.get_blocks(num_blocks);

        // add the first num_in_blocktree blocks to the blocktree
        let mut block_policy = PassthruBlockPolicy {};
        let state_backend = InMemoryStateInner::genesis(Balance::MAX, SeqNum(10));
        for full_block in full_blocks.iter().take(num_in_blocktree) {
            assert!(context
                .blocktree
                .add(full_block.clone(), &mut block_policy, &state_backend)
                .is_ok());
        }

        // request all num_blocks
        let requester = BlockSyncSelfRequester::Consensus;
        let block_range = BlockRange {
            last_block_id: full_blocks.last().unwrap().get_id(),
            root_seq_num: full_blocks.first().unwrap().get_seq_num(),
        };

        let (headers, payloads): (Vec<_>, Vec<_>) = full_blocks
            .into_iter()
            .map(|full_block| (full_block.block, full_block.payload))
            .unzip();

        // requesting a block range should initiate a headers request
        let cmds = context.handle_self_request(requester, block_range);
        assert_eq!(cmds.len(), 1);

        // found headers, should initiate payload requests for block 3, 4 and 5 only
        let cmds = context.handle_ledger_response(BlockSyncResponseMessage::found_headers(
            block_range,
            headers,
        ));

        let fetch_payload_cmds = find_fetch_payload_commands(&cmds);
        assert_eq!(fetch_payload_cmds.len(), 3);

        for payload in payloads.iter().skip(num_in_blocktree) {
            let payload_id = payload.get_id();
            assert!(fetch_payload_cmds.contains(&&BlockSyncCommand::FetchPayload(payload_id)));
        }
    }

    #[test]
    fn throttle_payload_requests() {
        let mut context = setup();
        let num_blocks = BLOCKSYNC_MAX_PAYLOAD_REQUESTS + 1;

        let full_blocks = context.get_blocks(num_blocks);

        let requester = BlockSyncSelfRequester::Consensus;
        let block_range = BlockRange {
            last_block_id: full_blocks.last().unwrap().get_id(),
            root_seq_num: full_blocks.first().unwrap().get_seq_num(),
        };

        // requesting a block range should initiate a headers request
        let cmds = context.handle_self_request(requester, block_range);
        assert_eq!(cmds.len(), 1);

        // headers not availabe in self ledger, should request from a peer
        let cmds = context
            .handle_ledger_response(BlockSyncResponseMessage::headers_not_available(block_range));

        let request_cmds = find_send_request_commands(&cmds);
        assert_eq!(request_cmds.len(), 1);
        let sender = match request_cmds[0] {
            BlockSyncCommand::SendRequest { to, .. } => to,
            _ => unreachable!(),
        };

        let (headers, payloads): (Vec<_>, Vec<_>) = full_blocks
            .into_iter()
            .map(|full_block| (full_block.block, full_block.payload))
            .unzip();
        // valid headers response from a peer should initiate all its payload requests
        let cmds = context.handle_peer_response(
            *sender,
            BlockSyncResponseMessage::found_headers(block_range, headers),
        );
        // BLOCKSYNC_MAX_PAYLOAD_REQUESTS payload fetch commands and 1 reset timeout command
        assert_eq!(cmds.len(), BLOCKSYNC_MAX_PAYLOAD_REQUESTS + 1);

        let payloads: BTreeMap<PayloadId, Payload> = payloads
            .into_iter()
            .map(|payload| (payload.get_id(), payload))
            .collect();

        let fetch_payload_cmds = find_fetch_payload_commands(&cmds);
        assert_eq!(fetch_payload_cmds.len(), BLOCKSYNC_MAX_PAYLOAD_REQUESTS);

        let requested_payload_ids = fetch_payload_cmds
            .iter()
            .take(2)
            .map(|cmd| match cmd {
                BlockSyncCommand::FetchPayload(payload_id) => *payload_id,
                _ => unreachable!(),
            })
            .collect_vec();

        let payload_1 = payloads.get(&requested_payload_ids[0]).unwrap().clone();
        let cmds =
            context.handle_ledger_response(BlockSyncResponseMessage::found_payload(payload_1));
        assert_eq!(find_fetch_payload_commands(&cmds).len(), 1);

        let payload_2 = payloads.get(&requested_payload_ids[1]).unwrap().clone();
        let cmds =
            context.handle_ledger_response(BlockSyncResponseMessage::found_payload(payload_2));
        assert_eq!(find_fetch_payload_commands(&cmds).len(), 0);
    }

    #[test]
    fn emit_requested_blocks() {
        let mut context = setup();
        let num_blocks = 5;

        let full_blocks = context.get_blocks(num_blocks);

        let requester = BlockSyncSelfRequester::Consensus;
        let block_range = BlockRange {
            last_block_id: full_blocks.last().unwrap().get_id(),
            root_seq_num: full_blocks.first().unwrap().get_seq_num(),
        };

        // requesting a block range should initiate a headers request
        let cmds = context.handle_self_request(requester, block_range);
        assert_eq!(cmds.len(), 1);

        // headers not availabe in self ledger, should request from a peer
        let cmds = context
            .handle_ledger_response(BlockSyncResponseMessage::headers_not_available(block_range));

        let request_cmds = find_send_request_commands(&cmds);
        assert_eq!(request_cmds.len(), 1);
        let sender = match request_cmds[0] {
            BlockSyncCommand::SendRequest { to, .. } => *to,
            _ => unreachable!(),
        };

        let (headers, payloads): (Vec<Block<SignatureCollectionType>>, Vec<Payload>) = full_blocks
            .iter()
            .map(|full_block| (full_block.block.clone(), full_block.payload.clone()))
            .unzip();
        // valid headers response from a peer should initiate all its payload requests
        context.handle_peer_response(
            sender,
            BlockSyncResponseMessage::found_headers(block_range, headers),
        );

        let mut payload_self_requests = Vec::new();
        for payload in payloads.iter() {
            let payload_id = payload.get_id();
            // payload not available in self ledger. should request from peer
            let cmds = context.handle_ledger_response(
                BlockSyncResponseMessage::payload_not_available(payload_id),
            );

            let request_cmds = find_send_request_commands(&cmds);
            payload_self_requests.push(request_cmds[0].clone());
        }

        for (index, (payload, payload_req)) in
            payloads.into_iter().zip(payload_self_requests).enumerate()
        {
            let payload_id = payload.get_id();
            let sender = match payload_req {
                BlockSyncCommand::SendRequest { to, request } => {
                    assert_eq!(request, BlockSyncRequestMessage::Payload(payload_id));
                    to
                }
                _ => unreachable!(),
            };

            let cmds = context
                .handle_peer_response(sender, BlockSyncResponseMessage::found_payload(payload));
            assert_eq!(find_reset_timeout_commands(&cmds).len(), 1);

            if index < num_blocks - 1 {
                assert_eq!(cmds.len(), 1);
            } else {
                assert_eq!(cmds.len(), 2);

                // received last payload, should emit the full blocks
                let emit_cmds = find_emit_commands(&cmds);
                assert_eq!(emit_cmds.len(), 1);
                assert_eq!(
                    emit_cmds[0],
                    &BlockSyncCommand::Emit(requester, (block_range, full_blocks.clone()))
                );
            }
        }

        context.assert_empty_block_sync_state();
    }

    #[test_case(true; "headers cached in blocktree")]
    #[test_case(false; "headers received from ledger")]
    fn peer_headers_request(cached_in_blocktree: bool) {
        let mut context = setup();
        let mut block_policy = PassthruBlockPolicy {};
        let state_backend = InMemoryStateInner::genesis(Balance::MAX, SeqNum(10));

        let num_blocks = 5;
        let full_blocks = context.get_blocks(num_blocks);
        let block_range = BlockRange {
            last_block_id: full_blocks.last().unwrap().get_id(),
            root_seq_num: full_blocks.first().unwrap().get_seq_num(),
        };

        let headers = full_blocks
            .iter()
            .map(|full_block| full_block.block.clone())
            .collect_vec();

        let sender_nodeid = NodeId::new(NopPubKey::from_bytes(&[0x00_u8; 32]).unwrap());

        let cmds = if cached_in_blocktree {
            for full_block in full_blocks {
                assert!(context
                    .blocktree
                    .add(full_block, &mut block_policy, &state_backend)
                    .is_ok());
            }
            // headers are in blocktree, should emit response command with all the headers
            context
                .handle_peer_request(sender_nodeid, BlockSyncRequestMessage::Headers(block_range))
        } else {
            // headers not in blocktree, should try fetch from ledger
            let cmds = context
                .handle_peer_request(sender_nodeid, BlockSyncRequestMessage::Headers(block_range));
            assert_eq!(cmds.len(), 1);
            let fetch_headers_cmds = find_fetch_headers_commands(&cmds);
            assert_eq!(fetch_headers_cmds.len(), 1);
            match fetch_headers_cmds[0] {
                BlockSyncCommand::FetchHeaders(fetch_range) => {
                    assert_eq!(fetch_range, &block_range);
                }
                _ => unreachable!(),
            }

            // ledger response should emit response command with all the headers
            context.handle_ledger_response(BlockSyncResponseMessage::found_headers(
                block_range,
                headers.clone(),
            ))
        };

        assert_eq!(cmds.len(), 1);
        let response_cmds = find_send_response_commands(&cmds);
        assert_eq!(response_cmds.len(), 1);
        match response_cmds[0] {
            BlockSyncCommand::SendResponse { to, response } => {
                assert_eq!(to, &sender_nodeid);
                assert_eq!(
                    response,
                    &BlockSyncResponseMessage::found_headers(block_range, headers)
                )
            }
            _ => unreachable!(),
        }

        context.assert_empty_block_sync_state();
    }

    #[test]
    fn peer_headers_request_not_available() {
        let mut context = setup();

        let num_blocks = 5;
        let full_blocks = context.get_blocks(num_blocks);
        let block_range = BlockRange {
            last_block_id: full_blocks.last().unwrap().get_id(),
            root_seq_num: full_blocks.first().unwrap().get_seq_num(),
        };

        let sender_nodeid = NodeId::new(NopPubKey::from_bytes(&[0x00_u8; 32]).unwrap());

        // headers not in blocktree, should try fetch from ledger
        let cmds = context
            .handle_peer_request(sender_nodeid, BlockSyncRequestMessage::Headers(block_range));
        assert_eq!(cmds.len(), 1);
        let fetch_headers_cmds = find_fetch_headers_commands(&cmds);
        assert_eq!(fetch_headers_cmds.len(), 1);
        match fetch_headers_cmds[0] {
            BlockSyncCommand::FetchHeaders(fetch_range) => {
                assert_eq!(fetch_range, &block_range);
            }
            _ => unreachable!(),
        }

        // ledger response should emit response command as headers not available
        let cmds = context
            .handle_ledger_response(BlockSyncResponseMessage::headers_not_available(block_range));

        assert_eq!(cmds.len(), 1);
        let response_cmds = find_send_response_commands(&cmds);
        assert_eq!(response_cmds.len(), 1);
        match response_cmds[0] {
            BlockSyncCommand::SendResponse { to, response } => {
                assert_eq!(to, &sender_nodeid);
                assert_eq!(
                    response,
                    &BlockSyncResponseMessage::headers_not_available(block_range)
                )
            }
            _ => unreachable!(),
        }

        context.assert_empty_block_sync_state();
    }

    #[test_case(true; "payload cached in blocktree")]
    #[test_case(false; "payload received from ledger")]
    fn peer_payload_request(cached_in_blocktree: bool) {
        let mut context = setup();
        let mut block_policy = PassthruBlockPolicy {};
        let state_backend = InMemoryStateInner::genesis(Balance::MAX, SeqNum(10));

        let num_blocks = 1;
        let full_blocks = context.get_blocks(num_blocks);

        let payload = full_blocks[0].payload.clone();
        let payload_id = payload.get_id();

        let sender_nodeid = NodeId::new(NopPubKey::from_bytes(&[0x00_u8; 32]).unwrap());

        let cmds = if cached_in_blocktree {
            assert!(context
                .blocktree
                .add(full_blocks[0].clone(), &mut block_policy, &state_backend)
                .is_ok());

            // payload in blocktree, should emit response command with the requested payload
            context.handle_peer_request(sender_nodeid, BlockSyncRequestMessage::Payload(payload_id))
        } else {
            // payload not in blocktree, should try fetch from ledger
            let cmds = context
                .handle_peer_request(sender_nodeid, BlockSyncRequestMessage::Payload(payload_id));
            assert_eq!(cmds.len(), 1);
            let fetch_payload_cmds = find_fetch_payload_commands(&cmds);
            assert_eq!(fetch_payload_cmds.len(), 1);
            match fetch_payload_cmds[0] {
                BlockSyncCommand::FetchPayload(fetch_id) => {
                    assert_eq!(fetch_id, &payload_id);
                }
                _ => unreachable!(),
            }

            // ledger response should emit response command with requested payload
            context.handle_ledger_response(BlockSyncResponseMessage::found_payload(payload.clone()))
        };

        assert_eq!(cmds.len(), 1);
        let response_cmds = find_send_response_commands(&cmds);
        assert_eq!(response_cmds.len(), 1);
        match response_cmds[0] {
            BlockSyncCommand::SendResponse { to, response } => {
                assert_eq!(to, &sender_nodeid);
                assert_eq!(response, &BlockSyncResponseMessage::found_payload(payload))
            }
            _ => unreachable!(),
        }

        context.assert_empty_block_sync_state();
    }

    #[test]
    fn peer_payload_request_not_available() {
        let mut context = setup();

        let num_blocks = 1;
        let full_blocks = context.get_blocks(num_blocks);

        let payload = full_blocks[0].payload.clone();
        let payload_id = payload.get_id();

        let sender_nodeid = NodeId::new(NopPubKey::from_bytes(&[0x00_u8; 32]).unwrap());

        // payload not in blocktree, should try fetch from ledger
        let cmds = context
            .handle_peer_request(sender_nodeid, BlockSyncRequestMessage::Payload(payload_id));
        assert_eq!(cmds.len(), 1);
        let fetch_headers_cmds = find_fetch_payload_commands(&cmds);
        assert_eq!(fetch_headers_cmds.len(), 1);
        match fetch_headers_cmds[0] {
            BlockSyncCommand::FetchPayload(fetch_id) => {
                assert_eq!(fetch_id, &payload_id);
            }
            _ => unreachable!(),
        }

        // ledger response should emit response command as payload not available
        let cmds = context
            .handle_ledger_response(BlockSyncResponseMessage::payload_not_available(payload_id));

        assert_eq!(cmds.len(), 1);
        let response_cmds = find_send_response_commands(&cmds);
        assert_eq!(response_cmds.len(), 1);
        match response_cmds[0] {
            BlockSyncCommand::SendResponse { to, response } => {
                assert_eq!(to, &sender_nodeid);
                assert_eq!(
                    response,
                    &BlockSyncResponseMessage::payload_not_available(payload_id)
                )
            }
            _ => unreachable!(),
        }

        context.assert_empty_block_sync_state();
    }
}
