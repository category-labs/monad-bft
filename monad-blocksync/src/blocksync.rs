// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::collections::{hash_map::Entry, BTreeMap, BTreeSet, HashMap};

use alloy_rlp::{encode_list, Decodable, Encodable, Header};
use bytes::BufMut;
use itertools::Itertools;
use monad_blocktree::blocktree::BlockTree;
use monad_chain_config::{revision::ChainRevision, ChainConfig};
use monad_consensus_types::{
    block::{BlockPolicy, BlockRange, ConsensusBlockHeader, ConsensusFullBlock},
    metrics::Metrics,
    payload::{ConsensusBlockBody, ConsensusBlockBodyId},
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable, PubKey,
};
use monad_state_backend::StateBackend;
use monad_types::{Epoch, ExecutionProtocol, NodeId, SeqNum, Stake};
use monad_validator::{
    epoch_manager::EpochManager,
    signature_collection::SignatureCollection,
    validator_set::{ValidatorSetType, ValidatorSetTypeFactory},
    validators_epoch_mapping::ValidatorsEpochMapping,
    weighted_round_robin::{generate_random_validator_with_randomizer, randomize_256_with_rng},
};
use rand::{seq::SliceRandom, Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use serde::Serialize;
use tracing::{debug, warn};

use crate::messages::message::{
    BlockSyncBodyResponse, BlockSyncHeadersResponse, BlockSyncRequestMessage,
    BlockSyncResponseMessage,
};

// TODO configurable
// determines the max number of parallel payload requests self can make
const BLOCKSYNC_MAX_PAYLOAD_REQUESTS: usize = 10;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum BlockSyncSelfRequester {
    /// Consensus requested this blocksync request
    Consensus,
    /// Statesync requested this blocksync request
    StateSync,
}

impl Encodable for BlockSyncSelfRequester {
    fn encode(&self, out: &mut dyn BufMut) {
        match self {
            Self::Consensus => {
                let enc: [&dyn Encodable; 1] = [&1u8];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
            Self::StateSync => {
                let enc: [&dyn Encodable; 1] = [&2u8];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
        }
    }
}

impl Decodable for BlockSyncSelfRequester {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let mut payload = Header::decode_bytes(buf, true)?;
        match u8::decode(&mut payload)? {
            1 => Ok(Self::Consensus),
            2 => Ok(Self::StateSync),
            _ => Err(alloy_rlp::Error::Custom(
                "failed to decode unknown BlockSyncSelfRequester",
            )),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BlockSyncCommand<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
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
        response: BlockSyncResponseMessage<ST, SCT, EPT>,
    },
    /// Fetch a range of headers from consensus ledger
    FetchHeaders(BlockRange),
    /// Fetch a single payload from consensus ledger
    FetchPayload(ConsensusBlockBodyId),
    /// Response to a BlockSyncEvent::SelfRequest
    Emit(
        BlockSyncSelfRequester,
        (BlockRange, Vec<ConsensusFullBlock<ST, SCT, EPT>>),
    ),
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
pub struct BlockSync<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    /// Requests from peers
    /// The map stores some of the blocks fetched from consensus blocktree while the
    /// rest of the blocks from the requested range are fetched from ledger
    /// e.g. If NodeX requests a -> c and blocks b -> c are fetched from blocktree,
    /// stored in the map is [a -> b, (NodeX, b -> c)] while a -> b is fetched from ledger
    headers_requests: HashMap<
        BlockRange,
        BTreeMap<NodeId<CertificateSignaturePubKey<ST>>, Vec<ConsensusBlockHeader<ST, SCT, EPT>>>,
    >,
    payload_requests:
        HashMap<ConsensusBlockBodyId, BTreeSet<NodeId<CertificateSignaturePubKey<ST>>>>,

    /// Headers requests for self
    self_headers_requests: HashMap<BlockRange, SelfRequest<CertificateSignaturePubKey<ST>>>,
    /// Payload requests for self
    self_payload_requests:
        HashMap<ConsensusBlockBodyId, Option<SelfRequest<CertificateSignaturePubKey<ST>>>>,
    /// Should be <= BLOCKSYNC_MAX_PAYLOAD_REQUESTS
    self_payload_requests_in_flight: usize,
    /// Parallel payload requests from self after receiving headers
    /// If payload is None, the payload request is still in flight and should be
    /// in self_payload_requests
    self_completed_headers_requests: HashMap<BlockRange, SelfCompletedHeader<ST, SCT, EPT>>,

    self_request_mode: BlockSyncSelfRequester,

    /// Excludes self node id
    override_peers: Vec<NodeId<CertificateSignaturePubKey<ST>>>,

    /// Excludes self node id
    secondary_raptorcast_peers: Vec<NodeId<CertificateSignaturePubKey<ST>>>,

    self_node_id: NodeId<CertificateSignaturePubKey<ST>>,

    rng: ChaCha8Rng,
}

// TODO move this to a separate file, restrict mutators to maintain invariants
#[derive(Debug)]
struct SelfCompletedHeader<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    requester: BlockSyncSelfRequester,
    blocks: Vec<(
        ConsensusBlockHeader<ST, SCT, EPT>,
        Option<ConsensusBlockBody<EPT>>,
    )>,
    payload_cache: HashMap<ConsensusBlockBodyId, ConsensusBlockBody<EPT>>,
}

impl<ST, SCT, EPT> BlockSync<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    pub fn new(
        override_peers_inc_self: Vec<NodeId<CertificateSignaturePubKey<ST>>>,
        self_node_id: NodeId<CertificateSignaturePubKey<ST>>,
    ) -> Self {
        let mut blocksync_instance = Self {
            headers_requests: Default::default(),
            payload_requests: Default::default(),
            self_headers_requests: Default::default(),
            self_payload_requests: Default::default(),
            self_payload_requests_in_flight: 0,
            self_completed_headers_requests: Default::default(),
            self_request_mode: BlockSyncSelfRequester::StateSync,
            override_peers: Default::default(),
            secondary_raptorcast_peers: Default::default(),
            self_node_id,
            rng: ChaCha8Rng::seed_from_u64(123456),
        };
        blocksync_instance.set_override_peers(override_peers_inc_self);

        blocksync_instance
    }

    pub fn set_override_peers(
        &mut self,
        override_peers_inc_self: Vec<NodeId<CertificateSignaturePubKey<ST>>>,
    ) {
        let peers_excl_self: Vec<_> = override_peers_inc_self
            .into_iter()
            .filter(|peer| peer != &self.self_node_id)
            .collect();
        self.override_peers = peers_excl_self;
    }

    pub fn set_secondary_raptorcast_peers(
        &mut self,
        secondary_raptorcast_peers_inc_self: Vec<NodeId<CertificateSignaturePubKey<ST>>>,
    ) {
        let peers_excl_self: Vec<_> = secondary_raptorcast_peers_inc_self
            .into_iter()
            .filter(|peer| peer != &self.self_node_id)
            .collect();
        self.secondary_raptorcast_peers = peers_excl_self;
    }

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

pub enum BlockCache<'a, ST, SCT, EPT, BPT, SBT, CCT, CRT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT, CCT, CRT>,
    SBT: StateBackend<ST, SCT>,
    CCT: ChainConfig<CRT>,
    CRT: ChainRevision,
{
    BlockTree(&'a BlockTree<ST, SCT, EPT, BPT, SBT, CCT, CRT>),
    BlockBuffer(&'a HashMap<ConsensusBlockBodyId, ConsensusBlockBody<EPT>>),
}

pub struct BlockSyncWrapper<'a, ST, SCT, EPT, BPT, SBT, VTF, CCT, CRT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT, CCT, CRT>,
    SBT: StateBackend<ST, SCT>,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    CCT: ChainConfig<CRT>,
    CRT: ChainRevision,
{
    pub block_sync: &'a mut BlockSync<ST, SCT, EPT>,

    pub block_cache: BlockCache<'a, ST, SCT, EPT, BPT, SBT, CCT, CRT>,
    pub metrics: &'a mut Metrics,
    pub nodeid: &'a NodeId<SCT::NodeIdPubKey>,
    pub current_epoch: Epoch,
    pub epoch_manager: &'a EpochManager,
    pub val_epoch_map: &'a ValidatorsEpochMapping<VTF, SCT>,
}

impl<ST, SCT, EPT, BPT, SBT, VTF, CCT, CRT>
    BlockSyncWrapper<'_, ST, SCT, EPT, BPT, SBT, VTF, CCT, CRT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT, CCT, CRT>,
    SBT: StateBackend<ST, SCT>,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    CCT: ChainConfig<CRT>,
    CRT: ChainRevision,
{
    #[must_use]
    pub fn handle_self_request(
        &mut self,
        requester: BlockSyncSelfRequester,
        block_range: BlockRange,
    ) -> Vec<BlockSyncCommand<ST, SCT, EPT>> {
        if block_range.num_blocks == SeqNum(0) {
            warn!(
                ?requester,
                ?block_range,
                "blocksync: received invalid self request"
            );
            return Vec::new();
        }

        debug!(?requester, ?block_range, "blocksync: self request");
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
        debug!(?requester, ?block_range, "blocksync: self cancel request");
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
            // NOTE: we don't remove the associated payload requests here since
            // it might be required for a different requested range
        }
    }

    #[must_use]
    pub fn handle_peer_request(
        &mut self,
        sender: NodeId<SCT::NodeIdPubKey>,
        request: BlockSyncRequestMessage,
    ) -> Vec<BlockSyncCommand<ST, SCT, EPT>> {
        let mut cmds = Vec::new();

        match request {
            BlockSyncRequestMessage::Headers(block_range) => {
                if block_range.num_blocks == SeqNum(0) {
                    debug!(
                        ?sender,
                        ?block_range,
                        "received invalid block range request"
                    );
                    return cmds;
                }

                self.metrics.blocksync_events.peer_headers_request += 1;
                debug!(?sender, ?block_range, "blocksync: peer headers request");

                let cached_blocks = match self.block_cache {
                    BlockCache::BlockTree(blocktree) => blocktree
                        .get_parent_block_chain(&block_range.last_block_id)
                        .into_iter()
                        .map(|block| block.header().clone())
                        .rev()
                        .take(block_range.num_blocks.0 as usize)
                        .rev()
                        .collect_vec(),
                    BlockCache::BlockBuffer(_) => Vec::new(), // TODO
                };

                // check if all blocks are cached
                if cached_blocks.len() == block_range.num_blocks.0 as usize {
                    // reply with the cached blocks
                    assert_eq!(
                        Some(block_range.last_block_id),
                        cached_blocks.last().map(|block| block.get_id())
                    );
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
                        .map(|block| block.get_parent_id())
                        .unwrap_or(block_range.last_block_id);
                    let ledger_fetch_range = BlockRange {
                        last_block_id: last_block_id_to_fetch,
                        num_blocks: block_range.num_blocks - SeqNum(cached_blocks.len() as u64),
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
                debug!(?sender, ?payload_id, "blocksync: peer payload request");

                if let Some(cached_payload) = self.get_cached_payload(payload_id) {
                    cmds.push(BlockSyncCommand::SendResponse {
                        to: sender,
                        response: BlockSyncResponseMessage::PayloadResponse(
                            BlockSyncBodyResponse::Found(cached_payload),
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

    fn choose_weighted(
        validators: Vec<(&NodeId<CertificateSignaturePubKey<ST>>, &Stake)>,
        mut gen: impl Rng,
    ) -> NodeId<CertificateSignaturePubKey<ST>> {
        let randomizer = |total_stake| randomize_256_with_rng(&mut gen, total_stake);
        generate_random_validator_with_randomizer(validators, randomizer)
    }

    /// Blocksync peers are selected based on the following rules:
    /// 1. If `override_peers` is set, randomly select from one of the override peers.
    /// 2. Otherwise if self is a full node and `secondary_raptorcast_peers` is not empty,
    ///    randomly select from `secondary_raptorcast_peers`
    /// 3. Otherwise, randomly select from validators based on stake weight
    fn pick_peer(
        self_node_id: &NodeId<CertificateSignaturePubKey<ST>>,
        current_epoch: Epoch,
        val_epoch_map: &ValidatorsEpochMapping<VTF, SCT>,
        override_peers: &[NodeId<CertificateSignaturePubKey<ST>>],
        secondary_raptorcast_peers: &[NodeId<CertificateSignaturePubKey<ST>>],
        rng: &mut ChaCha8Rng,
    ) -> NodeId<CertificateSignaturePubKey<ST>> {
        if !override_peers.is_empty() {
            // override peers is set
            return *override_peers.choose(rng).expect("non empty");
        }

        let validators = val_epoch_map
            .get_val_set(&current_epoch)
            .expect("current epoch exists");
        let validators = validators.get_members();
        let self_is_validator = validators.keys().contains(self_node_id);

        if !secondary_raptorcast_peers.is_empty() && !self_is_validator {
            // secondary peers is set and self is a full node
            *secondary_raptorcast_peers.choose(rng).expect("non empty")
        } else {
            // stake-weighted choose from validators
            let members = validators
                .iter()
                .filter(|(peer, _)| peer != &self_node_id)
                .collect_vec();
            assert!(!members.is_empty(), "no nodes to blocksync from");
            Self::choose_weighted(members, rng)
        }
    }

    // TODO return more informative errors instead of bool
    fn verify_block_headers(
        block_range: BlockRange,
        block_headers: &[ConsensusBlockHeader<ST, SCT, EPT>],
    ) -> bool {
        let num_blocks = block_headers.len();
        if num_blocks != block_range.num_blocks.0 as usize {
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

    fn get_cached_payload(
        &self,
        payload_id: ConsensusBlockBodyId,
    ) -> Option<ConsensusBlockBody<EPT>> {
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
    fn try_initiate_payload_requests_for_self(&mut self) -> Vec<BlockSyncCommand<ST, SCT, EPT>> {
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
                        if block.block_body_id == payload_id && maybe_payload.is_none() {
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
                debug!(?payload_id, "blocksync: self initiating payload request");

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
        headers_response: BlockSyncHeadersResponse<ST, SCT, EPT>,
    ) -> Vec<BlockSyncCommand<ST, SCT, EPT>> {
        let mut cmds = Vec::new();

        let block_range = headers_response.get_block_range();
        let Entry::Occupied(mut entry) = self.block_sync.self_headers_requests.entry(block_range)
        else {
            // unexpected respose. could be because the self request was cancelled
            // or fulfilled
            return cmds;
        };
        let self_request = entry.get_mut();

        if self_request.to != sender {
            // unexpected sender, but use the headers if it's valid
            self.metrics.blocksync_events.headers_response_unexpected += 1;
        }

        match headers_response {
            BlockSyncHeadersResponse::Found((block_range, block_headers)) => {
                assert_eq!(self_request.requester, self.block_sync.self_request_mode);

                // verify the headers
                // TODO: do we need to validate headers if response is from self ledger ?
                if Self::verify_block_headers(block_range, block_headers.as_slice()) {
                    debug!(
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
                    for payload_id in block_headers.iter().map(|block| block.block_body_id) {
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
                    debug!(
                        ?sender,
                        ?block_range,
                        "blocksync: headers response verifcation failed"
                    );

                    // response from ledger shouldn't fail headers verification
                    assert!(sender.is_some());

                    self.metrics.blocksync_events.headers_validation_failed += 1;
                    // headers response from peer is invalid, re-request after timeout
                }
            }
            BlockSyncHeadersResponse::NotAvailable(block_range) => {
                debug!(
                    ?sender,
                    ?block_range,
                    "blocksync: headers response not available"
                );
                if sender.is_some() {
                    // received not available from a peer. ignore the response and re-request
                    // after timeout
                    self.metrics.blocksync_events.headers_response_failed += 1;
                } else if self_request.to.is_none() {
                    // tried to fetch from ledger and received not available
                    self.metrics.blocksync_events.self_headers_response_failed += 1;

                    // request from a peer
                    let to = Self::pick_peer(
                        self.nodeid,
                        self.current_epoch,
                        self.val_epoch_map,
                        &self.block_sync.override_peers,
                        &self.block_sync.secondary_raptorcast_peers,
                        &mut self.block_sync.rng,
                    );
                    self_request.to = Some(to);
                    self.metrics.blocksync_events.self_headers_request += 1;

                    debug!(
                        ?to,
                        ?block_range,
                        "blocksync: header not found locally, sending request"
                    );
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

        // try start the payload requests
        cmds.extend(self.try_initiate_payload_requests_for_self());

        cmds
    }

    #[must_use]
    // if sender is None, response is from self ledger
    fn handle_payload_response_for_self(
        &mut self,
        sender: Option<NodeId<SCT::NodeIdPubKey>>,
        payload_response: BlockSyncBodyResponse<EPT>,
    ) -> Vec<BlockSyncCommand<ST, SCT, EPT>> {
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
            // unexpected sender, but use the payload if it's valid
            self.metrics.blocksync_events.payload_response_unexpected += 1;
        }

        let self_requester = self_request.requester;
        match payload_response {
            BlockSyncBodyResponse::Found(payload) => {
                assert_eq!(self_requester, self.block_sync.self_request_mode);

                self.block_sync.self_payload_requests_in_flight -= 1;

                self.metrics
                    .blocksync_events
                    .self_payload_requests_in_flight -= 1;
                if sender.is_some() {
                    // reset timeout if requested from peer
                    cmds.push(BlockSyncCommand::ResetTimeout(
                        BlockSyncRequestMessage::Payload(payload_id),
                    ));
                    self.metrics.blocksync_events.payload_response_successful += 1;
                } else {
                    self.metrics
                        .blocksync_events
                        .self_payload_response_successful += 1;
                }

                debug!(?sender, ?payload_id, "blocksync: received payload response");
                // remove entry and update existing requests
                entry.remove();

                for (_, completed_header) in
                    self.block_sync.self_completed_headers_requests.iter_mut()
                {
                    for (block, maybe_payload) in &mut completed_header.blocks {
                        if block.block_body_id == payload_id && maybe_payload.is_none() {
                            // clone incase there are multiple requests that require the same payload
                            *maybe_payload = Some(payload.clone());
                            completed_header
                                .payload_cache
                                .insert(block.block_body_id, payload.clone());
                        }
                    }
                }
            }
            BlockSyncBodyResponse::NotAvailable(payload_id) => {
                if sender.is_some() {
                    // received not available from a peer. ignore the response and re-request
                    // after timeout
                    self.metrics.blocksync_events.payload_response_failed += 1;
                } else if self_request.to.is_none() {
                    // tried to fetch from ledger and received not available
                    self.metrics.blocksync_events.self_payload_response_failed += 1;

                    // request from peer
                    let to = Self::pick_peer(
                        self.nodeid,
                        self.current_epoch,
                        self.val_epoch_map,
                        &self.block_sync.override_peers,
                        &self.block_sync.secondary_raptorcast_peers,
                        &mut self.block_sync.rng,
                    );
                    self_request.to = Some(to);
                    self.metrics.blocksync_events.self_payload_request += 1;

                    debug!(
                        ?to,
                        ?payload_id,
                        "blocksync: payload not found locally, sending request"
                    );
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

        cmds.extend(self.handle_completed_ranges());

        // try initiating more payload requests
        cmds.extend(self.try_initiate_payload_requests_for_self());

        cmds
    }

    #[must_use]
    fn handle_completed_ranges(&mut self) -> Vec<BlockSyncCommand<ST, SCT, EPT>> {
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
                .map(|(block, payload)| {
                    ConsensusFullBlock::new(block, payload.expect("asserted"))
                        .expect("blocksync'd block_body_id doesn't match")
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
        response: BlockSyncResponseMessage<ST, SCT, EPT>,
    ) -> Vec<BlockSyncCommand<ST, SCT, EPT>> {
        debug!(?response, "blocksync: self response from ledger");
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
                        num_blocks: block_range.num_blocks + SeqNum(cached_blocks.len() as u64),
                    };
                    let headers_response = match headers_response.clone() {
                        BlockSyncHeadersResponse::Found((_, mut requested_blocks)) => {
                            requested_blocks.extend(cached_blocks);
                            self.metrics
                                .blocksync_events
                                .peer_headers_request_successful += 1;
                            assert!(requested_blocks
                                .iter()
                                .zip(requested_blocks.iter().skip(1))
                                .all(|(b_1, b_2)| b_1.get_id() == b_2.get_parent_id()));
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
                    BlockSyncBodyResponse::Found(_) => {
                        self.metrics
                            .blocksync_events
                            .peer_payload_request_successful += 1
                    }
                    BlockSyncBodyResponse::NotAvailable(_) => {
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
        response: BlockSyncResponseMessage<ST, SCT, EPT>,
    ) -> Vec<BlockSyncCommand<ST, SCT, EPT>> {
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
    ) -> Vec<BlockSyncCommand<ST, SCT, EPT>> {
        debug!(?request, "blocksync: self request timeout");
        self.metrics.blocksync_events.request_timeout += 1;
        let mut cmds = Vec::new();

        match request {
            BlockSyncRequestMessage::Headers(block_range) => {
                if let Entry::Occupied(mut entry) =
                    self.block_sync.self_headers_requests.entry(block_range)
                {
                    let self_request = entry.get_mut();
                    if let Some(previous_to) = self_request.to {
                        let to = Self::pick_peer(
                            self.nodeid,
                            self.current_epoch,
                            self.val_epoch_map,
                            &self.block_sync.override_peers,
                            &self.block_sync.secondary_raptorcast_peers,
                            &mut self.block_sync.rng,
                        );
                        self_request.to = Some(to);

                        debug!(
                            ?previous_to,
                            ?to,
                            ?block_range,
                            "blocksync: header request timed out, sending new request"
                        );
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
                        // or fulfilled by a different request
                        return cmds;
                    };

                    if let Some(previous_to) = self_request.to {
                        let to = Self::pick_peer(
                            self.nodeid,
                            self.current_epoch,
                            self.val_epoch_map,
                            &self.block_sync.override_peers,
                            &self.block_sync.secondary_raptorcast_peers,
                            &mut self.block_sync.rng,
                        );
                        self_request.to = Some(to);

                        debug!(
                            ?previous_to,
                            ?to,
                            ?payload_id,
                            "blocksync: payload request timed out, sending new request"
                        );
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
    use monad_chain_config::{revision::MockChainRevision, MockChainConfig};
    use monad_consensus_types::{
        block::{
            BlockPolicy, BlockRange, ConsensusBlockHeader, ConsensusFullBlock, MockExecutionBody,
            MockExecutionProposedHeader, MockExecutionProtocol, PassthruBlockPolicy,
            PassthruWrappedBlock, GENESIS_TIMESTAMP,
        },
        checkpoint::RootInfo,
        metrics::Metrics,
        payload::{
            ConsensusBlockBody, ConsensusBlockBodyId, ConsensusBlockBodyInner, RoundSignature,
        },
        quorum_certificate::QuorumCertificate,
        voting::Vote,
    };
    use monad_crypto::{
        certificate_signature::{
            CertificateKeyPair, CertificateSignature, CertificateSignaturePubKey,
            CertificateSignatureRecoverable,
        },
        signing_domain, NopPubKey, NopSignature,
    };
    use monad_multi_sig::MultiSig;
    use monad_state_backend::{InMemoryState, StateBackend};
    use monad_testutil::validators::create_keys_w_validators;
    use monad_types::{
        BlockId, Epoch, ExecutionProtocol, Hash, NodeId, Round, SeqNum, GENESIS_BLOCK_ID,
        GENESIS_SEQ_NUM,
    };
    use monad_validator::{
        epoch_manager::EpochManager,
        leader_election::LeaderElection,
        signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
        simple_round_robin::SimpleRoundRobin,
        validator_mapping::ValidatorMapping,
        validator_set::{ValidatorSetFactory, ValidatorSetType, ValidatorSetTypeFactory},
        validators_epoch_mapping::ValidatorsEpochMapping,
    };
    use test_case::test_case;

    use super::{
        BlockCache, BlockSync, BlockSyncCommand, BlockSyncSelfRequester, BlockSyncWrapper,
    };
    use crate::{
        blocksync::BLOCKSYNC_MAX_PAYLOAD_REQUESTS,
        messages::message::{BlockSyncRequestMessage, BlockSyncResponseMessage},
    };

    const BASE_FEE: u64 = 100_000_000_000;
    const BASE_FEE_TREND: u64 = 0;
    const BASE_FEE_MOMENT: u64 = 0;

    struct BlockSyncContext<ST, SCT, EPT, BPT, SBT, VTF, LT>
    where
        ST: CertificateSignatureRecoverable,
        SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
        EPT: ExecutionProtocol,
        BPT: BlockPolicy<ST, SCT, EPT, SBT, MockChainConfig, MockChainRevision>,
        SBT: StateBackend<ST, SCT>,
        VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
        LT: LeaderElection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    {
        block_sync: BlockSync<ST, SCT, EPT>,

        // TODO: include BlockBuffer
        blocktree: BlockTree<ST, SCT, EPT, BPT, SBT, MockChainConfig, MockChainRevision>,
        metrics: Metrics,
        self_node_id: NodeId<SCT::NodeIdPubKey>,
        peer_id: NodeId<SCT::NodeIdPubKey>,
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
    type ExecutionProtocolType = MockExecutionProtocol;
    type BlockPolicyType = PassthruBlockPolicy;
    type StateBackendType = InMemoryState<SignatureType, SignatureCollectionType>;
    type LeaderElectionType = SimpleRoundRobin<PubKeyType>;
    type ChainConfigType = MockChainConfig;
    type ChainRevisionType = MockChainRevision;

    impl<BPT, SBT, VTF, LT>
        BlockSyncContext<
            SignatureType,
            SignatureCollectionType,
            ExecutionProtocolType,
            BPT,
            SBT,
            VTF,
            LT,
        >
    where
        BPT: BlockPolicy<
            SignatureType,
            SignatureCollectionType,
            ExecutionProtocolType,
            SBT,
            ChainConfigType,
            ChainRevisionType,
        >,
        SBT: StateBackend<SignatureType, SignatureCollectionType>,
        VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<SignatureType>>,
        LT: LeaderElection<NodeIdPubKey = CertificateSignaturePubKey<SignatureType>>,
    {
        fn wrapped_state(
            &mut self,
        ) -> BlockSyncWrapper<
            SignatureType,
            SignatureCollectionType,
            ExecutionProtocolType,
            BPT,
            SBT,
            VTF,
            ChainConfigType,
            ChainRevisionType,
        > {
            let block_cache = BlockCache::BlockTree(&self.blocktree);
            BlockSyncWrapper {
                block_sync: &mut self.block_sync,
                block_cache,
                metrics: &mut self.metrics,
                nodeid: &self.self_node_id,
                current_epoch: self.current_epoch,
                epoch_manager: &self.epoch_manager,
                val_epoch_map: &self.val_epoch_map,
            }
        }

        fn handle_self_request(
            &mut self,
            requester: BlockSyncSelfRequester,
            block_range: BlockRange,
        ) -> Vec<BlockSyncCommand<SignatureType, SignatureCollectionType, ExecutionProtocolType>>
        {
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
            sender: NodeId<NopPubKey>,
            request: BlockSyncRequestMessage,
        ) -> Vec<BlockSyncCommand<SignatureType, SignatureCollectionType, ExecutionProtocolType>>
        {
            self.wrapped_state().handle_peer_request(sender, request)
        }

        fn handle_ledger_response(
            &mut self,
            response: BlockSyncResponseMessage<
                SignatureType,
                SignatureCollectionType,
                ExecutionProtocolType,
            >,
        ) -> Vec<BlockSyncCommand<SignatureType, SignatureCollectionType, ExecutionProtocolType>>
        {
            self.wrapped_state().handle_ledger_response(response)
        }

        fn handle_peer_response(
            &mut self,
            sender: NodeId<NopPubKey>,
            response: BlockSyncResponseMessage<
                SignatureType,
                SignatureCollectionType,
                ExecutionProtocolType,
            >,
        ) -> Vec<BlockSyncCommand<SignatureType, SignatureCollectionType, ExecutionProtocolType>>
        {
            self.wrapped_state().handle_peer_response(sender, response)
        }

        fn handle_timeout(
            &mut self,
            request: BlockSyncRequestMessage,
        ) -> Vec<BlockSyncCommand<SignatureType, SignatureCollectionType, ExecutionProtocolType>>
        {
            self.wrapped_state().handle_timeout(request)
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
        fn get_blocks(
            &mut self,
            num_blocks: usize,
        ) -> Vec<ConsensusFullBlock<SignatureType, SignatureCollectionType, ExecutionProtocolType>>
        {
            let mut qc = QuorumCertificate::genesis_qc();
            let mut qc_seq_num = GENESIS_SEQ_NUM;
            let mut timestamp = 1;
            let mut round = Round(1);

            let mut full_blocks = Vec::new();

            for i in 0..num_blocks {
                let execution_body = MockExecutionBody {
                    data: Bytes::copy_from_slice(&i.to_le_bytes()),
                };

                let epoch = self.epoch_manager.get_epoch(round).expect("epoch exists");
                let validators = self
                    .val_epoch_map
                    .get_val_set(&epoch)
                    .unwrap()
                    .get_members();
                let leader = self.election.get_leader(round, epoch, validators).pubkey();
                let (leader_key, leader_certkey) = self
                    .keys
                    .iter()
                    .zip(&self.cert_keys)
                    .find(|(k, _)| k.pubkey() == leader)
                    .expect("key not in valset");

                let seq_num = qc_seq_num + SeqNum(1);
                let body = ConsensusBlockBody::new(ConsensusBlockBodyInner { execution_body });
                let header = ConsensusBlockHeader::new(
                    NodeId::new(leader_key.pubkey()),
                    epoch,
                    round,
                    Vec::new(), // delayed_execution_results
                    MockExecutionProposedHeader {},
                    body.get_id(),
                    qc,
                    seq_num,
                    timestamp,
                    RoundSignature::new(round, leader_certkey),
                    Some(BASE_FEE),
                    Some(BASE_FEE_TREND),
                    Some(BASE_FEE_MOMENT),
                );

                let validator_cert_pubkeys = self
                    .val_epoch_map
                    .get_cert_pubkeys(&epoch)
                    .expect("should have the current validator certificate pubkeys");

                qc = self.get_qc(&self.cert_keys, &header, validator_cert_pubkeys);
                qc_seq_num = seq_num;
                timestamp += 1;
                round += Round(1);

                full_blocks
                    .push(ConsensusFullBlock::new(header, body).expect("body matches header"));
            }

            full_blocks
        }

        fn get_qc(
            &self,
            certkeys: &[SignatureCollectionKeyPairType<SignatureCollectionType>],
            block: &ConsensusBlockHeader<
                SignatureType,
                SignatureCollectionType,
                ExecutionProtocolType,
            >,
            validator_mapping: &ValidatorMapping<
                NopPubKey,
                SignatureCollectionKeyPairType<SignatureCollectionType>,
            >,
        ) -> QuorumCertificate<SignatureCollectionType> {
            let vote = Vote {
                id: block.get_id(),
                epoch: block.epoch,
                round: block.block_round,
            };

            let msg = alloy_rlp::encode(vote);

            let mut sigs = Vec::new();
            for ck in certkeys {
                let sig = NopSignature::sign::<signing_domain::Vote>(msg.as_ref(), ck);

                for (node_id, pubkey) in validator_mapping.map.iter() {
                    if *pubkey == ck.pubkey() {
                        sigs.push((*node_id, sig));
                    }
                }
            }

            let sigcol = SignatureCollectionType::new::<signing_domain::Vote>(
                sigs,
                validator_mapping,
                msg.as_ref(),
            )
            .unwrap();

            QuorumCertificate::new(vote, sigcol)
        }
    }

    fn setup() -> BlockSyncContext<
        SignatureType,
        SignatureCollectionType,
        ExecutionProtocolType,
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
            timestamp_ns: GENESIS_TIMESTAMP,
        });

        let self_node_id = NodeId::new(keys[0].pubkey());
        let peer_id = NodeId::new(keys[1].pubkey());

        BlockSyncContext {
            block_sync: BlockSync::new(Default::default(), self_node_id),
            blocktree,
            metrics: Metrics::default(),
            self_node_id,
            peer_id,
            current_epoch: Epoch(1),
            epoch_manager,
            val_epoch_map,

            keys,
            cert_keys,
            election: SimpleRoundRobin::default(),
        }
    }

    fn find_fetch_headers_commands(
        cmds: &[BlockSyncCommand<SignatureType, SignatureCollectionType, ExecutionProtocolType>],
    ) -> Vec<&BlockSyncCommand<SignatureType, SignatureCollectionType, ExecutionProtocolType>> {
        cmds.iter()
            .filter(|cmd| matches!(cmd, BlockSyncCommand::FetchHeaders(..)))
            .collect_vec()
    }

    fn find_fetch_payload_commands(
        cmds: &[BlockSyncCommand<SignatureType, SignatureCollectionType, ExecutionProtocolType>],
    ) -> Vec<&BlockSyncCommand<SignatureType, SignatureCollectionType, ExecutionProtocolType>> {
        cmds.iter()
            .filter(|cmd| matches!(cmd, BlockSyncCommand::FetchPayload(..)))
            .collect_vec()
    }

    fn find_send_request_commands(
        cmds: &[BlockSyncCommand<SignatureType, SignatureCollectionType, ExecutionProtocolType>],
    ) -> Vec<&BlockSyncCommand<SignatureType, SignatureCollectionType, ExecutionProtocolType>> {
        cmds.iter()
            .filter(|cmd| matches!(cmd, BlockSyncCommand::SendRequest { .. }))
            .collect_vec()
    }

    fn find_send_response_commands(
        cmds: &[BlockSyncCommand<SignatureType, SignatureCollectionType, ExecutionProtocolType>],
    ) -> Vec<&BlockSyncCommand<SignatureType, SignatureCollectionType, ExecutionProtocolType>> {
        cmds.iter()
            .filter(|cmd| matches!(cmd, BlockSyncCommand::SendResponse { .. }))
            .collect_vec()
    }

    fn find_schedule_timeout_commands(
        cmds: &[BlockSyncCommand<SignatureType, SignatureCollectionType, ExecutionProtocolType>],
    ) -> Vec<&BlockSyncCommand<SignatureType, SignatureCollectionType, ExecutionProtocolType>> {
        cmds.iter()
            .filter(|cmd| matches!(cmd, BlockSyncCommand::ScheduleTimeout(..)))
            .collect_vec()
    }

    fn find_reset_timeout_commands(
        cmds: &[BlockSyncCommand<SignatureType, SignatureCollectionType, ExecutionProtocolType>],
    ) -> Vec<&BlockSyncCommand<SignatureType, SignatureCollectionType, ExecutionProtocolType>> {
        cmds.iter()
            .filter(|cmd| matches!(cmd, BlockSyncCommand::ResetTimeout(..)))
            .collect_vec()
    }

    fn find_emit_commands(
        cmds: &[BlockSyncCommand<SignatureType, SignatureCollectionType, ExecutionProtocolType>],
    ) -> Vec<&BlockSyncCommand<SignatureType, SignatureCollectionType, ExecutionProtocolType>> {
        cmds.iter()
            .filter(|cmd| matches!(cmd, BlockSyncCommand::Emit(..)))
            .collect_vec()
    }

    #[test]
    fn initiate_headers_request() {
        // Handle self request should emit a fetch headers command
        // If not available in ledger, request from peer with a timeout
        let mut context = setup();

        let requester = BlockSyncSelfRequester::Consensus;
        let block_range = BlockRange {
            last_block_id: BlockId(Hash([0x00_u8; 32])),
            num_blocks: SeqNum(1),
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

        // headers not available in self ledger, should request from a peer
        let cmds = context
            .handle_ledger_response(BlockSyncResponseMessage::headers_not_available(block_range));
        assert_eq!(cmds.len(), 2);
        let headers_request = BlockSyncRequestMessage::Headers(block_range);
        let expected_request_command = BlockSyncCommand::SendRequest {
            to: context.peer_id,
            request: headers_request,
        };
        let expected_timeout_command = BlockSyncCommand::ScheduleTimeout(headers_request);

        let request_cmds = find_send_request_commands(&cmds);
        assert_eq!(request_cmds.len(), 1);
        assert_eq!(request_cmds[0], &expected_request_command);

        let timeout_cmds = find_schedule_timeout_commands(&cmds);
        assert_eq!(timeout_cmds.len(), 1);
        assert_eq!(timeout_cmds[0], &expected_timeout_command);

        // duplicate response from ledger should not emit more commands
        let cmds = context
            .handle_ledger_response(BlockSyncResponseMessage::headers_not_available(block_range));
        assert_eq!(cmds.len(), 0);
    }

    #[test]
    fn timeout_headers_request_to_peer() {
        // Handle self request should emit a fetch headers command
        // If not available in ledger, request from peer with a timeout
        // Re-request when timeout hits
        let mut context = setup();

        let requester = BlockSyncSelfRequester::Consensus;
        let block_range = BlockRange {
            last_block_id: BlockId(Hash([0x00_u8; 32])),
            num_blocks: SeqNum(1),
        };

        // requesting a block range should initiate a headers request
        let cmds = context.handle_self_request(requester, block_range);
        assert_eq!(cmds.len(), 1);

        // headers not available in self ledger, should request from a peer
        let cmds = context
            .handle_ledger_response(BlockSyncResponseMessage::headers_not_available(block_range));
        assert_eq!(cmds.len(), 2);

        let cmds = context.handle_timeout(BlockSyncRequestMessage::Headers(block_range));
        assert_eq!(cmds.len(), 2);
        let headers_request = BlockSyncRequestMessage::Headers(block_range);
        let expected_request_command = BlockSyncCommand::SendRequest {
            to: context.peer_id,
            request: headers_request,
        };
        let expected_timeout_command = BlockSyncCommand::ScheduleTimeout(headers_request);

        let request_cmds = find_send_request_commands(&cmds);
        assert_eq!(request_cmds.len(), 1);
        assert_eq!(request_cmds[0], &expected_request_command);

        let timeout_cmds = find_schedule_timeout_commands(&cmds);
        assert_eq!(timeout_cmds.len(), 1);
        assert_eq!(timeout_cmds[0], &expected_timeout_command);
    }

    #[test]
    fn initiate_payload_requests() {
        // Handle self request should fetch headers from ledger/peer
        // After headers are received, emit fetch payload commands
        // If payload is not available in ledger, request from peer with timeout
        let mut context = setup();
        let num_blocks = 5;

        // test expects to initiate all payloads requests on headers response
        assert!(num_blocks < BLOCKSYNC_MAX_PAYLOAD_REQUESTS);

        let full_blocks = context.get_blocks(num_blocks);

        let requester = BlockSyncSelfRequester::Consensus;
        let block_range = BlockRange {
            last_block_id: full_blocks.last().unwrap().get_id(),
            num_blocks: full_blocks.last().unwrap().get_seq_num(),
        };

        // requesting a block range should initiate a headers request
        let cmds = context.handle_self_request(requester, block_range);
        assert_eq!(cmds.len(), 1);

        // headers not available in self ledger, should request from a peer
        let cmds = context
            .handle_ledger_response(BlockSyncResponseMessage::headers_not_available(block_range));

        let request_cmds = find_send_request_commands(&cmds);
        assert_eq!(request_cmds.len(), 1);

        let (headers, payloads): (Vec<_>, Vec<_>) = full_blocks
            .into_iter()
            .map(ConsensusFullBlock::split)
            .unzip();
        // valid headers response from a peer should initiate all its payload requests
        let cmds = context.handle_peer_response(
            context.peer_id,
            BlockSyncResponseMessage::found_headers(block_range, headers),
        );
        // num_blocks payload fetch commands and 1 reset timeout command
        assert_eq!(cmds.len(), num_blocks + 1);
        let headers_request = BlockSyncRequestMessage::Headers(block_range);
        let expected_reset_command = BlockSyncCommand::ResetTimeout(headers_request);

        let reset_timeout_cmds = find_reset_timeout_commands(&cmds);
        assert_eq!(reset_timeout_cmds.len(), 1);
        assert_eq!(reset_timeout_cmds[0], &expected_reset_command);

        let fetch_payload_cmds = find_fetch_payload_commands(&cmds);
        assert_eq!(fetch_payload_cmds.len(), num_blocks);

        for payload in payloads {
            let payload_id = payload.get_id();
            assert!(fetch_payload_cmds.contains(&&BlockSyncCommand::FetchPayload(payload_id)));

            let payload_request = BlockSyncRequestMessage::Payload(payload_id);
            let expected_request_command = BlockSyncCommand::SendRequest {
                to: context.peer_id,
                request: payload_request,
            };
            let expected_timeout_command = BlockSyncCommand::ScheduleTimeout(payload_request);

            // payload not available in self ledger. should request from peer
            let cmds = context.handle_ledger_response(
                BlockSyncResponseMessage::payload_not_available(payload_id),
            );
            assert_eq!(cmds.len(), 2);

            let request_cmds = find_send_request_commands(&cmds);
            assert_eq!(request_cmds.len(), 1);
            assert_eq!(request_cmds[0], &expected_request_command);

            let timeout_cmds = find_schedule_timeout_commands(&cmds);
            assert_eq!(timeout_cmds.len(), 1);
            assert_eq!(timeout_cmds[0], &expected_timeout_command);

            // duplicate response from ledger should not emit more commands
            let cmds = context.handle_ledger_response(
                BlockSyncResponseMessage::payload_not_available(payload_id),
            );
            assert_eq!(cmds.len(), 0);
        }
    }

    #[test]
    fn timeout_payload_request() {
        // If payload is not available in ledger, request from peer with a timeout
        // Re-request when timeout hits
        let mut context = setup();

        let full_block = context.get_blocks(1).pop().unwrap();
        let payload_id = full_block.get_body_id();

        let requester = BlockSyncSelfRequester::Consensus;
        let block_range = BlockRange {
            last_block_id: full_block.get_id(),
            num_blocks: SeqNum(1),
        };

        // requesting a block range should initiate a headers request
        let cmds = context.handle_self_request(requester, block_range);
        assert_eq!(cmds.len(), 1);

        let (header, _) = full_block.split();
        // headers available, should start payload fetch
        let cmds = context.handle_ledger_response(BlockSyncResponseMessage::found_headers(
            block_range,
            vec![header],
        ));
        assert_eq!(cmds.len(), 1);

        // payload not available, should request from peer
        let cmds = context
            .handle_ledger_response(BlockSyncResponseMessage::payload_not_available(payload_id));
        assert_eq!(cmds.len(), 2);

        let payload_request = BlockSyncRequestMessage::Payload(payload_id);
        // re-request payload from peer on timeout
        let cmds = context.handle_timeout(payload_request);
        assert_eq!(cmds.len(), 2);
        let expected_request_command = BlockSyncCommand::SendRequest {
            to: context.peer_id,
            request: payload_request,
        };
        let expected_timeout_command = BlockSyncCommand::ScheduleTimeout(payload_request);

        let request_cmds = find_send_request_commands(&cmds);
        assert_eq!(request_cmds.len(), 1);
        assert_eq!(request_cmds[0], &expected_request_command);

        let timeout_cmds = find_schedule_timeout_commands(&cmds);

        assert_eq!(timeout_cmds.len(), 1);
        assert_eq!(timeout_cmds[0], &expected_timeout_command);
    }

    #[test]
    fn avoid_duplicate_payload_requests_if_in_blocktree() {
        // Handle self request should fetch headers from ledger/peer
        // After headers are received, emit fetch payload commands only if
        // the payload is not found in the blocktree
        let mut context = setup();

        let num_blocks = 5;
        let num_in_blocktree = 2;
        let full_blocks = context.get_blocks(num_blocks);

        // add the first num_in_blocktree blocks to the blocktree
        for full_block in full_blocks.iter().take(num_in_blocktree) {
            context
                .blocktree
                .add(PassthruWrappedBlock(full_block.clone()));
        }

        // request all num_blocks
        let requester = BlockSyncSelfRequester::Consensus;
        let block_range = BlockRange {
            last_block_id: full_blocks.last().unwrap().get_id(),
            num_blocks: full_blocks.last().unwrap().get_seq_num(),
        };

        let (headers, payloads): (Vec<_>, Vec<_>) = full_blocks
            .into_iter()
            .map(ConsensusFullBlock::split)
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
    fn avoid_payload_requests_if_in_flight() {
        // Handle self request should fetch headers from ledger/peer
        // After headers are received, emit fetch payload commands only if
        // the payload request isn't already in flight
        let mut context = setup();

        let num_blocks = 5;
        let num_in_flight = 2;
        let full_blocks = context.get_blocks(num_blocks);

        // request for all num_blocks
        let requester_1 = BlockSyncSelfRequester::Consensus;
        let block_range_1 = BlockRange {
            last_block_id: full_blocks.last().unwrap().get_id(),
            num_blocks: full_blocks.last().unwrap().get_seq_num(),
        };

        // request for only the first two blocks
        let requester_2 = BlockSyncSelfRequester::Consensus;
        let block_range_2 = BlockRange {
            last_block_id: full_blocks[num_in_flight - 1].get_id(),
            num_blocks: SeqNum(2),
        };
        let full_blocks_2 = full_blocks.iter().take(2).cloned().collect_vec();

        let (headers, payloads): (Vec<_>, Vec<_>) = full_blocks
            .into_iter()
            .map(ConsensusFullBlock::split)
            .unzip();

        // request all num_blocks
        let cmds = context.handle_self_request(requester_1, block_range_1);
        assert_eq!(cmds.len(), 1);

        // found headers, should initiate payload requests for all blocks
        let cmds = context.handle_ledger_response(BlockSyncResponseMessage::found_headers(
            block_range_1,
            headers.clone(),
        ));

        let fetch_payload_cmds = find_fetch_payload_commands(&cmds);
        assert_eq!(fetch_payload_cmds.len(), num_blocks);

        // request only the first two blocks
        let cmds = context.handle_self_request(requester_2, block_range_2);
        assert_eq!(cmds.len(), 1);

        // found headers, should not emit any payload requests since both are already in flight
        let cmds = context.handle_ledger_response(BlockSyncResponseMessage::found_headers(
            block_range_2,
            headers.iter().take(num_in_flight).cloned().collect_vec(),
        ));
        assert_eq!(cmds.len(), 0);

        // return the first two payloads, should emit the full blocks after second payload
        let cmds = context
            .handle_ledger_response(BlockSyncResponseMessage::found_payload(payloads[0].clone()));
        assert!(cmds.is_empty());

        let cmds = context
            .handle_ledger_response(BlockSyncResponseMessage::found_payload(payloads[1].clone()));
        assert_eq!(cmds.len(), 1);

        let emit_cmds = find_emit_commands(&cmds);
        assert_eq!(emit_cmds.len(), 1);
        assert_eq!(
            emit_cmds[0],
            &BlockSyncCommand::Emit(requester_2, (block_range_2, full_blocks_2))
        );
    }

    #[test]
    fn avoid_payload_requests_if_already_received() {
        // Handle self request should fetch headers from ledger/peer
        // After headers are received, emit fetch payload commands only if
        // the payload is not found in an already completed request range
        let mut context = setup();

        let num_blocks = 5;
        let num_already_received = 2;
        let full_blocks = context.get_blocks(num_blocks);

        // request for all num_blocks
        let requester_1 = BlockSyncSelfRequester::Consensus;
        let block_range_1 = BlockRange {
            last_block_id: full_blocks.last().unwrap().get_id(),
            num_blocks: full_blocks.last().unwrap().get_seq_num(),
        };

        // request for only the first two blocks
        let requester_2 = BlockSyncSelfRequester::Consensus;
        let block_range_2 = BlockRange {
            last_block_id: full_blocks[num_already_received - 1].get_id(),
            num_blocks: SeqNum(2),
        };
        let full_blocks_2 = full_blocks.iter().take(2).cloned().collect_vec();

        let (headers, payloads): (Vec<_>, Vec<_>) = full_blocks
            .into_iter()
            .map(ConsensusFullBlock::split)
            .unzip();

        // request all num_blocks
        let cmds = context.handle_self_request(requester_1, block_range_1);
        assert_eq!(cmds.len(), 1);

        // found headers, should initiate payload requests for all blocks
        let cmds = context.handle_ledger_response(BlockSyncResponseMessage::found_headers(
            block_range_1,
            headers.clone(),
        ));

        let fetch_payload_cmds = find_fetch_payload_commands(&cmds);
        assert_eq!(fetch_payload_cmds.len(), num_blocks);

        // return the first two payloads
        let cmds = context
            .handle_ledger_response(BlockSyncResponseMessage::found_payload(payloads[0].clone()));
        assert!(cmds.is_empty());
        let cmds = context
            .handle_ledger_response(BlockSyncResponseMessage::found_payload(payloads[1].clone()));
        assert!(cmds.is_empty());

        // request only the first two blocks
        let cmds = context.handle_self_request(requester_2, block_range_2);
        assert_eq!(cmds.len(), 1);

        // found headers, should emit full blocks immediately since it was already received
        let cmds = context.handle_ledger_response(BlockSyncResponseMessage::found_headers(
            block_range_2,
            headers
                .iter()
                .take(num_already_received)
                .cloned()
                .collect_vec(),
        ));
        assert_eq!(cmds.len(), 1);

        // should emit the full blocks
        let emit_cmds = find_emit_commands(&cmds);
        assert_eq!(emit_cmds.len(), 1);
        assert_eq!(
            emit_cmds[0],
            &BlockSyncCommand::Emit(requester_2, (block_range_2, full_blocks_2))
        );
    }

    #[test]
    fn throttle_payload_requests() {
        // Handle self request should fetch headers from ledger/peer
        // After headers are received, emit atmost BLOCKSYNC_MAX_PAYLOAD_REQUESTS
        // fetch payload commands.
        // For every payload received, emit another fetch payload command (if needed)
        let mut context = setup();
        let num_blocks = BLOCKSYNC_MAX_PAYLOAD_REQUESTS + 1;

        let full_blocks = context.get_blocks(num_blocks);

        let requester = BlockSyncSelfRequester::Consensus;
        let block_range = BlockRange {
            last_block_id: full_blocks.last().unwrap().get_id(),
            num_blocks: full_blocks.last().unwrap().get_seq_num(),
        };

        // requesting a block range should initiate a headers request
        let cmds = context.handle_self_request(requester, block_range);
        assert_eq!(cmds.len(), 1);

        // headers not available in self ledger, should request from a peer
        let cmds = context
            .handle_ledger_response(BlockSyncResponseMessage::headers_not_available(block_range));

        let request_cmds = find_send_request_commands(&cmds);
        assert_eq!(request_cmds.len(), 1);

        let (headers, payloads): (Vec<_>, Vec<_>) = full_blocks
            .into_iter()
            .map(ConsensusFullBlock::split)
            .unzip();
        // valid headers response from a peer should initiate all its payload requests
        let cmds = context.handle_peer_response(
            context.peer_id,
            BlockSyncResponseMessage::found_headers(block_range, headers),
        );
        // BLOCKSYNC_MAX_PAYLOAD_REQUESTS payload fetch commands and 1 reset timeout command
        assert_eq!(cmds.len(), BLOCKSYNC_MAX_PAYLOAD_REQUESTS + 1);

        let payloads: BTreeMap<ConsensusBlockBodyId, ConsensusBlockBody<ExecutionProtocolType>> =
            payloads
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
        // should request the payload that was queued to be requested
        assert_eq!(find_fetch_payload_commands(&cmds).len(), 1);

        let payload_2 = payloads.get(&requested_payload_ids[1]).unwrap().clone();
        let cmds =
            context.handle_ledger_response(BlockSyncResponseMessage::found_payload(payload_2));
        // all payloads have been requested
        assert_eq!(find_fetch_payload_commands(&cmds).len(), 0);
    }

    #[test]
    fn emit_requested_blocks() {
        // After receiving all payloads for the given range, emit the requested
        // block range with full blocks
        let mut context = setup();
        let num_blocks = 5;

        let full_blocks = context.get_blocks(num_blocks);

        let requester = BlockSyncSelfRequester::Consensus;
        let block_range = BlockRange {
            last_block_id: full_blocks.last().unwrap().get_id(),
            num_blocks: full_blocks.last().unwrap().get_seq_num(),
        };

        // requesting a block range should initiate a headers request
        let cmds = context.handle_self_request(requester, block_range);
        assert_eq!(cmds.len(), 1);

        // headers not available in self ledger, should request from a peer
        let cmds = context
            .handle_ledger_response(BlockSyncResponseMessage::headers_not_available(block_range));

        let request_cmds = find_send_request_commands(&cmds);
        assert_eq!(request_cmds.len(), 1);

        let (headers, payloads): (Vec<_>, Vec<_>) = full_blocks
            .clone()
            .into_iter()
            .map(ConsensusFullBlock::split)
            .unzip();
        // valid headers response from a peer should initiate all its payload requests
        let cmds = context.handle_peer_response(
            context.peer_id,
            BlockSyncResponseMessage::found_headers(block_range, headers),
        );
        assert_eq!(cmds.len(), num_blocks + 1);

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

        for (index, (payload, request_command)) in
            payloads.into_iter().zip(payload_self_requests).enumerate()
        {
            let payload_id = payload.get_id();
            let expected_payload_request = BlockSyncRequestMessage::Payload(payload_id);
            let expected_request_command = BlockSyncCommand::SendRequest {
                to: context.peer_id,
                request: expected_payload_request,
            };
            assert_eq!(request_command, expected_request_command);

            let cmds = context.handle_peer_response(
                context.peer_id,
                BlockSyncResponseMessage::found_payload(payload),
            );
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

    #[test_case(true; "all headers cached in blocktree")]
    #[test_case(false; "all headers received from ledger")]
    fn peer_headers_request(cached_in_blocktree: bool) {
        // If a peer requests headers and
        //      headers are in blocktree, respond with headers
        //      headers are not in blocktree, fetch from ledger, and send response
        let mut context = setup();

        let num_blocks = 5;
        let full_blocks = context.get_blocks(num_blocks);
        let block_range = BlockRange {
            last_block_id: full_blocks.last().unwrap().get_id(),
            num_blocks: full_blocks.last().unwrap().get_seq_num(),
        };

        let headers = full_blocks
            .iter()
            .map(|full_block| full_block.header().clone())
            .collect_vec();

        let cmds = if cached_in_blocktree {
            for full_block in full_blocks {
                context.blocktree.add(PassthruWrappedBlock(full_block));
            }
            // headers are in blocktree, should emit response command with all the headers
            context.handle_peer_request(
                context.peer_id,
                BlockSyncRequestMessage::Headers(block_range),
            )
        } else {
            // headers not in blocktree, should try fetch from ledger
            let cmds = context.handle_peer_request(
                context.peer_id,
                BlockSyncRequestMessage::Headers(block_range),
            );
            assert_eq!(cmds.len(), 1);
            let expected_fetch_command = BlockSyncCommand::FetchHeaders(block_range);

            let fetch_headers_cmds = find_fetch_headers_commands(&cmds);
            assert_eq!(fetch_headers_cmds.len(), 1);
            assert_eq!(fetch_headers_cmds[0], &expected_fetch_command);

            // ledger response should emit response command with all the headers
            context.handle_ledger_response(BlockSyncResponseMessage::found_headers(
                block_range,
                headers.clone(),
            ))
        };
        assert_eq!(cmds.len(), 1);
        let headers_response = BlockSyncResponseMessage::found_headers(block_range, headers);
        let expected_response_command = BlockSyncCommand::SendResponse {
            to: context.peer_id,
            response: headers_response,
        };

        let response_cmds = find_send_response_commands(&cmds);
        assert_eq!(response_cmds.len(), 1);
        assert_eq!(response_cmds[0], &expected_response_command);

        context.assert_empty_block_sync_state();
    }

    #[test_case(true; "partial headers received from ledger")]
    #[test_case(false; "partial headers not in ledger")]
    fn peer_headers_request_partially_cached(headers_in_ledger: bool) {
        // If a peer requests headers, retrieve as many as possible from blocktree
        // and fetch rest from ledger
        let mut context = setup();

        let num_blocks = 5;
        let num_blocks_cached = 2;
        let full_blocks = context.get_blocks(num_blocks);
        let full_block_range = BlockRange {
            last_block_id: full_blocks.last().unwrap().get_id(),
            num_blocks: full_blocks.last().unwrap().get_seq_num(),
        };
        let headers = full_blocks
            .iter()
            .map(|full_block| full_block.header().clone())
            .collect_vec();

        let headers_not_cached = headers
            .iter()
            .take(num_blocks - num_blocks_cached)
            .cloned()
            .collect_vec();
        let ledger_fetch_range = BlockRange {
            last_block_id: headers_not_cached.last().unwrap().get_id(),
            num_blocks: SeqNum(headers_not_cached.len() as u64),
        };

        for full_block in full_blocks.iter().rev().take(num_blocks_cached).cloned() {
            context.blocktree.add(PassthruWrappedBlock(full_block));
        }

        // 2 headers are in blocktree, should fetch 3 from ledger
        let cmds = context.handle_peer_request(
            context.peer_id,
            BlockSyncRequestMessage::Headers(full_block_range),
        );
        assert_eq!(cmds.len(), 1);
        let expected_fetch_command = BlockSyncCommand::FetchHeaders(ledger_fetch_range);

        let fetch_headers_cmds = find_fetch_headers_commands(&cmds);
        assert_eq!(fetch_headers_cmds.len(), 1);
        assert_eq!(fetch_headers_cmds[0], &expected_fetch_command);

        let (cmds, expected_response) = if headers_in_ledger {
            // ledger response should emit response with all the headers
            let cmds = context.handle_ledger_response(BlockSyncResponseMessage::found_headers(
                ledger_fetch_range,
                headers_not_cached,
            ));

            (
                cmds,
                BlockSyncResponseMessage::found_headers(full_block_range, headers),
            )
        } else {
            // ledger response should emit response as not available
            let cmds = context.handle_ledger_response(
                BlockSyncResponseMessage::headers_not_available(ledger_fetch_range),
            );
            (
                cmds,
                BlockSyncResponseMessage::headers_not_available(full_block_range),
            )
        };
        assert_eq!(cmds.len(), 1);
        let expected_response_command = BlockSyncCommand::SendResponse {
            to: context.peer_id,
            response: expected_response,
        };

        let response_cmds = find_send_response_commands(&cmds);
        assert_eq!(response_cmds.len(), 1);
        assert_eq!(response_cmds[0], &expected_response_command);

        context.assert_empty_block_sync_state();
    }

    #[test]
    fn peer_headers_request_not_available() {
        // If a peer requests headers and headers aren't in blocktree or ledger, emit not available
        let mut context = setup();

        let num_blocks = 5;
        let full_blocks = context.get_blocks(num_blocks);
        let block_range = BlockRange {
            last_block_id: full_blocks.last().unwrap().get_id(),
            num_blocks: full_blocks.last().unwrap().get_seq_num(),
        };

        // headers not in blocktree, should try fetch from ledger
        let cmds = context.handle_peer_request(
            context.peer_id,
            BlockSyncRequestMessage::Headers(block_range),
        );
        assert_eq!(cmds.len(), 1);
        let expected_fetch_command = BlockSyncCommand::FetchHeaders(block_range);

        let fetch_headers_cmds = find_fetch_headers_commands(&cmds);
        assert_eq!(fetch_headers_cmds.len(), 1);
        assert_eq!(fetch_headers_cmds[0], &expected_fetch_command);

        // ledger response should emit response command as headers not available
        let cmds = context
            .handle_ledger_response(BlockSyncResponseMessage::headers_not_available(block_range));

        assert_eq!(cmds.len(), 1);
        let headers_response = BlockSyncResponseMessage::headers_not_available(block_range);
        let expected_response_command = BlockSyncCommand::SendResponse {
            to: context.peer_id,
            response: headers_response,
        };

        let response_cmds = find_send_response_commands(&cmds);
        assert_eq!(response_cmds.len(), 1);
        assert_eq!(response_cmds[0], &expected_response_command);

        context.assert_empty_block_sync_state();
    }

    #[test_case(true; "payload cached in blocktree")]
    #[test_case(false; "payload received from ledger")]
    fn peer_payload_request(cached_in_blocktree: bool) {
        // If a peer requests a payload and
        //      payload is in blocktree, respond with payload
        //      payload is not in blocktree, fetch from ledger, and send response
        let mut context = setup();

        let num_blocks = 1;
        let full_blocks = context.get_blocks(num_blocks);

        let payload = full_blocks[0].body().clone();
        let payload_id = payload.get_id();

        let cmds = if cached_in_blocktree {
            context
                .blocktree
                .add(PassthruWrappedBlock(full_blocks[0].clone()));

            // payload in blocktree, should emit response command with the requested payload
            context.handle_peer_request(
                context.peer_id,
                BlockSyncRequestMessage::Payload(payload_id),
            )
        } else {
            // payload not in blocktree, should try fetch from ledger
            let cmds = context.handle_peer_request(
                context.peer_id,
                BlockSyncRequestMessage::Payload(payload_id),
            );
            assert_eq!(cmds.len(), 1);
            let expected_fetch_command = BlockSyncCommand::FetchPayload(payload_id);

            let fetch_payload_cmds = find_fetch_payload_commands(&cmds);
            assert_eq!(fetch_payload_cmds.len(), 1);
            assert_eq!(fetch_payload_cmds[0], &expected_fetch_command);

            // ledger response should emit response command with requested payload
            context.handle_ledger_response(BlockSyncResponseMessage::found_payload(payload.clone()))
        };
        assert_eq!(cmds.len(), 1);
        let payload_response = BlockSyncResponseMessage::found_payload(payload);
        let expected_response_command = BlockSyncCommand::SendResponse {
            to: context.peer_id,
            response: payload_response,
        };

        let response_cmds = find_send_response_commands(&cmds);
        assert_eq!(response_cmds.len(), 1);
        assert_eq!(response_cmds[0], &expected_response_command);

        context.assert_empty_block_sync_state();
    }

    #[test]
    fn peer_payload_request_not_available() {
        // If a peer requests payload and payload is not in blocktree or ledger, emit not available
        let mut context = setup();

        let num_blocks = 1;
        let full_blocks = context.get_blocks(num_blocks);

        let payload = full_blocks[0].body().clone();
        let payload_id = payload.get_id();

        // payload not in blocktree, should try fetch from ledger
        let cmds = context.handle_peer_request(
            context.peer_id,
            BlockSyncRequestMessage::Payload(payload_id),
        );
        assert_eq!(cmds.len(), 1);
        let expected_fetch_command = BlockSyncCommand::FetchPayload(payload_id);

        let fetch_payload_cmds = find_fetch_payload_commands(&cmds);
        assert_eq!(fetch_payload_cmds.len(), 1);
        assert_eq!(fetch_payload_cmds[0], &expected_fetch_command);

        // ledger response should emit response command as payload not available
        let cmds = context
            .handle_ledger_response(BlockSyncResponseMessage::payload_not_available(payload_id));
        assert_eq!(cmds.len(), 1);
        let payload_response = BlockSyncResponseMessage::payload_not_available(payload_id);
        let expected_response_command = BlockSyncCommand::SendResponse {
            to: context.peer_id,
            response: payload_response,
        };

        let response_cmds = find_send_response_commands(&cmds);
        assert_eq!(response_cmds.len(), 1);
        assert_eq!(response_cmds[0], &expected_response_command);

        context.assert_empty_block_sync_state();
    }

    #[test]
    fn test_invalid_block_range_requests() {
        // Reject blocksync header requests with num_blocks = 0
        let mut context = setup();

        let requester = BlockSyncSelfRequester::Consensus;
        let invalid_block_range = BlockRange {
            last_block_id: BlockId(Hash([0x00_u8; 32])),
            num_blocks: SeqNum(0),
        };

        let cmds = context.handle_self_request(requester, invalid_block_range);
        assert!(cmds.is_empty());
        context.assert_empty_block_sync_state();

        let invalid_request_msg = BlockSyncRequestMessage::Headers(invalid_block_range);
        let cmds = context.handle_peer_request(context.peer_id, invalid_request_msg);

        assert!(cmds.is_empty());
        context.assert_empty_block_sync_state();
    }
}
