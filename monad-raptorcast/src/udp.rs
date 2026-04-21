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

use std::collections::BTreeMap;

use bytes::Bytes;
use monad_crypto::{
    certificate_signature::{CertificateSignaturePubKey, CertificateSignatureRecoverable, PubKey},
    signing_domain,
};
use monad_executor::ExecutorMetricsChain;
use monad_types::{Epoch, NodeId, Round};
use monad_validator::validator_set::{ValidatorSet, ValidatorSetType as _};

pub use crate::packet::build_messages;
use crate::{
    decoding::{DecoderCache, DecodingContext, TryDecodeError, TryDecodeStatus},
    metrics::{
        UdpStateMetrics, COUNTER_RAPTORCAST_CHUNKS_DROPPED_INCOMPATIBLE_VERSION,
        COUNTER_RAPTORCAST_SECONDARY_CHUNKS_DROPPED_INCOMPATIBLE_VERSION,
        COUNTER_RAPTORCAST_V0_PRIMARY_CHUNKS_ACCEPTED,
        COUNTER_RAPTORCAST_V0_SECONDARY_CHUNKS_ACCEPTED,
        COUNTER_RAPTORCAST_V1_PRIMARY_CHUNKS_ACCEPTED,
        COUNTER_RAPTORCAST_V1_SECONDARY_CHUNKS_ACCEPTED,
        GAUGE_RAPTORCAST_DECODING_CACHE_SIGNATURE_VERIFICATIONS_RATE_LIMITED,
        GAUGE_RAPTORCAST_DETERMINISTIC_ROLLOUT_STAGE,
    },
    packet::deterministic,
    parser::{
        packet_parser::{ChunkValidationEnv, MalformedPacket, RaptorcastPacket, SignedOverData},
        signature_verifier::{SignatureVerifier, SignatureVerifierError},
    },
    round_info::RoundInfoCache,
    util::{
        compute_app_message_hash, compute_hash, unix_ts_ms_now, AppMessageHash, BroadcastMode,
        EncodingScheme, FullNodeGroupMap, GlobalMerkleRoot, MerkleRoot, NodeIdHash,
        PrimaryBroadcastGroup, SecondaryBroadcastGroup,
    },
    v1_rollout::{self, DeterministicProtocolRolloutStage},
};

pub const SIGNATURE_CACHE_SIZE: usize = 10_000;

// Drop a message to be transmitted if it would lead to more than this number of packets
// to be transmitted.  This can happen in Broadcast mode when the message is large or
// if we have many peers to transmit the message to.
pub const MAX_NUM_PACKETS: usize = 65535;

/// The maximum sane validator set size. Defined in
/// <execution>/monad/staking/util/constants.hpp.
pub const MAX_VALIDATOR_SET_SIZE: usize = 200;
pub const MIN_VALIDATOR_SET_SIZE: usize = 1;

/// Cache key for signature verification: header + merkle root
pub type SignatureCacheKey = SignedOverData;
pub type ChunkSignatureVerifier<ST> =
    SignatureVerifier<ST, SignatureCacheKey, signing_domain::RaptorcastChunk>;

pub(crate) struct UdpState<ST: CertificateSignatureRecoverable> {
    self_id: NodeId<CertificateSignaturePubKey<ST>>,
    self_id_hash: NodeIdHash,
    max_age_ms: u64,

    // TODO add a cap on max number of chunks that will be forwarded per message? so that a DOS
    // can't be induced by spamming broadcast chunks to any given node
    // TODO we also need to cap the max number chunks that are decoded - because an adversary could
    // generate a bunch of linearly dependent chunks and cause unbounded memory usage.
    decoder_cache: DecoderCache<CertificateSignaturePubKey<ST>>,

    round_info_cache: RoundInfoCache<CertificateSignaturePubKey<ST>>,

    signature_verifier: ChunkSignatureVerifier<ST>,
    v1_rollout: DeterministicProtocolRolloutStage,

    metrics: UdpStateMetrics,
}

impl<ST: CertificateSignatureRecoverable> UdpState<ST> {
    pub fn new(
        self_id: NodeId<CertificateSignaturePubKey<ST>>,
        max_age_ms: u64,
        sig_verification_rate_limit: u32,
    ) -> Self {
        let self_id_hash = compute_hash(&self_id);
        let signature_verifier = SignatureVerifier::new()
            .with_cache(SIGNATURE_CACHE_SIZE)
            .with_rate_limit(sig_verification_rate_limit);

        Self {
            self_id,
            self_id_hash,
            max_age_ms,

            decoder_cache: DecoderCache::default(),
            signature_verifier,
            round_info_cache: RoundInfoCache::new(),
            v1_rollout: v1_rollout::CURRENT_STAGE,

            metrics: UdpStateMetrics::new(),
        }
    }

    pub fn set_v1_rollout(&mut self, stage: DeterministicProtocolRolloutStage) {
        self.v1_rollout = stage;
        self.metrics.executor_metrics_mut()[GAUGE_RAPTORCAST_DETERMINISTIC_ROLLOUT_STAGE] =
            stage as u64;
    }

    pub fn metrics(&self) -> &UdpStateMetrics {
        &self.metrics
    }

    pub fn decoder_metrics(&self) -> ExecutorMetricsChain<'_> {
        self.decoder_cache.metrics()
    }

    pub fn update_current_round(&mut self, round: Round) {
        self.round_info_cache.update_current_round(round);
    }

    pub fn handle_unicast(
        &mut self,
        epoch_validators: &BTreeMap<Epoch, ValidatorSet<CertificateSignaturePubKey<ST>>>,
        chunk: &ValidatedChunk<CertificateSignaturePubKey<ST>>,
        _sender: Option<&NodeId<CertificateSignaturePubKey<ST>>>,
    ) -> Option<(NodeId<CertificateSignaturePubKey<ST>>, Bytes)> {
        if chunk.recipient_hash != Some(self.self_id_hash) {
            tracing::debug!(
                ?self.self_id,
                recipient_hash =? chunk.recipient_hash,
                "dropping spoofed unicast message"
            );
            return None;
        }

        let validator_set = match chunk.group_id {
            GroupId::Primary(epoch) => epoch_validators.get(&epoch),
            GroupId::Secondary(_round) => None,
        };

        let decoding_context = DecodingContext::new(validator_set, unix_ts_ms_now());
        let message = self.try_decode(chunk, &decoding_context)?;

        if let Some((_author, message)) = &message {
            if let Some(hash_valid) = chunk.check_message_hash(message) {
                if !hash_valid {
                    self.decoder_cache.mark_tainted(chunk);
                    tracing::error!(
                        author =? chunk.author,
                        "message failed hash validation"
                    );
                    return None;
                }
            }
        }

        message
    }

    pub fn handle_primary_raptorcast(
        &mut self,
        epoch_validators: &BTreeMap<Epoch, ValidatorSet<CertificateSignaturePubKey<ST>>>,
        chunk: &ValidatedChunk<CertificateSignaturePubKey<ST>>,
        rebroadcast_to: &mut impl FnMut(Vec<NodeId<CertificateSignaturePubKey<ST>>>),
        sender: Option<&NodeId<CertificateSignaturePubKey<ST>>>,
    ) -> Option<(NodeId<CertificateSignaturePubKey<ST>>, Bytes)> {
        let epoch = match chunk.group_id {
            GroupId::Primary(epoch) => epoch,
            GroupId::Secondary(_round) => unreachable!(),
        };
        let Ok(group) = PrimaryBroadcastGroup::of_epoch(epoch, &chunk.author, epoch_validators)
        else {
            tracing::debug!(
                ?chunk.group_id,
                author =? chunk.author,
                "dropping message from unknown author/group"
            );
            return None;
        };

        if let Some(sender) = sender {
            if !group.is_sender_valid(sender) {
                tracing::debug!(
                    ?chunk.group_id,
                    author =? chunk.author,
                    ?sender,
                    "dropping message from invalid sender"
                );
                return None;
            }
        }

        let validator_set = group.validator_set();
        let decoding_context = DecodingContext::new(Some(validator_set), unix_ts_ms_now());

        match chunk.encoding_scheme {
            EncodingScheme::Deterministic25(round) => self.handle_deterministic_primary(
                &group,
                round,
                chunk,
                &decoding_context,
                rebroadcast_to,
            ),
            _ => self.handle_regular_primary(&group, chunk, &decoding_context, rebroadcast_to),
        }
    }

    fn handle_deterministic_primary(
        &mut self,
        group: &PrimaryBroadcastGroup<'_, CertificateSignaturePubKey<ST>>,
        round: Round,
        chunk: &ValidatedChunk<CertificateSignaturePubKey<ST>>,
        decoding_context: &DecodingContext<CertificateSignaturePubKey<ST>>,
        rebroadcast_to: &mut impl FnMut(Vec<NodeId<CertificateSignaturePubKey<ST>>>),
    ) -> Option<(NodeId<CertificateSignaturePubKey<ST>>, Bytes)> {
        let Some(round_info) = self.round_info_cache.get_or_insert_primary(round) else {
            tracing::debug!(
                ?chunk.group_id,
                ?chunk.chunk_id,
                author =? chunk.author,
                "dropping primary chunk for round that is too far in the past or future"
            );
            return None;
        };

        // already logged the equivocation in try_commit.
        round_info.try_commit(chunk)?;

        let Some(routing) = round_info.chunk_routing(group, chunk) else {
            tracing::debug!(
                ?chunk.group_id,
                ?chunk.chunk_id,
                author =? chunk.author,
                "dropping chunk that is not routed to self or any peers"
            );
            return None;
        };

        let is_recipient = *routing.recipient() == self.self_id;
        let rebroadcast_targets = if is_recipient {
            Some(routing.rebroadcast_targets())
        } else {
            None
        };

        let message = self.try_decode(chunk, decoding_context)?;
        if let Some((_author, message)) = &message {
            if let Some(encoding_valid) = chunk.check_deterministic_encoding(message, group) {
                if !encoding_valid {
                    self.decoder_cache.mark_tainted(chunk);
                    tracing::warn!(
                        author =? chunk.author,
                        "message failed deterministic encoding validation"
                    );
                    return None;
                }
            }
        }

        if let Some(targets) = rebroadcast_targets {
            rebroadcast_to(targets);
        }

        message
    }

    fn handle_regular_primary(
        &mut self,
        group: &PrimaryBroadcastGroup<'_, CertificateSignaturePubKey<ST>>,
        chunk: &ValidatedChunk<CertificateSignaturePubKey<ST>>,
        decoding_context: &DecodingContext<CertificateSignaturePubKey<ST>>,
        rebroadcast_to: &mut impl FnMut(Vec<NodeId<CertificateSignaturePubKey<ST>>>),
    ) -> Option<(NodeId<CertificateSignaturePubKey<ST>>, Bytes)> {
        let message = self.try_decode(chunk, decoding_context)?;

        if let Some((_author, message)) = &message {
            if let Some(hash_valid) = chunk.check_message_hash(message) {
                if !hash_valid {
                    self.decoder_cache.mark_tainted(chunk);
                    tracing::warn!(
                        author =? chunk.author,
                        "message failed hash validation"
                    );
                    return None;
                }
            }
        }

        let is_first_hop_recipient = chunk.recipient_hash == Some(self.self_id_hash);
        if let Some(ctx) = group.try_rebroadcast(&self.self_id, is_first_hop_recipient) {
            rebroadcast_to(ctx.peers().cloned().collect());
        }

        message
    }

    pub fn handle_secondary_raptorcast(
        &mut self,
        epoch_validators: &BTreeMap<Epoch, ValidatorSet<CertificateSignaturePubKey<ST>>>,
        full_node_group_map: &FullNodeGroupMap<CertificateSignaturePubKey<ST>>,
        chunk: &ValidatedChunk<CertificateSignaturePubKey<ST>>,
        rebroadcast_to: &mut impl FnMut(Vec<NodeId<CertificateSignaturePubKey<ST>>>),
        sender: Option<&NodeId<CertificateSignaturePubKey<ST>>>,
    ) -> Option<(NodeId<CertificateSignaturePubKey<ST>>, Bytes)> {
        let round = match chunk.group_id {
            GroupId::Secondary(round) => round,
            _ => unreachable!(),
        };
        let Ok(group) =
            SecondaryBroadcastGroup::of_round(round, &chunk.author, full_node_group_map)
        else {
            tracing::debug!(
                ?chunk.group_id,
                author =? chunk.author,
                "dropping message from unknown author/group"
            );
            return None;
        };

        if let Some(sender) = sender {
            if !group.is_sender_valid(sender) {
                tracing::debug!(
                    ?chunk.group_id,
                    author =? chunk.author,
                    ?sender,
                    "dropping message from invalid sender"
                );
                return None;
            }
        }

        let validator_set = match chunk.group_id {
            GroupId::Primary(epoch) => epoch_validators.get(&epoch),
            GroupId::Secondary(_round) => None,
        };

        let decoding_context = DecodingContext::new(validator_set, unix_ts_ms_now());
        let message = self.try_decode(chunk, &decoding_context)?;

        if let Some((_author, message)) = &message {
            if let Some(hash_valid) = chunk.check_message_hash(message) {
                if !hash_valid {
                    self.decoder_cache.mark_tainted(chunk);
                    tracing::error!(
                        author =? chunk.author,
                        "message failed hash validation"
                    );
                    return None;
                }
            }
        }

        let is_first_hop_recipient = chunk.recipient_hash == Some(self.self_id_hash);
        if let Some(ctx) = group.try_rebroadcast(&self.self_id, is_first_hop_recipient) {
            // TODO: cap rebroadcast symbols based on some multiple of esis.
            rebroadcast_to(ctx.peers().cloned().collect());
        }

        message
    }

    // Outer Option: whether the chunk was admitted
    // Inner Option: the successfully decoded app message
    fn try_decode(
        &mut self,
        chunk: &ValidatedChunk<CertificateSignaturePubKey<ST>>,
        decoding_context: &DecodingContext<CertificateSignaturePubKey<ST>>,
    ) -> Option<Option<(NodeId<CertificateSignaturePubKey<ST>>, Bytes)>> {
        match self.decoder_cache.try_decode(chunk, decoding_context) {
            Err(TryDecodeError::InvalidSymbol(err)) => {
                err.log(chunk, &self.self_id);
                None
            }

            Err(TryDecodeError::UnableToReconstructSourceData) => {
                tracing::error!("failed to reconstruct source data");
                None
            }

            Err(TryDecodeError::MessageTainted) => {
                tracing::debug!(
                    author =? chunk.author,
                    "decoding message tainted"
                );
                None
            }

            Ok(TryDecodeStatus::RejectedByCache) => {
                tracing::debug!(
                    author =? chunk.author,
                    chunk_id = chunk.chunk_id,
                    "message rejected by cache, author may be flooding messages",
                );
                None
            }

            Ok(TryDecodeStatus::RecentlyDecoded) | Ok(TryDecodeStatus::NeedsMoreSymbols) => {
                Some(None)
            }

            Ok(TryDecodeStatus::Decoded {
                author,
                app_message,
            }) => {
                self.metrics
                    .record_broadcast_latency(chunk.broadcast_mode, chunk.unix_ts_ms);

                Some(Some((author, app_message)))
            }
        }
    }

    /// Given a RecvUdpMsg, emits all decoded messages while rebroadcasting as necessary
    #[tracing::instrument(level = "debug", name = "udp_handle_message", skip_all)]
    pub fn handle_message(
        &mut self,
        epoch_validators: &BTreeMap<Epoch, ValidatorSet<CertificateSignaturePubKey<ST>>>,
        full_node_group_map: &FullNodeGroupMap<CertificateSignaturePubKey<ST>>,
        rebroadcast: impl FnMut(Vec<NodeId<CertificateSignaturePubKey<ST>>>, Bytes, u16),
        message: crate::auth::AuthRecvMsg<CertificateSignaturePubKey<ST>>,
    ) -> Vec<(NodeId<CertificateSignaturePubKey<ST>>, Bytes)> {
        let mut broadcast_batcher =
            BroadcastBatcher::new(self.self_id, rebroadcast, &message.payload, message.stride);

        let mut messages = Vec::new(); // The return result; decoded messages

        for payload_start_idx in (0..message.payload.len()).step_by(message.stride.into()) {
            // scoped variables are dropped in reverse order of declaration.
            // when *batch_guard is dropped, packets can get flushed
            let mut batch_guard = broadcast_batcher.create_flush_guard();

            let payload_end_idx =
                (payload_start_idx + usize::from(message.stride)).min(message.payload.len());
            let payload = message.payload.slice(payload_start_idx..payload_end_idx);

            // "message" here means a raptor-casted chunk (AKA r10 symbol), not the whole final message (proposal)
            let bypass_rate_limiter = |epoch: Epoch| {
                // validator senders are allowed to bypass signature
                // verification rate limiting
                message.sender.as_ref().is_some_and(|sender| {
                    epoch_validators
                        .get(&epoch)
                        .iter()
                        .any(|ev| ev.is_member(sender))
                })
            };

            let packet = match RaptorcastPacket::parse(&payload) {
                Ok(packet) => packet,
                Err(err) => {
                    tracing::debug!(src_addr = ?message.src_addr, ?err, "malformed message");
                    continue;
                }
            };

            let env = ChunkValidationEnv {
                signature_verifier: &mut self.signature_verifier,
                max_age_ms: self.max_age_ms,
                bypass_rate_limiter,
                self_id: &self.self_id,
            };

            let chunk = match packet.validate_chunk(&payload, env) {
                Ok(chunk) => chunk,
                Err(InvalidChunk::RateLimited) => {
                    tracing::debug!(
                        src_addr = ?message.src_addr,
                        "rate limited raptorcast chunk signature verification"
                    );
                    self.metrics.executor_metrics_mut()
                        [GAUGE_RAPTORCAST_DECODING_CACHE_SIGNATURE_VERIFICATIONS_RATE_LIMITED] += 1;
                    continue;
                }
                Err(err) => {
                    tracing::debug!(src_addr = ?message.src_addr, ?err, "invalid chunk");
                    continue;
                }
            };

            tracing::trace!(
                src_addr = ?message.src_addr,
                app_message_len = ?chunk.app_message_len,
                self_id =? self.self_id,
                author =? chunk.author,
                unix_ts_ms = chunk.unix_ts_ms,
                app_message_hash =? chunk.app_message_hash,
                encoding_symbol_id =? chunk.chunk_id as usize,
                "received encoded symbol"
            );

            let rebroadcast_to = &mut |targets| {
                batch_guard.queue_broadcast(
                    payload_start_idx,
                    payload_end_idx,
                    &chunk.author,
                    || targets,
                )
            };

            let maybe_decoded_message = match chunk.broadcast_mode {
                BroadcastMode::Unspecified => {
                    self.handle_unicast(epoch_validators, &chunk, message.sender.as_ref())
                }
                BroadcastMode::Primary => {
                    if !v1_rollout::should_accept(self.v1_rollout, &chunk) {
                        self.metrics.executor_metrics_mut()
                            [COUNTER_RAPTORCAST_CHUNKS_DROPPED_INCOMPATIBLE_VERSION] += 1;
                        continue;
                    }
                    let accepted_metric = match chunk.version {
                        1 => COUNTER_RAPTORCAST_V1_PRIMARY_CHUNKS_ACCEPTED,
                        _ => COUNTER_RAPTORCAST_V0_PRIMARY_CHUNKS_ACCEPTED,
                    };
                    self.metrics.executor_metrics_mut()[accepted_metric] += 1;
                    self.handle_primary_raptorcast(
                        epoch_validators,
                        &chunk,
                        rebroadcast_to,
                        message.sender.as_ref(),
                    )
                }

                BroadcastMode::Secondary => {
                    if !v1_rollout::should_accept(self.v1_rollout, &chunk) {
                        self.metrics.executor_metrics_mut()
                            [COUNTER_RAPTORCAST_SECONDARY_CHUNKS_DROPPED_INCOMPATIBLE_VERSION] += 1;
                        continue;
                    }
                    let accepted_metric = match chunk.version {
                        1 => COUNTER_RAPTORCAST_V1_SECONDARY_CHUNKS_ACCEPTED,
                        _ => COUNTER_RAPTORCAST_V0_SECONDARY_CHUNKS_ACCEPTED,
                    };
                    self.metrics.executor_metrics_mut()[accepted_metric] += 1;
                    self.handle_secondary_raptorcast(
                        epoch_validators,
                        full_node_group_map,
                        &chunk,
                        rebroadcast_to,
                        message.sender.as_ref(),
                    )
                }
            };

            if let Some((author, decoded_message)) = maybe_decoded_message {
                messages.push((author, decoded_message))
            }
        }

        messages
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GroupId {
    Primary(Epoch),
    Secondary(Round),
}

impl From<GroupId> for u64 {
    fn from(group_id: GroupId) -> Self {
        match group_id {
            GroupId::Primary(epoch) => epoch.0,
            GroupId::Secondary(round) => round.0,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ValidatedChunk<PT>
where
    PT: PubKey,
{
    pub chunk: Bytes, // raptor-coded portion
    pub message: Bytes,
    pub signature: Bytes,

    pub author: NodeId<PT>,
    // group_id is set to
    // - epoch number for validator-to-validator raptorcast
    // - round number for validator-to-fullnode raptorcast
    pub group_id: GroupId,
    pub unix_ts_ms: u64,
    pub app_message_hash: Option<AppMessageHash>,
    pub merkle_root: MerkleRoot,
    pub app_message_len: u32,
    pub version: u16,
    pub encoding_scheme: EncodingScheme,
    pub broadcast_mode: BroadcastMode,
    pub recipient_hash: Option<NodeIdHash>, // V0: hash of first-hop recipient; V1 (deterministic): None
    pub chunk_id: u16,
    pub num_source_symbols: usize,
    pub encoded_symbol_capacity: usize,
}

impl<PT: PubKey> ValidatedChunk<PT> {
    // Return None if chunk is not applicable for app message hash
    // check (e.g. deterministic raptorcast)
    pub fn app_message_hash(&self) -> Option<&AppMessageHash> {
        self.app_message_hash.as_ref()
    }

    pub fn global_merkle_root(&self) -> Option<&GlobalMerkleRoot> {
        match self.encoding_scheme {
            EncodingScheme::Deterministic25(_) => Some(&self.merkle_root),
            _ => None,
        }
    }

    // Return None if chunk is not encoded using deterministic raptorcast
    pub fn check_deterministic_encoding(
        &self,
        app_message: &[u8],
        group: &PrimaryBroadcastGroup<'_, PT>,
    ) -> Option<bool> {
        if !matches!(self.encoding_scheme, EncodingScheme::Deterministic25(_)) {
            return None;
        }

        match deterministic::calc_global_merkle_root(
            app_message,
            group,
            self.encoding_scheme,
            self.unix_ts_ms,
        ) {
            Ok(expected_root) => Some(expected_root == self.merkle_root.0),
            Err(_err) => Some(false),
        }
    }

    // Return None if chunk is not applicable for app message hash
    // check (e.g. deterministic raptorcast)
    pub fn check_message_hash(&self, app_message: &[u8]) -> Option<bool> {
        if let Some(&expected) = self.app_message_hash() {
            let actual = compute_app_message_hash(app_message);
            return Some(expected == actual);
        }

        None
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum InvalidChunk {
    // Packet parsing error
    Malformed(MalformedPacket),

    // Chunk metadata validation error
    InvalidAppMessageLen(usize),
    InvalidChunkLen,
    InvalidChunkId,
    InvalidMerkleTreeDepth,
    InvalidTimestamp {
        packet_ts_ms: u64,
        current_ts_ms: u64,
    },

    // Chunk rejected for integrity/authenticity reasons
    InvalidMerkleProof,
    InvalidSignature,
    Loopback,

    // Chunk rejected by rate limiter
    RateLimited,
}

impl From<SignatureVerifierError> for InvalidChunk {
    fn from(err: SignatureVerifierError) -> Self {
        match err {
            SignatureVerifierError::RateLimited => InvalidChunk::RateLimited,
            SignatureVerifierError::InvalidSignature => InvalidChunk::InvalidSignature,
        }
    }
}

struct BroadcastBatch<PT: PubKey> {
    author: NodeId<PT>,
    targets: Vec<NodeId<PT>>,

    start_idx: usize,
    end_idx: usize,
}
pub(crate) struct BroadcastBatcher<'a, F, PT>
where
    F: FnMut(Vec<NodeId<PT>>, Bytes, u16),
    PT: PubKey,
{
    self_id: NodeId<PT>,
    rebroadcast: F,
    message: &'a Bytes,
    stride: u16,

    batch: Option<BroadcastBatch<PT>>,
}
impl<F, PT> Drop for BroadcastBatcher<'_, F, PT>
where
    F: FnMut(Vec<NodeId<PT>>, Bytes, u16),
    PT: PubKey,
{
    fn drop(&mut self) {
        self.flush()
    }
}
impl<'a, F, PT> BroadcastBatcher<'a, F, PT>
where
    F: FnMut(Vec<NodeId<PT>>, Bytes, u16),
    PT: PubKey,
{
    pub fn new(self_id: NodeId<PT>, rebroadcast: F, message: &'a Bytes, stride: u16) -> Self {
        Self {
            self_id,
            rebroadcast,
            message,
            stride,
            batch: None,
        }
    }

    pub fn create_flush_guard<'g>(&'g mut self) -> BatcherGuard<'a, 'g, F, PT>
    where
        'a: 'g,
    {
        BatcherGuard {
            batcher: self,
            flush_batch: true,
        }
    }

    fn flush(&mut self) {
        if let Some(batch) = self.batch.take() {
            tracing::trace!(
                self_id =? self.self_id,
                author =? batch.author,
                num_targets = batch.targets.len(),
                num_bytes = batch.end_idx - batch.start_idx,
                "rebroadcasting chunks"
            );
            (self.rebroadcast)(
                batch.targets,
                self.message.slice(batch.start_idx..batch.end_idx),
                self.stride,
            );
        }
    }
}
pub(crate) struct BatcherGuard<'a, 'g, F, PT>
where
    'a: 'g,
    F: FnMut(Vec<NodeId<PT>>, Bytes, u16),
    PT: PubKey,
{
    batcher: &'g mut BroadcastBatcher<'a, F, PT>,
    flush_batch: bool,
}
impl<'a, 'g, F, PT> BatcherGuard<'a, 'g, F, PT>
where
    'a: 'g,
    F: FnMut(Vec<NodeId<PT>>, Bytes, u16),
    PT: PubKey,
{
    pub(crate) fn queue_broadcast(
        &mut self,
        payload_start_idx: usize,
        payload_end_idx: usize,
        author: &NodeId<PT>,
        targets: impl FnOnce() -> Vec<NodeId<PT>>,
    ) {
        self.flush_batch = false;
        if self
            .batcher
            .batch
            .as_ref()
            .is_some_and(|batch| &batch.author == author)
        {
            let batch = self.batcher.batch.as_mut().unwrap();
            assert_eq!(batch.end_idx, payload_start_idx);
            batch.end_idx = payload_end_idx;
        } else {
            self.batcher.flush();
            self.batcher.batch = Some(BroadcastBatch {
                author: *author,
                targets: targets(),

                start_idx: payload_start_idx,
                end_idx: payload_end_idx,
            })
        }
    }
}
impl<'a, 'g, F, PT> Drop for BatcherGuard<'a, 'g, F, PT>
where
    'a: 'g,
    F: FnMut(Vec<NodeId<PT>>, Bytes, u16),
    PT: PubKey,
{
    fn drop(&mut self) {
        if self.flush_batch {
            self.batcher.flush();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{HashMap, HashSet},
        net::{IpAddr, Ipv4Addr, SocketAddr},
    };

    use bytes::{Bytes, BytesMut};
    use itertools::Itertools as _;
    use monad_crypto::{
        certificate_signature::CertificateSignaturePubKey,
        hasher::{Hasher, HasherType},
    };
    use monad_dataplane::udp::DEFAULT_SEGMENT_SIZE;
    use monad_secp::{KeyPair, SecpSignature};
    use monad_types::{Epoch, NodeId, Round, Stake};
    use monad_validator::validator_set::{ValidatorSet, ValidatorSetType as _};
    use rstest::*;

    use super::{ChunkSignatureVerifier, InvalidChunk, UdpState, ValidatedChunk};
    use crate::{
        packet::{regular, MessageBuilder},
        parser::{
            packet_parser::{ChunkValidationEnv, MalformedPacket, RaptorcastPacket},
            signature_verifier::SignatureVerifier,
        },
        udp::{build_messages, MAX_VALIDATOR_SET_SIZE, SIGNATURE_CACHE_SIZE},
        util::{
            compute_app_message_hash, BroadcastMode, BuildTarget, FullNodeGroupMap,
            PrimaryBroadcastGroup, Redundancy, SecondaryBroadcastGroup, SecondaryGroup,
            ValidatorGroupMap,
        },
    };

    struct ChunkParser {
        self_id: NodeId<CertificateSignaturePubKey<SignatureType>>,
        max_age_ms: u64,
        signature_verifier: ChunkSignatureVerifier<SignatureType>,
    }

    impl ChunkParser {
        fn new() -> Self {
            Self {
                self_id: NodeId::new(make_key_pair(0xff).pubkey()),
                max_age_ms: u64::MAX,
                signature_verifier: signature_verifier(),
            }
        }

        fn with_max_age_ms(mut self, max_age_ms: u64) -> Self {
            self.max_age_ms = max_age_ms;
            self
        }

        fn with_rate_limit(mut self, rate_limit: u32) -> Self {
            self.signature_verifier = SignatureVerifier::new()
                .with_cache(SIGNATURE_CACHE_SIZE)
                .with_rate_limit(rate_limit);
            self
        }

        fn parse(&mut self, message: &Bytes) -> Result<ValidatedChunk<PubKeyType>, InvalidChunk> {
            self.parse_with(message, |_| true)
        }

        fn parse_with(
            &mut self,
            message: &Bytes,
            bypass_rate_limiter: impl FnOnce(Epoch) -> bool,
        ) -> Result<ValidatedChunk<PubKeyType>, InvalidChunk> {
            let packet = RaptorcastPacket::parse(message)?;
            packet.validate_chunk(
                message,
                ChunkValidationEnv {
                    signature_verifier: &mut self.signature_verifier,
                    max_age_ms: self.max_age_ms,
                    bypass_rate_limiter,
                    self_id: &self.self_id,
                },
            )
        }
    }

    type SignatureType = SecpSignature;
    type PubKeyType = CertificateSignaturePubKey<SignatureType>;
    type KeyPairType = KeyPair;
    type TestSignatureVerifier = ChunkSignatureVerifier<SignatureType>;

    fn signature_verifier() -> TestSignatureVerifier {
        SignatureVerifier::new().with_cache(SIGNATURE_CACHE_SIZE)
    }

    fn make_key_pair(n: u8) -> KeyPairType {
        let mut hasher = HasherType::new();
        hasher.update(n.to_le_bytes());
        let mut hash = hasher.hash();
        KeyPairType::from_bytes(&mut hash.0).unwrap()
    }

    fn validator_set() -> (
        KeyPairType,
        ValidatorSet<PubKeyType>,
        HashMap<NodeId<PubKeyType>, SocketAddr>,
    ) {
        const NUM_KEYS: u8 = 100;
        let mut keys = (0_u8..NUM_KEYS).map(make_key_pair).collect_vec();

        let valset = keys
            .iter()
            .map(|key| (NodeId::new(key.pubkey()), Stake::ONE))
            .collect();
        let validators = ValidatorSet::new_unchecked(valset);

        let known_addresses = keys
            .iter()
            .skip(NUM_KEYS as usize / 10) // test some missing known_addresses
            .enumerate()
            .map(|(idx, key)| {
                (
                    NodeId::new(key.pubkey()),
                    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), idx as u16),
                )
            })
            .collect();

        (keys.pop().unwrap(), validators, known_addresses)
    }

    const EPOCH: Epoch = Epoch(5);
    const UNIX_TS_MS: u64 = 5;

    fn make_group_map(validators: &ValidatorSet<PubKeyType>) -> ValidatorGroupMap<PubKeyType> {
        [(EPOCH, validators.clone())].into()
    }

    #[test]
    fn test_roundtrip() {
        let (key, validators, known_addresses) = validator_set();
        let self_id = NodeId::new(key.pubkey());
        let group_map = make_group_map(&validators);
        let group = PrimaryBroadcastGroup::of_epoch(EPOCH, &self_id, &group_map).unwrap();

        let app_message: Bytes = vec![1_u8; 1024 * 1024].into();
        let app_message_hash = compute_app_message_hash(&app_message);

        let messages = build_messages::<SignatureType>(
            &key,
            DEFAULT_SEGMENT_SIZE, // segment_size
            app_message.clone(),
            Redundancy::from_u8(2),
            UNIX_TS_MS,
            BuildTarget::raptorcast(group),
            &known_addresses,
        );

        let mut parser = ChunkParser::new();
        for (_to, mut aggregate_message) in messages {
            while !aggregate_message.is_empty() {
                let message = aggregate_message.split_to(DEFAULT_SEGMENT_SIZE.into());
                let chunk = parser.parse(&message).expect("valid message");
                assert_eq!(chunk.message, message);
                assert_eq!(chunk.app_message_hash, Some(app_message_hash));
                assert_eq!(chunk.unix_ts_ms, UNIX_TS_MS);
                assert!(matches!(chunk.broadcast_mode, BroadcastMode::Primary));
                assert_eq!(chunk.app_message_len, app_message.len() as u32);
                assert_eq!(chunk.author, NodeId::new(key.pubkey()));
            }
        }
    }

    #[test]
    fn test_bit_flip_parse_failure_slow() {
        let (key, validators, known_addresses) = validator_set();
        let self_id = NodeId::new(key.pubkey());
        let group_map = make_group_map(&validators);
        let group = PrimaryBroadcastGroup::of_epoch(EPOCH, &self_id, &group_map).unwrap();

        let app_message: Bytes = vec![1_u8; 1024 * 2].into();

        let messages = build_messages::<SignatureType>(
            &key,
            DEFAULT_SEGMENT_SIZE, // segment_size
            app_message,
            Redundancy::from_u8(2),
            UNIX_TS_MS,
            BuildTarget::raptorcast(group),
            &known_addresses,
        );

        let mut parser = ChunkParser::new();
        for (_to, mut aggregate_message) in messages {
            while !aggregate_message.is_empty() {
                let mut message: BytesMut = aggregate_message
                    .split_to(DEFAULT_SEGMENT_SIZE.into())
                    .as_ref()
                    .into();
                // try flipping each bit
                for bit_idx in 0..message.len() * 8 {
                    let old_byte = message[bit_idx / 8];
                    // flip bit
                    message[bit_idx / 8] = old_byte ^ (1 << (bit_idx % 8));
                    let flipped: Bytes = message.clone().into();
                    let maybe_parsed = parser.parse(&flipped);

                    // check that decoding fails
                    assert!(
                        maybe_parsed.is_err()
                            || maybe_parsed.unwrap().author != NodeId::new(key.pubkey())
                    );

                    // reset bit
                    message[bit_idx / 8] = old_byte;
                }
            }
        }
    }

    #[test]
    fn test_raptorcast_chunk_ids() {
        let (key, validators, known_addresses) = validator_set();
        let self_id = NodeId::new(key.pubkey());
        let group_map = make_group_map(&validators);
        let group = PrimaryBroadcastGroup::of_epoch(EPOCH, &self_id, &group_map).unwrap();

        let app_message: Bytes = vec![1_u8; 1024 * 1024].into();

        let messages = build_messages::<SignatureType>(
            &key,
            DEFAULT_SEGMENT_SIZE, // segment_size
            app_message,
            Redundancy::from_u8(2),
            UNIX_TS_MS,
            BuildTarget::raptorcast(group),
            &known_addresses,
        );

        let mut parser = ChunkParser::new();
        let mut used_ids = HashSet::new();

        for (_to, mut aggregate_message) in messages {
            while !aggregate_message.is_empty() {
                let message = aggregate_message.split_to(DEFAULT_SEGMENT_SIZE.into());
                let chunk = parser.parse(&message).expect("valid message");
                let newly_inserted = used_ids.insert(chunk.chunk_id);
                assert!(newly_inserted);
            }
        }
    }

    #[test]
    fn test_broadcast_bit() {
        let (key, validators, known_addresses) = validator_set();
        let self_id = NodeId::new(key.pubkey());
        let group_map = make_group_map(&validators);
        let primary_group = PrimaryBroadcastGroup::of_epoch(EPOCH, &self_id, &group_map).unwrap();

        let full_nodes = SecondaryGroup::new_unchecked(
            validators
                .get_members()
                .keys()
                .filter(|&n| n != &self_id)
                .cloned()
                .collect(),
        );
        let secondary_group = {
            let round = Round(1);
            SecondaryBroadcastGroup::as_publisher(&self_id, round, &full_nodes)
        };

        let app_message: Bytes = vec![1_u8; 1024 * 1024].into();
        let build_targets = vec![
            BuildTarget::raptorcast(primary_group),
            BuildTarget::fullnode_raptorcast(secondary_group),
        ];

        let mut parser = ChunkParser::new();
        for build_target in build_targets {
            let messages = build_messages::<SignatureType>(
                &key,
                DEFAULT_SEGMENT_SIZE, // segment_size
                app_message.clone(),
                Redundancy::from_u8(2),
                UNIX_TS_MS,
                build_target,
                &known_addresses,
            );

            for (_to, mut aggregate_message) in messages {
                while !aggregate_message.is_empty() {
                    let message = aggregate_message.split_to(DEFAULT_SEGMENT_SIZE.into());
                    let chunk = parser.parse(&message).expect("valid message");

                    match build_target {
                        BuildTarget::Raptorcast { .. } => {
                            assert!(matches!(chunk.broadcast_mode, BroadcastMode::Primary));
                        }
                        BuildTarget::FullNodeRaptorCast { .. } => {
                            assert!(matches!(chunk.broadcast_mode, BroadcastMode::Secondary));
                        }
                        _ => unreachable!(),
                    }
                }
            }
        }
    }

    #[test]
    fn test_broadcast_chunk_ids() {
        let (key, validators, known_addresses) = validator_set();
        let self_id = NodeId::new(key.pubkey());
        let group_map = make_group_map(&validators);
        let group = PrimaryBroadcastGroup::of_epoch(EPOCH, &self_id, &group_map).unwrap();

        let app_message: Bytes = vec![1_u8; 1024 * 8].into();

        let messages = build_messages::<SignatureType>(
            &key,
            DEFAULT_SEGMENT_SIZE, // segment_size
            app_message,
            Redundancy::from_u8(2),
            UNIX_TS_MS,
            BuildTarget::Broadcast(group),
            &known_addresses,
        );

        let mut parser = ChunkParser::new();
        let mut used_ids: HashMap<SocketAddr, HashSet<_>> = HashMap::new();

        for (to, mut aggregate_message) in messages {
            while !aggregate_message.is_empty() {
                let message = aggregate_message.split_to(DEFAULT_SEGMENT_SIZE.into());
                let chunk = parser.parse(&message).expect("valid message");
                let newly_inserted = used_ids.entry(to).or_default().insert(chunk.chunk_id);
                assert!(newly_inserted);
            }
        }

        let ids = used_ids.values().next().unwrap().clone();
        assert!(used_ids.values().all(|x| x == &ids)); // check that all recipients are sent same ids
        assert!(ids.contains(&0)); // check that starts from idx 0
    }

    #[test]
    fn test_handle_message_stride_slice() {
        let (key, validators, _known_addresses) = validator_set();
        let self_id = NodeId::new(key.pubkey());
        let epoch_validators = [(Epoch(1), validators)].into_iter().collect();
        let full_node_groups = FullNodeGroupMap::default();

        let mut udp_state = UdpState::<SignatureType>::new(self_id, u64::MAX, 10_000);

        // payload will fail to parse but shouldn't panic on index error
        let payload: Bytes = vec![1_u8; 1024 * 8 + 1].into();
        let recv_msg = crate::auth::AuthRecvMsg {
            src_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8000),
            payload,
            stride: 1024,
            sender: None,
        };

        udp_state.handle_message(
            &epoch_validators,
            &full_node_groups,
            |_targets, _payload, _stride| {},
            recv_msg,
        );
    }

    #[rstest]
    #[case(-2 * 60 * 60 * 1000, u64::MAX, true)]
    #[case(2 * 60 * 60 * 1000, u64::MAX, true)]
    #[case(-2 * 60 * 60 * 1000, 0, false)]
    #[case(2 * 60 * 60 * 1000, 0, false)]
    #[case(-30_000, 60_000, true)]
    #[case(-120_000, 60_000, false)]
    #[case(120_000, 60_000, false)]
    #[case(30_000, 60_000, true)]
    #[case(-90_000, 60_000, false)]
    #[case(90_000, 60_000, false)]
    fn test_timestamp_validation(
        #[case] timestamp_offset_ms: i64,
        #[case] max_age_ms: u64,
        #[case] should_succeed: bool,
    ) {
        let (key, validators, known_addresses) = validator_set();
        let self_id = NodeId::new(key.pubkey());
        let group_map = make_group_map(&validators);
        let group = PrimaryBroadcastGroup::of_epoch(EPOCH, &self_id, &group_map).unwrap();

        let current_time = std::time::UNIX_EPOCH.elapsed().unwrap().as_millis() as u64;
        let test_timestamp = (current_time as i64 + timestamp_offset_ms) as u64;

        let app_message = Bytes::from_static(b"test message");
        let messages = build_messages::<SignatureType>(
            &key,
            DEFAULT_SEGMENT_SIZE,
            app_message,
            Redundancy::from_u8(1),
            test_timestamp,
            BuildTarget::Broadcast(group),
            &known_addresses,
        );
        let message = messages.into_iter().next().unwrap().1;
        let mut parser = ChunkParser::new().with_max_age_ms(max_age_ms);
        let result = parser.parse(&message);

        if should_succeed {
            assert!(result.is_ok(), "unexpected success: {:?}", result.err());
        } else {
            assert!(result.is_err());
            match result.err().unwrap() {
                InvalidChunk::InvalidTimestamp { .. } => {}
                other => panic!("unexpected error {:?}", other),
            }
        }
    }

    pub const MERKLE_TREE_DEPTH: u8 = 6;
    pub const SYMBOL_LEN: usize =
        regular::PacketLayout::new(DEFAULT_SEGMENT_SIZE as usize, MERKLE_TREE_DEPTH).symbol_len();
    pub const MAX_REDUNDANCY: u16 = 3;

    #[rstest]
    #[case(SYMBOL_LEN * 2, 1, false, true)] // sanity check
    #[case(SYMBOL_LEN * 2, MAX_REDUNDANCY * 2 - 1, false, true)]
    #[case(SYMBOL_LEN * 2, MAX_REDUNDANCY * 2, false, false)]
    #[case(SYMBOL_LEN * 2, MAX_REDUNDANCY * 2, true, true)]
    #[case(SYMBOL_LEN * 2, MAX_REDUNDANCY * 2 + MAX_VALIDATOR_SET_SIZE as u16 - 1, true, true)]
    #[case(SYMBOL_LEN * 2, MAX_REDUNDANCY * 2 + MAX_VALIDATOR_SET_SIZE as u16, true, false)]
    fn test_chunk_id_validation(
        #[case] app_msg_len: usize,
        #[case] chunk_id: u16,
        #[case] raptorcast: bool,
        #[case] should_succeed: bool,
    ) {
        let (key, validators, _known_addresses) = validator_set();
        let self_id = NodeId::new(key.pubkey());
        let group_map = make_group_map(&validators);
        let group = PrimaryBroadcastGroup::of_epoch(EPOCH, &self_id, &group_map).unwrap();
        let target = if raptorcast {
            BuildTarget::raptorcast(group)
        } else {
            BuildTarget::Broadcast(group)
        };
        let app_msg = vec![0; app_msg_len];
        let messages = MessageBuilder::<SignatureType>::new(&key)
            .segment_size(DEFAULT_SEGMENT_SIZE as usize)
            .redundancy(Redundancy::from_u8(1))
            .merkle_tree_depth(MERKLE_TREE_DEPTH)
            .build_vec(&app_msg, &target);
        let message = messages.unwrap().into_iter().next().unwrap();
        let mut payload = BytesMut::from(&message.payload[..message.stride]);

        let layout = regular::PacketLayout::new(DEFAULT_SEGMENT_SIZE as usize, MERKLE_TREE_DEPTH);
        let chunk_header = &mut payload[layout.chunk_header_range()];
        let chunk_id_buf: &mut [u8] = &mut chunk_header[22..24];
        chunk_id_buf.copy_from_slice(&chunk_id.to_le_bytes()); // override chunk id

        let payload = payload.freeze();
        let result = ChunkParser::new().parse(&payload);

        if should_succeed {
            // modifying the chunk_id field can still result in invalid leaf hash/signature.
            assert!(matches!(
                result,
                Ok(_) | Err(InvalidChunk::InvalidMerkleProof) | Err(InvalidChunk::InvalidSignature)
            ));
        } else {
            assert!(matches!(result, Err(InvalidChunk::InvalidChunkId)));
        }
    }

    #[test]
    fn test_zero_len_chunk() {
        let payload = {
            const PACKET_LEN: usize = 132;
            let mut packet = vec![0u8; PACKET_LEN];

            // Bytes 0-64: Signature (65 bytes) - arbitrary, not verified before crash
            // Bytes 65-66: Version = 0 (already zero)

            // Byte 67: tree_depth=1 (bits 0-3), no broadcast flags (bits 6-7)
            packet[67] = 0x01;

            // Bytes 68-75: Epoch/GroupId (any value)
            packet[68..76].copy_from_slice(&1u64.to_le_bytes());

            // Bytes 76-83: Timestamp (current time in milliseconds)

            // Bytes 84-103: App message hash (zeros are fine)

            // Bytes 104-107: App message length = 1 (MUST BE > 0!)
            packet[104..108].copy_from_slice(&1u32.to_le_bytes());

            // Bytes 108-127: Recipient hash (zeros are fine)
            // Byte 128: Merkle leaf idx = 0
            // Byte 129: Reserved = 0
            // Bytes 130-131: Chunk ID = 0

            // NO PAYLOAD - packet ends at 132 bytes
            // This makes symbol_len = cursor.len() = 0

            packet
        };
        let payload: Bytes = payload.into();
        assert_eq!(
            ChunkParser::new().parse(&payload).err(),
            Some(InvalidChunk::Malformed(MalformedPacket::TooShort))
        )
    }

    #[test]
    fn test_parse_message_signature_verifier() {
        let (key, validators, known_addresses) = validator_set();
        let self_id = NodeId::new(key.pubkey());
        let group_map = make_group_map(&validators);
        let group = PrimaryBroadcastGroup::of_epoch(EPOCH, &self_id, &group_map).unwrap();

        let app_message: Bytes = vec![1_u8; 1024].into();

        let messages = build_messages::<SignatureType>(
            &key,
            DEFAULT_SEGMENT_SIZE,
            app_message,
            Redundancy::from_u8(1),
            UNIX_TS_MS,
            BuildTarget::raptorcast(group),
            &known_addresses,
        );

        let message_a: Bytes = messages[0].1.slice(0..(DEFAULT_SEGMENT_SIZE as usize));
        let message_b: Bytes = messages
            .last()
            .unwrap()
            .1
            .slice(0..(DEFAULT_SEGMENT_SIZE as usize));

        let mut parser = ChunkParser::new().with_rate_limit(1);

        // Case 1: cache miss, verify signature, cache saved
        let result1 = parser.parse(&message_a);
        let author = result1.expect("first parse should succeed").author;
        assert_eq!(author, NodeId::new(key.pubkey()));

        // Case 2: parse with same message: cache hit, no rate limit consumed
        let result2 = parser.parse_with(&message_a, |_| false);
        assert_eq!(
            result2.expect("cache hit should succeed").author,
            author,
            "cache hit should return same author"
        );

        // Case 3: parse different message without bypass: rate limited
        let result3 = parser.parse_with(&message_b, |_| false);
        assert!(
            matches!(result3, Err(InvalidChunk::RateLimited)),
            "new message without bypass should be rate limited"
        );

        // Case 4: Same message with bypass: succeeds
        let result4 = parser.parse(&message_b);
        assert!(result4.is_ok());
    }
}

#[cfg(test)]
mod tests_deterministic {
    use std::{
        collections::{BTreeMap, HashMap, HashSet},
        net::{IpAddr, Ipv4Addr, SocketAddr},
    };

    use bytes::{Bytes, BytesMut};
    use itertools::Itertools as _;
    use monad_crypto::{
        certificate_signature::CertificateSignaturePubKey,
        hasher::{Hasher, HasherType},
    };
    use monad_secp::{KeyPair, SecpSignature};
    use monad_types::{Epoch, NodeId, Round, Stake};
    use monad_validator::validator_set::{ValidatorSet, ValidatorSetType as _};
    use rstest::*;

    use super::{ChunkSignatureVerifier, InvalidChunk, UdpState, ValidatedChunk};
    use crate::{
        auth::AuthRecvMsg,
        packet::{
            deterministic::{self, build_with_given_header},
            MessageBuilder,
        },
        parser::{
            packet_parser::{ChunkValidationEnv, RaptorcastPacket},
            signature_verifier::SignatureVerifier,
        },
        udp::SIGNATURE_CACHE_SIZE,
        util::{
            BroadcastMode, BuildTarget, EncodingScheme, FullNodeGroupMap, PrimaryBroadcastGroup,
            UdpMessage, ValidatorGroupMap,
        },
        v1_rollout::DeterministicProtocolRolloutStage,
    };

    type SignatureType = SecpSignature;
    type PubKeyType = CertificateSignaturePubKey<SignatureType>;
    type KeyPairType = KeyPair;
    type TestSignatureVerifier = ChunkSignatureVerifier<SignatureType>;

    fn signature_verifier() -> TestSignatureVerifier {
        SignatureVerifier::new().with_cache(SIGNATURE_CACHE_SIZE)
    }

    struct ChunkParser {
        self_id: NodeId<PubKeyType>,
        max_age_ms: u64,
        signature_verifier: ChunkSignatureVerifier<SecpSignature>,
    }

    impl ChunkParser {
        fn new() -> Self {
            Self {
                self_id: NodeId::new(make_key_pair(0xff).pubkey()),
                max_age_ms: u64::MAX,
                signature_verifier: signature_verifier(),
            }
        }

        fn with_max_age_ms(mut self, max_age_ms: u64) -> Self {
            self.max_age_ms = max_age_ms;
            self
        }

        fn with_rate_limit(mut self, rate_limit: u32) -> Self {
            self.signature_verifier = SignatureVerifier::new()
                .with_cache(SIGNATURE_CACHE_SIZE)
                .with_rate_limit(rate_limit);
            self
        }

        fn parse(&mut self, message: &Bytes) -> Result<ValidatedChunk<PubKeyType>, InvalidChunk> {
            self.parse_with(message, |_| true)
        }

        fn parse_with(
            &mut self,
            message: &Bytes,
            bypass_rate_limiter: impl FnOnce(Epoch) -> bool,
        ) -> Result<ValidatedChunk<PubKeyType>, InvalidChunk> {
            let packet = RaptorcastPacket::parse(message)?;
            packet.validate_chunk(
                message,
                ChunkValidationEnv {
                    signature_verifier: &mut self.signature_verifier,
                    max_age_ms: self.max_age_ms,
                    bypass_rate_limiter,
                    self_id: &self.self_id,
                },
            )
        }
    }

    fn make_key_pair(n: u8) -> KeyPairType {
        let mut hasher = HasherType::new();
        hasher.update(n.to_le_bytes());
        let mut hash = hasher.hash();
        KeyPairType::from_bytes(&mut hash.0).unwrap()
    }

    fn validator_set() -> (
        KeyPairType,
        ValidatorSet<PubKeyType>,
        HashMap<NodeId<PubKeyType>, SocketAddr>,
    ) {
        const NUM_KEYS: u8 = 100;
        let mut keys = (0_u8..NUM_KEYS).map(make_key_pair).collect_vec();

        let valset = keys
            .iter()
            .map(|key| (NodeId::new(key.pubkey()), Stake::ONE))
            .collect();
        let validators = ValidatorSet::new_unchecked(valset);

        let known_addresses = keys
            .iter()
            .skip(NUM_KEYS as usize / 10) // test some missing known_addresses
            .enumerate()
            .map(|(idx, key)| {
                (
                    NodeId::new(key.pubkey()),
                    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), idx as u16),
                )
            })
            .collect();

        (keys.pop().unwrap(), validators, known_addresses)
    }

    const EPOCH: Epoch = Epoch(5);
    const UNIX_TS_MS: u64 = 5;
    const ROUND: Round = Round(42);

    fn make_group_map(validators: &ValidatorSet<PubKeyType>) -> ValidatorGroupMap<PubKeyType> {
        [(EPOCH, validators.clone())].into()
    }

    fn build_packets(
        key: &KeyPairType,
        app_message: &[u8],
        group: PrimaryBroadcastGroup<'_, PubKeyType>,
    ) -> Vec<UdpMessage<PubKeyType>> {
        MessageBuilder::<SignatureType>::new(key)
            .unix_ts_ms(UNIX_TS_MS)
            .build_vec(
                app_message,
                &BuildTarget::deterministic_raptorcast(group, ROUND),
            )
            .expect("build should succeed")
    }

    #[test]
    fn test_roundtrip() {
        let (key, validators, _known_addresses) = validator_set();
        let self_id = NodeId::new(key.pubkey());
        let group_map = make_group_map(&validators);
        let group = PrimaryBroadcastGroup::of_epoch(EPOCH, &self_id, &group_map).unwrap();

        let app_message: Bytes = vec![0x1_u8; 1024 * 1024].into();
        let packets = build_packets(&key, &app_message, group);

        assert!(!packets.is_empty());
        assert!(packets
            .iter()
            .all(|msg| msg.payload.len() == deterministic::DEFAULT_SEGMENT_LEN));

        let mut parser = ChunkParser::new();
        let mut global_root: Option<_> = None;

        for msg in &packets {
            let message = msg.payload.clone();
            let parsed = parser.parse(&message).expect("valid message");

            assert_eq!(parsed.message, message);
            assert_eq!(parsed.unix_ts_ms, UNIX_TS_MS);
            assert_eq!(parsed.app_message_len, app_message.len() as u32);
            assert_eq!(parsed.author, NodeId::new(key.pubkey()));
            assert_eq!(parsed.broadcast_mode, BroadcastMode::Primary);
            assert_eq!(
                parsed.encoding_scheme,
                EncodingScheme::Deterministic25(ROUND)
            );
            let root = parsed
                .global_merkle_root()
                .expect("deterministic chunks must have global_merkle_root");

            match &global_root {
                None => global_root = Some(*root),
                Some(first) => assert_eq!(
                    root, first,
                    "all chunks must share the same global merkle root"
                ),
            }
        }
    }

    #[test]
    fn test_bit_flip_parse_failure_slow() {
        let (key, validators, _known_addresses) = validator_set();
        let self_id = NodeId::new(key.pubkey());
        let group_map = make_group_map(&validators);
        let group = PrimaryBroadcastGroup::of_epoch(EPOCH, &self_id, &group_map).unwrap();

        let app_message: Bytes = vec![1_u8; 1024 * 2].into();
        let packets = build_packets(&key, &app_message, group);

        let mut parser = ChunkParser::new();

        for msg in &packets {
            let mut payload: BytesMut = msg.payload.as_ref().into();
            // try flipping each bit
            for bit_idx in 0..payload.len() * 8 {
                let old_byte = payload[bit_idx / 8];
                // flip bit
                payload[bit_idx / 8] = old_byte ^ (1 << (bit_idx % 8));
                let flipped: Bytes = payload.clone().into();
                let maybe_parsed = parser.parse(&flipped);

                // check that decoding fails
                assert!(
                    maybe_parsed.is_err()
                        || maybe_parsed.unwrap().author != NodeId::new(key.pubkey())
                );

                // reset bit
                payload[bit_idx / 8] = old_byte;
            }
        }
    }

    #[test]
    fn test_raptorcast_chunk_ids() {
        let (key, validators, _known_addresses) = validator_set();
        let self_id = NodeId::new(key.pubkey());
        let group_map = make_group_map(&validators);
        let group = PrimaryBroadcastGroup::of_epoch(EPOCH, &self_id, &group_map).unwrap();

        let app_message: Bytes = vec![1_u8; 1024 * 1024].into();
        let packets = build_packets(&key, &app_message, group);

        let mut parser = ChunkParser::new();
        let mut used_ids = HashSet::new();

        for msg in &packets {
            let parsed = parser.parse(&msg.payload).expect("valid message");
            let newly_inserted = used_ids.insert(parsed.chunk_id);
            assert!(newly_inserted);
        }
    }

    #[test]
    fn test_handle_message_stride_slice() {
        let (key, validators, _known_addresses) = validator_set();
        let self_id = NodeId::new(key.pubkey());
        let epoch_validators = [(Epoch(1), validators)].into_iter().collect();
        let full_node_groups = FullNodeGroupMap::default();

        let mut udp_state = UdpState::<SignatureType>::new(self_id, u64::MAX, 10_000);
        udp_state.set_v1_rollout(DeterministicProtocolRolloutStage::AlwaysV1);

        // payload will fail to parse but shouldn't panic on index error
        let stride = deterministic::DEFAULT_SEGMENT_LEN;
        let payload: Bytes = vec![1_u8; stride * 8 + 1].into();
        let recv_msg = AuthRecvMsg {
            src_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8000),
            payload,
            stride: stride as u16,
            sender: None,
        };

        udp_state.handle_message(
            &epoch_validators,
            &full_node_groups,
            |_targets, _payload, _stride| {},
            recv_msg,
        );
    }

    #[rstest]
    #[case(-2 * 60 * 60 * 1000, u64::MAX, true)]
    #[case(2 * 60 * 60 * 1000, u64::MAX, true)]
    #[case(-2 * 60 * 60 * 1000, 0, false)]
    #[case(2 * 60 * 60 * 1000, 0, false)]
    #[case(-30_000, 60_000, true)]
    #[case(-120_000, 60_000, false)]
    #[case(120_000, 60_000, false)]
    #[case(30_000, 60_000, true)]
    #[case(-90_000, 60_000, false)]
    #[case(90_000, 60_000, false)]
    fn test_timestamp_validation(
        #[case] timestamp_offset_ms: i64,
        #[case] max_age_ms: u64,
        #[case] should_succeed: bool,
    ) {
        let (key, validators, _known_addresses) = validator_set();

        let current_time = std::time::UNIX_EPOCH.elapsed().unwrap().as_millis() as u64;
        let test_timestamp = (current_time as i64 + timestamp_offset_ms) as u64;

        let self_id = NodeId::new(key.pubkey());
        let group_map = make_group_map(&validators);
        let group = PrimaryBroadcastGroup::of_epoch(EPOCH, &self_id, &group_map).unwrap();

        let app_message = Bytes::from_static(b"test message");
        let packets = MessageBuilder::<SignatureType>::new(&key)
            .unix_ts_ms(test_timestamp)
            .build_vec(
                &app_message,
                &BuildTarget::deterministic_raptorcast(group, ROUND),
            )
            .expect("build should succeed");

        let message = packets.into_iter().next().unwrap().payload;
        let mut parser = ChunkParser::new().with_max_age_ms(max_age_ms);
        let result = parser.parse(&message);

        if should_succeed {
            assert!(result.is_ok(), "unexpected success: {:?}", result.err());
        } else {
            assert!(result.is_err());
            match result.err().unwrap() {
                InvalidChunk::InvalidTimestamp { .. } => {}
                other => panic!("unexpected error {:?}", other),
            }
        }
    }

    #[test]
    fn test_zero_len_chunk() {
        let (key, validators, _known_addresses) = validator_set();
        let self_id = NodeId::new(key.pubkey());
        let group_map = make_group_map(&validators);
        let group = PrimaryBroadcastGroup::of_epoch(EPOCH, &self_id, &group_map).unwrap();
        let app_message: Bytes = vec![1_u8; 1024 * 1024].into();
        let encoding = deterministic::PrimaryEncoding::new(
            EncodingScheme::Deterministic25(ROUND),
            &group,
            app_message.len(),
            UNIX_TS_MS,
        )
        .unwrap();
        let layout = encoding.layout();
        let mut packet = build_packets(&key, &app_message, group).pop().unwrap();
        packet.payload.truncate(layout.symbol_range().start);
        assert_eq!(
            packet.payload.len(),
            layout.segment_len() - layout.symbol_len()
        );

        let mut parser = ChunkParser::new();
        let result = parser.parse(&packet.payload);
        // should error instead of panicking
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_message_signature_verifier() {
        let (key, validators, _known_addresses) = validator_set();
        let self_id = NodeId::new(key.pubkey());
        let group_map = make_group_map(&validators);
        let group = PrimaryBroadcastGroup::of_epoch(EPOCH, &self_id, &group_map).unwrap();

        // Two different app messages to get distinct global merkle roots (= distinct
        // signed_over_data cache keys), required to exercise the rate-limiter path.
        let app_message_a: Bytes = vec![1_u8; 1024].into();
        let app_message_b: Bytes = vec![2_u8; 1024].into();

        let packets_a = build_packets(&key, &app_message_a, group);
        let packets_b = build_packets(&key, &app_message_b, group);

        let message_a: Bytes = packets_a[0].payload.clone();
        let message_b: Bytes = packets_b[0].payload.clone();

        let mut parser = ChunkParser::new().with_rate_limit(1);

        // Case 1: cache miss, verify signature, cache saved
        let result1 = parser.parse(&message_a);
        let author = result1.expect("first parse should succeed").author;
        assert_eq!(author, NodeId::new(key.pubkey()));

        // Case 2: parse with same message: cache hit, no rate limit consumed
        let result2 = parser.parse_with(&message_a, |_| false);
        assert_eq!(
            result2.expect("cache hit should succeed").author,
            author,
            "cache hit should return same author"
        );

        // Case 3: parse different message without bypass: rate limited
        let result3 = parser.parse_with(&message_b, |_| false);
        assert!(
            matches!(result3, Err(InvalidChunk::RateLimited)),
            "new message without bypass should be rate limited"
        );

        // Case 4: Same message with bypass: succeeds
        let result4 = parser.parse(&message_b);
        assert!(result4.is_ok());
    }

    #[test]
    fn test_deterministic_end_to_end_decode_and_rebroadcast() {
        let (sender_key, validators, _) = validator_set();
        let sender_id = NodeId::new(sender_key.pubkey());
        let group_map = make_group_map(&validators);
        let group = PrimaryBroadcastGroup::of_epoch(EPOCH, &sender_id, &group_map).unwrap();

        let app_message: Bytes = vec![0xAB_u8; 64 * 1024].into();
        let packets = build_packets(&sender_key, &app_message, group);
        assert!(!packets.is_empty());

        // Pick a receiver that is in the validator set but not the sender
        let receiver_id = validators
            .get_members()
            .keys()
            .find(|id| **id != sender_id)
            .copied()
            .unwrap();
        let epoch_validators: BTreeMap<_, _> = [(EPOCH, validators)].into();
        let full_node_groups = FullNodeGroupMap::default();
        let mut udp_state = UdpState::<SignatureType>::new(receiver_id, u64::MAX, 10_000);
        udp_state.set_v1_rollout(DeterministicProtocolRolloutStage::AlwaysV1);

        let stride = deterministic::DEFAULT_SEGMENT_LEN;
        let mut all_decoded = Vec::new();
        let mut rebroadcast_count = 0usize;

        for packet in &packets {
            let recv_msg = AuthRecvMsg {
                src_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8000),
                payload: packet.payload.clone(),
                stride: stride as u16,
                sender: None,
            };
            let decoded = udp_state.handle_message(
                &epoch_validators,
                &full_node_groups,
                |_targets, _payload, _stride| rebroadcast_count += 1,
                recv_msg,
            );
            all_decoded.extend(decoded);
        }

        assert_eq!(all_decoded.len(), 1, "should decode exactly one message");
        let (author, decoded) = &all_decoded[0];
        assert_eq!(*author, sender_id);
        assert_eq!(*decoded, app_message);

        // Reconstruct chunk assignment to count chunks addressed to receiver
        let group = PrimaryBroadcastGroup::of_epoch(EPOCH, &sender_id, &epoch_validators).unwrap();
        let encoding_scheme = EncodingScheme::Deterministic25(ROUND);
        let encoding = deterministic::PrimaryEncoding::new(
            encoding_scheme,
            &group,
            app_message.len(),
            UNIX_TS_MS,
        )
        .unwrap();
        let chunks = encoding.make_chunks().unwrap();
        let addressed_to_receiver = chunks
            .iter()
            .filter(|c| *c.recipient().node_id() == receiver_id)
            .count();
        assert_eq!(rebroadcast_count, addressed_to_receiver);
        assert!(
            rebroadcast_count > 0,
            "receiver should be assigned some chunks"
        );
    }

    #[rstest]
    #[case::tiny(1)]
    #[case::one_symbol(900)]
    #[case::medium(64 * 1024)]
    #[case::large(1024 * 1024)]
    #[case::max(crate::message::MAX_MESSAGE_SIZE)]
    fn test_deterministic_roundtrip_various_sizes(#[case] msg_size: usize) {
        let (key, validators, _) = validator_set();
        let self_id = NodeId::new(key.pubkey());
        let group_map = make_group_map(&validators);
        let group = PrimaryBroadcastGroup::of_epoch(EPOCH, &self_id, &group_map).unwrap();

        let app_message: Bytes = vec![0x42_u8; msg_size].into();
        let packets = build_packets(&key, &app_message, group);

        assert!(!packets.is_empty());
        assert!(
            packets
                .iter()
                .all(|msg| msg.payload.len() == deterministic::DEFAULT_SEGMENT_LEN),
            "all packets must be exactly SEGMENT_LEN"
        );

        let mut parser = ChunkParser::new();
        let mut seen_ids = HashSet::new();
        let mut global_root = None;

        for msg in &packets {
            let parsed = parser.parse(&msg.payload).expect("valid message");

            assert_eq!(parsed.app_message_len, msg_size as u32);
            assert!(seen_ids.insert(parsed.chunk_id), "duplicate chunk_id");

            let root = parsed
                .global_merkle_root()
                .expect("deterministic chunks must have global_merkle_root");
            match &global_root {
                None => global_root = Some(*root),
                Some(first) => assert_eq!(root, first),
            }
        }
    }

    // The v1 builder must generate enough encoding symbols
    // >= redundancy * num_source_symbols for the raptor10 decoder to
    // reconstruct.
    #[test]
    fn test_deterministic_decode_with_partial_chunks() {
        let (sender_key, validators, _) = validator_set();
        let sender_id = NodeId::new(sender_key.pubkey());
        let group_map = make_group_map(&validators);
        let group = PrimaryBroadcastGroup::of_epoch(EPOCH, &sender_id, &group_map).unwrap();

        let app_message: Bytes = vec![0xAB_u8; 200 * 1024].into();

        // With 3x redundancy we should have >> num_source_symbols chunks.
        let encoding = deterministic::PrimaryEncoding::new(
            EncodingScheme::Deterministic25(ROUND),
            &group,
            app_message.len(),
            UNIX_TS_MS,
        )
        .unwrap();
        let num_source = app_message
            .len()
            .div_ceil(encoding.layout().symbol_len())
            .max(1);
        let packets = build_packets(&sender_key, &app_message, group);
        assert!(
            packets.len() > num_source * 2,
            "expected > 2K chunks from 3x redundancy, got {} chunks for {} source symbols",
            packets.len(),
            num_source,
        );

        // Feed only 2/3 of the chunks to a receiver. This should be
        // enough for reconstruction under 3x redundancy.
        let receiver_id = validators
            .get_members()
            .keys()
            .find(|id| **id != sender_id)
            .copied()
            .unwrap();
        let epoch_validators: BTreeMap<_, _> = [(EPOCH, validators)].into();
        let full_node_groups = FullNodeGroupMap::default();
        let mut udp_state = UdpState::<SignatureType>::new(receiver_id, u64::MAX, 10_000);
        udp_state.set_v1_rollout(DeterministicProtocolRolloutStage::AlwaysV1);

        let subset_len = packets.len() * 2 / 3;
        let stride = deterministic::DEFAULT_SEGMENT_LEN;
        let mut decoded = Vec::new();

        for packet in packets.iter().take(subset_len) {
            let recv_msg = AuthRecvMsg {
                src_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8000),
                payload: packet.payload.clone(),
                stride: stride as u16,
                sender: None,
            };
            decoded.extend(udp_state.handle_message(
                &epoch_validators,
                &full_node_groups,
                |_, _, _| {},
                recv_msg,
            ));
        }

        assert_eq!(decoded.len(), 1, "should decode from 2/3 of chunks");
        assert_eq!(decoded[0].0, sender_id);
        assert_eq!(decoded[0].1, app_message);
    }

    #[test]
    fn test_deterministic_reproducibility() {
        let (proposer_key, validators, _) = validator_set();
        let proposer_id = NodeId::new(proposer_key.pubkey());

        // Validator: a different member of the validator set
        let validator_key = {
            let mut hasher = HasherType::new();
            hasher.update(0_u8.to_le_bytes());
            let mut hash = hasher.hash();
            KeyPairType::from_bytes(&mut hash.0).unwrap()
        };
        let validator_id = NodeId::new(validator_key.pubkey());
        assert_ne!(proposer_id, validator_id);
        assert!(validators.is_member(&validator_id));

        let group_map = make_group_map(&validators);
        let app_message: Bytes = vec![0xFF_u8; 64 * 1024].into();
        let group = PrimaryBroadcastGroup::of_epoch(EPOCH, &proposer_id, &group_map).unwrap();

        // Step 1: Proposer builds packets
        let proposer_packets = build_packets(&proposer_key, &app_message, group);

        // Step 2: Validator receives and decodes
        let epoch_validators: BTreeMap<_, _> = [(EPOCH, validators)].into();
        let full_node_groups = FullNodeGroupMap::default();
        let mut udp_state = UdpState::<SignatureType>::new(validator_id, u64::MAX, 10_000);
        udp_state.set_v1_rollout(DeterministicProtocolRolloutStage::AlwaysV1);

        let stride = deterministic::DEFAULT_SEGMENT_LEN;

        let mut decoded_msg = None;
        for packet in &proposer_packets {
            let recv_msg = AuthRecvMsg {
                src_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8000),
                payload: packet.payload.clone(),
                stride: stride as u16,
                sender: None,
            };
            for (_, msg) in udp_state.handle_message(
                &epoch_validators,
                &full_node_groups,
                |_, _, _| {},
                recv_msg,
            ) {
                decoded_msg = Some(msg);
            }
        }
        let decoded_msg = decoded_msg.expect("should decode");
        assert_eq!(decoded_msg, app_message);

        // Step 3: Validator rebuilds the packets from the decoded
        // message using the same parameters (validator set & header).
        let header = proposer_packets[0]
            .payload
            .slice(0..deterministic::PacketLayout::HEADER_LEN);
        let mut validator_packets = vec![];
        build_with_given_header(
            &header,
            &app_message,
            &group,
            EncodingScheme::Deterministic25(ROUND),
            UNIX_TS_MS,
            &mut validator_packets,
        )
        .expect("should build successfully");
        assert_eq!(proposer_packets.len(), validator_packets.len());

        // Step 4: For each rebuilt chunk, write proposer's signature
        // then it should be bit-identical to proposer's original
        // chunk.
        for (pp, vp) in proposer_packets.iter().zip(validator_packets.iter()) {
            assert_eq!(pp.payload, vp.payload);
        }
    }
}
