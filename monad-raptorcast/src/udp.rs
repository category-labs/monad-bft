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

use std::{collections::BTreeMap, num::NonZero, ops::Range};

use bytes::Bytes;
use governor::{DefaultDirectRateLimiter, Quota, RateLimiter};
use lru::LruCache;
use monad_crypto::{
    certificate_signature::{
        CertificateSignature, CertificateSignaturePubKey, CertificateSignatureRecoverable, PubKey,
    },
    hasher::{Hasher, HasherType},
    signing_domain,
};
use monad_dataplane::udp::{segment_size_for_mtu, ETHERNET_SEGMENT_SIZE};
use monad_executor::ExecutorMetricsChain;
use monad_merkle::{MerkleHash, MerkleProof};
use monad_types::{Epoch, NodeId, Round};
use monad_validator::validator_set::ValidatorSetType as _;
use tracing::warn;

pub use crate::packet::build_messages;
use crate::{
    decoding::{DecoderCache, DecodingContext, TryDecodeError, TryDecodeStatus},
    message::MAX_MESSAGE_SIZE,
    metrics::{
        UdpStateMetrics, GAUGE_RAPTORCAST_DECODING_CACHE_SIGNATURE_VERIFICATIONS_RATE_LIMITED,
    },
    packet::{assembler::HEADER_LEN, PacketLayout},
    util::{
        compute_hash, unix_ts_ms_now, AppMessageHash, BroadcastMode, EpochValidators, HexBytes,
        NodeIdHash, ReBroadcastGroupMap, Redundancy,
    },
    SIGNATURE_SIZE,
};

const _: () = assert!(
    MAX_MERKLE_TREE_DEPTH <= 0xF,
    "merkle tree depth must be <= 4 bits"
);

const _: () = assert!(
    MIN_SEGMENT_LENGTH == segment_size_for_mtu(1280) as usize,
    "MIN_SEGMENT_LENGTH should be the segment size for the IPv6 minimum MTU of 1280 bytes"
);

pub const SIGNATURE_CACHE_SIZE: NonZero<usize> = NonZero::new(10_000).unwrap();

// We assume an MTU of at least 1280 (the IPv6 minimum MTU), which for the maximum Merkle tree
// depth of 9 gives a symbol size of 960 bytes, which we will use as the minimum chunk length for
// received packets, and we'll drop received chunks that are smaller than this to mitigate attacks
// involving a peer sending us a message as a very large set of very small chunks.
pub const MIN_CHUNK_LENGTH: usize = 960;

// Drop a message to be transmitted if it would lead to more than this number of packets
// to be transmitted.  This can happen in Broadcast mode when the message is large or
// if we have many peers to transmit the message to.
pub const MAX_NUM_PACKETS: usize = 65535;

// For a message with K source symbols, we accept up to the first MAX_REDUNDANCY * K
// encoded symbols.
//
// Any received encoded symbol with an ESI equal to or greater than MAX_REDUNDANCY * K
// will be discarded, as a protection against DoS and algorithmic complexity attacks.
//
// We pick 7 because that is the largest value that works for all values of K, as K
// can be at most 8192, and there can be at most 65521 encoding symbol IDs.
pub const MAX_REDUNDANCY: Redundancy = Redundancy::from_u8(7);

// For a tree depth of 1, every encoded symbol is its own Merkle tree, and there will be no
// Merkle proof section in the constructed RaptorCast packets.
//
// For a tree depth of 9, the index of the rightmost Merkle tree leaf will be 0xff, and the
// Merkle leaf index field is 8 bits wide.
pub const MIN_MERKLE_TREE_DEPTH: u8 = 1;
pub const MAX_MERKLE_TREE_DEPTH: u8 = 9;

/// The min segment length should be large enough to hold at least
/// MAX_CHUNK_LENGTH of payload plus all headers with the smallest
/// merkle tree depth.
pub const MIN_SEGMENT_LENGTH: usize =
    PacketLayout::calc_segment_len(MIN_CHUNK_LENGTH, MAX_MERKLE_TREE_DEPTH);

/// The max segment length should not exceed the standard MTU for
/// Ethernet to avoid fragmentation when routed across the internet.
pub const MAX_SEGMENT_LENGTH: usize = ETHERNET_SEGMENT_SIZE as usize;

/// The maximum sane validator set size. Defined in
/// <execution>/monad/staking/util/constants.hpp.
pub const MAX_VALIDATOR_SET_SIZE: usize = 200;

pub(crate) struct UdpState<ST: CertificateSignatureRecoverable> {
    self_id: NodeId<CertificateSignaturePubKey<ST>>,
    max_age_ms: u64,

    // TODO add a cap on max number of chunks that will be forwarded per message? so that a DOS
    // can't be induced by spamming broadcast chunks to any given node
    // TODO we also need to cap the max number chunks that are decoded - because an adversary could
    // generate a bunch of linearly dependent chunks and cause unbounded memory usage.
    decoder_cache: DecoderCache<CertificateSignaturePubKey<ST>>,

    signature_cache: LruCache<[u8; HEADER_LEN + 20], NodeId<CertificateSignaturePubKey<ST>>>,

    sig_verification_rate_limiter: DefaultDirectRateLimiter,

    metrics: UdpStateMetrics,
}

impl<ST: CertificateSignatureRecoverable> UdpState<ST> {
    pub fn new(
        self_id: NodeId<CertificateSignaturePubKey<ST>>,
        max_age_ms: u64,
        sig_verification_rate_limit: u32,
    ) -> Self {
        let quota = Quota::per_second(
            NonZero::new(sig_verification_rate_limit)
                .expect("sig_verification_rate_limit must be non-zero"),
        );
        let sig_verification_rate_limiter = RateLimiter::direct(quota);

        Self {
            self_id,
            max_age_ms,

            decoder_cache: DecoderCache::default(),
            signature_cache: LruCache::new(SIGNATURE_CACHE_SIZE),
            sig_verification_rate_limiter,

            metrics: UdpStateMetrics::new(),
        }
    }

    pub fn metrics(&self) -> &UdpStateMetrics {
        &self.metrics
    }

    pub fn decoder_metrics(&self) -> ExecutorMetricsChain<'_> {
        self.decoder_cache.metrics()
    }

    /// Given a RecvUdpMsg, emits all decoded messages while rebroadcasting as necessary
    #[tracing::instrument(level = "debug", name = "udp_handle_message", skip_all)]
    pub fn handle_message(
        &mut self,
        group_map: &ReBroadcastGroupMap<ST>,
        epoch_validators: &BTreeMap<Epoch, EpochValidators<ST>>,
        rebroadcast: impl FnMut(Vec<NodeId<CertificateSignaturePubKey<ST>>>, Bytes, u16),
        message: crate::auth::AuthRecvMsg<CertificateSignaturePubKey<ST>>,
    ) -> Vec<(NodeId<CertificateSignaturePubKey<ST>>, Bytes)> {
        let self_id = self.self_id;
        let self_hash = compute_hash(&self_id);

        let mut broadcast_batcher =
            BroadcastBatcher::new(self_id, rebroadcast, &message.payload, message.stride);

        let mut messages = Vec::new(); // The return result; decoded messages

        for payload_start_idx in (0..message.payload.len()).step_by(message.stride.into()) {
            // scoped variables are dropped in reverse order of declaration.
            // when *batch_guard is dropped, packets can get flushed
            let mut batch_guard = broadcast_batcher.create_flush_guard();

            let payload_end_idx =
                (payload_start_idx + usize::from(message.stride)).min(message.payload.len());
            let payload = message.payload.slice(payload_start_idx..payload_end_idx);

            // "message" here means a raptor-casted chunk (AKA r10 symbol), not the whole final message (proposal)
            let parsed_message = match parse_message::<ST, _>(
                &mut self.signature_cache,
                payload,
                self.max_age_ms,
                |group_id| {
                    let allowed = self.sig_verification_rate_limiter.check().is_ok();
                    let is_validator = match (message.auth_public_key.as_ref(), group_id) {
                        (Some(pk), GroupId::Primary(epoch)) => {
                            let node_id = NodeId::new(*pk);
                            epoch_validators.get(&epoch).is_some_and(|ev| {
                                ev.validators.get_members().contains_key(&node_id)
                            })
                        }
                        _ => false,
                    };
                    if allowed || is_validator {
                        Ok(())
                    } else {
                        Err(MessageValidationError::RateLimited)
                    }
                },
            ) {
                Ok(message) => message,
                Err(MessageValidationError::RateLimited) => {
                    tracing::debug!(
                        src_addr = ?message.src_addr,
                        "rate limited raptorcast chunk signature verification"
                    );
                    self.metrics.executor_metrics_mut()
                        [GAUGE_RAPTORCAST_DECODING_CACHE_SIGNATURE_VERIFICATIONS_RATE_LIMITED] += 1;
                    continue;
                }
                Err(err) => {
                    tracing::debug!(src_addr = ?message.src_addr, ?err, "unable to parse message");
                    continue;
                }
            };

            // Ignore chunk if self is the author
            // This can happen if a peer validator rebroadcasts a message back to self
            if parsed_message.author == self.self_id {
                tracing::trace!(
                    app_message_hash =? parsed_message.app_message_hash,
                    encoding_symbol_id =? parsed_message.chunk_id,
                    "received raptor chunk generated by self"
                );
                continue;
            }

            // Enforce a minimum chunk size for messages consisting of multiple source chunks.
            if parsed_message.chunk.len() < MIN_CHUNK_LENGTH
                && usize::try_from(parsed_message.app_message_len).unwrap()
                    > parsed_message.chunk.len()
            {
                tracing::debug!(
                    src_addr = ?message.src_addr,
                    chunk_length = parsed_message.chunk.len(),
                    MIN_CHUNK_LENGTH,
                    "dropping undersized received message",
                );
                continue;
            }

            // Note: The check that parsed_message.author is valid is already
            // done in iterate_rebroadcast_peers(), but we want to drop invalid
            // chunks ASAP, before changing `recently_decoded_state`.
            if parsed_message.maybe_broadcast_mode.is_some() {
                if !group_map.check_source(
                    parsed_message.group_id,
                    &parsed_message.author,
                    &message.src_addr,
                ) {
                    continue;
                }
            } else if self_hash != parsed_message.recipient_hash {
                tracing::debug!(
                    src_addr = ?message.src_addr,
                    ?self_hash,
                    recipient_hash =? parsed_message.recipient_hash,
                    "dropping spoofed message"
                );
                continue;
            }

            tracing::trace!(
                src_addr = ?message.src_addr,
                app_message_len = ?parsed_message.app_message_len,
                self_id =? self.self_id,
                author =? parsed_message.author,
                unix_ts_ms = parsed_message.unix_ts_ms,
                app_message_hash =? parsed_message.app_message_hash,
                encoding_symbol_id =? parsed_message.chunk_id as usize,
                "received encoded symbol"
            );

            let mut try_rebroadcast_symbol = || {
                // rebroadcast raptorcast chunks if broadcast mode is set and
                // we're the assigned rebroadcaster
                if parsed_message.maybe_broadcast_mode.is_some()
                    && self_hash == parsed_message.recipient_hash
                {
                    let maybe_targets = group_map
                        .iterate_rebroadcast_peers(parsed_message.group_id, &parsed_message.author);
                    if let Some(targets) = maybe_targets {
                        batch_guard.queue_broadcast(
                            payload_start_idx,
                            payload_end_idx,
                            &parsed_message.author,
                            || targets.cloned().collect(),
                        )
                    }
                }
            };

            let validator_set = match parsed_message.group_id {
                GroupId::Primary(epoch) => epoch_validators.get(&epoch).map(|ev| &ev.validators),
                GroupId::Secondary(_round) => None,
            };

            let decoding_context = DecodingContext::new(validator_set, unix_ts_ms_now());

            match self
                .decoder_cache
                .try_decode(&parsed_message, &decoding_context)
            {
                Err(TryDecodeError::InvalidSymbol(err)) => {
                    err.log(&parsed_message, &self.self_id);
                }

                Err(TryDecodeError::UnableToReconstructSourceData) => {
                    tracing::error!("failed to reconstruct source data");
                }

                Err(TryDecodeError::AppMessageHashMismatch { expected, actual }) => {
                    tracing::error!(
                        ?self_id,
                        author =? parsed_message.author,
                        ?expected,
                        ?actual,
                        "mismatch message hash"
                    );
                }

                Ok(TryDecodeStatus::RejectedByCache) => {
                    tracing::warn!(
                        ?self_id,
                        author =? parsed_message.author,
                        chunk_id = parsed_message.chunk_id,
                        "message rejected by cache, author may be flooding messages",
                    );
                }

                Ok(TryDecodeStatus::RecentlyDecoded) | Ok(TryDecodeStatus::NeedsMoreSymbols) => {
                    // TODO: cap rebroadcast symbols based on some multiple of esis.
                    try_rebroadcast_symbol();
                }

                Ok(TryDecodeStatus::Decoded {
                    author,
                    app_message,
                }) => {
                    // TODO: cap rebroadcast symbols based on some multiple of esis.
                    try_rebroadcast_symbol();

                    if let Some(mode) = parsed_message.maybe_broadcast_mode {
                        self.metrics
                            .record_broadcast_latency(mode, parsed_message.unix_ts_ms);
                    }

                    messages.push((author, app_message));
                }
            }
        }

        messages
    }
}

#[derive(Clone, Copy, Debug)]
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

#[derive(Clone, Debug)]
pub struct ValidatedMessage<PT>
where
    PT: PubKey,
{
    pub message: Bytes,

    // `author` is recovered from the public key in the chunk signature, which
    // was signed by the validator who encoded the proposal into raptorcast.
    // This applies to both validator-to-validator and validator-to-full-node
    // raptorcasting.
    pub author: NodeId<PT>,
    // group_id is set to
    // - epoch number for validator-to-validator raptorcast
    // - round number for validator-to-fullnode raptorcast
    pub group_id: GroupId,
    pub unix_ts_ms: u64,
    pub app_message_hash: AppMessageHash,
    pub app_message_len: u32,
    pub maybe_broadcast_mode: Option<BroadcastMode>,
    pub recipient_hash: NodeIdHash, // if this matches our node_id, then we need to re-broadcast RaptorCast chunks
    pub chunk_id: u16,
    pub chunk: Bytes, // raptor-coded portion
}

#[derive(Debug, PartialEq, Eq)]
pub enum MessageValidationError {
    UnknownVersion,
    TooShort,
    TooLong,
    InvalidSignature,
    InvalidTreeDepth,
    InvalidMerkleProof,
    InvalidChunkId,
    InvalidTimestamp {
        timestamp: u64,
        max: u64,
        delta: i64,
    },
    InvalidBroadcastBits,
    RateLimited,
}

/// - 65 bytes => Signature of sender over hash(rest of message up to merkle proof, concatenated with merkle root)
/// - 2 bytes => Version: bumped on protocol updates
/// - 1 bit => broadcast or not
/// - 1 bit => secondary broadcast or not (full-node raptorcast)
/// - 2 bits => unused
/// - 4 bits => Merkle tree depth
/// - 8 bytes (u64) => Epoch #
/// - 8 bytes (u64) => Unix timestamp
/// - 20 bytes => first 20 bytes of hash of AppMessage
///   - this isn't technically necessary if payload_len is small enough to fit in 1 chunk, but keep
///     for simplicity
/// - 4 bytes (u32) => Serialized AppMessage length (bytes)
/// - 20 bytes * (merkle_tree_depth - 1) => merkle proof (leaves include everything that follows,
///   eg hash(chunk_recipient + chunk_byte_offset + symbol_len + payload))
///
/// - 20 bytes => first 20 bytes of hash of chunk's first hop recipient
///   - we set this even if broadcast bit is not set so that it's known if a message was intended
///     to be sent to self
/// - 1 byte => Chunk's merkle leaf idx
/// - 1 byte => reserved
/// - 2 bytes (u16) => This chunk's id
/// - rest => data
pub fn parse_message<ST, F>(
    signature_cache: &mut LruCache<[u8; HEADER_LEN + 20], NodeId<CertificateSignaturePubKey<ST>>>,
    message: Bytes,
    max_age_ms: u64,
    rate_limit_check: F,
) -> Result<ValidatedMessage<CertificateSignaturePubKey<ST>>, MessageValidationError>
where
    ST: CertificateSignatureRecoverable,
    F: Fn(GroupId) -> Result<(), MessageValidationError>,
{
    let mut cursor: Bytes = message.clone();
    let mut split_off = |mid| {
        if mid > cursor.len() {
            Err(MessageValidationError::TooShort)
        } else {
            Ok(cursor.split_to(mid))
        }
    };
    let cursor_signature = split_off(SIGNATURE_SIZE)?;
    let signature = <ST as CertificateSignature>::deserialize(&cursor_signature)
        .map_err(|_| MessageValidationError::InvalidSignature)?;

    let cursor_version = split_off(2)?;
    let version = u16::from_le_bytes(cursor_version.as_ref().try_into().expect("u16 is 2 bytes"));
    if version != 0 {
        return Err(MessageValidationError::UnknownVersion);
    }

    let cursor_broadcast_tree_depth = split_off(1)?[0];
    let broadcast = (cursor_broadcast_tree_depth & (1 << 7)) != 0;
    let secondary_broadcast = (cursor_broadcast_tree_depth & (1 << 6)) != 0;
    let tree_depth = cursor_broadcast_tree_depth & 0b0000_1111; // bottom 4 bits

    let maybe_broadcast_mode = match (broadcast, secondary_broadcast) {
        (true, false) => Some(BroadcastMode::Primary),
        (false, true) => Some(BroadcastMode::Secondary),
        (false, false) => None, // broadcast or unicast
        (true, true) => {
            return Err(MessageValidationError::InvalidBroadcastBits);
        }
    };

    if !(MIN_MERKLE_TREE_DEPTH..=MAX_MERKLE_TREE_DEPTH).contains(&tree_depth) {
        return Err(MessageValidationError::InvalidTreeDepth);
    }

    let cursor_group_id = split_off(8)?;
    let group_id = u64::from_le_bytes(cursor_group_id.as_ref().try_into().expect("u64 is 8 bytes"));
    let group_id = match maybe_broadcast_mode {
        Some(BroadcastMode::Primary) | None => GroupId::Primary(Epoch(group_id)),
        Some(BroadcastMode::Secondary) => GroupId::Secondary(Round(group_id)),
    };

    let cursor_unix_ts_ms = split_off(8)?;
    let unix_ts_ms = u64::from_le_bytes(
        cursor_unix_ts_ms
            .as_ref()
            .try_into()
            .expect("u64 is 8 bytes"),
    );

    ensure_valid_timestamp(unix_ts_ms, max_age_ms)?;

    let cursor_app_message_hash = split_off(20)?;
    let app_message_hash: AppMessageHash = HexBytes(
        cursor_app_message_hash
            .as_ref()
            .try_into()
            .expect("Hash is 20 bytes"),
    );

    let cursor_app_message_len = split_off(4)?;
    let app_message_len = u32::from_le_bytes(
        cursor_app_message_len
            .as_ref()
            .try_into()
            .expect("u32 is 4 bytes"),
    ) as usize;

    if app_message_len > MAX_MESSAGE_SIZE {
        return Err(MessageValidationError::TooLong);
    };

    let proof_size: u16 = 20 * (u16::from(tree_depth) - 1);

    let mut merkle_proof = Vec::new();
    for _ in 0..tree_depth - 1 {
        let cursor_sibling = split_off(20)?;
        let sibling =
            MerkleHash::try_from(cursor_sibling.as_ref()).expect("MerkleHash is 20 bytes");
        merkle_proof.push(sibling);
    }

    let cursor_recipient = split_off(20)?;
    let recipient_hash: NodeIdHash = HexBytes(
        cursor_recipient
            .as_ref()
            .try_into()
            .expect("Hash is 20 bytes"),
    );

    let cursor_merkle_idx = split_off(1)?[0];
    let merkle_proof = MerkleProof::new_from_leaf_idx(merkle_proof, cursor_merkle_idx)
        .ok_or(MessageValidationError::InvalidMerkleProof)?;

    let _cursor_reserved = split_off(1)?;

    let cursor_chunk_id = split_off(2)?;
    let chunk_id = u16::from_le_bytes(cursor_chunk_id.as_ref().try_into().expect("u16 is 2 bytes"));

    let cursor_payload = cursor;
    let symbol_len = cursor_payload.len();
    if symbol_len == 0 {
        // handle the degenerate case
        return Err(MessageValidationError::TooShort);
    }

    let chunk_id_range = match maybe_broadcast_mode {
        None | Some(BroadcastMode::Secondary) => valid_chunk_id_range(app_message_len, symbol_len)?,
        Some(BroadcastMode::Primary) => {
            // only perform a basic sanity check here. more precise
            // check of chunk_id is in decoding.rs when the validator
            // set is available.
            valid_chunk_id_range_raptorcast(app_message_len, symbol_len, MAX_VALIDATOR_SET_SIZE)?
        }
    };
    if !chunk_id_range.contains(&(chunk_id as usize)) {
        return Err(MessageValidationError::InvalidChunkId);
    }

    let leaf_hash = {
        let mut hasher = HasherType::new();
        hasher.update(
            &message[HEADER_LEN + proof_size as usize..
                // HEADER_LEN as usize
                //     + proof_size as usize
                //     + CHUNK_HEADER_LEN as usize
                //     + payload_len as usize
                ],
        );
        hasher.hash()
    };
    let root = merkle_proof
        .compute_root(&leaf_hash)
        .ok_or(MessageValidationError::InvalidMerkleProof)?;
    let mut signed_over = [0_u8; HEADER_LEN + 20];
    // TODO can avoid this copy if necessary
    signed_over[..HEADER_LEN].copy_from_slice(&message[..HEADER_LEN]);
    signed_over[HEADER_LEN..].copy_from_slice(&root);

    let author = *signature_cache.try_get_or_insert(signed_over, || {
        rate_limit_check(group_id)?;
        let author = signature
            .recover_pubkey::<signing_domain::RaptorcastChunk>(&signed_over[SIGNATURE_SIZE..])
            .map_err(|_| MessageValidationError::InvalidSignature)?;
        Ok(NodeId::new(author))
    })?;

    Ok(ValidatedMessage {
        message,
        author,
        group_id,
        unix_ts_ms,
        app_message_hash,
        app_message_len: app_message_len as u32,
        maybe_broadcast_mode,
        recipient_hash,
        chunk_id,
        chunk: cursor_payload,
    })
}

fn ensure_valid_timestamp(unix_ts_ms: u64, max_age_ms: u64) -> Result<(), MessageValidationError> {
    let current_time_ms = if let Ok(current_time_elapsed) = std::time::UNIX_EPOCH.elapsed() {
        current_time_elapsed.as_millis() as u64
    } else {
        warn!("system time is before unix epoch, ignoring timestamp");
        return Ok(());
    };
    let delta = (current_time_ms as i64).saturating_sub(unix_ts_ms as i64);
    if delta.unsigned_abs() > max_age_ms {
        Err(MessageValidationError::InvalidTimestamp {
            timestamp: unix_ts_ms,
            max: max_age_ms,
            delta,
        })
    } else {
        Ok(())
    }
}

fn valid_chunk_id_range_raptorcast(
    app_message_len: usize,
    symbol_len: usize,
    num_validators: usize,
) -> Result<Range<usize>, MessageValidationError> {
    if symbol_len == 0 {
        return Err(MessageValidationError::TooShort);
    }
    let base_chunks = app_message_len.div_ceil(symbol_len);
    let rounding_chunks = num_validators;
    let num_chunks = MAX_REDUNDANCY
        .scale(base_chunks)
        .ok_or(MessageValidationError::TooLong)?
        + rounding_chunks;
    Ok(0..num_chunks)
}

fn valid_chunk_id_range(
    app_message_len: usize,
    symbol_len: usize,
) -> Result<Range<usize>, MessageValidationError> {
    if symbol_len == 0 {
        return Err(MessageValidationError::TooShort);
    }
    let base_chunks = app_message_len.div_ceil(symbol_len);
    let num_chunks = MAX_REDUNDANCY
        .scale(base_chunks)
        .ok_or(MessageValidationError::TooLong)?;
    Ok(0..num_chunks)
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
        num::NonZero,
    };

    use bytes::{Bytes, BytesMut};
    use governor::{Quota, RateLimiter};
    use itertools::Itertools as _;
    use lru::LruCache;
    use monad_crypto::{
        certificate_signature::CertificateSignaturePubKey,
        hasher::{Hasher, HasherType},
    };
    use monad_dataplane::udp::DEFAULT_SEGMENT_SIZE;
    use monad_secp::{KeyPair, SecpSignature};
    use monad_types::{Epoch, NodeId, Round, RoundSpan, Stake};
    use monad_validator::validator_set::{ValidatorSet, ValidatorSetType as _};
    use rstest::*;

    use super::{GroupId, MessageValidationError, UdpState};
    use crate::{
        packet::{MessageBuilder, PacketLayout},
        udp::{build_messages, parse_message, MAX_VALIDATOR_SET_SIZE, SIGNATURE_CACHE_SIZE},
        util::{
            BroadcastMode, BuildTarget, EpochValidators, Group, ReBroadcastGroupMap, Redundancy,
        },
    };

    type SignatureType = SecpSignature;
    type KeyPairType = KeyPair;

    fn validator_set() -> (
        KeyPairType,
        EpochValidators<SignatureType>,
        HashMap<NodeId<CertificateSignaturePubKey<SignatureType>>, SocketAddr>,
    ) {
        const NUM_KEYS: u8 = 100;
        let mut keys = (0_u8..NUM_KEYS)
            .map(|n| {
                let mut hasher = HasherType::new();
                hasher.update(n.to_le_bytes());
                let mut hash = hasher.hash();
                KeyPairType::from_bytes(&mut hash.0).unwrap()
            })
            .collect_vec();

        let valset = keys
            .iter()
            .map(|key| (NodeId::new(key.pubkey()), Stake::ONE))
            .collect();
        let validators = EpochValidators {
            validators: ValidatorSet::new_unchecked(valset),
        };

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

    #[test]
    fn test_roundtrip() {
        let (key, validators, known_addresses) = validator_set();
        let epoch_validators = validators.view_without(vec![&NodeId::new(key.pubkey())]);

        let app_message: Bytes = vec![1_u8; 1024 * 1024].into();
        let app_message_hash = {
            let mut hasher = HasherType::new();
            hasher.update(&app_message);
            hasher.hash()
        };

        let messages = build_messages::<SignatureType>(
            &key,
            DEFAULT_SEGMENT_SIZE, // segment_size
            app_message.clone(),
            Redundancy::from_u8(2),
            GroupId::Primary(EPOCH), // epoch_no
            UNIX_TS_MS,
            BuildTarget::Raptorcast(epoch_validators),
            &known_addresses,
        );

        let mut signature_cache = LruCache::new(SIGNATURE_CACHE_SIZE);

        for (_to, mut aggregate_message) in messages {
            while !aggregate_message.is_empty() {
                let message = aggregate_message.split_to(DEFAULT_SEGMENT_SIZE.into());
                let parsed_message = parse_message::<SignatureType, _>(
                    &mut signature_cache,
                    message.clone(),
                    u64::MAX,
                    |_| Ok(()),
                )
                .expect("valid message");
                assert_eq!(parsed_message.message, message);
                assert_eq!(parsed_message.app_message_hash.0, app_message_hash.0[..20]);
                assert_eq!(parsed_message.unix_ts_ms, UNIX_TS_MS);
                assert!(matches!(
                    parsed_message.maybe_broadcast_mode,
                    Some(BroadcastMode::Primary)
                ));
                assert_eq!(parsed_message.app_message_len, app_message.len() as u32);
                assert_eq!(parsed_message.author, NodeId::new(key.pubkey()));
            }
        }
    }

    #[test]
    fn test_bit_flip_parse_failure_slow() {
        let (key, validators, known_addresses) = validator_set();
        let epoch_validators = validators.view_without(vec![&NodeId::new(key.pubkey())]);

        let app_message: Bytes = vec![1_u8; 1024 * 2].into();

        let messages = build_messages::<SignatureType>(
            &key,
            DEFAULT_SEGMENT_SIZE, // segment_size
            app_message,
            Redundancy::from_u8(2),
            GroupId::Primary(EPOCH), // epoch_no
            UNIX_TS_MS,
            BuildTarget::Raptorcast(epoch_validators),
            &known_addresses,
        );

        let mut signature_cache = LruCache::new(SIGNATURE_CACHE_SIZE);

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
                    let maybe_parsed = parse_message::<SignatureType, _>(
                        &mut signature_cache,
                        message.clone().into(),
                        u64::MAX,
                        |_| Ok(()),
                    );

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
        let epoch_validators = validators.view_without(vec![&NodeId::new(key.pubkey())]);

        let app_message: Bytes = vec![1_u8; 1024 * 1024].into();

        let messages = build_messages::<SignatureType>(
            &key,
            DEFAULT_SEGMENT_SIZE, // segment_size
            app_message,
            Redundancy::from_u8(2),
            GroupId::Primary(EPOCH), // epoch_no
            UNIX_TS_MS,
            BuildTarget::Raptorcast(epoch_validators),
            &known_addresses,
        );

        let mut signature_cache = LruCache::new(SIGNATURE_CACHE_SIZE);

        let mut used_ids = HashSet::new();

        for (_to, mut aggregate_message) in messages {
            while !aggregate_message.is_empty() {
                let message = aggregate_message.split_to(DEFAULT_SEGMENT_SIZE.into());
                let parsed_message = parse_message::<SignatureType, _>(
                    &mut signature_cache,
                    message.clone(),
                    u64::MAX,
                    |_| Ok(()),
                )
                .expect("valid message");
                let newly_inserted = used_ids.insert(parsed_message.chunk_id);
                assert!(newly_inserted);
            }
        }
    }

    #[test]
    fn test_broadcast_bit() {
        let (key, validators, known_addresses) = validator_set();
        let self_id = NodeId::new(key.pubkey());
        let epoch_validators = validators.view_without(vec![&self_id]);
        let full_nodes = Group::new_fullnode_group(
            epoch_validators.iter_nodes().cloned().collect(),
            &self_id,
            self_id,
            RoundSpan::new(Round(1), Round(100)).unwrap(),
        );

        let app_message: Bytes = vec![1_u8; 1024 * 1024].into();
        let build_targets = vec![
            BuildTarget::Raptorcast(epoch_validators),
            BuildTarget::FullNodeRaptorCast(&full_nodes),
        ];

        for build_target in build_targets {
            let messages = build_messages::<SignatureType>(
                &key,
                DEFAULT_SEGMENT_SIZE, // segment_size
                app_message.clone(),
                Redundancy::from_u8(2),
                GroupId::Primary(EPOCH), // epoch_no
                UNIX_TS_MS,
                build_target.clone(),
                &known_addresses,
            );

            let mut signature_cache = LruCache::new(SIGNATURE_CACHE_SIZE);

            for (_to, mut aggregate_message) in messages {
                while !aggregate_message.is_empty() {
                    let message = aggregate_message.split_to(DEFAULT_SEGMENT_SIZE.into());
                    let parsed_message = parse_message::<SignatureType, _>(
                        &mut signature_cache,
                        message.clone(),
                        u64::MAX,
                        |_| Ok(()),
                    )
                    .expect("valid message");

                    match build_target {
                        BuildTarget::Raptorcast(_) => {
                            assert!(matches!(
                                parsed_message.maybe_broadcast_mode,
                                Some(BroadcastMode::Primary)
                            ));
                        }
                        BuildTarget::FullNodeRaptorCast(_) => {
                            assert!(matches!(
                                parsed_message.maybe_broadcast_mode,
                                Some(BroadcastMode::Secondary)
                            ));
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
        let epoch_validators = validators.view_without(vec![&NodeId::new(key.pubkey())]);

        let app_message: Bytes = vec![1_u8; 1024 * 8].into();

        let messages = build_messages::<SignatureType>(
            &key,
            DEFAULT_SEGMENT_SIZE, // segment_size
            app_message,
            Redundancy::from_u8(2),
            GroupId::Primary(EPOCH), // epoch_no
            UNIX_TS_MS,
            BuildTarget::Broadcast(epoch_validators.into()),
            &known_addresses,
        );

        let mut signature_cache = LruCache::new(SIGNATURE_CACHE_SIZE);

        let mut used_ids: HashMap<SocketAddr, HashSet<_>> = HashMap::new();

        for (to, mut aggregate_message) in messages {
            while !aggregate_message.is_empty() {
                let message = aggregate_message.split_to(DEFAULT_SEGMENT_SIZE.into());
                let parsed_message = parse_message::<SignatureType, _>(
                    &mut signature_cache,
                    message.clone(),
                    u64::MAX,
                    |_| Ok(()),
                )
                .expect("valid message");
                let newly_inserted = used_ids
                    .entry(to)
                    .or_default()
                    .insert(parsed_message.chunk_id);
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
        let mut group_map = ReBroadcastGroupMap::new(self_id);
        let node_stake_pairs: Vec<_> = validators
            .validators
            .get_members()
            .iter()
            .map(|(node_id, stake)| (*node_id, *stake))
            .collect();
        group_map.push_group_validator_set(node_stake_pairs, Epoch(1));
        let validator_set = [(Epoch(1), validators)].into_iter().collect();

        let mut udp_state = UdpState::<SignatureType>::new(self_id, u64::MAX, 10_000);

        // payload will fail to parse but shouldn't panic on index error
        let payload: Bytes = vec![1_u8; 1024 * 8 + 1].into();
        let recv_msg = crate::auth::AuthRecvMsg {
            src_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8000),
            payload,
            stride: 1024,
            auth_public_key: None::<CertificateSignaturePubKey<SignatureType>>,
        };

        udp_state.handle_message(
            &group_map,
            &validator_set,
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
        let epoch_validators = validators.view_without(vec![&NodeId::new(key.pubkey())]);
        let mut signature_cache = LruCache::new(SIGNATURE_CACHE_SIZE);

        let current_time = std::time::UNIX_EPOCH.elapsed().unwrap().as_millis() as u64;
        let test_timestamp = (current_time as i64 + timestamp_offset_ms) as u64;

        let app_message = Bytes::from_static(b"test message");
        let messages = build_messages::<SignatureType>(
            &key,
            DEFAULT_SEGMENT_SIZE,
            app_message,
            Redundancy::from_u8(1),
            GroupId::Primary(EPOCH),
            test_timestamp,
            BuildTarget::Broadcast(epoch_validators.into()),
            &known_addresses,
        );
        let message = messages.into_iter().next().unwrap().1;
        let result = parse_message::<SignatureType, _>(
            &mut signature_cache,
            message,
            max_age_ms,
            |_| Ok(()),
        );

        if should_succeed {
            assert!(result.is_ok(), "unexpected success: {:?}", result.err());
        } else {
            assert!(result.is_err());
            match result.err().unwrap() {
                MessageValidationError::InvalidTimestamp { .. } => {}
                other => panic!("unexpected error {:?}", other),
            }
        }
    }

    pub const MERKLE_TREE_DEPTH: u8 = 6;
    pub const SYMBOL_LEN: usize =
        PacketLayout::new(DEFAULT_SEGMENT_SIZE as usize, MERKLE_TREE_DEPTH).symbol_len();
    pub const MAX_REDUNDANCY: u16 = 7;

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
        let epoch_validators = validators.view_without(vec![&NodeId::new(key.pubkey())]);
        let target = if raptorcast {
            BuildTarget::Raptorcast(epoch_validators)
        } else {
            BuildTarget::Broadcast(epoch_validators.into())
        };
        let app_msg = vec![0; app_msg_len];
        let messages = MessageBuilder::new(&key)
            .segment_size(DEFAULT_SEGMENT_SIZE as usize)
            .group_id(GroupId::Primary(EPOCH))
            .redundancy(Redundancy::from_u8(1))
            .merkle_tree_depth(MERKLE_TREE_DEPTH)
            .build_vec(&app_msg, &target);
        let message = messages.unwrap().into_iter().next().unwrap();
        let mut payload = BytesMut::from(&message.payload[..message.stride]);

        let layout = PacketLayout::new(DEFAULT_SEGMENT_SIZE as usize, MERKLE_TREE_DEPTH);
        let chunk_header = &mut payload[layout.chunk_header_range()];
        let chunk_id_buf: &mut [u8] = &mut chunk_header[22..24];
        chunk_id_buf.copy_from_slice(&chunk_id.to_le_bytes()); // override chunk id

        let mut signature_cache = LruCache::new(SIGNATURE_CACHE_SIZE);
        let result = parse_message::<SignatureType, _>(
            &mut signature_cache,
            payload.freeze(),
            u64::MAX,
            |_| Ok(()),
        );

        if should_succeed {
            // modifying the chunk_id field can still result in invalid leaf hash/signature.
            assert!(matches!(
                result,
                Ok(_)
                    | Err(MessageValidationError::InvalidMerkleProof)
                    | Err(MessageValidationError::InvalidSignature)
            ));
        } else {
            assert!(matches!(
                result,
                Err(MessageValidationError::InvalidChunkId)
            ));
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
        let mut signature_cache = LruCache::new(SIGNATURE_CACHE_SIZE);
        let result = parse_message::<SignatureType, _>(
            &mut signature_cache,
            payload.into(),
            u64::MAX,
            |_| Ok(()),
        );
        assert_eq!(result.err(), Some(MessageValidationError::TooShort))
    }

    #[test]
    fn test_rate_limiting_per_signature() {
        let (key, validators, known_addresses) = validator_set();
        let epoch_validators = validators.view_without(vec![&NodeId::new(key.pubkey())]);

        // 1 second is long enough for governor to be reliable
        let quota = Quota::per_second(NonZero::new(10).unwrap());
        let rate_limiter = RateLimiter::direct(quota);
        let mut signature_cache = LruCache::new(SIGNATURE_CACHE_SIZE);

        const UNIX_TS_MS: u64 = 1000;
        const EPOCH: Epoch = Epoch(1);

        // Create 11 different messages to force signature verification on each call
        for i in 0..11 {
            let message = format!("test message {}", i);
            let app_message: Bytes = message.as_bytes().to_vec().into();
            let messages = build_messages::<SignatureType>(
                &key,
                DEFAULT_SEGMENT_SIZE,
                app_message,
                Redundancy::from_u8(1),
                GroupId::Primary(EPOCH),
                UNIX_TS_MS,
                BuildTarget::Broadcast(epoch_validators.clone().into()),
                &known_addresses,
            );

            let first_message = messages.into_iter().next().unwrap().1;

            let result = parse_message::<SignatureType, _>(
                &mut signature_cache,
                first_message.clone(),
                u64::MAX,
                |_| {
                    if rate_limiter.check().is_ok() {
                        Ok(())
                    } else {
                        Err(MessageValidationError::RateLimited)
                    }
                },
            );

            if i < 10 {
                assert!(
                    result.is_ok(),
                    "parse_message #{} should succeed, got error: {:?}",
                    i + 1,
                    result.err()
                );
            } else {
                // 11th call should fail due to rate limiting
                assert!(
                    matches!(result, Err(MessageValidationError::RateLimited)),
                    "parse_message #11 should be rate limited, got: {:?}",
                    result
                );
            }
        }
    }
}
