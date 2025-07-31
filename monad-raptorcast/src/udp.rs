use std::{
    collections::{BTreeMap, HashMap},
    net::SocketAddr,
    num::NonZero,
    ops::Range,
};

use bitvec::prelude::*;
use bytes::{Bytes, BytesMut};
use itertools::Itertools;
use lru::LruCache;
use monad_crypto::{
    certificate_signature::{
        CertificateKeyPair, CertificateSignaturePubKey, CertificateSignatureRecoverable, PubKey,
    },
    hasher::{Hasher, HasherType},
};
use monad_dataplane::RecvUdpMsg;
use monad_merkle::{MerkleHash, MerkleProof, MerkleTree};
use monad_raptor::{ManagedDecoder, SOURCE_SYMBOLS_MIN};
use monad_types::{Epoch, NodeId};
use rand::seq::SliceRandom;
use tracing::warn;

use crate::{
    util::{
        compute_hash, AppMessageHash, BuildTarget, HexBytes, NodeIdHash, ReBroadcastGroupMap,
        Redundancy,
    },
    SIGNATURE_SIZE,
};

pub const PENDING_MESSAGE_CACHE_SIZE: NonZero<usize> = unsafe { NonZero::new_unchecked(1_000) };

pub const SIGNATURE_CACHE_SIZE: NonZero<usize> = unsafe { NonZero::new_unchecked(10_000) };
pub const RECENTLY_DECODED_CACHE_SIZE: NonZero<usize> = unsafe { NonZero::new_unchecked(10_000) };

// We assume an MTU of at least 1280 (the IPv6 minimum MTU), which for the maximum Merkle tree
// depth of 9 gives a symbol size of 960 bytes, which we will use as the minimum chunk length for
// received packets, and we'll drop received chunks that are smaller than this to mitigate attacks
// involving a peer sending us a message as a very large set of very small chunks.
const MIN_CHUNK_LENGTH: usize = 960;

// Drop a message to be transmitted if it would lead to more than this number of packets
// to be transmitted.  This can happen in Broadcast mode when the message is large or
// if we have many peers to transmit the message to.
const MAX_NUM_PACKETS: usize = 65535;

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
const MIN_MERKLE_TREE_DEPTH: u8 = 1;
const MAX_MERKLE_TREE_DEPTH: u8 = 9;

struct DecoderState {
    decoder: ManagedDecoder,
    recipient_chunks: BTreeMap<NodeIdHash, usize>,
    encoded_symbol_capacity: usize,
    seen_esis: BitVec<usize, Lsb0>,
}

struct RecentlyDecodedState {
    symbol_len: usize,
    encoded_symbol_capacity: usize,
    seen_esis: BitVec<usize, Lsb0>,
    excess_chunk_count: usize,
}

pub(crate) struct UdpState<ST: CertificateSignatureRecoverable> {
    self_id: NodeId<CertificateSignaturePubKey<ST>>,
    max_age_ms: u64,

    // TODO add a cap on max number of chunks that will be forwarded per message? so that a DOS
    // can't be induced by spamming broadcast chunks to any given node
    // TODO we also need to cap the max number chunks that are decoded - because an adversary could
    // generate a bunch of linearly dependent chunks and cause unbounded memory usage.
    // TODO strong bound on max amount of memory used per decoder?
    // TODO make eviction more sophisticated than LRU - should look at unix_ts_ms as well
    pending_message_cache: LruCache<MessageCacheKey<CertificateSignaturePubKey<ST>>, DecoderState>,
    signature_cache:
        LruCache<[u8; HEADER_LEN as usize + 20], NodeId<CertificateSignaturePubKey<ST>>>,
    /// Value in this map represents the # of excess chunks received for a successfully decoded msg
    recently_decoded_cache:
        LruCache<MessageCacheKey<CertificateSignaturePubKey<ST>>, RecentlyDecodedState>,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
struct MessageCacheKey<PT>
where
    PT: PubKey,
{
    unix_ts_ms: u64,
    author: NodeId<PT>,
    app_message_hash: AppMessageHash,
    app_message_len: usize,
}

impl<ST: CertificateSignatureRecoverable> UdpState<ST> {
    pub fn new(self_id: NodeId<CertificateSignaturePubKey<ST>>, max_age_ms: u64) -> Self {
        Self {
            self_id,
            max_age_ms,

            pending_message_cache: LruCache::unbounded(),
            signature_cache: LruCache::new(SIGNATURE_CACHE_SIZE),
            recently_decoded_cache: LruCache::new(RECENTLY_DECODED_CACHE_SIZE),
        }
    }

    /// Given a RecvMsg, emits all decoded messages while rebroadcasting as necessary
    #[tracing::instrument(level = "debug", name = "udp_handle_message", skip_all)]
    pub fn handle_message(
        &mut self,
        group_map: &ReBroadcastGroupMap<ST>,
        rebroadcast: impl FnMut(Vec<NodeId<CertificateSignaturePubKey<ST>>>, Bytes, u16),
        forward: impl FnMut(Bytes, u16),
        message: RecvUdpMsg,
    ) -> Vec<(NodeId<CertificateSignaturePubKey<ST>>, Bytes)> {
        let self_id = self.self_id;
        let self_hash = compute_hash(&self_id);

        let mut broadcast_batcher =
            BroadcastBatcher::new(self_id, rebroadcast, &message.payload, message.stride);
        // batch packets forwarding to full nodes
        let mut full_node_forward_batcher =
            ForwardBatcher::new(self_id, forward, &message.payload, message.stride);

        let mut messages = Vec::new(); // The return result; decoded messages

        for payload_start_idx in (0..message.payload.len()).step_by(message.stride.into()) {
            // scoped variables are dropped in reverse order of declaration.
            // when *batch_guard is dropped, packets can get flushed
            //
            // Declaring (validator) batch guard last to give rebroadcast
            // priorities to validators
            let mut full_node_forward_batch_guard = full_node_forward_batcher.create_flush_guard();
            let mut batch_guard = broadcast_batcher.create_flush_guard();

            let payload_end_idx =
                (payload_start_idx + usize::from(message.stride)).min(message.payload.len());
            let payload = message.payload.slice(payload_start_idx..payload_end_idx);
            // "message" here means a raptor-casted chunk (AKA r10 symbol), not the whole final message (proposal)
            let parsed_message = match parse_message::<ST>(
                &mut self.signature_cache,
                payload,
                self.max_age_ms,
            ) {
                Ok(message) => message,
                Err(err) => {
                    tracing::debug!(src_addr = ?message.src_addr, ?err, "unable to parse message");
                    continue;
                }
            };

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
            if parsed_message.broadcast {
                if !group_map.check_source(Epoch(parsed_message.epoch), &parsed_message.author) {
                    tracing::debug!(
                        src_addr = ?message.src_addr,
                        author =? parsed_message.author,
                        epoch =? parsed_message.epoch,
                        "not in raptorcast group"
                    );
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

            let encoding_symbol_id = parsed_message.chunk_id.into();

            tracing::trace!(
                src_addr = ?message.src_addr,
                app_message_len = ?parsed_message.app_message_len,
                self_id =? self.self_id,
                author =? parsed_message.author,
                unix_ts_ms = parsed_message.unix_ts_ms,
                app_message_hash =? parsed_message.app_message_hash,
                encoding_symbol_id,
                "received encoded symbol"
            );

            let app_message_len: usize = parsed_message.app_message_len.try_into().unwrap();
            let key = MessageCacheKey {
                unix_ts_ms: parsed_message.unix_ts_ms,
                author: parsed_message.author,
                app_message_hash: parsed_message.app_message_hash,
                app_message_len,
            };

            let mut try_rebroadcast_symbol = || {
                // rebroadcast raptorcast chunks if necessary
                if parsed_message.broadcast {
                    if self_hash == parsed_message.recipient_hash {
                        let maybe_targets = group_map.iterate_rebroadcast_peers(
                            Epoch(parsed_message.epoch),
                            &parsed_message.author,
                        );
                        if let Some(targets) = maybe_targets {
                            batch_guard.queue_broadcast(
                                payload_start_idx,
                                payload_end_idx,
                                &parsed_message.author,
                                || targets.cloned().collect(),
                            )
                        }
                    }

                    // forward all broadcast packets to full nodes
                    full_node_forward_batch_guard.queue_forward(payload_start_idx, payload_end_idx);
                }
            };

            if let Some(recently_decoded_state) = self.recently_decoded_cache.get_mut(&key) {
                if is_valid_symbol::<ST>(
                    self.self_id,
                    &parsed_message,
                    recently_decoded_state.symbol_len,
                    recently_decoded_state.encoded_symbol_capacity,
                    &recently_decoded_state.seen_esis,
                ) {
                    // already decoded but valid symbol
                    recently_decoded_state
                        .seen_esis
                        .set(encoding_symbol_id, true);
                    recently_decoded_state.excess_chunk_count += 1;

                    try_rebroadcast_symbol();
                }

                continue;
            }

            let decoder_state_result =
                self.pending_message_cache.try_get_or_insert_mut(key, || {
                    let symbol_len = parsed_message.chunk.len();

                    // symbol_len is always greater than zero, so this division is safe
                    let num_source_symbols =
                        app_message_len.div_ceil(symbol_len).max(SOURCE_SYMBOLS_MIN);
                    let encoded_symbol_capacity = MAX_REDUNDANCY
                        .scale(num_source_symbols)
                        .expect("redundancy-scaled num_source_symbols doesn't fit in usize");

                    ManagedDecoder::new(num_source_symbols, encoded_symbol_capacity, symbol_len)
                        .map(|decoder| DecoderState {
                            decoder,
                            recipient_chunks: BTreeMap::new(),
                            encoded_symbol_capacity,
                            seen_esis: bitvec![usize, Lsb0; 0; encoded_symbol_capacity],
                        })
                });

            let decoder_state = match decoder_state_result {
                Ok(decoder_state) => decoder_state,
                Err(err) => {
                    tracing::warn!(?err, "unable to create DecoderState, dropping message");
                    continue;
                }
            };

            if !is_valid_symbol::<ST>(
                self.self_id,
                &parsed_message,
                decoder_state.decoder.symbol_len(),
                decoder_state.encoded_symbol_capacity,
                &decoder_state.seen_esis,
            ) {
                // invalid symbol
                continue;
            }
            decoder_state.seen_esis.set(encoding_symbol_id, true);

            try_rebroadcast_symbol();

            // can we assert!(!decoder_state.decoder.decoding_done()) ?

            decoder_state
                .decoder
                .received_encoded_symbol(&parsed_message.chunk, encoding_symbol_id);

            *decoder_state
                .recipient_chunks
                .entry(parsed_message.recipient_hash)
                .or_insert(0) += 1;

            if decoder_state.decoder.try_decode() {
                let Some(mut decoded) = decoder_state.decoder.reconstruct_source_data() else {
                    tracing::error!("failed to reconstruct source data");
                    continue;
                };

                if app_message_len > 10_000 {
                    tracing::debug!(
                        ?self_id,
                        author =? parsed_message.author,
                        unix_ts_ms = parsed_message.unix_ts_ms,
                        app_message_hash =? parsed_message.app_message_hash,
                        encoding_symbol_id,
                        app_message_len,
                        "reconstructed large message"
                    );
                }

                decoded.truncate(app_message_len);
                let decoded = Bytes::from(decoded);

                // successfully decoded, so pop out from pending_messages
                let decoded_state = self
                    .pending_message_cache
                    .pop(&key)
                    .expect("decoder exists");

                let decoded_message_hash: AppMessageHash = HexBytes({
                    let mut hasher = HasherType::new();
                    hasher.update(&decoded);
                    hasher.hash().0[..20].try_into().unwrap()
                });
                if decoded_message_hash != key.app_message_hash {
                    tracing::error!(
                        ?self_id,
                        author =? key.author,
                        expected_hash =? key.app_message_hash,
                        actual_hash =? decoded_message_hash,
                        "unexpected app message hash. dropping message"
                    );
                    continue;
                }

                self.recently_decoded_cache.push(
                    key,
                    RecentlyDecodedState {
                        symbol_len: decoded_state.decoder.symbol_len(),
                        encoded_symbol_capacity: decoded_state.encoded_symbol_capacity,
                        seen_esis: decoded_state.seen_esis,
                        excess_chunk_count: 0,
                    },
                );

                messages.push((parsed_message.author, decoded));
            }
        }

        while self.pending_message_cache.len() > PENDING_MESSAGE_CACHE_SIZE.get() {
            let (key, decoder_state) = self.pending_message_cache.pop_lru().expect("nonempty");
            tracing::debug!(
                num_source_symbols = decoder_state.decoder.num_source_symbols(),
                num_encoded_symbols_received = decoder_state.decoder.num_encoded_symbols_received(),
                inactivation_symbol_threshold =
                    decoder_state.decoder.inactivation_symbol_threshold(),
                recipient_chunks =? decoder_state.recipient_chunks,
                ?key,
                "dropped unfinished ManagedDecoder"
            )
        }

        messages
    }
}

fn is_valid_symbol<ST>(
    self_id: NodeId<CertificateSignaturePubKey<ST>>,
    parsed_message: &ValidatedMessage<CertificateSignaturePubKey<ST>>,
    symbol_len: usize,
    encoded_symbol_capacity: usize,
    seen_esis: &BitVec,
) -> bool
where
    ST: CertificateSignatureRecoverable,
{
    let encoding_symbol_id: usize = parsed_message.chunk_id.into();

    if symbol_len != parsed_message.chunk.len() {
        // invalid symbol length
        tracing::warn!(
            ?self_id,
            author =? parsed_message.author,
            unix_ts_ms = parsed_message.unix_ts_ms,
            app_message_hash =? parsed_message.app_message_hash,
            encoding_symbol_id,
            expected_len = symbol_len,
            received_len = parsed_message.chunk.len(),
            "received invalid symbol len"
        );
        return false;
    }

    if encoding_symbol_id >= encoded_symbol_capacity {
        // invalid symbol id
        tracing::warn!(
            ?self_id,
            author =? parsed_message.author,
            unix_ts_ms = parsed_message.unix_ts_ms,
            app_message_hash =? parsed_message.app_message_hash,
            encoded_symbol_capacity,
            encoding_symbol_id,
            "received invalid symbol id"
        );
        return false;
    }

    if seen_esis[encoding_symbol_id] {
        // duplicate symbol
        tracing::trace!(
            ?self_id,
            author =? parsed_message.author,
            unix_ts_ms = parsed_message.unix_ts_ms,
            app_message_hash =? parsed_message.app_message_hash,
            encoding_symbol_id,
            "received duplicate symbol"
        );
        return false;
    }

    true
}

/// Stuff to include:
/// - 65 bytes => Signature of sender over hash(rest of message up to merkle proof, concatenated with merkle root)
/// - 2 bytes => Version: bumped on protocol updates
/// - 1 bit => broadcast or not
/// - 7 bits => Merkle tree depth
/// - 8 bytes (u64) => Epoch #
/// - 8 bytes (u64) => Unix timestamp in milliseconds
/// - 20 bytes => first 20 bytes of hash of AppMessage
///   - this isn't technically necessary if payload_len is small enough to fit in 1 chunk, but keep
///     for simplicity
/// - 4 bytes (u32) => Serialized AppMessage length (bytes)
/// - 20 bytes * (merkle_tree_depth - 1) => merkle proof (leaves include everything that follows,
///   eg hash(chunk_recipient + chunk_byte_offset + chunk_len + payload))
///
/// - 20 bytes => first 20 bytes of hash of chunk's first hop recipient
///   - we set this even if broadcast bit is not set so that it's known if a message was intended
///     to be sent to self
/// - 1 byte => Chunk's merkle leaf idx
/// - 1 byte => reserved
/// - 2 bytes (u16) => This chunk's id
/// - rest => data
///
//
//
//
// pub struct M {
//     signature: [u8; 65],
//     version: u16,
//     broadcast: bool,
//     merkle_tree_depth: u8,
//     epoch: u64,
//     unix_ts_ms: u64,
//     app_message_id: [u8; 20],
//     app_message_len: u32,
//
//     merkle_proof: Vec<[u8; 20]>,
//
//     chunk_recipient: [u8; 20],
//     chunk_merkle_leaf_idx: u8,
//     reserved: u8,
//     chunk_id: u16,
//
//     data: Bytes,
// }
pub const HEADER_LEN: u16 = SIGNATURE_SIZE as u16 // Sender signature
            + 2  // Version
            + 1  // Broadcast bit, 7 bits for Merkle Tree Depth
            + 8  // Epoch #
            + 8  // Unix timestamp
            + 20 // AppMessage hash
            + 4; // AppMessage length
const CHUNK_HEADER_LEN: u16 = 20 // Chunk recipient hash
            + 1  // Chunk's merkle leaf idx
            + 1  // reserved
            + 2; // Chunk idx

pub fn build_messages<ST>(
    key: &ST::KeyPairType,
    segment_size: u16, // Each chunk in the returned Vec (Bytes element of the tuple) will be limited to this size
    app_message: Bytes, // This is the actual message that gets raptor-10 encoded and split into UDP chunks
    redundancy: Redundancy,
    epoch_no: u64,
    unix_ts_ms: u64,
    build_target: BuildTarget<ST>,
    known_addresses: &HashMap<NodeId<CertificateSignaturePubKey<ST>>, SocketAddr>,
) -> Vec<(SocketAddr, Bytes)>
where
    ST: CertificateSignatureRecoverable,
{
    let app_message_len: u32 = app_message.len().try_into().expect("message too big");

    build_messages_with_length(
        key,
        segment_size,
        app_message,
        app_message_len,
        redundancy,
        epoch_no,
        unix_ts_ms,
        build_target,
        known_addresses,
    )
}

// This should be called with app_message.len() == app_message_len, but we allow the caller
// to specify a different app_message_len to allow one of the unit tests to build an invalid
// (oversized) message that build_messages() would normally not allow you to build, in order
// to verify that the RaptorCast receive path doesn't crash when it receives such a message,
// as previous versions of the RaptorCast receive path would indeed crash when receiving
// such a message.
pub fn build_messages_with_length<ST>(
    key: &ST::KeyPairType,
    segment_size: u16,
    app_message: Bytes,
    app_message_len: u32,
    redundancy: Redundancy,
    epoch_no: u64,
    unix_ts_ms: u64,
    build_target: BuildTarget<ST>,
    known_addresses: &HashMap<NodeId<CertificateSignaturePubKey<ST>>, SocketAddr>,
) -> Vec<(SocketAddr, Bytes)>
where
    ST: CertificateSignatureRecoverable,
{
    if app_message_len == 0 {
        tracing::warn!("build_messages_with_length() called with app_message_len = 0");
        return Vec::new();
    }

    if redundancy == Redundancy::ZERO {
        tracing::error!("build_messages_with_length() called with redundancy = 0");
        return Vec::new();
    }

    // body_size is the amount of space available for payload+proof in a single
    // UDP datagram. Our raptorcast encoding needs around 108 + 24 = 132 bytes
    // in each datagram. Typically:
    //      body_size = 1452 - (108 + 24) = 1320
    let body_size = segment_size
        .checked_sub(HEADER_LEN + CHUNK_HEADER_LEN)
        .expect("segment_size too small");

    let is_broadcast = matches!(
        build_target,
        BuildTarget::Broadcast(_) | BuildTarget::Raptorcast(_) | BuildTarget::FullNodeRaptorCast(_)
    );

    let self_id = NodeId::new(key.pubkey());

    // TODO make this more sophisticated
    let tree_depth: u8 = 6; // corresponds to 32 chunks (2^(h-1))
    assert!(tree_depth >= MIN_MERKLE_TREE_DEPTH);
    assert!(tree_depth <= MAX_MERKLE_TREE_DEPTH);

    let chunks_per_merkle_batch: usize = 2_usize // = 32
        .checked_pow(u32::from(tree_depth) - 1)
        .expect("tree depth too big");
    let proof_size: u16 = 20 * (u16::from(tree_depth) - 1); // = 100

    // data_size is the amount of space available for raw payload (app_message)
    // in a single UDP datagram. Typically:
    //      data_size = 1452 - (108 + 24) - 100 = 1220
    let data_size = body_size.checked_sub(proof_size).expect("proof too big");
    let is_raptor_broadcast = matches!(
        build_target,
        BuildTarget::Raptorcast(_) | BuildTarget::FullNodeRaptorCast(_)
    );

    // Determine how many UDP datagrams (packets) we need to send out. Each
    // datagram can only effectively transport `data_size` (~1220) bytes out of
    // a total of `app_message_len` bytes (~18% total overhead).
    let num_packets: usize = {
        let mut num_packets: usize = (app_message_len as usize)
            .div_ceil(usize::from(data_size))
            .max(SOURCE_SYMBOLS_MIN);
        // amplify by redundancy factor
        num_packets = redundancy
            .scale(num_packets)
            .expect("redundancy-scaled num_packets doesn't fit in usize");

        if let BuildTarget::Broadcast(epoch_validators) = &build_target {
            num_packets = num_packets
                .checked_mul(epoch_validators.view().len())
                .expect("num_packets doesn't fit in usize")
        }

        if num_packets > MAX_NUM_PACKETS {
            tracing::warn!(
                ?build_target,
                ?known_addresses,
                num_packets,
                MAX_NUM_PACKETS,
                "exceeded maximum number of packets in a message, dropping message",
            );
            return Vec::new();
        }

        num_packets
    };

    // Create a long flat message, concatenating the (future) UDP bodies of all
    // datagrams. This includes everything except Ethernet, IP and UDP headers.
    let mut message = BytesMut::zeroed(segment_size as usize * num_packets);
    let app_message_hash: AppMessageHash = HexBytes({
        let mut hasher = HasherType::new();
        hasher.update(&app_message);
        hasher.hash().0[..20].try_into().unwrap()
    });

    // Each chunk_data[0..num_packets-1] is a reference to a slice of bytes
    // from the long flat `message` above. Each slice excludes the UDP buffer
    // span where we'd write the raptorcast header and proof.
    // Each chunk_data[ii] is a tuple (chunk_index, slice), where the first 20
    // bytes of the slice contains the hash of the chunk's destination node id.
    //                  chunk_datas[0]                  chunk_datas[1]                  chunk_datas[2]
    // | HEADER, proof, _______________| HEADER, proof, _______________| HEADER, proof, _______________|
    // |...........................................message.............................................|
    // |.........MTU UDP DATAGRAM......|
    let mut chunk_datas = message
        .chunks_mut(segment_size.into())
        .map(|chunk| (None, &mut chunk[(HEADER_LEN + proof_size).into()..]))
        .collect_vec();
    assert_eq!(chunk_datas.len(), num_packets);

    // the GSO-aware indices into `message`
    let mut outbound_gso_idx: Vec<(SocketAddr, Range<usize>)> = Vec::new();
    let mut full_node_gso_idx: Vec<(SocketAddr, Range<usize>)> = Vec::new();
    // populate chunk_recipient and outbound_gso_idx
    match build_target {
        BuildTarget::PointToPoint(to) => {
            let Some(addr) = known_addresses.get(to) else {
                tracing::warn!(
                    ?to,
                    "RaptorCast build_message PointToPoint not sending message, address unknown"
                );
                return Vec::new();
            };
            outbound_gso_idx.push((*addr, 0..segment_size as usize * num_packets));
            for (chunk_idx, (chunk_symbol_id, chunk_data)) in chunk_datas.iter_mut().enumerate() {
                // populate chunk_recipient
                chunk_data[0..20].copy_from_slice(&compute_hash(to).0);
                *chunk_symbol_id = Some(chunk_idx as u16);
            }
        }
        BuildTarget::Broadcast(epoch_validators) => {
            assert!(is_broadcast && !is_raptor_broadcast);
            let total_validators = epoch_validators.view().len();
            let mut running_validator_count = 0;
            tracing::debug!(
                ?self_id,
                unix_ts_ms,
                app_message_len,
                ?redundancy,
                data_size,
                num_packets,
                ?app_message_hash,
                "RaptorCast Broadcast v2v message"
            );
            for (node_id, _validator) in epoch_validators.view().iter() {
                let start_idx: usize = num_packets * running_validator_count / total_validators;
                running_validator_count += 1;
                let end_idx: usize = num_packets * running_validator_count / total_validators;

                if start_idx == end_idx {
                    continue;
                }
                if let Some(addr) = known_addresses.get(node_id) {
                    outbound_gso_idx.push((
                        *addr,
                        start_idx * segment_size as usize..end_idx * segment_size as usize,
                    ));
                } else {
                    tracing::warn!(
                        ?node_id,
                        "RaptorCast build_message Broadcast not sending message, address unknown"
                    )
                }
                for (chunk_idx, (chunk_symbol_id, chunk_data)) in
                    chunk_datas[start_idx..end_idx].iter_mut().enumerate()
                {
                    // populate chunk_recipient
                    chunk_data[0..20].copy_from_slice(&compute_hash(node_id).0);
                    *chunk_symbol_id = Some(chunk_idx as u16);
                }
            }
        }
        BuildTarget::Raptorcast((epoch_validators, full_nodes_view)) => {
            assert!(is_broadcast && is_raptor_broadcast);

            tracing::trace!(
                ?self_id,
                unix_ts_ms,
                app_message_len,
                ?redundancy,
                data_size,
                num_packets,
                ?app_message_hash,
                "RaptorCast v2v message"
            );

            assert!(!epoch_validators.view().is_empty() || !full_nodes_view.view().is_empty());

            if epoch_validators.view().is_empty() {
                // generate chunks and self-assign if we're the only validator
                // and have downstream full nodes
                let self_node_id_hash = compute_hash(&NodeId::new(key.pubkey()));
                let mut chunk_idx = 0_u16;
                for (chunk_symbol_id, chunk_data) in chunk_datas.iter_mut() {
                    // use self (only validator) as chunk recipient
                    chunk_data[0..20].copy_from_slice(&self_node_id_hash.0);
                    *chunk_symbol_id = Some(chunk_idx);
                    chunk_idx += 1;
                }
            } else {
                // generate chunks if epoch validators is not empty
                // FIXME should self be included in total_stake?
                let total_stake: u64 = epoch_validators
                    .view()
                    .values()
                    .map(|validator| validator.stake.0)
                    .sum();
                let mut running_stake = 0;
                let mut chunk_idx = 0_u16;
                let mut nodes: Vec<_> = epoch_validators.view().iter().collect();
                // Group shuffling so chunks for small proposals aren't always assigned
                // to the same nodes, until researchers come up with something better.
                nodes.shuffle(&mut rand::thread_rng());
                for (node_id, validator) in &nodes {
                    let start_idx: usize =
                        (num_packets as u64 * running_stake / total_stake) as usize;
                    running_stake += validator.stake.0;
                    let end_idx: usize =
                        (num_packets as u64 * running_stake / total_stake) as usize;

                    if start_idx == end_idx {
                        continue;
                    }
                    if let Some(addr) = known_addresses.get(node_id) {
                        outbound_gso_idx.push((
                            *addr,
                            start_idx * segment_size as usize..end_idx * segment_size as usize,
                        ));
                    } else {
                        tracing::warn!(?node_id, "RaptorCast build_message Raptorcast not sending message, address unknown")
                    }
                    for (chunk_symbol_id, chunk_data) in chunk_datas[start_idx..end_idx].iter_mut()
                    {
                        // populate chunk_recipient
                        chunk_data[0..20].copy_from_slice(&compute_hash(node_id).0);
                        *chunk_symbol_id = Some(chunk_idx);
                        chunk_idx += 1;
                    }
                }
            }

            // Dedicated full nodes get a copy of all raptorcast chunks.
            // When our node is not the leader, chunks are forwarded in the handle_message path.
            // When our node is the leader generating chunks, we're forwarding all chunks here.
            for node_id in full_nodes_view.view() {
                // TODO: assign a sub-segment of range to each full node
                if let Some(addr) = known_addresses.get(node_id) {
                    full_node_gso_idx.push((*addr, 0..(num_packets * segment_size as usize)));
                } else {
                    tracing::warn!(
                        ?node_id,
                        "not sending message to full node, address unknown"
                    );
                }
            }
        }
        BuildTarget::FullNodeRaptorCast(group) => {
            assert!(is_broadcast && is_raptor_broadcast);

            tracing::trace!(
                ?self_id,
                unix_ts_ms,
                app_message_len,
                ?redundancy,
                data_size,
                num_packets,
                ?app_message_hash,
                "RaptorCast v2fn message"
            );

            let total_peers = group.size_excl_self();
            let mut pp = 0;
            // Group shuffling so chunks for small proposals aren't always assigned
            // to the same nodes, until researchers come up with something better.
            for node_id in group.iter_skip_self_and_author(&self_id, rand::random::<usize>()) {
                let start_idx: usize = num_packets * pp / total_peers;
                pp += 1;
                let end_idx: usize = num_packets * pp / total_peers;

                if start_idx == end_idx {
                    continue;
                }
                if let Some(addr) = known_addresses.get(node_id) {
                    outbound_gso_idx.push((
                        *addr,
                        start_idx * segment_size as usize..end_idx * segment_size as usize,
                    ));
                } else {
                    tracing::warn!(?node_id, "not sending v2fn message, address unknown")
                }
                for (chunk_idx, (chunk_symbol_id, chunk_data)) in
                    chunk_datas[start_idx..end_idx].iter_mut().enumerate()
                {
                    // populate chunk_recipient
                    chunk_data[0..20].copy_from_slice(&compute_hash(node_id).0);
                    *chunk_symbol_id = Some(chunk_idx as u16);
                }
            }
        }
    };

    // In practice, a "symbol" is a UDP datagram payload of some 1220 bytes
    let encoder = match monad_raptor::Encoder::new(&app_message, usize::from(data_size)) {
        Ok(encoder) => encoder,
        Err(err) => {
            // TODO: signal this error to the caller
            tracing::warn!(?err, "unable to create Encoder, dropping message");
            return Vec::new();
        }
    };

    // populates the following chunk-specific stuff
    // - chunk_id: u16
    // - chunk_payload
    for (maybe_chunk_id, chunk_data) in chunk_datas.iter_mut() {
        let chunk_id = maybe_chunk_id.expect("generated chunk was not assigned an id");
        let chunk_len: u16 = data_size;

        let cursor = chunk_data;
        let (_cursor_chunk_recipient, cursor) = cursor.split_at_mut(20);
        let (_cursor_chunk_merkle_leaf_idx, cursor) = cursor.split_at_mut(1);
        let (_cursor_chunk_reserved, cursor) = cursor.split_at_mut(1);
        let (cursor_chunk_id, cursor) = cursor.split_at_mut(2);
        cursor_chunk_id.copy_from_slice(&chunk_id.to_le_bytes());
        let (cursor_chunk_payload, _cursor) = cursor.split_at_mut(chunk_len.into());

        // for BuildTarget::Broadcast, we will be encoding each chunk_id once per recipient
        //
        // we could cache these as an optimization, but probably doesn't make a big difference in
        // practice, because we're generally using BuildTarget::Broadcast for small messages.
        //
        // can revisit this later
        encoder.encode_symbol(
            &mut cursor_chunk_payload[..chunk_len.into()],
            chunk_id.into(),
        );
    }

    // At this point, everything BELOW chunk_merkle_leaf_idx is populated
    // populate merkle trees/roots/leaf_idx + signatures (cached)
    let version: u16 = 0;
    let epoch_no: u64 = epoch_no;
    let unix_ts_ms: u64 = unix_ts_ms;
    message
        // .par_chunks_mut(segment_size as usize * chunks_per_merkle_batch)
        .chunks_mut(segment_size as usize * chunks_per_merkle_batch)
        .for_each(|merkle_batch| {
            let mut merkle_batch = merkle_batch.chunks_mut(segment_size as usize).collect_vec();
            let merkle_leaves = merkle_batch
                .iter_mut()
                .enumerate()
                .map(|(chunk_idx, chunk)| {
                    let chunk_payload = &mut chunk[(HEADER_LEN + proof_size).into()..];
                    assert_eq!(
                        chunk_payload.len(),
                        CHUNK_HEADER_LEN as usize + data_size as usize
                    );
                    // populate merkle_leaf_idx
                    chunk_payload[20] = chunk_idx.try_into().expect("chunk idx doesn't fit in u8");

                    let mut hasher = HasherType::new();
                    hasher.update(chunk_payload);
                    hasher.hash()
                })
                .collect_vec();
            let merkle_tree = MerkleTree::new_with_depth(&merkle_leaves, tree_depth);
            let mut header_with_root = {
                let mut data = [0_u8; HEADER_LEN as usize + 20];
                let cursor = &mut data;
                let (_cursor_signature, cursor) = cursor.split_at_mut(SIGNATURE_SIZE);
                let (cursor_version, cursor) = cursor.split_at_mut(2);
                cursor_version.copy_from_slice(&version.to_le_bytes());
                let (cursor_broadcast_merkle_depth, cursor) = cursor.split_at_mut(1);
                cursor_broadcast_merkle_depth[0] = ((is_raptor_broadcast as u8) << 7) | tree_depth;
                let (cursor_epoch_no, cursor) = cursor.split_at_mut(8);
                cursor_epoch_no.copy_from_slice(&epoch_no.to_le_bytes());
                let (cursor_unix_ts_ms, cursor) = cursor.split_at_mut(8);
                cursor_unix_ts_ms.copy_from_slice(&unix_ts_ms.to_le_bytes());
                let (cursor_app_message_hash, cursor) = cursor.split_at_mut(20);
                cursor_app_message_hash.copy_from_slice(&app_message_hash.0);
                let (cursor_app_message_len, cursor) = cursor.split_at_mut(4);
                cursor_app_message_len.copy_from_slice(&app_message_len.to_le_bytes());

                cursor.copy_from_slice(merkle_tree.root());
                // 65 // Sender signature
                // 2  // Version
                // 1  // Broadcast bit, 7 bits for Merkle Tree Depth
                // 8  // Epoch #
                // 8  // Unix timestamp
                // 20 // AppMessage hash
                // 4  // AppMessage length
                // --
                // 20 // Merkle root

                data
            };
            let signature = ST::sign(&header_with_root[SIGNATURE_SIZE..], key).serialize();
            assert_eq!(signature.len(), SIGNATURE_SIZE);
            header_with_root[..SIGNATURE_SIZE].copy_from_slice(&signature);
            let header = &header_with_root[..HEADER_LEN as usize];
            for (leaf_idx, chunk) in merkle_batch.into_iter().enumerate() {
                chunk[..HEADER_LEN as usize].copy_from_slice(header);
                for (proof_idx, proof) in merkle_tree
                    .proof(leaf_idx as u8)
                    .siblings()
                    .iter()
                    .enumerate()
                {
                    let offset = HEADER_LEN as usize + 20 * proof_idx;
                    chunk[offset..offset + 20].copy_from_slice(proof);
                }
            }
        });

    let message = message.freeze();

    // FIXME: this doesn't guarantee priority to validators, as everything can
    // be in one sendmmsg
    let full_node_chunks = full_node_gso_idx
        .into_iter()
        .map(|(addr, range)| (addr, message.slice(range)));

    outbound_gso_idx
        .into_iter()
        .map(|(addr, range)| (addr, message.slice(range)))
        .chain(full_node_chunks)
        .collect()
}

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
    pub epoch: u64,
    pub unix_ts_ms: u64,
    pub app_message_hash: AppMessageHash,
    pub app_message_len: u32,
    pub broadcast: bool,
    pub recipient_hash: NodeIdHash, // if this matches our node_id, then we need to re-broadcast RaptorCast chunks
    pub chunk_id: u16,
    pub chunk: Bytes, // raptor-coded portion
}

#[derive(Debug)]
pub enum MessageValidationError {
    UnknownVersion,
    TooShort,
    InvalidSignature,
    InvalidTreeDepth,
    InvalidMerkleProof,
    InvalidTimestamp {
        timestamp: u64,
        max: u64,
        delta: i64,
    },
}

/// - 65 bytes => Signature of sender over hash(rest of message up to merkle proof, concatenated with merkle root)
/// - 2 bytes => Version: bumped on protocol updates
/// - 1 bit => broadcast or not
/// - 7 bits => Merkle tree depth
/// - 8 bytes (u64) => Epoch #
/// - 8 bytes (u64) => Unix timestamp
/// - 20 bytes => first 20 bytes of hash of AppMessage
///   - this isn't technically necessary if payload_len is small enough to fit in 1 chunk, but keep
///     for simplicity
/// - 4 bytes (u32) => Serialized AppMessage length (bytes)
/// - 20 bytes * (merkle_tree_depth - 1) => merkle proof (leaves include everything that follows,
///   eg hash(chunk_recipient + chunk_byte_offset + chunk_len + payload))
///
/// - 20 bytes => first 20 bytes of hash of chunk's first hop recipient
///   - we set this even if broadcast bit is not set so that it's known if a message was intended
///     to be sent to self
/// - 1 byte => Chunk's merkle leaf idx
/// - 1 byte => reserved
/// - 2 bytes (u16) => This chunk's id
/// - rest => data
pub fn parse_message<ST>(
    signature_cache: &mut LruCache<
        [u8; HEADER_LEN as usize + 20],
        NodeId<CertificateSignaturePubKey<ST>>,
    >,
    message: Bytes,
    max_age_ms: u64,
) -> Result<ValidatedMessage<CertificateSignaturePubKey<ST>>, MessageValidationError>
where
    ST: CertificateSignatureRecoverable,
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
    let signature =
        ST::deserialize(&cursor_signature).map_err(|_| MessageValidationError::InvalidSignature)?;

    let cursor_version = split_off(2)?;
    let version = u16::from_le_bytes(cursor_version.as_ref().try_into().expect("u16 is 2 bytes"));
    if version != 0 {
        return Err(MessageValidationError::UnknownVersion);
    }

    let cursor_broadcast_tree_depth = split_off(1)?[0];
    let broadcast = (cursor_broadcast_tree_depth >> 7) != 0;
    let tree_depth = cursor_broadcast_tree_depth & !(1 << 7);

    if !(MIN_MERKLE_TREE_DEPTH..=MAX_MERKLE_TREE_DEPTH).contains(&tree_depth) {
        return Err(MessageValidationError::InvalidTreeDepth);
    }

    let cursor_epoch = split_off(8)?;
    let epoch = u64::from_le_bytes(cursor_epoch.as_ref().try_into().expect("u64 is 8 bytes"));

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
    );

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
    if cursor_payload.is_empty() {
        // handle the degenerate case
        return Err(MessageValidationError::TooShort);
    }

    let leaf_hash = {
        let mut hasher = HasherType::new();
        hasher.update(
            &message[HEADER_LEN as usize + proof_size as usize..
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
    let mut signed_over = [0_u8; HEADER_LEN as usize + 20];
    // TODO can avoid this copy if necessary
    signed_over[..HEADER_LEN as usize].copy_from_slice(&message[..HEADER_LEN as usize]);
    signed_over[HEADER_LEN as usize..].copy_from_slice(&root);

    let author = *signature_cache.try_get_or_insert(signed_over, || {
        let author = signature
            .recover_pubkey(&signed_over[SIGNATURE_SIZE..])
            .map_err(|_| MessageValidationError::InvalidSignature)?;
        Ok(NodeId::new(author))
    })?;

    Ok(ValidatedMessage {
        message,

        author,
        epoch,
        unix_ts_ms,
        app_message_hash,
        app_message_len,
        broadcast,
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
struct ForwardBatch {
    start_idx: usize,
    end_idx: usize,
}
pub(crate) struct ForwardBatcher<'a, F, PT>
where
    F: FnMut(Bytes, u16),
    PT: PubKey,
{
    self_id: NodeId<PT>,
    forward: F,
    message: &'a Bytes,
    stride: u16,

    batch: Option<ForwardBatch>,
}
impl<F, PT> Drop for ForwardBatcher<'_, F, PT>
where
    F: FnMut(Bytes, u16),
    PT: PubKey,
{
    fn drop(&mut self) {
        self.flush()
    }
}
impl<'a, F, PT> ForwardBatcher<'a, F, PT>
where
    F: FnMut(Bytes, u16),
    PT: PubKey,
{
    pub fn new(self_id: NodeId<PT>, forward: F, message: &'a Bytes, stride: u16) -> Self {
        Self {
            self_id,
            forward,
            message,
            stride,
            batch: None,
        }
    }

    pub fn create_flush_guard<'g>(&'g mut self) -> ForwardBatcherGuard<'a, 'g, F, PT>
    where
        'a: 'g,
    {
        ForwardBatcherGuard {
            batcher: self,
            flush_batch: true,
        }
    }

    fn flush(&mut self) {
        if let Some(batch) = self.batch.take() {
            tracing::trace!(
                self_id =? self.self_id,
                num_bytes = batch.end_idx - batch.start_idx,
                "forwarding chunks"
            );
            (self.forward)(
                self.message.slice(batch.start_idx..batch.end_idx),
                self.stride,
            );
        }
    }
}

pub(crate) struct ForwardBatcherGuard<'a, 'g, F, PT>
where
    'a: 'g,
    F: FnMut(Bytes, u16),
    PT: PubKey,
{
    batcher: &'g mut ForwardBatcher<'a, F, PT>,
    flush_batch: bool,
}
impl<'a, 'g, F, PT> ForwardBatcherGuard<'a, 'g, F, PT>
where
    'a: 'g,
    F: FnMut(Bytes, u16),
    PT: PubKey,
{
    pub(crate) fn queue_forward(&mut self, payload_start_idx: usize, payload_end_idx: usize) {
        self.flush_batch = false;
        // batch any contiguous message
        if self.batcher.batch.as_ref().is_some() {
            let batch = self.batcher.batch.as_mut().unwrap();
            assert_eq!(batch.end_idx, payload_start_idx);
            batch.end_idx = payload_end_idx;
        } else {
            self.batcher.flush();
            self.batcher.batch = Some(ForwardBatch {
                start_idx: payload_start_idx,
                end_idx: payload_end_idx,
            })
        }
    }
}
impl<'a, 'g, F, PT> Drop for ForwardBatcherGuard<'a, 'g, F, PT>
where
    'a: 'g,
    F: FnMut(Bytes, u16),
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
    use itertools::Itertools;
    use lru::LruCache;
    use monad_crypto::{
        certificate_signature::CertificateSignaturePubKey,
        hasher::{Hasher, HasherType},
    };
    use monad_dataplane::{udp::DEFAULT_SEGMENT_SIZE, RecvUdpMsg};
    use monad_secp::{KeyPair, SecpSignature};
    use monad_types::{Epoch, NodeId, Stake};
    use rstest::*;

    use super::{MessageValidationError, UdpState};
    use crate::{
        udp::{build_messages, parse_message, SIGNATURE_CACHE_SIZE},
        util::{
            BuildTarget, EpochValidators, FullNodes, ReBroadcastGroupMap, Redundancy, Validator,
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

        let validators = EpochValidators {
            validators: keys
                .iter()
                .map(|key| (NodeId::new(key.pubkey()), Validator { stake: Stake(1) }))
                .collect(),
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

    const EPOCH: u64 = 5;
    const UNIX_TS_MS: u64 = 5;

    #[test]
    fn test_roundtrip() {
        let (key, mut validators, known_addresses) = validator_set();
        let epoch_validators = validators.view_without(vec![&NodeId::new(key.pubkey())]);
        let full_nodes = FullNodes::new(Vec::new());

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
            EPOCH, // epoch_no
            UNIX_TS_MS,
            BuildTarget::Raptorcast((epoch_validators, full_nodes.view())),
            &known_addresses,
        );

        let mut signature_cache = LruCache::new(SIGNATURE_CACHE_SIZE);

        for (_to, mut aggregate_message) in messages {
            while !aggregate_message.is_empty() {
                let message = aggregate_message.split_to(DEFAULT_SEGMENT_SIZE.into());
                let parsed_message =
                    parse_message::<SignatureType>(&mut signature_cache, message.clone(), u64::MAX)
                        .expect("valid message");
                assert_eq!(parsed_message.message, message);
                assert_eq!(parsed_message.app_message_hash.0, app_message_hash.0[..20]);
                assert_eq!(parsed_message.unix_ts_ms, UNIX_TS_MS);
                assert!(parsed_message.broadcast);
                assert_eq!(parsed_message.app_message_len, app_message.len() as u32);
                assert_eq!(parsed_message.author, NodeId::new(key.pubkey()));
            }
        }
    }

    #[test]
    fn test_bit_flip_parse_failure() {
        let (key, mut validators, known_addresses) = validator_set();
        let epoch_validators = validators.view_without(vec![&NodeId::new(key.pubkey())]);
        let full_nodes = FullNodes::new(Vec::new());

        let app_message: Bytes = vec![1_u8; 1024 * 2].into();

        let messages = build_messages::<SignatureType>(
            &key,
            DEFAULT_SEGMENT_SIZE, // segment_size
            app_message,
            Redundancy::from_u8(2),
            EPOCH, // epoch_no
            UNIX_TS_MS,
            BuildTarget::Raptorcast((epoch_validators, full_nodes.view())),
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
                    let maybe_parsed = parse_message::<SignatureType>(
                        &mut signature_cache,
                        message.clone().into(),
                        u64::MAX,
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
        let (key, mut validators, known_addresses) = validator_set();
        let epoch_validators = validators.view_without(vec![&NodeId::new(key.pubkey())]);
        let full_nodes = FullNodes::new(Vec::new());

        let app_message: Bytes = vec![1_u8; 1024 * 1024].into();

        let messages = build_messages::<SignatureType>(
            &key,
            DEFAULT_SEGMENT_SIZE, // segment_size
            app_message,
            Redundancy::from_u8(2),
            EPOCH, // epoch_no
            UNIX_TS_MS,
            BuildTarget::Raptorcast((epoch_validators, full_nodes.view())),
            &known_addresses,
        );

        let mut signature_cache = LruCache::new(SIGNATURE_CACHE_SIZE);

        let mut used_ids = HashSet::new();

        for (_to, mut aggregate_message) in messages {
            while !aggregate_message.is_empty() {
                let message = aggregate_message.split_to(DEFAULT_SEGMENT_SIZE.into());
                let parsed_message =
                    parse_message::<SignatureType>(&mut signature_cache, message.clone(), u64::MAX)
                        .expect("valid message");
                let newly_inserted = used_ids.insert(parsed_message.chunk_id);
                assert!(newly_inserted);
            }
        }
    }

    #[test]
    fn test_broadcast_chunk_ids() {
        let (key, mut validators, known_addresses) = validator_set();
        let epoch_validators = validators.view_without(vec![&NodeId::new(key.pubkey())]);

        let app_message: Bytes = vec![1_u8; 1024 * 8].into();

        let messages = build_messages::<SignatureType>(
            &key,
            DEFAULT_SEGMENT_SIZE, // segment_size
            app_message,
            Redundancy::from_u8(2),
            EPOCH, // epoch_no
            UNIX_TS_MS,
            BuildTarget::Broadcast(epoch_validators),
            &known_addresses,
        );

        let mut signature_cache = LruCache::new(SIGNATURE_CACHE_SIZE);

        let mut used_ids: HashMap<SocketAddr, HashSet<_>> = HashMap::new();

        let messages_len = messages.len();
        for (to, mut aggregate_message) in messages {
            while !aggregate_message.is_empty() {
                let message = aggregate_message.split_to(DEFAULT_SEGMENT_SIZE.into());
                let parsed_message =
                    parse_message::<SignatureType>(&mut signature_cache, message.clone(), u64::MAX)
                        .expect("valid message");
                let newly_inserted = used_ids
                    .entry(to)
                    .or_default()
                    .insert(parsed_message.chunk_id);
                assert!(newly_inserted);
            }
        }

        assert_eq!(used_ids.len(), messages_len);
        let ids = used_ids.values().next().unwrap().clone();
        assert!(used_ids.values().all(|x| x == &ids)); // check that all recipients are sent same ids
        assert!(ids.contains(&0)); // check that starts from idx 0
    }

    #[test]
    fn test_handle_message_stride_slice() {
        let (key, validators, _known_addresses) = validator_set();
        let self_id = NodeId::new(key.pubkey());
        let mut group_map = ReBroadcastGroupMap::new(self_id, false);
        let node_stake_pairs: Vec<_> = validators
            .validators
            .iter()
            .map(|(node_id, validator)| (*node_id, validator.stake))
            .collect();
        group_map.push_group_validator_set(node_stake_pairs, Epoch(1));

        let mut udp_state = UdpState::<SignatureType>::new(self_id, u64::MAX);

        // payload will fail to parse but shouldn't panic on index error
        let payload: Bytes = vec![1_u8; 1024 * 8 + 1].into();
        let recv_msg = RecvUdpMsg {
            src_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8000),
            payload,
            stride: 1024,
        };

        udp_state.handle_message(
            &group_map,
            |_targets, _payload, _stride| {},
            |_payload, _stride| {},
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
        let (key, mut validators, known_addresses) = validator_set();
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
            0,
            test_timestamp,
            BuildTarget::Broadcast(epoch_validators),
            &known_addresses,
        );
        let message = messages.into_iter().next().unwrap().1;
        let result = parse_message::<SignatureType>(&mut signature_cache, message, max_age_ms);

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
}
