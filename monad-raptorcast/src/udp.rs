use std::{
    collections::{BTreeMap, HashMap},
    net::SocketAddr,
    num::NonZero,
    ops::Range,
};

use bytes::{Bytes, BytesMut};
use itertools::Itertools;
use lru::LruCache;
use monad_crypto::{
    certificate_signature::{CertificateSignaturePubKey, CertificateSignatureRecoverable, PubKey},
    hasher::{Hasher, HasherType},
};
use monad_dataplane::event_loop::RecvMsg;
use monad_merkle::{MerkleHash, MerkleProof, MerkleTree};
use monad_raptor::{ManagedDecoder, SOURCE_SYMBOLS_MIN};
use monad_types::{Epoch, NodeId};

use crate::{
    util::{compute_hash, BuildTarget, EpochValidators},
    SIGNATURE_SIZE,
};

pub const PENDING_MESSAGE_CACHE_SIZE: NonZero<usize> = unsafe { NonZero::new_unchecked(1_000) };

pub const SIGNATURE_CACHE_SIZE: NonZero<usize> = unsafe { NonZero::new_unchecked(10_000) };
pub const RECENTLY_DECODED_CACHE_SIZE: NonZero<usize> = unsafe { NonZero::new_unchecked(10_000) };

pub(crate) struct UdpState<ST: CertificateSignatureRecoverable> {
    self_id: NodeId<CertificateSignaturePubKey<ST>>,

    // TODO add a cap on max number of chunks that will be forwarded per message? so that a DOS
    // can't be induced by spamming broadcast chunks to any given node
    // TODO we also need to cap the max number chunks that are decoded - because an adversary could
    // generate a bunch of linearly dependent chunks and cause unbounded memory usage.
    // TODO strong bound on max amount of memory used per decoder?
    // TODO make eviction more sophisticated than LRU - should look at unix_ts_ms as well
    pending_message_cache:
        LruCache<MessageCacheKey<CertificateSignaturePubKey<ST>>, ManagedDecoder>,
    signature_cache: LruCache<
        [u8; HEADER_LEN as usize - SIGNATURE_SIZE + 20],
        NodeId<CertificateSignaturePubKey<ST>>,
    >,
    /// Value in this map represents the # of excess chunks received for a successfully decoded msg
    recently_decoded_cache: LruCache<MessageCacheKey<CertificateSignaturePubKey<ST>>, usize>,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
struct MessageCacheKey<PT>
where
    PT: PubKey,
{
    unix_ts_ms: u64,
    author: NodeId<PT>,
    app_message_hash: [u8; 20],
    app_message_len: usize,
}

impl<ST: CertificateSignatureRecoverable> UdpState<ST> {
    pub fn new(self_id: NodeId<CertificateSignaturePubKey<ST>>) -> Self {
        Self {
            self_id,

            pending_message_cache: LruCache::unbounded(),
            signature_cache: LruCache::new(SIGNATURE_CACHE_SIZE),
            recently_decoded_cache: LruCache::new(RECENTLY_DECODED_CACHE_SIZE),
        }
    }

    /// Given a RecvMsg, emits all decoded messages while rebroadcasting as necessary
    pub fn handle_message(
        &mut self,
        epoch_validators: &mut BTreeMap<Epoch, EpochValidators<ST>>,
        rebroadcast: impl FnMut(Vec<NodeId<CertificateSignaturePubKey<ST>>>, Bytes),
        message: RecvMsg,
    ) -> Vec<(NodeId<CertificateSignaturePubKey<ST>>, Bytes)> {
        let self_id = self.self_id;
        let self_hash = compute_hash(&self_id);

        let mut broadcast_batcher = BroadcastBatcher::new(rebroadcast, &message.payload);

        let mut messages = Vec::new();

        for payload_start_idx in (0..message.payload.len()).step_by(message.stride) {
            let mut batch_guard = broadcast_batcher.create_flush_guard();

            let payload_end_idx = payload_start_idx + message.stride.min(message.payload.len());
            let payload = message.payload.slice(payload_start_idx..payload_end_idx);
            let parsed_message = match parse_message::<ST>(&mut self.signature_cache, payload) {
                Ok(message) => message,
                Err(err) => {
                    tracing::debug!(?err, "unable to parse message");
                    continue;
                }
            };

            if parsed_message.broadcast {
                let Some(epoch_validators) = epoch_validators.get_mut(&Epoch(parsed_message.epoch))
                else {
                    tracing::debug!(
                        epoch =? parsed_message.epoch,
                        "don't have epoch validators populated",
                    );
                    continue;
                };
                if !epoch_validators
                    .validators
                    .contains_key(&parsed_message.author)
                {
                    tracing::debug!(
                        author =? parsed_message.author,
                        "not in validator set"
                    );
                    continue;
                }
                if self_hash == parsed_message.recipient_hash {
                    let targets =
                        epoch_validators.view_without(vec![&parsed_message.author, &self_id]);
                    batch_guard.queue_broadcast(
                        payload_start_idx,
                        payload_end_idx,
                        &parsed_message.author,
                        || targets.view().keys().copied().collect(),
                    )
                }
            } else if self_hash != parsed_message.recipient_hash {
                tracing::debug!(
                    ?self_hash,
                    recipient_hash =? parsed_message.recipient_hash,
                    "dropping spoofed message"
                );
                continue;
            }

            let app_message_len: usize = parsed_message.app_message_len.try_into().unwrap();
            let key = MessageCacheKey {
                unix_ts_ms: parsed_message.unix_ts_ms,
                author: parsed_message.author,
                app_message_hash: parsed_message.app_message_hash,
                app_message_len,
            };

            if let Some(excess_chunk_count) = self.recently_decoded_cache.get_mut(&key) {
                // already decoded
                *excess_chunk_count += 1;
                continue;
            }

            let decoder = self.pending_message_cache.get_or_insert_mut(key, || {
                let symbol_len = parsed_message.chunk.len();

                // data_size is always greater than zero, so this division is safe
                let num_source_symbols =
                    app_message_len.div_ceil(symbol_len).max(SOURCE_SYMBOLS_MIN);

                // TODO: verify unwrap
                ManagedDecoder::new(num_source_symbols, symbol_len).unwrap()
            });

            // can we assert!(!decoder.decoding_done()) ?

            decoder.received_encoded_symbol(&parsed_message.chunk, parsed_message.chunk_id.into());

            if decoder.try_decode() {
                let Some(mut decoded) = decoder.reconstruct_source_data() else {
                    tracing::error!("failed to reconstruct source data");
                    continue;
                };

                if app_message_len > 10_000 {
                    tracing::debug!(app_message_len, "reconstructed large message");
                }

                decoded.truncate(app_message_len);
                // successfully decoded, so pop out from pending_messages
                self.pending_message_cache.pop(&key);
                self.recently_decoded_cache.push(key, 0);

                messages.push((parsed_message.author, Bytes::from(decoded)));
            }
        }

        while self.pending_message_cache.len() > PENDING_MESSAGE_CACHE_SIZE.into() {
            let (key, decoder) = self.pending_message_cache.pop_lru().expect("nonempty");
            tracing::debug!(
                num_source_symbols = decoder.num_source_symbols(),
                num_encoded_symbols_received = decoder.num_encoded_symbols_received(),
                inactivation_symbol_threshold = decoder.inactivation_symbol_threshold(),
                ?key,
                "dropped unfinished ManagedDecoder"
            )
        }

        messages
    }
}

/// Stuff to include:
/// - 65 bytes => Signature of sender over hash(rest of message up to merkle proof, concatenated
///               with merkle root)
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
    gso_size: u16,
    app_message: Bytes,
    redundancy: u8, // 2 == send 1 extra packet for every 1 original
    epoch_no: u64,
    unix_ts_ms: u64,
    build_target: BuildTarget<ST>,

    known_addresses: &HashMap<NodeId<CertificateSignaturePubKey<ST>>, SocketAddr>,
) -> Vec<(SocketAddr, Bytes)>
where
    ST: CertificateSignatureRecoverable,
{
    let body_size = gso_size
        .checked_sub(HEADER_LEN + CHUNK_HEADER_LEN)
        .expect("GSO too small");

    let app_message_len: u32 = app_message.len().try_into().expect("message too big");

    let is_broadcast = matches!(
        build_target,
        BuildTarget::Broadcast(_) | BuildTarget::Raptorcast(_)
    );

    // TODO make this more sophisticated
    let tree_depth: u8 = 6; // corresponds to 32 chunks (2^(h-1))
    assert!(
        tree_depth & (1 << 7) == 0,
        "tree depth doesn't fit in 7 bits"
    );
    let chunks_per_merkle_batch: u8 = 2_u8
        .checked_pow(u32::from(tree_depth) - 1)
        .expect("tree depth too big");
    let proof_size: u16 = 20 * (u16::from(tree_depth) - 1);

    let data_size = body_size.checked_sub(proof_size).expect("proof too big");
    let is_raptor_broadcast = matches!(build_target, BuildTarget::Raptorcast(_));

    let num_packets: u16 = {
        let mut num_packets: u16 = (app_message_len
            .div_ceil(u32::from(data_size))
            .max(SOURCE_SYMBOLS_MIN.try_into().unwrap())
            * u32::from(redundancy))
        .try_into()
        .expect("is redundancy too high? doesn't fit in u16");

        if let BuildTarget::Broadcast(epoch_validators) = &build_target {
            num_packets = num_packets
                .checked_mul(epoch_validators.view().len() as u16)
                .expect("num_packets doesn't fit in u16")
        }

        num_packets
    };

    let mut message = BytesMut::zeroed(gso_size as usize * num_packets as usize);

    let mut chunk_datas = message
        .chunks_mut(gso_size.into())
        .map(|chunk| &mut chunk[(HEADER_LEN + proof_size).into()..])
        .collect_vec();
    assert_eq!(chunk_datas.len(), num_packets as usize);

    // the GSO-aware indices into `message`
    let mut outbound_gso_idx: Vec<(SocketAddr, Range<usize>)> = Vec::new();
    // popuate chunk_recipient and outbound_gso_idx
    match build_target {
        BuildTarget::PointToPoint(to) => {
            let Some(addr) = known_addresses.get(to) else {
                tracing::warn!(?to, "not sending message, address unknown");
                return Vec::new();
            };
            outbound_gso_idx.push((*addr, 0..gso_size as usize * num_packets as usize));
            for chunk_data in &mut chunk_datas {
                // populate chunk_recipient
                chunk_data[0..20].copy_from_slice(&compute_hash(to));
            }
        }
        BuildTarget::Broadcast(epoch_validators) => {
            assert!(is_broadcast && !is_raptor_broadcast);
            let total_validators = epoch_validators.view().len();
            let mut running_validator_count = 0;
            for (node_id, _validator) in epoch_validators.view().iter() {
                let start_idx: usize =
                    num_packets as usize * running_validator_count / total_validators;
                running_validator_count += 1;
                let end_idx: usize =
                    num_packets as usize * running_validator_count / total_validators;

                if start_idx == end_idx {
                    continue;
                }
                if let Some(addr) = known_addresses.get(node_id) {
                    outbound_gso_idx.push((
                        *addr,
                        start_idx * gso_size as usize..end_idx * gso_size as usize,
                    ));
                } else {
                    tracing::warn!(?node_id, "not sending message, address unknown")
                }
                for chunk_data in &mut chunk_datas[start_idx..end_idx] {
                    // populate chunk_recipient
                    chunk_data[0..20].copy_from_slice(&compute_hash(node_id));
                }
            }
        }
        BuildTarget::Raptorcast(epoch_validators) => {
            assert!(is_broadcast && is_raptor_broadcast);
            // FIXME should self be included in total_stake?
            let total_stake: i64 = epoch_validators
                .view()
                .values()
                .map(|validator| validator.stake.0)
                .sum();
            let mut running_stake = 0;
            for (node_id, validator) in epoch_validators.view().iter() {
                let start_idx: usize = (num_packets as i64 * running_stake / total_stake) as usize;
                running_stake += validator.stake.0;
                let end_idx: usize = (num_packets as i64 * running_stake / total_stake) as usize;

                if start_idx == end_idx {
                    continue;
                }
                if let Some(addr) = known_addresses.get(node_id) {
                    outbound_gso_idx.push((
                        *addr,
                        start_idx * gso_size as usize..end_idx * gso_size as usize,
                    ));
                } else {
                    tracing::warn!(?node_id, "not sending message, address unknown")
                }
                for chunk_data in &mut chunk_datas[start_idx..end_idx] {
                    // populate chunk_recipient
                    chunk_data[0..20].copy_from_slice(&compute_hash(node_id));
                }
            }
        }
    };

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
    for (chunk_id, mut chunk_data) in chunk_datas.iter_mut().enumerate() {
        let chunk_id = chunk_id as u16;
        let chunk_len: u16 = data_size;

        let cursor = &mut chunk_data;
        let (_cursor_chunk_recipient, cursor) = cursor.split_at_mut(20);
        let (_cursor_chunk_merkle_leaf_idx, cursor) = cursor.split_at_mut(1);
        let (_cursor_chunk_reserved, cursor) = cursor.split_at_mut(1);
        let (cursor_chunk_id, cursor) = cursor.split_at_mut(2);
        cursor_chunk_id.copy_from_slice(&chunk_id.to_le_bytes());
        let (cursor_chunk_payload, _cursor) = cursor.split_at_mut(chunk_len.into());
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
    let app_message_hash: [u8; 20] = {
        let mut hasher = HasherType::new();
        hasher.update(app_message);
        hasher.hash().0[..20].try_into().unwrap()
    };
    message
        // .par_chunks_mut(gso_size as usize * chunks_per_merkle_batch as usize)
        .chunks_mut(gso_size as usize * chunks_per_merkle_batch as usize)
        .for_each(|merkle_batch| {
            let mut merkle_batch = merkle_batch.chunks_mut(gso_size as usize).collect_vec();
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
                cursor_app_message_hash.copy_from_slice(&app_message_hash);
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

    outbound_gso_idx
        .into_iter()
        .map(|(addr, range)| (addr, message.slice(range)))
        .collect()
}

pub struct ValidatedMessage<PT>
where
    PT: PubKey,
{
    pub message: Bytes,

    pub author: NodeId<PT>,
    pub epoch: u64,
    pub unix_ts_ms: u64,
    pub app_message_hash: [u8; 20],
    pub app_message_len: u32,
    pub broadcast: bool,
    pub recipient_hash: [u8; 20],
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
}

/// - 65 bytes => Signature of sender over hash(rest of message up to merkle proof, concatenated
///               with merkle root)
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
        [u8; HEADER_LEN as usize - 65 + 20],
        NodeId<CertificateSignaturePubKey<ST>>,
    >,
    message: Bytes,
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
    let cursor_signature = split_off(65)?;
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

    if tree_depth < 1 {
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

    let cursor_app_message_hash = split_off(20)?;
    let app_message_hash: [u8; 20] = cursor_app_message_hash
        .as_ref()
        .try_into()
        .expect("Hash is 20 bytes");

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
    let recipient_hash: [u8; 20] = cursor_recipient
        .as_ref()
        .try_into()
        .expect("Hash is 20 bytes");

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
    let mut signed_over = [0_u8; HEADER_LEN as usize - 65 + 20];
    // TODO can avoid this copy if necessary
    signed_over[..HEADER_LEN as usize - 65].copy_from_slice(&message[65..HEADER_LEN as usize]);
    signed_over[HEADER_LEN as usize - 65..].copy_from_slice(&root);

    let author = *signature_cache.try_get_or_insert(signed_over, || {
        let author = signature
            .recover_pubkey(&signed_over)
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

struct BroadcastBatch<PT: PubKey> {
    author: NodeId<PT>,
    targets: Vec<NodeId<PT>>,

    start_idx: usize,
    end_idx: usize,
}
pub struct BroadcastBatcher<'a, F, PT>
where
    F: FnMut(Vec<NodeId<PT>>, Bytes),
    PT: PubKey,
{
    rebroadcast: F,
    message: &'a Bytes,

    batch: Option<BroadcastBatch<PT>>,
}
impl<'a, F, PT> Drop for BroadcastBatcher<'a, F, PT>
where
    F: FnMut(Vec<NodeId<PT>>, Bytes),
    PT: PubKey,
{
    fn drop(&mut self) {
        self.flush()
    }
}
impl<'a, F, PT> BroadcastBatcher<'a, F, PT>
where
    F: FnMut(Vec<NodeId<PT>>, Bytes),
    PT: PubKey,
{
    pub fn new(rebroadcast: F, message: &'a Bytes) -> Self {
        Self {
            rebroadcast,
            message,
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
                author =? batch.author,
                num_targets = batch.targets.len(),
                num_bytes = batch.end_idx - batch.start_idx,
                "rebroadcasting chunks"
            );
            (self.rebroadcast)(
                batch.targets,
                self.message.slice(batch.start_idx..batch.end_idx),
            );
        }
    }
}
pub struct BatcherGuard<'a, 'g, F, PT>
where
    'a: 'g,
    F: FnMut(Vec<NodeId<PT>>, Bytes),
    PT: PubKey,
{
    batcher: &'g mut BroadcastBatcher<'a, F, PT>,
    flush_batch: bool,
}
impl<'a, 'g, F, PT> BatcherGuard<'a, 'g, F, PT>
where
    'a: 'g,
    F: FnMut(Vec<NodeId<PT>>, Bytes),
    PT: PubKey,
{
    pub fn queue_broadcast(
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
    F: FnMut(Vec<NodeId<PT>>, Bytes),
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
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    use bytes::{Bytes, BytesMut};
    use itertools::Itertools;
    use lru::LruCache;
    use monad_crypto::hasher::{Hasher, HasherType};
    use monad_secp::SecpSignature;
    use monad_types::{NodeId, Stake};

    use crate::{
        udp::{build_messages, parse_message, SIGNATURE_CACHE_SIZE},
        util::{BuildTarget, EpochValidators, Validator},
    };

    #[test]
    fn test_roundtrip() {
        let keys = (0_u8..100_u8)
            .map(|n| {
                let mut hasher = HasherType::new();
                hasher.update(n.to_le_bytes());
                let mut hash = hasher.hash();
                monad_secp::KeyPair::from_bytes(&mut hash.0).unwrap()
            })
            .collect_vec();

        let mut validators = EpochValidators {
            validators: keys
                .iter()
                .map(|key| (NodeId::new(key.pubkey()), Validator { stake: Stake(1) }))
                .collect(),
        };
        let epoch_validators = validators.view_without(vec![&NodeId::new(keys[0].pubkey())]);

        let known_addresses = keys
            .iter()
            .map(|key| {
                (
                    NodeId::new(key.pubkey()),
                    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
                )
            })
            .collect();

        let app_message: Bytes = vec![1_u8; 1024 * 1024].into();
        let app_message_hash = {
            let mut hasher = HasherType::new();
            hasher.update(&app_message);
            hasher.hash()
        };

        const EPOCH: u64 = 5;
        const UNIX_TS_MS: u64 = 5;
        const GSO_SIZE: u16 = 1500;
        let messages = build_messages::<SecpSignature>(
            &keys[0],
            GSO_SIZE, // gso_size
            app_message.clone(),
            2,     // redundancy,
            EPOCH, // epoch_no
            UNIX_TS_MS,
            BuildTarget::Raptorcast(epoch_validators),
            &known_addresses,
        );

        let mut signature_cache = LruCache::new(SIGNATURE_CACHE_SIZE);

        for (to, mut aggregate_message) in messages {
            while !aggregate_message.is_empty() {
                let message = aggregate_message.split_to(GSO_SIZE.into());
                let parsed_message =
                    parse_message::<SecpSignature>(&mut signature_cache, message.clone())
                        .expect("valid message");
                assert_eq!(parsed_message.message, message);
                assert_eq!(parsed_message.app_message_hash, app_message_hash.0[..20]);
                assert_eq!(parsed_message.unix_ts_ms, UNIX_TS_MS);
                assert!(parsed_message.broadcast);
                assert_eq!(parsed_message.app_message_len, app_message.len() as u32);
                assert_eq!(parsed_message.author, NodeId::new(keys[0].pubkey()));
            }
        }
    }

    #[test]
    fn test_bit_flip_parse_failure() {
        let keys = (0_u8..100_u8)
            .map(|n| {
                let mut hasher = HasherType::new();
                hasher.update(n.to_le_bytes());
                let mut hash = hasher.hash();
                monad_secp::KeyPair::from_bytes(&mut hash.0).unwrap()
            })
            .collect_vec();

        let mut validators = EpochValidators {
            validators: keys
                .iter()
                .map(|key| (NodeId::new(key.pubkey()), Validator { stake: Stake(1) }))
                .collect(),
        };
        let epoch_validators = validators.view_without(vec![&NodeId::new(keys[0].pubkey())]);

        let known_addresses = keys
            .iter()
            .map(|key| {
                (
                    NodeId::new(key.pubkey()),
                    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
                )
            })
            .collect();

        let app_message: Bytes = vec![1_u8; 1024 * 2].into();

        const EPOCH: u64 = 5;
        const UNIX_TS_MS: u64 = 5;
        const GSO_SIZE: u16 = 1500;
        let messages = build_messages::<SecpSignature>(
            &keys[0],
            GSO_SIZE, // gso_size
            app_message,
            2,     // redundancy,
            EPOCH, // epoch_no
            UNIX_TS_MS,
            BuildTarget::Raptorcast(epoch_validators),
            &known_addresses,
        );

        let mut signature_cache = LruCache::new(SIGNATURE_CACHE_SIZE);

        for (to, mut aggregate_message) in messages {
            while !aggregate_message.is_empty() {
                let mut message: BytesMut =
                    aggregate_message.split_to(GSO_SIZE.into()).as_ref().into();
                // try flipping each bit
                for bit_idx in 0..message.len() * 8 {
                    let old_byte = message[bit_idx / 8];
                    // flip bit
                    message[bit_idx / 8] = old_byte ^ (1 << (bit_idx % 8));
                    let maybe_parsed = parse_message::<SecpSignature>(
                        &mut signature_cache,
                        message.clone().into(),
                    );

                    // check that decoding fails
                    assert!(
                        maybe_parsed.is_err()
                            || maybe_parsed.unwrap().author != NodeId::new(keys[0].pubkey())
                    );

                    // reset bit
                    message[bit_idx / 8] = old_byte;
                }
            }
        }
    }
}
