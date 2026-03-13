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

#![allow(clippy::identity_op)]
use std::ops::Range;

use bytes::{Bytes, BytesMut};
use monad_crypto::{
    certificate_signature::{
        CertificateKeyPair as _, CertificateSignature, CertificateSignaturePubKey,
        CertificateSignatureRecoverable, PubKey,
    },
    hasher::{Hash, Hasher as _, HasherType},
    signing_domain,
};
use monad_dataplane::udp::{segment_size_for_mtu, ETHERNET_SEGMENT_SIZE};
use monad_merkle::{MerkleHash, MerkleTree};
use monad_raptor::Encoder;
use monad_types::NodeId;
use rand::{CryptoRng, Rng};

use super::{
    assigner::{ChunkAssignment, ChunkOrder, Partitioned, Replicated, StakeBasedWithRC},
    chunk::{AggregatedChunk, Chunk},
    BuildError, ChunkAssigner, Result,
};
use crate::{
    udp::{GroupId, MAX_NUM_PACKETS},
    util::{
        compute_app_message_hash, BroadcastMode, BuildTarget, Collector, Redundancy, UdpMessage,
    },
    SIGNATURE_SIZE,
};

// For a message with K source symbols, we accept up to the first MAX_REDUNDANCY * K
// encoded symbols.
//
// Any received encoded symbol with an ESI equal to or greater than MAX_REDUNDANCY * K
// will be discarded, as a protection against DoS and algorithmic complexity attacks.
//
// 7 is the largest value that works for all values of K, as K
// can be at most 8192, and there can be at most 65521 encoding symbol IDs.
//
// We set this to 3 as a more reasonable upper bound.
pub const MAX_REDUNDANCY: Redundancy = Redundancy::from_u8(3);

// For a tree depth of 1, every encoded symbol is its own Merkle tree, and there will be no
// Merkle proof section in the constructed RaptorCast packets.
//
// For a tree depth of 9, the index of the rightmost Merkle tree leaf will be 0xff, and the
// Merkle leaf index field is 8 bits wide.
pub const MIN_MERKLE_TREE_DEPTH: u8 = 1;
pub const MAX_MERKLE_TREE_DEPTH: u8 = 9;

const _: () = assert!(
    MAX_MERKLE_TREE_DEPTH <= 0b1111,
    "merkle tree depth must be <= 4 bits"
);

const _: () = assert!(
    MIN_SEGMENT_LENGTH == segment_size_for_mtu(1280) as usize,
    "MIN_SEGMENT_LENGTH should be the segment size for the IPv6 minimum MTU of 1280 bytes"
);

// We assume an MTU of at least 1280 (the IPv6 minimum MTU), which for the maximum Merkle tree
// depth of 9 gives a symbol size of 960 bytes, which we will use as the minimum chunk length for
// received packets, and we'll drop received chunks that are smaller than this to mitigate attacks
// involving a peer sending us a message as a very large set of very small chunks.
pub const MIN_CHUNK_LENGTH: usize = 960;

/// The min segment length should be large enough to hold at least
/// MAX_CHUNK_LENGTH of payload plus all headers with the smallest
/// merkle tree depth.
pub const MIN_SEGMENT_LENGTH: usize =
    PacketLayout::calc_segment_len(MIN_CHUNK_LENGTH, MAX_MERKLE_TREE_DEPTH);

/// The max segment length should not exceed the standard MTU for
/// Ethernet to avoid fragmentation when routed across the internet.
pub const MAX_SEGMENT_LENGTH: usize = ETHERNET_SEGMENT_SIZE as usize;

#[derive(Default, Clone, Copy, PartialEq, Eq)]
pub enum AssembleMode {
    // Compatible with existing build_messages logic, does not support
    // streaming per merkle batch
    GsoFull,

    // Gso concatenated chunks only within a merkle batch.
    GsoBestEffort,

    // Each recipient gets its own packet in round-robin order.
    #[default]
    RoundRobin,
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn assemble<ST>(
    key: &ST::KeyPairType,
    layout: PacketLayout,
    app_message: &[u8],
    header_buf: &[u8],
    assignment: ChunkAssignment<CertificateSignaturePubKey<ST>>,
    mode: AssembleMode,
    collector: &mut impl Collector<UdpMessage<CertificateSignaturePubKey<ST>>>,
) -> Result<()>
where
    ST: CertificateSignature,
{
    // step 1. generate the chunks
    let mut chunks = assignment.generate(layout.segment_len());

    // step 2. encode and write raptor symbols to each chunk
    if assignment.unique_chunk_id() {
        encode_unique_symbols(app_message, &mut chunks, layout)?;
    } else {
        encode_symbols(app_message, &mut chunks, layout)?;
    }

    if mode.stream_mode() {
        for mut batch in owned_merkle_batches(chunks, layout) {
            // step 3. sign and write headers for this merkle batch
            let merkle_batch = MerkleBatch::from(&mut batch[..]);
            merkle_batch.write_header::<ST>(layout, key, header_buf)?;

            // step 4. assemble udp messages
            mode.assemble_udp_messages_into(collector, batch);
        }
    } else {
        // step 3. sign and write headers for this merkle batch
        for batch in merkle_batches(&mut chunks, layout) {
            batch.write_header::<ST>(layout, key, header_buf)?;
        }

        // step 4. assemble udp messages
        mode.assemble_udp_messages_into(collector, chunks);
    }

    Ok(())
}

/// Stuff to include:
///
/// - 65 bytes => Signature of sender over hash(rest of message up to merkle proof, concatenated with merkle root)
/// - 2 bytes => Version: bumped on protocol updates
/// - 1 bit => broadcast or not
/// - 1 bit => secondary broadcast or not (full-node raptorcast)
/// - 2 bits => unused
/// - 4 bits => Merkle tree depth
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
//
// pub struct M {
//     signature: [u8; 65],
//     version: u16,
//     broadcast: bool,
//     secondary_broadcast: bool,
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
#[derive(Clone, Copy)]
pub(crate) struct PacketLayout {
    chunk_header_start: usize,
    segment_len: usize,
}

impl PacketLayout {
    pub const HEADER_LEN: usize = 0
        + SIGNATURE_SIZE // Sender signature (65 bytes)
        + 2  // Version
        + 1  // Broadcast bit, Secondary Broadcast bit, 2 unused bits, 4 bits for Merkle Tree Depth
        + 8  // Epoch #
        + 8  // Unix timestamp
        + 20 // AppMessage hash
        + 4; // AppMessage length

    pub const CHUNK_HEADER_LEN: usize = 0
        + 20 // Chunk recipient hash
        + 1  // Chunk's merkle leaf idx
        + 1  // reserved
        + 2; // Chunk idx

    // the size of individual merkle hash
    pub const MERKLE_HASH_LEN: usize = 20;

    pub const fn new(segment_len: usize, merkle_tree_depth: u8) -> Self {
        let merkle_proof_len = Self::calc_merkle_proof_len(merkle_tree_depth);
        let chunk_header_start = Self::HEADER_LEN + merkle_proof_len;

        Self {
            chunk_header_start,
            segment_len,
        }
    }

    pub const fn calc_segment_len(chunk_len: usize, merkle_tree_depth: u8) -> usize {
        let merkle_proof_len = Self::calc_merkle_proof_len(merkle_tree_depth);
        Self::HEADER_LEN + merkle_proof_len + Self::CHUNK_HEADER_LEN + chunk_len
    }

    pub const fn calc_merkle_proof_len(merkle_tree_depth: u8) -> usize {
        let merkle_hash_count = merkle_tree_depth as usize - 1;
        merkle_hash_count * Self::MERKLE_HASH_LEN
    }

    pub const fn calc_num_symbols(
        &self,
        app_message_len: usize,
        redundancy: Redundancy,
    ) -> Option<usize> {
        let base_num_symbols = app_message_len.div_ceil(self.symbol_len());
        redundancy.scale(base_num_symbols)
    }

    pub const fn signature_range(&self) -> Range<usize> {
        0..SIGNATURE_SIZE
    }

    pub const fn header_sans_signature_range(&self) -> Range<usize> {
        SIGNATURE_SIZE..Self::HEADER_LEN
    }

    pub const fn merkle_proof_range(&self) -> Range<usize> {
        Self::HEADER_LEN..self.chunk_header_start
    }

    pub const fn merkle_hashed_range(&self) -> Range<usize> {
        self.chunk_header_start..self.segment_len
    }

    pub const fn chunk_header_range(&self) -> Range<usize> {
        self.chunk_header_start..(self.chunk_header_start + Self::CHUNK_HEADER_LEN)
    }

    pub const fn symbol_range(&self) -> Range<usize> {
        let symbol_start = self.chunk_header_start + Self::CHUNK_HEADER_LEN;
        symbol_start..self.segment_len
    }

    pub const fn symbol_len(&self) -> usize {
        let symbol_start = self.chunk_header_start + Self::CHUNK_HEADER_LEN;
        self.segment_len - symbol_start
    }

    pub const fn segment_len(&self) -> usize {
        self.segment_len
    }

    pub const fn merkle_tree_depth(&self) -> u8 {
        let proof_len = self.chunk_header_start - Self::HEADER_LEN;
        debug_assert!(proof_len < u8::MAX as usize * Self::MERKLE_HASH_LEN);
        debug_assert!(proof_len.is_multiple_of(Self::MERKLE_HASH_LEN));
        (proof_len / Self::MERKLE_HASH_LEN) as u8 + 1
    }

    pub const fn merkle_batch_len(&self) -> usize {
        2usize
            .checked_pow((self.merkle_tree_depth() - 1) as u32)
            .expect("merkle tree depth too large")
    }

    pub fn symbol<'a, PT: PubKey>(&self, chunk: &'a Chunk<PT>) -> &'a [u8] {
        &chunk.payload()[self.symbol_range()]
    }

    pub fn symbol_mut<'a, PT: PubKey>(&self, chunk: &'a mut Chunk<PT>) -> &'a mut [u8] {
        &mut chunk.payload_mut()[self.symbol_range()]
    }

    pub fn write_header<PT: PubKey>(
        &self,
        chunk: &mut Chunk<PT>,
        signature: &[u8; SIGNATURE_SIZE],
        header: &[u8], // excluding signature
    ) {
        chunk.payload_mut()[self.signature_range()].copy_from_slice(signature);
        chunk.payload_mut()[self.header_sans_signature_range()].copy_from_slice(header);
    }

    pub fn write_chunk_header<PT: PubKey>(
        &self,
        chunk: &mut Chunk<PT>,
        merkle_leaf_index: u8,
    ) -> Result<()> {
        let recipient_hash = *chunk.recipient().node_hash();
        let chunk_id: [u8; 2] = u16::try_from(chunk.chunk_id())
            .map_err(|_| BuildError::ChunkIdOverflow)?
            .to_le_bytes();

        let buffer = &mut chunk.payload_mut()[self.chunk_header_range()];
        debug_assert_eq!(buffer.len(), Self::CHUNK_HEADER_LEN);

        buffer[0..20].copy_from_slice(&recipient_hash); // node_id hash
        buffer[20] = merkle_leaf_index;
        // buffer[21] = 0; // reserved
        buffer[22..24].copy_from_slice(&chunk_id);

        Ok(())
    }

    pub fn write_merkle_proof<PT: PubKey>(&self, chunk: &mut Chunk<PT>, proof: &[MerkleHash]) {
        let buffer = &mut chunk.payload_mut()[self.merkle_proof_range()];
        debug_assert_eq!(buffer.len() % Self::MERKLE_HASH_LEN, 0);

        for (idx, hash) in proof.iter().enumerate() {
            let start = idx * Self::MERKLE_HASH_LEN;
            let end = (idx + 1) * Self::MERKLE_HASH_LEN;
            buffer[start..end].copy_from_slice(hash);
        }
    }

    pub fn chunk_hash<PT: PubKey>(&self, chunk: &Chunk<PT>) -> Hash {
        let mut hasher = HasherType::new();
        hasher.update(&chunk.payload()[self.merkle_hashed_range()]);
        hasher.hash()
    }
}

pub(super) struct MerkleBatch<'a, PT: PubKey> {
    chunks: &'a mut [Chunk<PT>],
}

pub(super) fn merkle_batches<PT: PubKey>(
    all_chunks: &mut [Chunk<PT>],
    layout: PacketLayout,
) -> impl Iterator<Item = MerkleBatch<'_, PT>> {
    let batch_len = layout.merkle_batch_len();
    debug_assert!(batch_len > 0);
    all_chunks.chunks_mut(batch_len).map(MerkleBatch::from)
}

fn owned_merkle_batches<PT: PubKey>(
    mut chunks: Vec<Chunk<PT>>,
    layout: PacketLayout,
) -> impl Iterator<Item = Vec<Chunk<PT>>> {
    let batch_len = layout.merkle_batch_len();
    debug_assert!(batch_len > 0);

    std::iter::from_fn(move || {
        if chunks.is_empty() {
            return None;
        }

        // After split_off, `chunks` stores the merkle batch and
        // `rest` stores the rest of the chunks. We take `chunks` out
        // to get the merkle batch for returning and swap `rest` into
        // `chunks` for next iteration.
        let actual_batch_len = batch_len.min(chunks.len());
        let rest = chunks.split_off(actual_batch_len);
        let batch = std::mem::replace(&mut chunks, rest);

        Some(batch)
    })
}

impl<'a, 'b, PT: PubKey> From<&'b mut [Chunk<PT>]> for MerkleBatch<'a, PT>
where
    'b: 'a,
{
    fn from(chunks: &'b mut [Chunk<PT>]) -> Self {
        Self { chunks }
    }
}

impl<'a, PT: PubKey> MerkleBatch<'a, PT> {
    fn write_header<ST>(
        mut self,
        layout: PacketLayout,
        key: &ST::KeyPairType,
        header: &[u8],
    ) -> Result<()>
    where
        ST: CertificateSignature,
    {
        // write chunk header and calculate chunk hash to build the
        // merkle tree.
        let merkle_tree = self.build_merkle_tree(layout)?;
        let signature = self.sign::<ST>(key, header, merkle_tree.root());
        let num_chunks = self.chunks.len();

        for (leaf_index, chunk) in self.chunks.iter_mut().enumerate() {
            // write signature and the rest of the header
            layout.write_header(chunk, &signature, header);

            // write merkle proof
            debug_assert!(leaf_index < num_chunks);
            let proof = merkle_tree.proof(leaf_index as u16);
            layout.write_merkle_proof(chunk, proof.siblings());
        }

        Ok(())
    }

    fn build_merkle_tree(&mut self, layout: PacketLayout) -> Result<MerkleTree> {
        let mut hashes = Vec::with_capacity(self.chunks.len());
        let depth = layout.merkle_tree_depth();
        debug_assert!(self.chunks.len() <= 2usize.pow((depth - 1) as u32));

        for (leaf_index, chunk) in self.chunks.iter_mut().enumerate() {
            let leaf_index = leaf_index as u8;
            layout.write_chunk_header(chunk, leaf_index)?;
            let hash = layout.chunk_hash(chunk);
            hashes.push(hash);
        }

        Ok(MerkleTree::new_with_depth(&hashes, depth))
    }

    fn sign<ST>(
        &self,
        key: &ST::KeyPairType,
        header: &[u8],
        merkle_root: &[u8; PacketLayout::MERKLE_HASH_LEN],
    ) -> [u8; SIGNATURE_SIZE]
    where
        ST: CertificateSignature,
    {
        let mut buffer = BytesMut::with_capacity(header.len() + PacketLayout::MERKLE_HASH_LEN);
        buffer.extend_from_slice(header);
        buffer.extend_from_slice(merkle_root);

        let signature = ST::sign::<signing_domain::RaptorcastChunk>(&buffer, key);
        let signature = CertificateSignature::serialize(&signature);
        debug_assert_eq!(signature.len(), SIGNATURE_SIZE);
        signature.try_into().expect("invalid signature size")
    }
}

fn encode_unique_symbols<PT: PubKey>(
    app_message: &[u8],
    chunks: &mut [Chunk<PT>],
    layout: PacketLayout,
) -> Result<()> {
    let symbol_len = layout.symbol_len();
    let encoder =
        Encoder::new(app_message, symbol_len).map_err(|_| BuildError::EncoderCreationFailed)?;
    for chunk in chunks.iter_mut() {
        let chunk_id = chunk.chunk_id();
        let symbol_buffer = layout.symbol_mut(chunk);
        encoder.encode_symbol(symbol_buffer, chunk_id);
    }
    Ok(())
}

fn encode_symbols<PT: PubKey>(
    app_message: &[u8],
    chunks: &mut [Chunk<PT>],
    layout: PacketLayout,
) -> Result<()> {
    let symbol_len = layout.symbol_len();
    let encoder =
        Encoder::new(app_message, symbol_len).map_err(|_| BuildError::EncoderCreationFailed)?;

    // A map from chunk_id to index to `chunks` slice. Stores the
    // 1+index of the chunk of a given symbol.
    //
    // Over-allocated to avoid re-allocation on the premise that
    // |chunks| >= max(chunk_id). Switch to a proper Map if the
    // premise no longer holds.
    let mut symbol_chunks = vec![0; chunks.len()];

    for i in 0..chunks.len() {
        let chunk_id = chunks[i].chunk_id();

        debug_assert!(chunk_id < usize::MAX); // or 1+index will overflow.
        debug_assert!(chunk_id < chunks.len()); // or symbol_chunks access is OOB.

        match symbol_chunks[chunk_id] {
            0 => {
                // This is the first encounter of symbol `chunk_id`.
                let symbol_buffer = layout.symbol_mut(&mut chunks[i]);
                encoder.encode_symbol(symbol_buffer, chunk_id);
                symbol_chunks[chunk_id] = i + 1;
            }
            j => {
                // If the symbol has been encoded, reuse the result
                // to avoid the (somewhat) expensive encoding again.
                let [src_chunk, dst_chunk] = chunks
                    .get_disjoint_mut([j - 1, i])
                    .expect("the two chunk index never overlap");

                let dst_buffer = layout.symbol_mut(dst_chunk);
                let src_buffer = layout.symbol(src_chunk);
                dst_buffer.copy_from_slice(src_buffer);
            }
        }
    }

    Ok(())
}

const VERSION: u16 = 0x0;

// return the shared header for all chunks sans the signature.
pub(crate) fn build_header(
    broadcast_mode: BroadcastMode,
    merkle_tree_depth: u8,
    group_id: GroupId,
    unix_ts_ms: u64,
    app_message_hash: &[u8; 20],
    app_message_len: usize,
) -> Result<Bytes> {
    // 2  // Version
    // 1  // Broadcast bit
    //       Secondary broadcast bit,
    //       2 unused bits,
    //       4 bits for Merkle Tree Depth
    // 8  // Group id
    // 8  // Unix timestamp
    // 20 // AppMessage hash
    // 4  // AppMessage length
    let mut buffer = BytesMut::zeroed(PacketLayout::HEADER_LEN - SIGNATURE_SIZE);
    let cursor = &mut buffer;

    let (cursor_version, cursor) = cursor.split_at_mut_checked(2).expect("header to short");
    cursor_version.copy_from_slice(&VERSION.to_le_bytes());

    let (cursor_broadcast_merkle_depth, cursor) =
        cursor.split_at_mut_checked(1).expect("header to short");
    let mut broadcast_byte: u8 = match broadcast_mode {
        BroadcastMode::Primary => 0b10 << 6,
        BroadcastMode::Secondary => 0b01 << 6,
        BroadcastMode::Unspecified => 0b00 << 6,
    };
    // tree_depth max 4 bits
    if (merkle_tree_depth & 0b1111_0000) != 0 {
        return Err(BuildError::MerkleTreeTooDeep);
    }
    broadcast_byte |= merkle_tree_depth & 0b0000_1111;
    cursor_broadcast_merkle_depth[0] = broadcast_byte;

    let group_id: u64 = group_id.into();
    let (cursor_group_id, cursor) = cursor.split_at_mut_checked(8).expect("header too short");
    cursor_group_id.copy_from_slice(&group_id.to_le_bytes());

    let (cursor_unix_ts_ms, cursor) = cursor.split_at_mut_checked(8).expect("header too short");
    cursor_unix_ts_ms.copy_from_slice(&unix_ts_ms.to_le_bytes());

    let (cursor_app_message_hash, cursor) =
        cursor.split_at_mut_checked(20).expect("header too short");
    cursor_app_message_hash.copy_from_slice(app_message_hash);

    let (cursor_app_message_len, cursor) =
        cursor.split_at_mut_checked(4).expect("header too short");
    let app_message_len: u32 = app_message_len
        .try_into()
        .map_err(|_| BuildError::AppMessageTooLarge)?;
    cursor_app_message_len.copy_from_slice(&app_message_len.to_le_bytes());

    // should have consumed the whole buffer
    debug_assert_eq!(cursor.len(), 0);

    Ok(buffer.freeze())
}

// Derive the seed used to shuffling validator set for regular raptorcast
pub fn derive_seed(app_message_hash: &[u8; 20]) -> [u8; 32] {
    let mut padded_seed = [0u8; 32];
    padded_seed[..20].copy_from_slice(app_message_hash);
    padded_seed
}

/// Build regular raptorcast packets from validated parameters.
///
/// The caller is responsible for validating inputs (segment size bounds,
/// merkle tree depth bounds, redundancy bounds, message size).
pub(crate) fn build_into<ST, C>(
    key: &ST::KeyPairType,
    layout: PacketLayout,
    redundancy: Redundancy,
    unix_ts_ms: u64,
    app_message: &[u8],
    build_target: &BuildTarget<'_, CertificateSignaturePubKey<ST>>,
    collector: &mut C,
) -> Result<()>
where
    ST: CertificateSignatureRecoverable,
    C: Collector<UdpMessage<CertificateSignaturePubKey<ST>>>,
{
    // compute app message hash
    let app_message_hash = compute_app_message_hash(app_message).0;

    // select chunk assignment algorithm based on build target
    let rng = &mut rand::thread_rng();
    let self_node_id = NodeId::new(key.pubkey());
    let assigner = make_assigner(build_target, &self_node_id, &app_message_hash, rng);

    // calculate the number of symbols needed for assignment
    let num_symbols = layout
        .calc_num_symbols(app_message.len(), redundancy)
        .ok_or(BuildError::TooManyChunks)?;
    if num_symbols > MAX_NUM_PACKETS {
        return Err(BuildError::TooManyChunks);
    }

    // assign the chunks to recipients
    let assemble_mode = AssembleMode::default();
    let order = assemble_mode.expected_chunk_order();
    let mut assignment = assigner.assign_chunks(num_symbols, order)?;
    assignment.ensure_order(order);

    if assignment.is_empty() {
        tracing::warn!(app_msg_len = app_message.len(), "no chunk generated");
        return Ok(());
    }

    if assignment.total_chunks() > MAX_NUM_PACKETS {
        return Err(BuildError::TooManyChunks);
    }

    // build the shared header
    let header = build_header(
        broadcast_mode_from_build_target(build_target),
        layout.merkle_tree_depth(),
        build_target.group_id(),
        unix_ts_ms,
        &app_message_hash,
        app_message.len(),
    )?;

    // assemble the chunks's headers and content
    assemble::<ST>(
        key,
        layout,
        app_message,
        &header,
        assignment,
        assemble_mode,
        collector,
    )?;

    Ok(())
}

fn make_assigner<PT: PubKey>(
    build_target: &BuildTarget<'_, PT>,
    self_node_id: &NodeId<PT>,
    app_message_hash: &[u8; 20],
    rng: &mut (impl Rng + CryptoRng),
) -> Box<dyn ChunkAssigner<PT>> {
    match build_target {
        BuildTarget::PointToPoint { recipient, .. } => {
            let assigner = Replicated::from_broadcast(
                std::iter::once(*recipient)
                    .filter(|node_id| *node_id != self_node_id)
                    .cloned(),
            );
            Box::new(assigner)
        }
        BuildTarget::Broadcast(nodes) => {
            let assigner = Replicated::from_broadcast(
                nodes
                    .iter()
                    .map(|(n, _)| n)
                    .filter(|node_id| *node_id != self_node_id)
                    .cloned(),
            );
            Box::new(assigner)
        }
        BuildTarget::Raptorcast(validators) => {
            let mut validators = validators
                .iter()
                .filter(|(node_id, _stake)| *node_id != self_node_id)
                .map(|(node_id, stake)| (*node_id, *stake))
                .collect::<Vec<_>>();
            let seed = derive_seed(app_message_hash);
            StakeBasedWithRC::shuffle_validators(&mut validators, seed);
            let assigner = StakeBasedWithRC::from_validator_set(validators);
            Box::new(assigner)
        }
        BuildTarget::FullNodeRaptorCast(group) => {
            let seed = rng.gen::<usize>();
            let mut members: Vec<_> = group.iter().cloned().collect();
            randomize_secondary_group_nodes(&mut members, seed);
            Box::new(Partitioned::from_homogeneous_peers(members))
        }
    }
}

fn broadcast_mode_from_build_target<PT: PubKey>(
    build_target: &BuildTarget<'_, PT>,
) -> BroadcastMode {
    match build_target {
        BuildTarget::Raptorcast { .. } => BroadcastMode::Primary,
        BuildTarget::FullNodeRaptorCast { .. } => BroadcastMode::Secondary,
        BuildTarget::Broadcast(_) | BuildTarget::PointToPoint { .. } => BroadcastMode::Unspecified,
    }
}

// introduce variation in the group member ordering
fn randomize_secondary_group_nodes<PT: PubKey>(group: &mut [NodeId<PT>], seed: usize) {
    let n = seed % group.len();
    group.rotate_left(n);
}

impl AssembleMode {
    pub fn expected_chunk_order(self) -> Option<ChunkOrder> {
        match self {
            AssembleMode::GsoFull => Some(ChunkOrder::GsoFriendly),
            AssembleMode::GsoBestEffort => Some(ChunkOrder::GsoFriendly),
            AssembleMode::RoundRobin => Some(ChunkOrder::RoundRobin),
        }
    }

    fn stream_mode(self) -> bool {
        match self {
            AssembleMode::GsoFull => false,
            AssembleMode::RoundRobin | AssembleMode::GsoBestEffort => true,
        }
    }

    fn assemble_udp_messages_into<PT: PubKey>(
        self,
        collector: &mut impl Collector<UdpMessage<PT>>,
        chunks: Vec<Chunk<PT>>,
    ) {
        match self {
            AssembleMode::GsoFull | AssembleMode::GsoBestEffort => {
                Self::assemble_gso_udp_messages_into(collector, chunks);
            }
            AssembleMode::RoundRobin => {
                Self::assemble_standalone_udp_messages_into(collector, chunks);
            }
        }
    }

    fn assemble_standalone_udp_messages_into<PT: PubKey>(
        collector: &mut impl Collector<UdpMessage<PT>>,
        chunks: Vec<Chunk<PT>>,
    ) {
        collector.reserve(chunks.len());
        for chunk in chunks {
            collector.push(chunk.into());
        }
    }

    fn assemble_gso_udp_messages_into<PT: PubKey>(
        collector: &mut impl Collector<UdpMessage<PT>>,
        chunks: Vec<Chunk<PT>>,
    ) {
        let mut agg_chunk = None;

        for chunk in chunks {
            let Some(agg) = &mut agg_chunk else {
                // first chunk, start a new aggregation
                agg_chunk = Some(AggregatedChunk::from(chunk));
                continue;
            };

            if let Some(to_flush) = agg.aggregate(chunk) {
                // different recipient, flush the previous message
                collector.push(to_flush.into());
            }
        }

        if let Some(agg) = agg_chunk.take() {
            collector.push(agg.into());
        }
    }
}
