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
use std::{cell::OnceCell, net::SocketAddr, ops::Range, rc::Rc};

use bytes::{Bytes, BytesMut};
use monad_crypto::{
    certificate_signature::{CertificateSignature, CertificateSignaturePubKey, PubKey},
    hasher::{Hash, Hasher as _, HasherType},
    signing_domain,
};
use monad_merkle::{MerkleHash, MerkleTree};
use monad_raptor::Encoder;
use monad_types::NodeId;

use super::{
    assigner::ChunkOrder, BuildError, ChunkAssigner, Collector, PeerAddrLookup, Result, UdpMessage,
};
use crate::{
    message::MAX_MESSAGE_SIZE,
    udp::{MAX_NUM_PACKETS, MAX_REDUNDANCY, MIN_CHUNK_LENGTH},
    util::Redundancy,
    SIGNATURE_SIZE,
};

#[derive(Default, Clone, Copy, PartialEq, Eq)]
pub enum AssembleMode {
    // Compatible with existing build_messages logic, does not support
    // streaming per merkle batch
    #[default]
    GsoFull,

    // Gso concatenated chunks only within a merkle batch.
    GsoBestEffort,

    // Each recipient gets its own packet in round-robin order.
    RoundRobin,
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn assemble<ST, CA, PL>(
    key: &ST::KeyPairType,
    // the size of each udp packet
    segment_len: usize,
    app_message: Bytes,
    redundancy: Redundancy,
    epoch: u64,
    unix_ts_ms: u64,
    broadcast_type: BroadcastType,
    mode: AssembleMode,
    peer_lookup: &PL,
    assigner: &CA,
    collector: &mut impl Collector<UdpMessage>,
) -> Result<()>
where
    ST: CertificateSignature,
    // ?Sized to allow for passing trait objects
    CA: ChunkAssigner<CertificateSignaturePubKey<ST>> + ?Sized,
    PL: PeerAddrLookup<CertificateSignaturePubKey<ST>>,
{
    let merkle_tree_depth = 6; // TODO: calculate the actual merkle tree depth

    debug_assert!(segment_len > MIN_CHUNK_LENGTH);
    debug_assert!(merkle_tree_depth > 0 && merkle_tree_depth <= 15);

    // run sanity checks
    match () {
        _ if app_message.is_empty() => {
            tracing::warn!("empty application message");
            return Ok(());
        }
        _ if app_message.len() > MAX_MESSAGE_SIZE => {
            return Err(BuildError::AppMessageTooLarge);
        }
        _ if redundancy > MAX_REDUNDANCY => {
            return Err(BuildError::RedundancyTooHigh);
        }
        _ => { /* ok */ }
    }

    // step 1. calculate chunk layout
    let layout = PacketLayout::new(segment_len, merkle_tree_depth);
    let num_symbols = layout
        .calc_num_symbols(app_message.len(), redundancy)
        .ok_or(BuildError::TooManyChunks)?;

    // step 2. generate chunks
    let order = mode.expected_chunk_order();
    let mut assignment = assigner.assign_chunks(num_symbols, order)?;
    assignment.ensure_order(order);

    match () {
        _ if assignment.is_empty() => {
            tracing::warn!(app_msg_len = ?app_message.len(), "no chunk generated");
            return Ok(());
        }
        _ if assignment.total_chunks() > MAX_NUM_PACKETS => {
            return Err(BuildError::TooManyChunks);
        }
        _ => { /* ok */ }
    }

    let mut chunks = assignment.generate(layout);

    // step 3. encode and write raptor symbols to each chunk
    if assignment.unique_chunk_id() {
        encode_unique_symbols(&app_message, &mut chunks, layout)?;
    } else {
        encode_symbols(&app_message, &mut chunks, layout)?;
    }

    // step 4. build header (sans signature)
    let header_buf = build_header(
        0, // fixed version
        broadcast_type,
        merkle_tree_depth,
        epoch,
        unix_ts_ms,
        &app_message,
    )?;
    debug_assert_eq!(header_buf.len(), layout.header_sans_signature_range().len());

    // step 5. lookup recipient addresses
    lookup_recipient_addrs(&chunks, peer_lookup);

    if mode.stream_mode() {
        for mut batch in owned_merkle_batches(chunks, layout) {
            // step 6. sign and write headers for this merkle batch
            let merkle_batch = MerkleBatch::from(&mut batch[..]);
            merkle_batch.write_header::<ST>(layout, key, &header_buf)?;

            // step 7. assemble udp messages
            mode.assemble_udp_messages_into(collector, batch, layout);
        }
    } else {
        // step 6. sign and write headers for this merkle batch
        for batch in merkle_batches(&mut chunks, layout) {
            batch.write_header::<ST>(layout, key, &header_buf)?;
        }

        // step 7. assemble udp messages
        mode.assemble_udp_messages_into(collector, chunks, layout);
    }

    Ok(())
}

pub(crate) struct Chunk<PT: PubKey> {
    chunk_id: usize,
    recipient: Recipient<PT>,
    payload: BytesMut,
}

impl<PT: PubKey> Chunk<PT> {
    pub fn new(chunk_id: usize, recipient: Recipient<PT>, payload: BytesMut) -> Self {
        Self {
            chunk_id,
            recipient,
            payload,
        }
    }
}

// A cheaply cloned wrapper around a node_id with pre-calculated hash
// and a lazy socket address.
//
// Change to Arc if we need parallel processing.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct Recipient<PT: PubKey>(Rc<RecipientInner<PT>>);

impl<PT: PubKey> std::hash::Hash for Recipient<PT> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.node_hash.hash(state);
    }
}

impl<PT: PubKey> std::fmt::Debug for Recipient<PT> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "<node-{}>", &hex::encode(&self.0.node_hash[..6]))?;
        if let Some(addr) = self.0.addr.get() {
            if let Some(addr) = addr {
                write!(f, "@{}", addr)?;
            } else {
                write!(f, "@<unknown>")?;
            }
        }
        Ok(())
    }
}

#[derive(Clone, Eq)]
struct RecipientInner<PT: PubKey> {
    node_id: NodeId<PT>,
    node_hash: [u8; 20],
    addr: OnceCell<Option<SocketAddr>>,
}

impl<PT: PubKey> PartialEq for RecipientInner<PT> {
    fn eq(&self, other: &Self) -> bool {
        self.node_hash == other.node_hash
    }
}

impl<PT: PubKey> Recipient<PT> {
    pub fn new(node_id: NodeId<PT>) -> Self {
        let node_hash = crate::util::compute_hash(&node_id).0;
        let addr = OnceCell::new();
        let inner = RecipientInner {
            node_id,
            node_hash,
            addr,
        };
        Self(Rc::new(inner))
    }

    pub(super) fn node_hash(&self) -> &[u8; 20] {
        &self.0.node_hash
    }

    // Expect `lookup` or `set_addr` performed earlier, otherwise panic.
    pub(super) fn get_addr(&self) -> Option<SocketAddr> {
        *self.0.addr.get().expect("get addr called before lookup")
    }

    fn lookup(&self, handle: &impl PeerAddrLookup<PT>) -> &Option<SocketAddr> {
        self.0.addr.get_or_init(|| {
            let addr = handle.lookup(&self.0.node_id);
            if addr.is_none() {
                tracing::warn!("raptorcast: unknown address for node {}", self.0.node_id);
            }
            addr
        })
    }
}

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
const MERKLE_HASH_LEN: usize = 20;

#[derive(Clone, Copy)]
pub(crate) struct PacketLayout {
    chunk_header_start: usize,
    segment_len: usize,
}

impl PacketLayout {
    pub const fn new(segment_len: usize, merkle_tree_depth: u8) -> Self {
        let merkle_hash_count = merkle_tree_depth as usize - 1;
        let merkle_proof_len = merkle_hash_count * MERKLE_HASH_LEN;
        let chunk_header_start = HEADER_LEN + merkle_proof_len;

        Self {
            chunk_header_start,
            segment_len,
        }
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
        SIGNATURE_SIZE..HEADER_LEN
    }

    pub const fn merkle_proof_range(&self) -> Range<usize> {
        HEADER_LEN..self.chunk_header_start
    }

    pub const fn merkle_hashed_range(&self) -> Range<usize> {
        self.chunk_header_start..self.segment_len
    }

    pub const fn chunk_header_range(&self) -> Range<usize> {
        self.chunk_header_start..(self.chunk_header_start + CHUNK_HEADER_LEN)
    }

    pub const fn symbol_range(&self) -> Range<usize> {
        let symbol_start = self.chunk_header_start + CHUNK_HEADER_LEN;
        symbol_start..self.segment_len
    }

    pub const fn symbol_len(&self) -> usize {
        let symbol_start = self.chunk_header_start + CHUNK_HEADER_LEN;
        self.segment_len - symbol_start
    }

    pub const fn segment_len(&self) -> usize {
        self.segment_len
    }

    pub const fn merkle_tree_depth(&self) -> u8 {
        let proof_len = self.chunk_header_start - HEADER_LEN;
        debug_assert!(proof_len < u8::MAX as usize * MERKLE_HASH_LEN);
        debug_assert!(proof_len % MERKLE_HASH_LEN == 0);
        (proof_len / MERKLE_HASH_LEN) as u8 + 1
    }

    pub const fn merkle_batch_len(&self) -> usize {
        2usize
            .checked_pow((self.merkle_tree_depth() - 1) as u32)
            .expect("merkle tree depth too large")
    }
}

impl<PT: PubKey> Chunk<PT> {
    fn chunk_hash(&self, layout: PacketLayout) -> Hash {
        let mut hasher = HasherType::new();
        hasher.update(&self.payload[layout.merkle_hashed_range()]);
        hasher.hash()
    }

    fn symbol(&self, layout: PacketLayout) -> &[u8] {
        &self.payload[layout.symbol_range()]
    }

    fn symbol_mut(&mut self, layout: PacketLayout) -> &mut [u8] {
        &mut self.payload[layout.symbol_range()]
    }

    fn chunk_header_mut(&mut self, layout: PacketLayout) -> &mut [u8] {
        &mut self.payload[layout.chunk_header_range()]
    }

    fn merkle_proof_mut(&mut self, layout: PacketLayout) -> &mut [u8] {
        &mut self.payload[layout.merkle_proof_range()]
    }

    #[inline]
    fn write_chunk_header(&mut self, layout: PacketLayout, merkle_leaf_index: u8) -> Result<()> {
        let recipient_hash = *self.recipient.node_hash();
        let chunk_id: [u8; 2] = u16::try_from(self.chunk_id)
            .map_err(|_| BuildError::ChunkIdOverflow)?
            .to_le_bytes();

        let buffer = self.chunk_header_mut(layout);
        debug_assert_eq!(buffer.len(), CHUNK_HEADER_LEN);

        buffer[0..20].copy_from_slice(&recipient_hash); // node_id hash
        buffer[20] = merkle_leaf_index;
        // buffer[21] = 0; // reserved
        buffer[22..24].copy_from_slice(&chunk_id);

        Ok(())
    }

    fn write_merkle_proof(&mut self, layout: PacketLayout, proof: &[MerkleHash]) {
        let buffer = &mut self.merkle_proof_mut(layout);
        debug_assert_eq!(buffer.len() % MERKLE_HASH_LEN, 0);

        for (idx, hash) in proof.iter().enumerate() {
            let start = idx * MERKLE_HASH_LEN;
            let end = (idx + 1) * MERKLE_HASH_LEN;
            buffer[start..end].copy_from_slice(hash);
        }
    }

    fn write_header(
        &mut self,
        layout: PacketLayout,
        signature: &[u8; SIGNATURE_SIZE],
        header: &Bytes,
    ) {
        self.payload[layout.signature_range()].copy_from_slice(signature);
        self.payload[layout.header_sans_signature_range()].copy_from_slice(header);
    }
}

pub(super) struct MerkleBatch<'a, PT: PubKey> {
    chunks: &'a mut [Chunk<PT>],
}

pub(super) fn merkle_batches<PT: PubKey>(
    all_chunks: &mut [Chunk<PT>],
    layout: PacketLayout,
) -> impl Iterator<Item = MerkleBatch<PT>> {
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
        header: &Bytes,
    ) -> Result<()>
    where
        ST: CertificateSignature,
    {
        // write chunk header and calculate chunk hash to build the
        // merkle tree.
        let merkle_tree = self.build_merkle_tree(layout)?;
        let signature = self.sign::<ST>(key, header, merkle_tree.root());

        for (leaf_index, chunk) in self.chunks.iter_mut().enumerate() {
            // write signature and the rest of the header
            chunk.write_header(layout, &signature, header);

            // write merkle proof
            let proof = merkle_tree.proof(leaf_index as u8);
            chunk.write_merkle_proof(layout, proof.siblings());
        }

        Ok(())
    }

    fn build_merkle_tree(&mut self, layout: PacketLayout) -> Result<MerkleTree> {
        let mut hashes = Vec::with_capacity(self.chunks.len());
        let depth = layout.merkle_tree_depth();
        debug_assert!(self.chunks.len() <= 2usize.pow((depth - 1) as u32));

        for (leaf_index, chunk) in self.chunks.iter_mut().enumerate() {
            let leaf_index = u8::try_from(leaf_index).map_err(|_| BuildError::MerkleTreeTooDeep)?;
            chunk.write_chunk_header(layout, leaf_index)?;
            hashes.push(chunk.chunk_hash(layout));
        }

        Ok(MerkleTree::new_with_depth(&hashes, depth))
    }

    fn sign<ST>(
        &self,
        key: &ST::KeyPairType,
        header: &Bytes,
        merkle_root: &[u8; MERKLE_HASH_LEN],
    ) -> [u8; SIGNATURE_SIZE]
    where
        ST: CertificateSignature,
    {
        let mut buffer = BytesMut::with_capacity(header.len() + MERKLE_HASH_LEN);
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
        let chunk_id = chunk.chunk_id;
        let symbol_buffer = chunk.symbol_mut(layout);
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
        let chunk_id = chunks[i].chunk_id;

        debug_assert!(chunk_id < usize::MAX); // or 1+index will overflow.
        debug_assert!(chunk_id < chunks.len()); // or symbol_chunks access is OOB.

        match symbol_chunks[chunk_id] {
            0 => {
                // This is the first encounter of symbol `chunk_id`.
                let symbol_buffer = chunks[i].symbol_mut(layout);
                encoder.encode_symbol(symbol_buffer, chunk_id);
                symbol_chunks[chunk_id] = i + 1;
            }
            j => {
                // If the symbol has been encoded, reuse the result
                // to avoid the (somewhat) expensive encoding again.
                let [src_chunk, dst_chunk] = chunks
                    .get_disjoint_mut([j - 1, i])
                    .expect("the two chunk index never overlap");

                let dst_buffer = dst_chunk.symbol_mut(layout);
                let src_buffer = src_chunk.symbol(layout);
                dst_buffer.copy_from_slice(src_buffer);
            }
        }
    }

    Ok(())
}

pub(crate) enum BroadcastType {
    Secondary,
    Primary,
    Unspecified,
}

// return the shared header for all chunks sans the signature.
fn build_header(
    version: u16,
    broadcast_type: BroadcastType,
    merkle_tree_depth: u8,
    epoch_no: u64,
    unix_ts_ms: u64,
    app_message: &[u8],
) -> Result<Bytes> {
    // 2  // Version
    // 1  // Broadcast bit
    //       Secondary broadcast bit,
    //       2 unused bits,
    //       4 bits for Merkle Tree Depth
    // 8  // Epoch #
    // 8  // Unix timestamp
    // 20 // AppMessage hash
    // 4  // AppMessage length
    let mut buffer = BytesMut::zeroed(HEADER_LEN - SIGNATURE_SIZE);
    let cursor = &mut buffer;

    let (cursor_version, cursor) = cursor.split_at_mut_checked(2).expect("header to short");
    cursor_version.copy_from_slice(&version.to_le_bytes());

    let (cursor_broadcast_merkle_depth, cursor) =
        cursor.split_at_mut_checked(1).expect("header to short");
    let mut broadcast_byte: u8 = match broadcast_type {
        BroadcastType::Primary => 0b10 << 6,
        BroadcastType::Secondary => 0b01 << 6,
        BroadcastType::Unspecified => 0b00 << 6,
    };
    // tree_depth max 4 bits
    if (merkle_tree_depth & 0b1111_0000) != 0 {
        return Err(BuildError::MerkleTreeTooDeep);
    }
    broadcast_byte |= merkle_tree_depth & 0b0000_1111;
    cursor_broadcast_merkle_depth[0] = broadcast_byte;

    let (cursor_epoch_no, cursor) = cursor.split_at_mut_checked(8).expect("header too short");
    cursor_epoch_no.copy_from_slice(&epoch_no.to_le_bytes());

    let (cursor_unix_ts_ms, cursor) = cursor.split_at_mut_checked(8).expect("header too short");
    cursor_unix_ts_ms.copy_from_slice(&unix_ts_ms.to_le_bytes());

    let (cursor_app_message_hash, cursor) =
        cursor.split_at_mut_checked(20).expect("header too short");
    let app_message_hash = calc_full_hash(app_message);
    cursor_app_message_hash.copy_from_slice(&app_message_hash[..20]);

    let (cursor_app_message_len, cursor) =
        cursor.split_at_mut_checked(4).expect("header too short");
    let app_message_len: u32 = app_message
        .len()
        .try_into()
        .map_err(|_| BuildError::AppMessageTooLarge)?;
    cursor_app_message_len.copy_from_slice(&app_message_len.to_le_bytes());

    // should have consumed the whole buffer
    debug_assert_eq!(cursor.len(), 0);

    Ok(buffer.freeze())
}

fn lookup_recipient_addrs<PT: PubKey, PL: PeerAddrLookup<PT>>(
    chunks: &Vec<Chunk<PT>>,
    handle: &PL,
) {
    for chunk in chunks {
        chunk.recipient.lookup(handle);
    }
}

fn calc_full_hash(bytes: &[u8]) -> Hash {
    let mut hasher = HasherType::new();
    hasher.update(bytes);
    hasher.hash()
}

impl AssembleMode {
    fn expected_chunk_order(self) -> Option<ChunkOrder> {
        match self {
            AssembleMode::GsoFull => Some(ChunkOrder::GsoFriendly),
            AssembleMode::GsoBestEffort => None,
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
        collector: &mut impl Collector<UdpMessage>,
        chunks: Vec<Chunk<PT>>,
        layout: PacketLayout,
    ) {
        match self {
            AssembleMode::GsoFull | AssembleMode::GsoBestEffort => {
                Self::assemble_gso_udp_messages_into(collector, chunks, layout);
            }
            AssembleMode::RoundRobin => {
                Self::assemble_standalone_udp_messages_into(collector, chunks, layout);
            }
        }
    }

    fn assemble_standalone_udp_messages_into<PT: PubKey>(
        collector: &mut impl Collector<UdpMessage>,
        chunks: Vec<Chunk<PT>>,
        layout: PacketLayout,
    ) {
        collector.reserve(chunks.len());
        for chunk in chunks {
            let Some(dest) = chunk.recipient.get_addr() else {
                continue;
            };
            collector.push(UdpMessage {
                dest,
                payload: chunk.payload.freeze(),
                stride: layout.segment_len(),
            });
        }
    }

    fn assemble_gso_udp_messages_into<PT: PubKey>(
        collector: &mut impl Collector<UdpMessage>,
        chunks: Vec<Chunk<PT>>,
        layout: PacketLayout,
    ) {
        struct AggregatedChunk {
            dest: SocketAddr,
            payload: BytesMut,
        }

        impl AggregatedChunk {
            fn from_chunk<PT: PubKey>(chunk: Chunk<PT>, dest: SocketAddr) -> Self {
                Self {
                    dest,
                    payload: chunk.payload,
                }
            }

            fn into_udp_message(self, stride: usize) -> UdpMessage {
                UdpMessage {
                    dest: self.dest,
                    payload: self.payload.freeze(),
                    stride,
                }
            }
        }

        let stride = layout.segment_len();
        let mut agg_chunk = None;

        for chunk in chunks {
            let Some(dest) = chunk.recipient.get_addr() else {
                // skip chunks with unknown recipient
                continue;
            };

            let Some(agg) = &mut agg_chunk else {
                // first chunk, start a new aggregation
                agg_chunk = Some(AggregatedChunk::from_chunk(chunk, dest));
                continue;
            };

            if agg.dest == dest {
                // same recipient, merge the payload BytesMut::unsplit
                // is O(1) when the chunk payload are consecutive.
                agg.payload.unsplit(chunk.payload);
                continue;
            }

            // different recipient, flush the previous message
            let next_agg = AggregatedChunk::from_chunk(chunk, dest);
            let udp_msg = std::mem::replace(agg, next_agg).into_udp_message(stride);
            collector.push(udp_msg);
        }

        if let Some(agg) = agg_chunk.take() {
            collector.push(agg.into_udp_message(stride));
        }
    }
}
