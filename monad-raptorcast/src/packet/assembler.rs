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
use std::{
    cell::OnceCell,
    collections::{HashMap, VecDeque},
    net::SocketAddr,
    ops::Range,
    rc::Rc,
};

use bytes::{Bytes, BytesMut};
use monad_crypto::{
    certificate_signature::{CertificateSignature, CertificateSignaturePubKey, PubKey},
    hasher::{Hash, Hasher as _, HasherType},
    signing_domain,
};
use monad_merkle::{MerkleHash, MerkleTree};
use monad_raptor::Encoder;
use monad_types::NodeId;

use super::{BuildError, ChunkGenerator, PeerAddrLookup, Result, UdpMessage};
use crate::{
    message::MAX_MESSAGE_SIZE,
    udp::{MAX_NUM_PACKETS, MAX_REDUNDANCY, MIN_CHUNK_LENGTH},
    util::Redundancy,
    SIGNATURE_SIZE,
};

#[derive(Debug, Clone, Copy)]
pub(crate) enum BatchingStrategy {
    // each UDP message is a GSO batch of merged adjacent chunks
    GsoConcat,
    // each UDP message is a GSO batch of merged chunks, slightly
    // slower than GsoConcat
    #[allow(unused)]
    GsoGroup,
    // each UDP message contains a single chunk
    #[allow(unused)]
    Standalone,
    // each recipient get their chunks in round-robin order
    #[allow(unused)]
    RoundRobin,
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn assemble<ST, CG, PL>(
    key: &ST::KeyPairType,
    // the size of each udp packet
    segment_len: usize,
    app_message: Bytes,
    redundancy: Redundancy,
    epoch: u64,
    unix_ts_ms: u64,
    broadcast_type: BroadcastType,
    generator: &CG,
    peer_lookup: &PL,
    batching: BatchingStrategy,
) -> Result<Vec<UdpMessage>>
where
    ST: CertificateSignature,
    // ?Sized to allow for passing trait objects
    CG: ChunkGenerator<CertificateSignaturePubKey<ST>> + ?Sized,
    PL: PeerAddrLookup<CertificateSignaturePubKey<ST>>,
{
    let merkle_tree_depth = 6; // TODO: calculate the actual merkle tree depth

    debug_assert!(segment_len > MIN_CHUNK_LENGTH);
    debug_assert!(merkle_tree_depth > 0 && merkle_tree_depth <= 15);

    // run sanity checks
    match () {
        _ if app_message.is_empty() => {
            tracing::warn!("empty application message");
            return Ok(Vec::new());
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

    // step 2. generate chunks
    let mut chunks = generator.generate_chunks(app_message.len(), redundancy, layout)?;
    match () {
        _ if chunks.is_empty() => {
            tracing::warn!(app_msg_len = ?app_message.len(), "no chunk generated");
            return Ok(Vec::new());
        }
        _ if chunks.len() > MAX_NUM_PACKETS => {
            return Err(BuildError::TooManyChunks);
        }
        _ => { /* ok */ }
    }

    // step 3. encode and write raptor symbols to each chunk
    if generator.unique_chunk_id() {
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
    )
    .expect("invalid header fields");
    debug_assert_eq!(header_buf.len(), layout.header_sans_signature_range().len());

    // step 5. write header, chunk_header and signature for each merkle batch
    for merkle_batch in build_merkle_batches(merkle_tree_depth, &mut chunks)? {
        merkle_batch.write_header::<ST>(layout, key, &header_buf)?;
    }

    // step 6. assemble udp messages
    lookup_recipient_addrs(&chunks, peer_lookup);
    Ok(batching.assemble(chunks, layout))
}

pub(crate) struct Chunk<PT: PubKey> {
    chunk_id: usize,
    recipient: Recipient<PT>,
    priority: Option<u8>,
    payload: BytesMut,
}

impl<PT: PubKey> Chunk<PT> {
    pub fn new(chunk_id: usize, recipient: Recipient<PT>, payload: BytesMut) -> Self {
        Self {
            chunk_id,
            recipient,
            priority: None,
            payload,
        }
    }

    pub fn set_priority(&mut self, priority: u8) {
        self.priority = Some(priority);
    }
}

// A cheaply cloned wrapper around a node_id with pre-calculated hash
// and a lazy socket address.
//
// Change to Arc if we need parallel processing.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct Recipient<PT: PubKey>(Rc<RecipientInner<PT>>);

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

    fn node_hash(&self) -> &[u8; 20] {
        &self.0.node_hash
    }

    // Expect `lookup` or `set_addr` performed earlier, otherwise panic.
    fn get_addr(&self) -> Option<SocketAddr> {
        *self.0.addr.get().expect("get addr called before lookup")
    }

    fn lookup(&self, handle: &impl PeerAddrLookup<PT>) -> &Option<SocketAddr> {
        self.0.addr.get_or_init(|| handle.lookup(&self.0.node_id))
    }

    #[cfg(test)]
    fn set_addr(&self, addr: SocketAddr) {
        let _ = self.0.addr.set(Some(addr));
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

    #[cfg(test)]
    pub const fn merkle_tree_depth(&self) -> u8 {
        let proof_len = self.chunk_header_start - HEADER_LEN;
        debug_assert!(proof_len < u8::MAX as usize * MERKLE_HASH_LEN);
        debug_assert!(proof_len % MERKLE_HASH_LEN == 0);
        (proof_len / MERKLE_HASH_LEN) as u8 + 1
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

struct MerkleBatch<'a, PT: PubKey> {
    depth: u8,
    chunks: Vec<&'a mut Chunk<PT>>,
}

fn build_merkle_batches<PT: PubKey>(
    depth: u8,
    all_chunks: &mut [Chunk<PT>],
) -> Result<Vec<MerkleBatch<PT>>> {
    let merkle_chunk_len = 2usize
        .checked_pow(depth as u32 - 1)
        .ok_or(BuildError::MerkleTreeTooDeep)?;

    let num_batches = all_chunks.len().div_ceil(merkle_chunk_len);
    let mut batches = Vec::with_capacity(num_batches);

    for chunks in all_chunks.chunks_mut(merkle_chunk_len) {
        let chunks = chunks.iter_mut().collect();
        batches.push(MerkleBatch { depth, chunks });
    }

    Ok(batches)
}

impl<PT: PubKey> MerkleBatch<'_, PT> {
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
        debug_assert!(self.chunks.len() <= 2usize.pow(self.depth as u32));

        for (leaf_index, chunk) in self.chunks.iter_mut().enumerate() {
            let leaf_index = u8::try_from(leaf_index).map_err(|_| BuildError::MerkleTreeTooDeep)?;
            chunk.write_chunk_header(layout, leaf_index)?;
            hashes.push(chunk.chunk_hash(layout));
        }

        Ok(MerkleTree::new_with_depth(&hashes, self.depth))
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

        let signature = ST::sign::<signing_domain::RaptorcastChunk>(&buffer, key).serialize();
        debug_assert_eq!(signature.len(), SIGNATURE_SIZE);
        signature.try_into().expect("invalid signature size")
    }

    #[cfg(test)]
    fn write_chunk_headers(&mut self, layout: PacketLayout) -> Result<()> {
        for (leaf_index, chunk) in self.chunks.iter_mut().enumerate() {
            let leaf_index = u8::try_from(leaf_index).map_err(|_| BuildError::MerkleTreeTooDeep)?;
            chunk.write_chunk_header(layout, leaf_index)?;
        }
        Ok(())
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
) -> Option<Bytes> {
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

    let (cursor_version, cursor) = cursor.split_at_mut_checked(2)?;
    cursor_version.copy_from_slice(&version.to_le_bytes());

    let (cursor_broadcast_merkle_depth, cursor) = cursor.split_at_mut_checked(1)?;
    let mut broadcast_byte: u8 = match broadcast_type {
        BroadcastType::Primary => 0b10 << 6,
        BroadcastType::Secondary => 0b01 << 6,
        BroadcastType::Unspecified => 0b00 << 6,
    };
    // tree_depth max 4 bits
    debug_assert!((merkle_tree_depth & 0b1111_0000) == 0);
    broadcast_byte |= merkle_tree_depth & 0b0000_1111;
    cursor_broadcast_merkle_depth[0] = broadcast_byte;

    let (cursor_epoch_no, cursor) = cursor.split_at_mut_checked(8)?;
    cursor_epoch_no.copy_from_slice(&epoch_no.to_le_bytes());

    let (cursor_unix_ts_ms, cursor) = cursor.split_at_mut_checked(8)?;
    cursor_unix_ts_ms.copy_from_slice(&unix_ts_ms.to_le_bytes());

    let (cursor_app_message_hash, cursor) = cursor.split_at_mut_checked(20)?;
    let app_message_hash = calc_full_hash(app_message);
    cursor_app_message_hash.copy_from_slice(&app_message_hash[..20]);

    let (cursor_app_message_len, cursor) = cursor.split_at_mut_checked(4)?;
    let app_message_len: u32 = app_message.len().try_into().ok()?;
    cursor_app_message_len.copy_from_slice(&app_message_len.to_le_bytes());

    // should have consumed the whole buffer
    debug_assert_eq!(cursor.len(), 0);

    Some(buffer.freeze())
}

fn lookup_recipient_addrs<PT: PubKey, PL: PeerAddrLookup<PT>>(
    chunks: &Vec<Chunk<PT>>,
    handle: &PL,
) {
    for chunk in chunks {
        chunk.recipient.lookup(handle);
    }
}

impl BatchingStrategy {
    pub(crate) fn assemble<PT>(
        self,
        chunks: Vec<Chunk<PT>>,
        layout: PacketLayout,
    ) -> Vec<UdpMessage>
    where
        PT: PubKey,
    {
        use BatchingStrategy::{GsoConcat, GsoGroup, RoundRobin, Standalone};

        match self {
            GsoConcat => Self::concat(chunks, layout),
            GsoGroup => Self::group(chunks, layout),
            Standalone => Self::standalone(chunks, layout),
            RoundRobin => Self::round_robin(chunks, layout),
        }
    }

    fn concat<PT>(chunks: Vec<Chunk<PT>>, layout: PacketLayout) -> Vec<UdpMessage>
    where
        PT: PubKey,
    {
        struct AggregatedChunk {
            dest: SocketAddr,
            priority: Option<u8>,
            payload: BytesMut,
        }

        impl AggregatedChunk {
            fn from_chunk<PT: PubKey>(chunk: Chunk<PT>, dest: SocketAddr) -> Self {
                Self {
                    dest,
                    priority: chunk.priority,
                    payload: chunk.payload,
                }
            }

            fn into_udp_message(self, stride: usize) -> UdpMessage {
                UdpMessage {
                    dest: self.dest,
                    priority: self.priority,
                    payload: self.payload.freeze(),
                    stride,
                }
            }
        }

        let stride = layout.segment_len();
        let mut agg_chunk = None;
        let mut out_messages = Vec::with_capacity(chunks.len());

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

            if agg.dest == dest && agg.priority == chunk.priority {
                // same recipient and priority, merge the payload
                // BytesMut::unsplit is O(1) when the chunk payload
                // are consecutive.
                agg.payload.unsplit(chunk.payload);
                continue;
            }

            // different recipient or priority, flush the previous message
            let next_agg = AggregatedChunk::from_chunk(chunk, dest);
            let udp_msg = std::mem::replace(agg, next_agg).into_udp_message(stride);
            out_messages.push(udp_msg);
        }

        if let Some(agg) = agg_chunk.take() {
            out_messages.push(agg.into_udp_message(stride));
        }

        out_messages
    }

    fn group<PT>(chunks: Vec<Chunk<PT>>, layout: PacketLayout) -> Vec<UdpMessage>
    where
        PT: PubKey,
    {
        #[derive(Hash, PartialEq, Eq, Clone, Copy)]
        struct GroupKey {
            dest: SocketAddr,
            priority: Option<u8>,
        }

        let mut messages: HashMap<GroupKey, BytesMut> = HashMap::new();

        for chunk in chunks {
            let Some(dest) = chunk.recipient.get_addr() else {
                continue;
            };

            let key = GroupKey {
                dest,
                priority: chunk.priority,
            };

            // BytesMut::unsplit is O(1) when the chunk payload are consecutive.
            messages.entry(key).or_default().unsplit(chunk.payload);
        }

        messages
            .into_iter()
            .map(|(key, payload)| UdpMessage {
                dest: key.dest,
                priority: key.priority,
                payload: payload.freeze(),
                stride: layout.segment_len(),
            })
            .collect()
    }

    fn standalone<PT>(chunks: Vec<Chunk<PT>>, layout: PacketLayout) -> Vec<UdpMessage>
    where
        PT: PubKey,
    {
        chunks
            .into_iter()
            .filter_map(|chunk| {
                Some(UdpMessage {
                    dest: chunk.recipient.get_addr()?,
                    priority: chunk.priority,
                    payload: chunk.payload.freeze(),
                    stride: layout.segment_len(),
                })
            })
            .collect()
    }

    fn round_robin<PT>(chunks: Vec<Chunk<PT>>, layout: PacketLayout) -> Vec<UdpMessage>
    where
        PT: PubKey,
    {
        let mut out_messages = Vec::with_capacity(chunks.len());

        let mut buckets: HashMap<SocketAddr, VecDeque<Chunk<PT>>> = HashMap::new();
        for chunk in chunks {
            let Some(dest) = chunk.recipient.get_addr() else {
                continue;
            };

            buckets.entry(dest).or_default().push_back(chunk);
        }

        // Each recipient get their own queue of chunks.
        let mut queues: Vec<VecDeque<Chunk<PT>>> = buckets.into_values().collect();

        // Optimized algorithm:
        //
        // 1. go through each recipient's queue in order
        // 2. if the queue is not empty, add the front chunk into the output
        // 3. otherwise, delete the empty queue from the Vec of queues
        // 4. repeat until there is no queue left
        while !queues.is_empty() {
            queues.retain_mut(|queue| {
                let Some(chunk) = queue.pop_front() else {
                    return false; // remove the empty queue
                };
                let dest = chunk
                    .recipient
                    .get_addr()
                    .expect("unknown addr already filtered out");
                let message = UdpMessage {
                    dest,
                    priority: chunk.priority,
                    payload: chunk.payload.freeze(),
                    stride: layout.segment_len(),
                };
                out_messages.push(message);
                true // keep the non-empty queue
            });
        }

        out_messages
    }
}

fn calc_full_hash(bytes: &[u8]) -> Hash {
    let mut hasher = HasherType::new();
    hasher.update(bytes);
    hasher.hash()
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, net::SocketAddr, ops::Range};

    use bytes::BytesMut;
    use monad_crypto::certificate_signature::CertificateSignaturePubKey;
    use monad_secp::SecpSignature;
    use monad_testutil::signing::get_key;
    use monad_types::NodeId;

    use super::{build_merkle_batches, Chunk, Recipient};
    use crate::packet::{assembler::BatchingStrategy, PacketLayout};

    const DEFAULT_SEGMENT_LEN: usize = 1400;
    const DEFAULT_MERKLE_TREE_DEPTH: u8 = 6;
    const DEFAULT_LAYOUT: PacketLayout =
        PacketLayout::new(DEFAULT_SEGMENT_LEN, DEFAULT_MERKLE_TREE_DEPTH);

    type ST = SecpSignature;
    type PT = CertificateSignaturePubKey<ST>;

    fn node_id(seed: u64) -> NodeId<PT> {
        let key_pair = get_key::<ST>(seed);
        NodeId::new(key_pair.pubkey())
    }

    #[test]
    fn test_round_robin() {
        let layout = DEFAULT_LAYOUT;
        let mut chunks = gen_chunks(
            &[
                (1, 0..4),    // 4 chunks for node 1
                (2, 4..5),    // 1 chunk for node 2
                (3, 5..6),    // 1 chunk for node 3
                (4, 10..100), // 90 chunks for node 4 (discontinuity intended)
            ],
            layout,
        );
        fill_chunk_headers(&mut chunks, layout);

        let num_chunks = chunks.len();
        assert_eq!(num_chunks, 96);

        let batching = BatchingStrategy::RoundRobin;
        let mut udp_messages = batching.assemble(chunks, layout);

        assert_eq!(udp_messages.len(), num_chunks);

        // each round is a set of (node_idx, chunk_id range)
        let expected_rounds: &[&[(u8, Range<usize>)]] = &[
            // first round
            &[(1, 0..1), (2, 4..5), (3, 5..6), (4, 10..11)],
            // second round
            &[(1, 1..2), (4, 11..12)],
            // third round
            &[(1, 2..3), (4, 12..13)],
            // fourth round
            &[(1, 3..4), (4, 13..14)],
            // remaining rounds
            &[(4, 14..100)],
        ];

        for round in expected_rounds {
            let mut num_msgs = 0;
            let mut expected_msgs = HashSet::new();

            for (node, chunk_range) in round.iter() {
                for chunk_id in chunk_range.clone() {
                    expected_msgs.insert((*node, chunk_id));
                }
                num_msgs += chunk_range.len();
            }

            for msg in udp_messages.drain(0..num_msgs) {
                assert_eq!(msg.payload.len(), layout.segment_len());
                let node = addr_to_node(msg.dest);
                let chunk_id = get_chunk_id(&msg.payload, layout);

                assert!(expected_msgs.contains(&(node, chunk_id as usize)));
                expected_msgs.remove(&(node, chunk_id as usize));
            }

            assert!(expected_msgs.is_empty());
        }
    }

    // Generate chunks with a given specification. Each entry in
    // `spec` is a pair of node index and the range of chunk_ids to
    // generate for that node.
    //
    // Note: chunk generated will already have the recipient's socket address
    // assigned.
    fn gen_chunks(spec: &[(u8, Range<usize>)], layout: PacketLayout) -> Vec<Chunk<PT>> {
        let mut chunks = Vec::new();

        for (recipient_idx, num_chunks) in spec {
            let recipient = Recipient::new(node_id(*recipient_idx as u64));
            recipient.set_addr(gen_addr(*recipient_idx));
            for chunk_id in num_chunks.clone() {
                let payload = BytesMut::zeroed(layout.segment_len());
                chunks.push(Chunk::new(chunk_id, recipient.clone(), payload));
            }
        }

        chunks
    }

    fn fill_chunk_headers(chunks: &mut [Chunk<PT>], layout: PacketLayout) {
        let depth = layout.merkle_tree_depth();
        for mut merkle_batch in build_merkle_batches(depth, chunks).unwrap() {
            merkle_batch.write_chunk_headers(layout).unwrap();
        }
    }

    fn gen_addr(node: u8) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, node], 1000 + node as u16))
    }

    fn addr_to_node(addr: SocketAddr) -> u8 {
        let SocketAddr::V4(addr) = addr else {
            panic!("invalid adr");
        };

        let [127, 0, 0, octet] = addr.ip().octets() else {
            panic!("invalid addr");
        };

        let port = addr.port();
        assert_eq!(octet as u16 + 1000, port);

        octet
    }

    fn get_chunk_id(segment: &[u8], layout: PacketLayout) -> u16 {
        let header = &segment[layout.chunk_header_range()];
        u16::from_le_bytes(header[22..24].try_into().expect("invalid chunk header"))
    }
}
