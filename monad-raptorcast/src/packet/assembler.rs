#![allow(clippy::identity_op)]

use std::{cell::OnceCell, collections::HashMap, net::SocketAddr, ops::Range, rc::Rc};

use bytes::{Bytes, BytesMut};
use monad_crypto::{
    certificate_signature::{CertificateSignature, CertificateSignaturePubKey, PubKey},
    hasher::{Hash, Hasher as _, HasherType},
    signing_domain,
};
use monad_merkle::{MerkleHash, MerkleTree};
use monad_raptor::Encoder;
use monad_types::NodeId;

use super::{BuildError, ChunkGenerator, PeerLookup, Result, UdpMessage};
use crate::{util::Redundancy, SIGNATURE_SIZE};

#[allow(clippy::too_many_arguments)]
pub(crate) fn assemble<ST, CG, PL>(
    key: &ST::KeyPairType,
    segment_len: usize, // the maximum size of each chunk
    app_message: Bytes,
    redundancy: Redundancy,
    epoch: u64,
    unix_ts_ms: u64,
    broadcast_type: BroadcastType,
    generator: &CG,
    peer_lookup: &PL,
) -> Result<Vec<UdpMessage>>
where
    ST: CertificateSignature,
    // ?Sized to allow for trait objects
    CG: ChunkGenerator<CertificateSignaturePubKey<ST>> + ?Sized,
    PL: PeerLookup<CertificateSignaturePubKey<ST>>,
{
    // step 1. calc chunk layout
    let merkle_tree_depth = 6; // TODO: calculate the actual merkle tree depth
    let layout = PacketLayout::new(segment_len, merkle_tree_depth);

    // step 2. generate chunks
    let mut chunks = generator.generate_chunks(app_message.len(), redundancy, layout)?;

    // step 3. encode and write raptor symbols
    encode_symbols(&app_message, layout, &mut chunks)?;

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

    // step 5. build merkle batches, write header, and sign the packet
    for merkle_batch in build_merkle_batches(6, &mut chunks)? {
        merkle_batch.write_header::<ST>(layout, key, &header_buf)?;
    }

    // step 6. combine chunks to udp messages
    Ok(serialize(chunks, peer_lookup))
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
#[derive(Clone)]
pub(crate) struct Recipient<PT: PubKey>(Rc<RecipientInner<PT>>);

struct RecipientInner<PT: PubKey> {
    node_id: NodeId<PT>,
    node_hash: [u8; 20],
    addr: OnceCell<Option<SocketAddr>>,
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

    fn get_addr(&self, lookup: &impl PeerLookup<PT>) -> Option<SocketAddr> {
        *self.0.addr.get_or_init(|| lookup.lookup(&self.0.node_id))
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

const CHUNK_HEADER_LEN: usize = 0
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
    pub fn new(segment_len: usize, merkle_tree_depth: u8) -> Self {
        let merkle_hash_count = 2usize.pow(merkle_tree_depth as u32 - 1);
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

    fn write_chunk_header(&mut self, layout: PacketLayout, merkle_leaf_index: u8) -> Result<()> {
        let recipient_hash = *self.recipient.node_hash();
        let chunk_id: [u8; 2] = u16::try_from(self.chunk_id)
            .map_err(|_| BuildError::ChunkIdOverflow)?
            .to_le_bytes();

        let buffer = self.chunk_header_mut(layout);
        debug_assert_eq!(buffer.len(), CHUNK_HEADER_LEN);

        buffer[0..20].copy_from_slice(&recipient_hash); // node_id hash
        buffer[20] = merkle_leaf_index;
        buffer[21] = 0; // reserved
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

    // TODO: handle the case where the same chunk is allocated to
    // multiple recipients.
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
        // write chunk header and build merkle tree
        let merkle_tree = self.build_merkle_tree(layout)?;
        let signature = self.sign::<ST>(key, header, &merkle_tree);

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
        merkle_tree: &MerkleTree,
    ) -> [u8; SIGNATURE_SIZE]
    where
        ST: CertificateSignature,
    {
        let mut buffer = BytesMut::with_capacity(header.len() + MERKLE_HASH_LEN);
        buffer.extend_from_slice(header);
        buffer.extend_from_slice(merkle_tree.root());

        let signature = ST::sign::<signing_domain::RaptorcastChunk>(&buffer, &key).serialize();
        debug_assert_eq!(signature.len(), SIGNATURE_SIZE);
        signature.try_into().expect("invalid signature size")
    }
}

fn encode_symbols<PT: PubKey>(
    app_message: &[u8],
    layout: PacketLayout,
    chunks: &mut [Chunk<PT>],
) -> Result<()> {
    let symbol_len = layout.symbol_len();
    let encoder =
        Encoder::new(&app_message, symbol_len).map_err(|_| BuildError::EncoderCreationFailed)?;

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
                symbol_chunks.insert(chunk_id, i);
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
    Unicast,
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

    let mut buffer = [0u8; HEADER_LEN - SIGNATURE_SIZE];
    let cursor = &mut buffer;

    let (cursor_version, cursor) = cursor.split_at_mut_checked(2)?;
    cursor_version.copy_from_slice(&version.to_le_bytes());

    let (cursor_broadcast_merkle_depth, cursor) = cursor.split_at_mut_checked(1)?;
    let mut broadcast_byte: u8 = match broadcast_type {
        BroadcastType::Primary   => 1 << 7,
        BroadcastType::Secondary => 1 << 6,
        BroadcastType::Unicast => 0,
    };
    // tree_depth max 4 bits
    debug_assert!(merkle_tree_depth & 0b1111_0000 == 0);
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

    Some(Bytes::from_owner(buffer))
}

fn serialize<PT, PL>(chunks: Vec<Chunk<PT>>, peer_lookup: &PL) -> Vec<UdpMessage>
where
    PT: PubKey,
    PL: PeerLookup<PT>,
{
    #[derive(Hash, PartialEq, Eq)]
    struct GroupKey {
        dest: SocketAddr,
        priority: Option<u8>,
        stride: usize,
    }

    let mut messages: HashMap<GroupKey, BytesMut> = HashMap::new();

    for chunk in chunks {
        let Some(dest) = chunk.recipient.get_addr(peer_lookup) else {
            continue;
        };

        let key = GroupKey {
            dest,
            priority: chunk.priority,
            stride: chunk.payload.len(),
        };

        // BytesMut::unsplit is O(1) when the chunk payload are consecutive.
        messages.entry(key).or_default().unsplit(chunk.payload);
    }

    messages
        .into_iter()
        .map(|(key, payload)| UdpMessage {
            dest: key.dest,
            stride: key.stride,
            priority: key.priority,
            payload: payload.freeze(),
        })
        .collect()
}

fn calc_full_hash(bytes: &[u8]) -> Hash {
    let mut hasher = HasherType::new();
    hasher.update(bytes);
    hasher.hash()
}
