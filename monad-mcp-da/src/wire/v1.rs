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

//! Deterministic raptorcast v1 packets, compatible with
//! monad-raptorcast.
//!
//! A v1 packet has a fixed length and carries one chunk: the header,
//! then the chunk body.
//!
//! Exports: write_chunk/read_chunk.

use bytes::{Buf as _, BufMut as _, Bytes, BytesMut};
use monad_crypto::hasher::{Hash, Hasher as _, HasherType};
use monad_merkle::{MerkleProof, MerkleTree};

use super::top_level::{
    chunk::{Chunk, ChunkData, WireChunkId},
    encoding_scheme::DAEncodingScheme as _,
    types::{D25, EncodingScheme, MerkleHash, MerkleRoot, ProposalHeader, ProposalSignature, Slot},
};
use crate::spec::{DAMerkleRoot as _, DAProposalSignature as _};

pub const VERSION: u16 = 1;

// ethernet MTU minus the ip, udp and wireauth headers
pub const SEGMENT_LEN: usize = 1440;

const SIGNATURE_LEN: usize = 65;
const ROOT_LEN: usize = 20;
const HASH_LEN: usize = 20;
const CHUNK_HEADER_LEN: usize = 4;

// version(2) mode+depth(1) scheme(1) round(8) epoch(8) unix_ts(8)
// root(20) msg_len(4)
pub const SIGNED_LEN: usize = 2 + 1 + 1 + 8 + 8 + 8 + ROOT_LEN + 4;
pub const HEADER_LEN: usize = SIGNATURE_LEN + SIGNED_LEN;

// proof(var) chunk_header(4) symbol(var)
pub const BODY_LEN: usize = SEGMENT_LEN - HEADER_LEN;

pub const MIN_DEPTH: u8 = 3;
pub const MAX_DEPTH: u8 = 15;

// the mode+depth byte: 2 mode bits, 2 unused bits, 4 depth bits
const PRIMARY_MODE: u8 = 0b10 << 6;
const DEPTH_MASK: u8 = 0b0000_1111;

const D25_VARIANT: u8 = 0x1;

// mcp only uses slot. fix the epoch field to constant.
const EPOCH: u64 = 0;

// the caller must ensure depth is in MIN_DEPTH..MAX_DEPTH.
pub const fn symbol_len(depth: u8) -> usize {
    BODY_LEN - proof_len(depth) - CHUNK_HEADER_LEN
}

const fn proof_len(depth: u8) -> usize {
    HASH_LEN * (depth as usize - 1)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MalformedPacket {
    // not exactly one segment
    BadLength(usize),
    UnknownVersion(u16),
    // the mode bits and the unused bits
    BadMode(u8),
    DepthOutOfRange(u8),
    UnknownScheme(u8),
    SlotOutOfRange(u64),
    NonZeroEpoch(u64),
    BadSignature,
    BadRoot,
    // the chunk header's reserved bytes
    ReservedNonZero,
}

// the signing preimage: the header after the signature. The caller
// must ensure the scheme's depth is in range.
pub(crate) fn signed_bytes(
    slot: Slot,
    scheme: &EncodingScheme,
    root: &MerkleRoot,
) -> [u8; SIGNED_LEN] {
    let EncodingScheme::D25(d25) = scheme;
    assert!((MIN_DEPTH..=MAX_DEPTH).contains(&d25.depth));

    let mut root_field = [0u8; ROOT_LEN];
    root.to_bytes(&mut root_field);

    let mut out = [0u8; SIGNED_LEN];
    let mut cursor = &mut out[..];
    cursor.put_u16_le(VERSION);
    cursor.put_u8(PRIMARY_MODE | d25.depth);
    cursor.put_u8(D25_VARIANT);
    cursor.put_u64_le(slot.get());
    cursor.put_u64_le(EPOCH);
    cursor.put_u64_le(d25.unix_ts);
    cursor.put_slice(&root_field);
    cursor.put_u32_le(d25.msg_len);
    debug_assert!(cursor.is_empty());
    out
}

pub fn write_chunk(chunk: &Chunk<'_>) -> Bytes {
    let header = chunk.header();
    let depth = header.scheme.depth();

    let mut out = BytesMut::with_capacity(SEGMENT_LEN);
    write_header(header, &mut out);
    write_body(depth, chunk.chunk_id(), chunk.data(), &mut out);
    debug_assert_eq!(out.len(), SEGMENT_LEN);
    out.freeze()
}

fn write_header(header: &ProposalHeader, out: &mut BytesMut) {
    let mut signature = [0u8; SIGNATURE_LEN];
    header.sig.to_bytes(&mut signature);
    out.put_slice(&signature);
    out.put_slice(&signed_bytes(header.slot, &header.scheme, &header.root));
}

// the caller must ensure the data is shaped by the depth
fn write_body(depth: u8, chunk_id: WireChunkId, data: &ChunkData, out: &mut BytesMut) {
    assert_eq!(data.proof.len(), depth as usize - 1);
    assert_eq!(data.symbol.len(), symbol_len(depth));

    for hash in &data.proof {
        out.put_slice(&hash.0);
    }
    out.put_slice(&chunk_header(chunk_id));
    out.put_slice(&data.symbol);
}

fn chunk_header(chunk_id: WireChunkId) -> [u8; CHUNK_HEADER_LEN] {
    let [lo, hi] = chunk_id.to_le_bytes();
    [0, 0, lo, hi]
}

pub fn read_chunk(bytes: Bytes) -> Result<Chunk<'static>, MalformedPacket> {
    let Some((header, body)) = bytes.split_first_chunk::<HEADER_LEN>() else {
        return Err(MalformedPacket::BadLength(bytes.len()));
    };
    if body.len() != BODY_LEN {
        return Err(MalformedPacket::BadLength(bytes.len()));
    }

    let header = read_header(header)?;
    let depth = header.scheme.depth();
    let (chunk_id, data) = read_body(depth, bytes.slice(HEADER_LEN..))?;
    Ok(Chunk::new(header, chunk_id, data))
}

fn read_header(bytes: &[u8; HEADER_LEN]) -> Result<ProposalHeader, MalformedPacket> {
    let (signature, mut signed) = bytes.split_at(SIGNATURE_LEN);
    let sig = ProposalSignature::from_bytes(signature).ok_or(MalformedPacket::BadSignature)?;

    let version = signed.get_u16_le();
    if version != VERSION {
        return Err(MalformedPacket::UnknownVersion(version));
    }

    let mode_depth = signed.get_u8();
    let mode = mode_depth & !DEPTH_MASK;
    if mode != PRIMARY_MODE {
        return Err(MalformedPacket::BadMode(mode));
    }
    let depth = mode_depth & DEPTH_MASK;
    if !(MIN_DEPTH..=MAX_DEPTH).contains(&depth) {
        return Err(MalformedPacket::DepthOutOfRange(depth));
    }

    let variant = signed.get_u8();
    if variant != D25_VARIANT {
        return Err(MalformedPacket::UnknownScheme(variant));
    }

    let slot = signed.get_u64_le();
    let slot = Slot::from_u64(slot).ok_or(MalformedPacket::SlotOutOfRange(slot))?;

    let epoch = signed.get_u64_le();
    if epoch != EPOCH {
        return Err(MalformedPacket::NonZeroEpoch(epoch));
    }

    let unix_ts = signed.get_u64_le();

    let (root, rest) = signed.split_at(ROOT_LEN);
    let root = MerkleRoot::from_bytes(root).ok_or(MalformedPacket::BadRoot)?;
    signed = rest;

    let msg_len = signed.get_u32_le();
    debug_assert!(signed.is_empty());

    let scheme = EncodingScheme::D25(D25 {
        msg_len,
        unix_ts,
        depth,
    });
    Ok(ProposalHeader {
        slot,
        root,
        scheme,
        sig,
    })
}

// the caller must pass exactly BODY_LEN bytes
fn read_body(depth: u8, body: Bytes) -> Result<(WireChunkId, ChunkData), MalformedPacket> {
    let proof_end = proof_len(depth);
    let symbol_start = proof_end + CHUNK_HEADER_LEN;

    let mut proof = Vec::with_capacity(depth as usize - 1);
    for hash in body[..proof_end].chunks_exact(HASH_LEN) {
        proof.push(MerkleHash(hash.try_into().expect("HASH_LEN bytes")));
    }

    let chunk_header: [u8; CHUNK_HEADER_LEN] = body[proof_end..symbol_start]
        .try_into()
        .expect("CHUNK_HEADER_LEN bytes");
    let [reserved0, reserved1, lo, hi] = chunk_header;
    if reserved0 != 0 || reserved1 != 0 {
        return Err(MalformedPacket::ReservedNonZero);
    }
    let chunk_id = WireChunkId::from_le_bytes([lo, hi]);

    let data = ChunkData {
        symbol: body.slice(symbol_start..),
        proof: proof.into_boxed_slice(),
    };
    Ok((chunk_id, data))
}

// the leaf commits to the chunk's wire bytes from its chunk header on
pub(crate) fn leaf_hash(chunk_id: WireChunkId, symbol: &[u8]) -> Hash {
    let mut hasher = HasherType::new();
    hasher.update(chunk_header(chunk_id));
    hasher.update(symbol);
    hasher.hash()
}

// the merkle tree over a proposal's leaves
pub(crate) struct Tree(MerkleTree);

impl Tree {
    // the caller must ensure the depth is in range and the leaves fit it
    pub(crate) fn new(depth: u8, leaves: &[Hash]) -> Self {
        Self(MerkleTree::new_with_depth(leaves, depth))
    }

    pub(crate) fn root(&self) -> MerkleRoot {
        MerkleRoot(MerkleHash(*self.0.root()))
    }

    // the caller must ensure the leaf exists
    pub(crate) fn proof(&self, leaf_idx: WireChunkId) -> Box<[MerkleHash]> {
        let proof = self.0.proof(leaf_idx);
        let mut siblings = Vec::with_capacity(proof.siblings().len());
        for hash in proof.siblings() {
            siblings.push(MerkleHash(*hash));
        }
        siblings.into_boxed_slice()
    }
}

pub(crate) fn verify_proof(
    root: &MerkleRoot,
    leaf_idx: WireChunkId,
    leaf: &Hash,
    proof: &[MerkleHash],
) -> bool {
    let mut siblings = Vec::with_capacity(proof.len());
    for hash in proof {
        siblings.push(hash.0);
    }
    let Some(proof) = MerkleProof::new_from_leaf_idx(siblings, leaf_idx) else {
        return false;
    };
    let Some(computed) = proof.compute_root(leaf) else {
        return false;
    };
    MerkleRoot(MerkleHash(computed)) == *root
}

#[cfg(test)]
mod tests {
    use super::{
        super::top_level::{
            chunk::ProposalEnvelope,
            test_util::{SLOT, author, epoch_handle, group, proposal_chunks, proposal_chunks_from},
            types::NodeId,
        },
        *,
    };

    fn corrupted(
        chunk: &Chunk<'_>,
        corrupt: impl FnOnce(&mut Vec<u8>),
    ) -> Result<Chunk<'static>, MalformedPacket> {
        let mut bytes = write_chunk(chunk).to_vec();
        corrupt(&mut bytes);
        read_chunk(Bytes::from(bytes))
    }

    #[test]
    fn a_one_chunk_envelope_is_one_segment_at_the_prod_offsets() {
        let epoch_handle = epoch_handle();
        let (header, chunks) = proposal_chunks(&epoch_handle, 1);
        let EncodingScheme::D25(d25) = header.scheme;
        let (_, chunk_id, data) = chunks[3].clone().into_parts();

        let bytes = write_chunk(&chunks[3]);
        assert_eq!(bytes.len(), SEGMENT_LEN);

        let mut signature = [0u8; 65];
        header.sig.to_bytes(&mut signature);
        assert_eq!(bytes[..65], signature);
        assert_eq!(bytes[65..67], [1, 0]);
        assert_eq!(bytes[67], 0b1000_0000 | d25.depth);
        assert_eq!(bytes[68], 0x1);
        assert_eq!(bytes[69..77], SLOT.get().to_le_bytes());
        assert_eq!(bytes[77..85], [0; 8]);
        assert_eq!(bytes[85..93], d25.unix_ts.to_le_bytes());
        assert_eq!(bytes[93..113], header.root.0.0);
        assert_eq!(bytes[113..117], d25.msg_len.to_le_bytes());

        let proof_end = 117 + 20 * (d25.depth as usize - 1);
        assert_eq!(bytes[117..137], data.proof[0].0);
        assert_eq!(bytes[proof_end..proof_end + 4], [0, 0, chunk_id as u8, 0]);
        assert_eq!(bytes[proof_end + 4..], data.symbol);
    }

    #[test]
    fn chunks_round_trip_and_regroup_into_their_envelope() {
        let epoch_handle = epoch_handle();
        let (_, chunks) = proposal_chunks(&epoch_handle, 1);
        for chunk in &chunks {
            assert_eq!(read_chunk(write_chunk(chunk)), Ok(chunk.clone()));
        }

        // an envelope crosses the wire as one packet per chunk
        let envelope = group(&chunks[1..4]);
        let mut received = Vec::new();
        for chunk in envelope.chunks() {
            received.push(read_chunk(write_chunk(&chunk)).expect("well-formed"));
        }
        let regrouped: Vec<_> = ProposalEnvelope::group(received).collect();
        assert_eq!(regrouped, vec![envelope]);
    }

    #[test]
    fn the_signature_binds_the_signed_bytes() {
        let epoch_handle = epoch_handle();
        let (header, _) = proposal_chunks(&epoch_handle, 1);
        let signed = signed_bytes(header.slot, &header.scheme, &header.root);
        assert_eq!(header.sig.recover_author(&signed), Some(author()));

        let mut altered = signed;
        altered[SIGNED_LEN - 1] ^= 1;
        assert_eq!(header.sig.recover_author(&altered), None);

        // the same bytes signed by another author recover that author
        let (other, _) = proposal_chunks_from(&epoch_handle, 2, SLOT, 1);
        assert_eq!(other.sig.recover_author(&signed), Some(NodeId::dummy(2)));
    }

    #[test]
    fn malformed_packets_are_rejected() {
        let epoch_handle = epoch_handle();
        let (header, chunks) = proposal_chunks(&epoch_handle, 1);
        let EncodingScheme::D25(d25) = header.scheme;
        let chunk = &chunks[0];
        let reserved_at = HEADER_LEN + proof_len(d25.depth);

        let truncated_header = corrupted(chunk, |b| b.truncate(HEADER_LEN - 1));
        assert_eq!(truncated_header, Err(MalformedPacket::BadLength(116)));
        let truncated_body = corrupted(chunk, |b| b.truncate(SEGMENT_LEN - 1));
        assert_eq!(truncated_body, Err(MalformedPacket::BadLength(1439)));
        let oversized = corrupted(chunk, |b| b.push(0));
        assert_eq!(oversized, Err(MalformedPacket::BadLength(1441)));

        let version = corrupted(chunk, |b| b[65] = 2);
        assert_eq!(version, Err(MalformedPacket::UnknownVersion(2)));
        let mode = corrupted(chunk, |b| b[67] |= 0b0100_0000);
        assert_eq!(mode, Err(MalformedPacket::BadMode(0b1100_0000)));
        let unused_bits = corrupted(chunk, |b| b[67] |= 0b0010_0000);
        assert_eq!(unused_bits, Err(MalformedPacket::BadMode(0b1010_0000)));
        let shallow = corrupted(chunk, |b| b[67] = 0b1000_0000 | 2);
        assert_eq!(shallow, Err(MalformedPacket::DepthOutOfRange(2)));
        let scheme = corrupted(chunk, |b| b[68] = 2);
        assert_eq!(scheme, Err(MalformedPacket::UnknownScheme(2)));
        let slot = corrupted(chunk, |b| {
            b[69..77].copy_from_slice(&u64::MAX.to_le_bytes())
        });
        assert_eq!(slot, Err(MalformedPacket::SlotOutOfRange(u64::MAX)));
        let epoch = corrupted(chunk, |b| b[77] = 1);
        assert_eq!(epoch, Err(MalformedPacket::NonZeroEpoch(1)));
        let padding = corrupted(chunk, |b| b[64] = 1);
        assert_eq!(padding, Err(MalformedPacket::BadSignature));
        let reserved = corrupted(chunk, |b| b[reserved_at] = 1);
        assert_eq!(reserved, Err(MalformedPacket::ReservedNonZero));
    }

    #[test]
    fn symbol_len_leaves_room_for_headers_and_proof() {
        assert_eq!(symbol_len(3), 1440 - 117 - 4 - 40);
        assert_eq!(symbol_len(15), 1440 - 117 - 4 - 280);
        assert_eq!(BODY_LEN, proof_len(5) + CHUNK_HEADER_LEN + symbol_len(5));
    }

    #[test]
    fn leaf_hash_covers_the_chunk_id() {
        assert_eq!(leaf_hash(1, b"x"), leaf_hash(1, b"x"));
        assert_ne!(leaf_hash(1, b"x"), leaf_hash(2, b"x"));
        assert_ne!(leaf_hash(1, b"x"), leaf_hash(1, b"y"));
    }
}
