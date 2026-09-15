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

//! Specifies the wire packet layout of chunk components (signature,
//! proposal header, merkle proof, chunk id, symbol).

pub mod proposal;
pub mod v1;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use monad_crypto::hasher::Hash;
use monad_mcp_chorus::spec::{ProposalHeader as _, SignedProposalHeader as _};

use super::{
    chunk::{Chunk, ChunkData, ProposalEnvelope, WireChunkId},
    chunk_tree::ChunkTree,
    types::{
        D25, EncodingScheme, MerkleRoot, ProposalHeader, ProposalSignature, S11,
        SignedProposalHeader,
    },
};
use crate::spec::DAProposalHeader as _;
pub use crate::spec::SIGNATURE_LEN;

pub fn version(packet: &[u8]) -> Option<u16> {
    let field = packet.get(SIGNATURE_LEN..SIGNATURE_LEN + 2)?;
    Some(u16::from_le_bytes(field.try_into().expect("2 bytes")))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MalformedPacket {
    // neither a header alone nor one segment
    BadLength(usize),
    UnknownVersion(u16),
    // the mode bits
    BadMode(u8),
    DepthOutOfRange(u8),
    UnknownScheme(u8),
    SlotOutOfRange(u64),
    NonZeroEpoch(u64),
    BadSignature,
    BadRoot,
    // a reserved bit or byte: in the mode byte or the chunk header
    ReservedNonZero,
}

// the scheme's part of the header: every field but the root, and the
// chunk header. the layout a proposal travels in is a function of it.
pub trait DAHeaderScheme: Sized {
    const VARIANT: u8;
    const PROPOSAL_HEADER_LEN: usize;
    const CHUNK_HEADER_LEN: usize;

    // writes exactly PROPOSAL_HEADER_LEN bytes
    fn pack_header(&self, root: &MerkleRoot, out: &mut impl BufMut);
    // the caller must pass exactly PROPOSAL_HEADER_LEN bytes
    fn unpack_header(buf: &mut impl Buf) -> Result<(MerkleRoot, Self), MalformedPacket>;

    // writes exactly CHUNK_HEADER_LEN bytes
    fn pack_chunk_header(chunk_id: WireChunkId, out: &mut impl BufMut);
    // the caller must pass exactly CHUNK_HEADER_LEN bytes
    fn unpack_chunk_header(buf: &mut impl Buf) -> Result<WireChunkId, MalformedPacket>;

    fn depth(&self) -> u8;

    fn v1_layout(&self) -> v1::Layout<'_, Self>;
}

// what a proposal's header determines about its packets: the frame
// around the header and each chunk, and the shape of its chunks
pub trait PacketLayout {
    // the signing preimage
    fn signed_bytes(&self, root: &MerkleRoot, out: &mut impl BufMut);

    // the bytes every packet of the proposal starts with: the
    // signature, then the signed bytes
    fn write_header(&self, root: &MerkleRoot, sig: &ProposalSignature, out: &mut impl BufMut);
    fn header_len(&self) -> usize;

    fn write_body(&self, chunk_id: WireChunkId, data: &ChunkData, out: &mut impl BufMut);
    // the length of one chunk body
    fn body_len(&self) -> usize;

    fn symbol_len(&self) -> usize;

    // the leaf commits to the chunk's wire bytes from its chunk header on
    fn leaf_hash(&self, chunk_id: WireChunkId, symbol: &[u8]) -> Hash;

    // the caller must pass one symbol per chunk, in chunk id order
    fn chunk_tree(&self, symbols: Vec<Bytes>) -> ChunkTree;
}

impl PacketLayout for EncodingScheme {
    fn signed_bytes(&self, root: &MerkleRoot, out: &mut impl BufMut) {
        match self {
            Self::D25(d25) => d25.v1_layout().signed_bytes(root, out),
            Self::S11(s11) => s11.v1_layout().signed_bytes(root, out),
        }
    }

    fn write_header(&self, root: &MerkleRoot, sig: &ProposalSignature, out: &mut impl BufMut) {
        match self {
            Self::D25(d25) => d25.v1_layout().write_header(root, sig, out),
            Self::S11(s11) => s11.v1_layout().write_header(root, sig, out),
        }
    }

    fn header_len(&self) -> usize {
        match self {
            Self::D25(d25) => d25.v1_layout().header_len(),
            Self::S11(s11) => s11.v1_layout().header_len(),
        }
    }

    fn write_body(&self, chunk_id: WireChunkId, data: &ChunkData, out: &mut impl BufMut) {
        match self {
            Self::D25(d25) => d25.v1_layout().write_body(chunk_id, data, out),
            Self::S11(s11) => s11.v1_layout().write_body(chunk_id, data, out),
        }
    }

    fn body_len(&self) -> usize {
        match self {
            Self::D25(d25) => d25.v1_layout().body_len(),
            Self::S11(s11) => s11.v1_layout().body_len(),
        }
    }

    fn symbol_len(&self) -> usize {
        match self {
            Self::D25(d25) => d25.v1_layout().symbol_len(),
            Self::S11(s11) => s11.v1_layout().symbol_len(),
        }
    }

    fn leaf_hash(&self, chunk_id: WireChunkId, symbol: &[u8]) -> Hash {
        match self {
            Self::D25(d25) => d25.v1_layout().leaf_hash(chunk_id, symbol),
            Self::S11(s11) => s11.v1_layout().leaf_hash(chunk_id, symbol),
        }
    }

    fn chunk_tree(&self, symbols: Vec<Bytes>) -> ChunkTree {
        match self {
            Self::D25(d25) => d25.v1_layout().chunk_tree(symbols),
            Self::S11(s11) => s11.v1_layout().chunk_tree(symbols),
        }
    }
}

// the scheme variant byte, once the version places it
fn scheme_variant(bytes: &Bytes) -> Result<u8, MalformedPacket> {
    let version = version(bytes).ok_or(MalformedPacket::BadLength(bytes.len()))?;
    if version != v1::VERSION {
        return Err(MalformedPacket::UnknownVersion(version));
    }
    let variant = bytes
        .get(v1::SCHEME_VARIANT_OFFSET)
        .ok_or(MalformedPacket::BadLength(bytes.len()))?;
    Ok(*variant)
}

// routed by the version, then by the scheme byte the version places
pub fn read_chunk(bytes: Bytes) -> Result<Chunk<'static>, MalformedPacket> {
    match scheme_variant(&bytes)? {
        D25::VARIANT => v1::Layout::<D25>::read_chunk(bytes),
        S11::VARIANT => v1::Layout::<S11>::read_chunk(bytes),
        other => Err(MalformedPacket::UnknownScheme(other)),
    }
}

pub fn read_envelope(bytes: Bytes) -> Result<ProposalEnvelope, MalformedPacket> {
    match scheme_variant(&bytes)? {
        D25::VARIANT => v1::Layout::<D25>::read_envelope(bytes),
        S11::VARIANT => v1::Layout::<S11>::read_envelope(bytes),
        other => Err(MalformedPacket::UnknownScheme(other)),
    }
}

pub fn signed_bytes(header: &ProposalHeader) -> Bytes {
    let layout = &header.scheme;
    let mut out = BytesMut::with_capacity(layout.header_len() - SIGNATURE_LEN);
    layout.signed_bytes(&header.root, &mut out);
    out.freeze()
}

pub fn header_bytes(signed: &SignedProposalHeader) -> Bytes {
    let layout = signed.scheme();
    let mut out = BytesMut::with_capacity(layout.header_len());
    layout.write_header(signed.root(), signed.sig(), &mut out);
    out.freeze()
}

// a packet is one segment carrying one chunk
pub fn write_chunk(chunk: &Chunk<'_>) -> Bytes {
    let signed = chunk.header();
    let layout = signed.scheme();
    let capacity = layout.header_len() + layout.body_len();
    let mut out = BytesMut::with_capacity(capacity);
    layout.write_header(signed.root(), signed.sig(), &mut out);
    layout.write_body(chunk.chunk_id(), chunk.data(), &mut out);
    out.freeze()
}

// one packet per chunk; the header alone when there is none
pub fn write_envelope(envelope: &ProposalEnvelope) -> Vec<Bytes> {
    let header_bytes = header_bytes(envelope.header());
    if envelope.header_only() {
        return vec![header_bytes];
    }

    let layout = envelope.header().scheme();
    let num_chunks = envelope.num_chunks();
    let segment_len = header_bytes.len() + layout.body_len();
    let mut buffer = BytesMut::with_capacity(segment_len * num_chunks);
    let mut packets = Vec::with_capacity(num_chunks);
    for (chunk_id, data) in envelope.chunk_data() {
        buffer.put_slice(&header_bytes);
        layout.write_body(*chunk_id, data, &mut buffer);
        packets.push(buffer.split().freeze());
    }
    packets
}

#[cfg(test)]
mod tests {
    use super::{
        super::{
            encoding_scheme::d25::D25Layout,
            test_util::{SLOT, author, epoch_handle, group, proposal_chunks, proposal_chunks_from},
            types::NodeId,
        },
        *,
    };
    use crate::spec::DAProposalSignature as _;

    #[test]
    fn packets_are_routed_by_version_then_scheme() {
        let (_, chunks) = proposal_chunks(&epoch_handle(), 1);
        let packet = write_chunk(&chunks[0]);
        assert_eq!(packet.len(), v1::SEGMENT_LEN);
        assert_eq!(version(&packet), Some(v1::VERSION));
        assert_eq!(read_chunk(packet.clone()), Ok(chunks[0].clone()));

        assert_eq!(version(&packet[..SIGNATURE_LEN + 1]), None);
        let short = packet.slice(..SIGNATURE_LEN + 1);
        assert_eq!(read_chunk(short), Err(MalformedPacket::BadLength(66)));

        let mut foreign = packet.to_vec();
        foreign[SIGNATURE_LEN] = 2;
        let foreign = read_chunk(Bytes::from(foreign));
        assert_eq!(foreign, Err(MalformedPacket::UnknownVersion(2)));

        let mut unknown = packet.to_vec();
        unknown[v1::SCHEME_VARIANT_OFFSET] = 3;
        let unknown = read_chunk(Bytes::from(unknown));
        assert_eq!(unknown, Err(MalformedPacket::UnknownScheme(3)));
    }

    #[test]
    fn an_envelope_writes_the_packet_of_each_of_its_chunks() {
        let (_, chunks) = proposal_chunks(&epoch_handle(), 1);
        let envelope = group(&chunks[1..4]);

        let mut expected = Vec::new();
        for chunk in envelope.chunks() {
            expected.push(write_chunk(&chunk));
        }
        assert_eq!(write_envelope(&envelope), expected);
    }

    #[test]
    fn a_header_alone_crosses_the_wire_as_one_packet() {
        let (header, chunks) = proposal_chunks(&epoch_handle(), 1);
        let envelope = ProposalEnvelope::from_header(header.clone());

        let packets = write_envelope(&envelope);
        assert_eq!(packets, vec![header_bytes(&header)]);
        assert_eq!(packets[0].len(), D25Layout::HEADER_LEN);
        assert_eq!(header.scheme().header_len(), D25Layout::HEADER_LEN);
        assert_eq!(read_envelope(packets[0].clone()), Ok(envelope));

        // a chunk packet starts with the header bytes
        let packet = write_chunk(&chunks[0]);
        assert_eq!(packet[..packets[0].len()], packets[0]);
        let envelope = read_envelope(packet).expect("well-formed");
        assert_eq!(envelope, group(&chunks[..1]));
        let header_only = read_chunk(packets[0].clone());
        assert_eq!(header_only, Err(MalformedPacket::BadLength(117)));
    }

    #[test]
    fn the_signature_binds_the_signed_bytes() {
        let epoch_handle = epoch_handle();
        let (header, _) = proposal_chunks(&epoch_handle, 1);
        let signed = signed_bytes(&header.header);
        assert_eq!(signed.len(), D25Layout::SIGNED_LEN);
        assert_eq!(header.sig().recover_author(&signed), Some(author()));

        let mut altered = signed.to_vec();
        altered[D25Layout::SIGNED_LEN - 1] ^= 1;
        assert_eq!(header.sig().recover_author(&altered), None);

        // the same bytes signed by another author recover that author
        let (other, _) = proposal_chunks_from(&epoch_handle, 2, SLOT, 1);
        assert_eq!(other.sig().recover_author(&signed), Some(NodeId::dummy(2)));
    }
}
