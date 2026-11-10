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

//! Deterministic raptorcast v1 packets, mostly compatible with
//! monad-raptorcast.
//!
//! A v1 packet carries one chunk, in the following order: the header
//! (signature, version, proposal header), the body (proof, chunk
//! header, symbol). A header-only packet is permitted. The body of a
//! hollow scheme's packet is a one-hash proof and the chunk header.

use bytes::{BufMut, Bytes};

use super::{
    super::{
        chunk::{Chunk, ChunkData, ProposalEnvelope, WireChunkId},
        types::{
            EncodingScheme, MerkleHash, MerkleRoot, ProposalHeader, ProposalSignature,
            SignedProposalHeader,
        },
    },
    DAHeaderScheme, MalformedPacket, PacketLayout, SIGNATURE_LEN,
};
use crate::spec::DAProposalSignature as _;

pub const VERSION: u16 = 1;

// ethernet MTU minus the ip, udp and wireauth headers
pub const SEGMENT_LEN: usize = 1440;

// after the signature and the version: the scheme header's second byte
pub const SCHEME_VARIANT_OFFSET: usize = SIGNATURE_LEN + VERSION_LEN + 1;

pub const VERSION_LEN: usize = 2;
pub const HASH_LEN: usize = 20;

const fn proof_len(depth: u8) -> usize {
    HASH_LEN * (depth as usize - 1)
}

// the deepest tree whose proof and chunk header leave room for a symbol
const fn max_depth(body_len: usize, chunk_header_len: usize) -> u8 {
    let mut depth = 1;
    while proof_len(depth + 1) + chunk_header_len < body_len {
        depth += 1;
    }
    depth
}

// the v1 frame instantiated by the header scheme it carries. Derived
// from the scheme alone, through DAHeaderScheme::v1_layout.
pub struct Layout<'a, H>(&'a H);

impl<'a, H> Layout<'a, H> {
    pub(crate) fn new(scheme: &'a H) -> Self {
        Self(scheme)
    }
}

impl<H: DAHeaderScheme> Layout<'_, H> {
    pub const SIGNED_LEN: usize = VERSION_LEN + H::PROPOSAL_HEADER_LEN;

    // signature, version, scheme header
    pub const HEADER_LEN: usize = SIGNATURE_LEN + Self::SIGNED_LEN;

    // proof(var) chunk_header symbol(var)
    const BODY_LEN: usize = SEGMENT_LEN - Self::HEADER_LEN;

    // the maximum feasible depth. Each HeaderScheme defines their own
    // more restrictive max depth tailored to its specific parameters.
    pub const MAX_DEPTH: u8 = max_depth(Self::BODY_LEN, H::CHUNK_HEADER_LEN);

    // the caller must ensure depth is at most MAX_DEPTH
    pub const fn symbol_len_at(depth: u8) -> usize {
        Self::BODY_LEN - proof_len(depth) - H::CHUNK_HEADER_LEN
    }

    fn depth(&self) -> u8 {
        self.0.depth()
    }

    // the caller must pass exactly HEADER_LEN bytes
    fn read_header(bytes: &[u8]) -> Result<(MerkleRoot, H, ProposalSignature), MalformedPacket> {
        use bytes::Buf as _;

        let (signature, mut signed) = bytes.split_at(SIGNATURE_LEN);
        let sig = ProposalSignature::from_bytes(signature).ok_or(MalformedPacket::BadSignature)?;

        let version = signed.get_u16_le();
        if version != VERSION {
            return Err(MalformedPacket::UnknownVersion(version));
        }
        let (root, scheme) = H::unpack_header(&mut signed)?;
        debug_assert!(signed.is_empty());

        Ok((root, scheme, sig))
    }

    // the caller must pass exactly BODY_LEN bytes
    fn read_body(&self, body: Bytes) -> Result<(WireChunkId, ChunkData), MalformedPacket> {
        let proof_end = proof_len(self.depth());
        let symbol_start = proof_end + H::CHUNK_HEADER_LEN;

        let mut proof = Vec::with_capacity(self.depth() as usize - 1);
        for hash in body[..proof_end].chunks_exact(HASH_LEN) {
            proof.push(MerkleHash(hash.try_into().expect("HASH_LEN bytes")));
        }

        let chunk_id = H::unpack_chunk_header(&mut &body[proof_end..symbol_start])?;

        let data = ChunkData {
            symbol: body.slice(symbol_start..),
            proof: proof.into_boxed_slice(),
        };
        Ok((chunk_id, data))
    }

    // todo: the parsing of header can be lru cached
    pub fn read_chunk(bytes: Bytes) -> Result<Chunk<'static>, MalformedPacket>
    where
        H: Into<EncodingScheme>,
    {
        if bytes.len() != SEGMENT_LEN {
            return Err(MalformedPacket::BadLength(bytes.len()));
        }
        let (root, scheme, sig) = Self::read_header(&bytes[..Self::HEADER_LEN])?;
        let body = bytes.slice(Self::HEADER_LEN..);
        let (chunk_id, data) = Layout::new(&scheme).read_body(body)?;
        Ok(Chunk::new(signed_header(root, scheme, sig), chunk_id, data))
    }

    // a header alone or one segment
    pub fn read_envelope(bytes: Bytes) -> Result<ProposalEnvelope, MalformedPacket>
    where
        H: Into<EncodingScheme>,
    {
        if bytes.len() == Self::HEADER_LEN {
            let (root, scheme, sig) = Self::read_header(&bytes)?;
            let header = signed_header(root, scheme, sig);
            return Ok(ProposalEnvelope::from_header(header));
        }
        let chunk = Self::read_chunk(bytes)?;
        Ok(ProposalEnvelope::from_chunk(chunk))
    }
}

// the v1 frame of a hollow scheme: the header, a one-hash proof, then
// the chunk header. no symbol.
#[cfg(feature = "enable_hollow")]
pub struct HollowLayout<'a, H>(&'a H);

#[cfg(feature = "enable_hollow")]
impl<'a, H> HollowLayout<'a, H> {
    pub(crate) fn new(scheme: &'a H) -> Self {
        Self(scheme)
    }
}

#[cfg(feature = "enable_hollow")]
impl<H: DAHeaderScheme> HollowLayout<'_, H> {
    pub const HEADER_LEN: usize = Layout::<H>::HEADER_LEN;
    const BODY_LEN: usize = HASH_LEN + H::CHUNK_HEADER_LEN;
    pub const PACKET_LEN: usize = Self::HEADER_LEN + Self::BODY_LEN;

    pub fn read_chunk(bytes: Bytes) -> Result<Chunk<'static>, MalformedPacket>
    where
        H: Into<EncodingScheme>,
    {
        if bytes.len() != Self::PACKET_LEN {
            return Err(MalformedPacket::BadLength(bytes.len()));
        }
        let (header, body) = bytes.split_at(Self::HEADER_LEN);
        let (root, scheme, sig) = Layout::<H>::read_header(header)?;
        let (proof, mut chunk_header) = body.split_at(HASH_LEN);
        let proof = MerkleHash(proof.try_into().expect("HASH_LEN bytes"));
        let chunk_id = H::unpack_chunk_header(&mut chunk_header)?;
        let data = ChunkData {
            symbol: Bytes::new(),
            proof: Box::new([proof]),
        };
        let header = signed_header(root, scheme, sig);
        Ok(Chunk::new(header, chunk_id, data))
    }

    // a header alone or one chunk
    pub fn read_envelope(bytes: Bytes) -> Result<ProposalEnvelope, MalformedPacket>
    where
        H: Into<EncodingScheme>,
    {
        if bytes.len() == Self::HEADER_LEN {
            let (root, scheme, sig) = Layout::<H>::read_header(&bytes)?;
            let header = signed_header(root, scheme, sig);
            return Ok(ProposalEnvelope::from_header(header));
        }
        let chunk = Self::read_chunk(bytes)?;
        Ok(ProposalEnvelope::from_chunk(chunk))
    }
}

fn signed_header(
    root: MerkleRoot,
    scheme: impl Into<EncodingScheme>,
    sig: ProposalSignature,
) -> SignedProposalHeader {
    let header = ProposalHeader {
        root,
        scheme: scheme.into(),
    };
    SignedProposalHeader { header, sig }
}

impl<H: DAHeaderScheme> PacketLayout for Layout<'_, H> {
    fn signed_bytes(&self, root: &MerkleRoot, out: &mut impl BufMut) {
        out.put_u16_le(VERSION);
        self.0.pack_header(root, out);
    }

    fn write_header(&self, root: &MerkleRoot, sig: &ProposalSignature, out: &mut impl BufMut) {
        sig.to_bytes(out);
        self.signed_bytes(root, out);
    }

    fn header_len(&self) -> usize {
        Self::HEADER_LEN
    }

    fn write_body(&self, chunk_id: WireChunkId, data: &ChunkData, out: &mut impl BufMut) {
        for hash in &data.proof {
            out.put_slice(&hash.0);
        }
        H::pack_chunk_header(chunk_id, out);
        out.put_slice(&data.symbol);
    }

    fn body_len(&self) -> usize {
        Self::BODY_LEN
    }

    fn symbol_len(&self) -> usize {
        Self::symbol_len_at(self.depth())
    }
}

// the header is v1's
#[cfg(feature = "enable_hollow")]
impl<H: DAHeaderScheme> PacketLayout for HollowLayout<'_, H> {
    fn signed_bytes(&self, root: &MerkleRoot, out: &mut impl BufMut) {
        Layout::new(self.0).signed_bytes(root, out);
    }

    fn write_header(&self, root: &MerkleRoot, sig: &ProposalSignature, out: &mut impl BufMut) {
        Layout::new(self.0).write_header(root, sig, out);
    }

    fn header_len(&self) -> usize {
        Self::HEADER_LEN
    }

    fn write_body(&self, chunk_id: WireChunkId, data: &ChunkData, out: &mut impl BufMut) {
        for hash in &data.proof {
            out.put_slice(&hash.0);
        }
        H::pack_chunk_header(chunk_id, out);
    }

    fn body_len(&self) -> usize {
        Self::BODY_LEN
    }

    fn symbol_len(&self) -> usize {
        0
    }
}

#[cfg(test)]
mod tests {
    use monad_mcp_cadence::spec::SignedProposalHeader as _;

    use super::{
        super::{
            super::{
                encoding_scheme::d33::D33Layout,
                test_util::{epoch_handle, group, proposal_chunks},
                types::D33,
            },
            header_bytes, write_chunk, write_envelope,
        },
        *,
    };
    use crate::spec::DAProposalHeader as _;

    fn depth_of(header: &SignedProposalHeader) -> u8 {
        let EncodingScheme::D33(d33) = header.scheme() else {
            panic!("the fixture's scheme is d33");
        };
        d33.depth
    }

    fn corrupted(
        chunk: &Chunk<'_>,
        corrupt: impl FnOnce(&mut Vec<u8>),
    ) -> Result<Chunk<'static>, MalformedPacket> {
        let mut bytes = write_chunk(chunk).to_vec();
        corrupt(&mut bytes);
        D33Layout::read_chunk(Bytes::from(bytes))
    }

    #[test]
    fn a_packet_is_the_signature_the_version_the_header_then_the_body() {
        let epoch_handle = epoch_handle();
        let (header, chunks) = proposal_chunks(&epoch_handle, 1);
        let depth = depth_of(&header);
        let (_, chunk_id, data) = chunks[3].clone().into_parts();

        let bytes = write_chunk(&chunks[3]);
        assert_eq!(bytes.len(), SEGMENT_LEN);

        let mut signature = Vec::new();
        header.sig().to_bytes(&mut signature);
        assert_eq!(signature.len(), SIGNATURE_LEN);
        assert_eq!(bytes[..SIGNATURE_LEN], signature);
        let version_end = SIGNATURE_LEN + VERSION_LEN;
        assert_eq!(bytes[SIGNATURE_LEN..version_end], VERSION.to_le_bytes());

        let body = D33Layout::HEADER_LEN;
        let proof_end = body + proof_len(depth);
        assert_eq!(bytes[body..body + HASH_LEN], data.proof[0].0);
        let mut chunk_header = Vec::new();
        D33::pack_chunk_header(chunk_id, &mut chunk_header);
        let symbol_start = proof_end + D33::CHUNK_HEADER_LEN;
        assert_eq!(bytes[proof_end..symbol_start], chunk_header);
        assert_eq!(bytes[symbol_start..], data.symbol);
    }

    #[test]
    fn chunks_round_trip_and_regroup_into_their_envelope() {
        let epoch_handle = epoch_handle();
        let (_, chunks) = proposal_chunks(&epoch_handle, 1);
        for chunk in &chunks {
            let packet = write_chunk(chunk);
            assert_eq!(D33Layout::read_chunk(packet), Ok(chunk.clone()));
        }

        // an envelope crosses the wire as one packet per chunk
        let envelope = group(&chunks[1..4]);
        let mut received = Vec::new();
        for chunk in envelope.chunks() {
            let packet = write_chunk(&chunk);
            received.push(D33Layout::read_chunk(packet).expect("well-formed"));
        }
        let regrouped: Vec<_> = ProposalEnvelope::group(received).collect();
        assert_eq!(regrouped, vec![envelope]);
    }

    #[test]
    fn a_header_alone_is_a_packet_that_stops_before_the_body() {
        let epoch_handle = epoch_handle();
        let (header, chunks) = proposal_chunks(&epoch_handle, 1);
        let header_only = ProposalEnvelope::from_header(header.clone());

        let packets = write_envelope(&header_only);
        assert_eq!(packets.len(), 1);
        assert_eq!(packets[0].len(), D33Layout::HEADER_LEN);
        assert_eq!(packets[0], header_bytes(&header));
        assert_eq!(
            D33Layout::read_envelope(packets[0].clone()),
            Ok(header_only)
        );

        // the header bytes are the prefix of every chunk packet
        let packet = write_chunk(&chunks[0]);
        assert_eq!(packet[..D33Layout::HEADER_LEN], packets[0]);
        assert_eq!(D33Layout::read_envelope(packet), Ok(group(&chunks[..1])));

        // in between is neither
        let mut between = packets[0].to_vec();
        between.push(0);
        let between = D33Layout::read_envelope(Bytes::from(between));
        let between_len = D33Layout::HEADER_LEN + 1;
        assert_eq!(between, Err(MalformedPacket::BadLength(between_len)));
    }

    #[test]
    fn malformed_frames_are_rejected() {
        let epoch_handle = epoch_handle();
        let (_, chunks) = proposal_chunks(&epoch_handle, 1);
        let chunk = &chunks[0];
        let body = D33Layout::HEADER_LEN;

        let truncated_prefix = corrupted(chunk, |b| b.truncate(body - 1));
        assert_eq!(truncated_prefix, Err(MalformedPacket::BadLength(body - 1)));
        let truncated_body = corrupted(chunk, |b| b.truncate(SEGMENT_LEN - 1));
        assert_eq!(truncated_body, Err(MalformedPacket::BadLength(1439)));
        let oversized = corrupted(chunk, |b| b.push(0));
        assert_eq!(oversized, Err(MalformedPacket::BadLength(1441)));

        let version = corrupted(chunk, |b| b[SIGNATURE_LEN] = 2);
        assert_eq!(version, Err(MalformedPacket::UnknownVersion(2)));
        let padding = corrupted(chunk, |b| b[SIGNATURE_LEN - 1] = 1);
        assert_eq!(padding, Err(MalformedPacket::BadSignature));
    }

    #[test]
    fn the_body_is_the_proof_the_chunk_header_and_the_symbol() {
        let (header, _) = proposal_chunks(&epoch_handle(), 1);
        let layout = header.scheme();

        let body_len = proof_len(5) + D33::CHUNK_HEADER_LEN + D33Layout::symbol_len_at(5);
        assert_eq!(D33Layout::BODY_LEN, body_len);
        assert_eq!(layout.body_len(), body_len);
        assert_eq!(
            layout.symbol_len(),
            D33Layout::symbol_len_at(depth_of(&header))
        );
        // the deepest tree still leaves a symbol
        assert!(D33Layout::symbol_len_at(D33Layout::MAX_DEPTH) > 0);
    }

    #[cfg(feature = "enable_hollow")]
    #[test]
    fn a_hollow_packet_is_the_d33_header_under_its_variant_then_a_proof_and_the_chunk_id() {
        use super::super::{
            super::{
                encoding_scheme::Hollow,
                test_util::{hollow_scheme, proposal_chunks_under},
            },
            signed_bytes,
        };

        let epoch_handle = epoch_handle();
        let (header, chunks) =
            proposal_chunks_under(&epoch_handle, hollow_scheme(&epoch_handle), 0, 1);
        type HollowD33Layout<'a> = HollowLayout<'a, Hollow<D33>>;

        for chunk in &chunks {
            let packet = write_chunk(chunk);
            assert_eq!(packet.len(), HollowD33Layout::PACKET_LEN);
            assert_eq!(packet[..D33Layout::HEADER_LEN], header_bytes(&header));
            let (proof, chunk_header) = packet[D33Layout::HEADER_LEN..].split_at(HASH_LEN);
            assert_eq!(proof, chunk.data().proof[0].0);
            assert_eq!(chunk_header, chunk.chunk_id().to_le_bytes());
            assert_eq!(HollowD33Layout::read_chunk(packet), Ok(chunk.clone()));
        }

        let header_only = ProposalEnvelope::from_header(header.clone());
        let packets = write_envelope(&header_only);
        assert_eq!(packets[0].len(), D33Layout::HEADER_LEN);
        assert_eq!(
            HollowD33Layout::read_envelope(packets[0].clone()),
            Ok(header_only)
        );

        // the same fields under d33 differ in the variant byte alone
        let EncodingScheme::D33Hollow(d33) = header.scheme() else {
            panic!("the fixture's scheme is hollow");
        };
        let as_d33 = ProposalHeader {
            root: header.header.root,
            scheme: EncodingScheme::D33(*d33),
        };
        let hollow = signed_bytes(&header.header);
        let real = signed_bytes(&as_d33);
        let variant = SCHEME_VARIANT_OFFSET - SIGNATURE_LEN;
        let differing: Vec<_> = (0..real.len()).filter(|i| hollow[*i] != real[*i]).collect();
        assert_eq!(differing, [variant]);
        assert_eq!(hollow[variant], Hollow::<D33>::VARIANT);
    }
}
