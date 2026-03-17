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

use bytes::Bytes;
use monad_crypto::{
    certificate_signature::{
        CertificateSignature, CertificateSignaturePubKey, CertificateSignatureRecoverable,
    },
    hasher::{Hasher as _, HasherType},
};
use monad_merkle::{MerkleHash, MerkleProof};
use monad_types::{Epoch, NodeId, Round};
use tracing::warn;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Ref, LE, U16, U32, U64};

use crate::{
    message::MAX_MESSAGE_SIZE,
    packet::{assigner::stake_partition_num_chunks_hint, deterministic, regular},
    udp::{ChunkSignatureVerifier, GroupId, InvalidChunk, ValidatedChunk, MAX_VALIDATOR_SET_SIZE},
    util::{ensure, BroadcastMode, EncodingScheme, HexBytes},
    SIGNATURE_SIZE,
};

struct ChunkMeta {
    app_message_len: usize,
    chunk_id: usize,
    num_source_symbols: usize,
    encoded_symbol_capacity: usize,
    timestamp: u64,
    broadcast_mode: BroadcastMode,
    encoding_scheme: EncodingScheme,
    group_id: GroupId,
    app_message_hash: Option<HexBytes<HASH_SIZE>>,
    merkle_root: HexBytes<MERKLE_HASH_SIZE>,
    recipient_hash: HexBytes<HASH_SIZE>,
    signed_over_data: SignedOverData,
}

impl ChunkMeta {
    fn get_epoch(&self) -> Option<Epoch> {
        match self.group_id {
            GroupId::Primary(epoch) => Some(epoch),
            GroupId::Secondary(_) => None,
        }
    }
}

const MERKLE_HASH_SIZE: usize = 20;
const HASH_SIZE: usize = MERKLE_HASH_SIZE;

#[derive(Debug, PartialEq, Eq)]
pub enum MalformedPacket {
    TooShort,
    TooLong,
    InvalidTreeDepth(u8),
    InvalidBroadcastBits(u8),
    InvalidEncodingScheme(u8),
    UnknownVersion(u16),
}

impl From<MalformedPacket> for InvalidChunk {
    fn from(err: MalformedPacket) -> Self {
        InvalidChunk::Malformed(err)
    }
}

/// Raptorcast packet header (common to all versions):
/// - 65 bytes => Signature of sender
/// - 2 bytes => Version number
#[repr(C, packed)]
#[derive(FromBytes, IntoBytes, Immutable, KnownLayout, Clone, Copy)]
pub struct RaptorcastHeader {
    signature: [u8; SIGNATURE_SIZE],
    version: U16<LE>,
}

impl RaptorcastHeader {
    const SIZE: usize = SIGNATURE_SIZE + 2;
}

/// Raptorcast packet V0 versioned header layout (follows common header):
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
#[repr(C, packed)]
#[derive(FromBytes, IntoBytes, Immutable, KnownLayout, Clone, Copy)]
pub struct RaptorcastHeaderV0 {
    broadcast_tree_depth: u8,
    group_id: U64<LE>,
    unix_ts_ms: U64<LE>,
    app_message_hash: [u8; MERKLE_HASH_SIZE],
    app_message_len: U32<LE>,
}

impl RaptorcastHeaderV0 {
    const SIZE: usize = 1 + 8 + 8 + MERKLE_HASH_SIZE + 4;

    pub fn broadcast(&self) -> bool {
        (self.broadcast_tree_depth & (1 << 7)) != 0
    }

    pub fn secondary_broadcast(&self) -> bool {
        (self.broadcast_tree_depth & (1 << 6)) != 0
    }

    pub fn tree_depth(&self) -> u8 {
        self.broadcast_tree_depth & 0b0000_1111
    }

    pub fn merkle_proof_size(&self) -> usize {
        MERKLE_HASH_SIZE * (self.tree_depth() as usize - 1)
    }

    pub fn broadcast_mode(&self) -> Result<BroadcastMode, MalformedPacket> {
        match (self.broadcast(), self.secondary_broadcast()) {
            (true, false) => Ok(BroadcastMode::Primary),
            (false, true) => Ok(BroadcastMode::Secondary),
            (false, false) => Ok(BroadcastMode::Unspecified),
            (true, true) => Err(MalformedPacket::InvalidBroadcastBits(0b11)),
        }
    }

    pub fn group_id(&self) -> Result<GroupId, MalformedPacket> {
        match self.broadcast_mode()? {
            BroadcastMode::Primary | BroadcastMode::Unspecified => {
                Ok(GroupId::Primary(Epoch(self.group_id.get())))
            }
            BroadcastMode::Secondary => Ok(GroupId::Secondary(Round(self.group_id.get()))),
        }
    }
}

/// Combined header size for V0 (common header + versioned header)
const RAPTORCAST_HEADER_V0_SIZE: usize = RaptorcastHeader::SIZE + RaptorcastHeaderV0::SIZE;

const _: () = assert!(
    RAPTORCAST_HEADER_V0_SIZE == crate::packet::regular::PacketLayout::HEADER_LEN,
    "RaptorcastHeader size must match HEADER_LEN"
);

const _: () = assert!(
    RaptorcastChunkHeaderV0::SIZE == crate::packet::regular::PacketLayout::CHUNK_HEADER_LEN,
    "RaptorcastChunkHeader size must match CHUNK_HEADER_LEN"
);

const _: () = assert!(
    RAPTORCAST_HEADER_V1_SIZE == crate::packet::deterministic::PacketLayout::HEADER_LEN,
    "RaptorcastHeader size must match HEADER_LEN"
);

const _: () = assert!(
    RaptorcastChunkHeaderV1::SIZE == crate::packet::deterministic::PacketLayout::CHUNK_HEADER_LEN,
    "RaptorcastChunkHeader size must match CHUNK_HEADER_LEN"
);

/// Raptorcast V0 chunk header layout (follows header and merkle proof):
/// - 20 bytes * (merkle_tree_depth - 1) => merkle proof (leaves include everything that follows,
///   eg hash(chunk_recipient + chunk_byte_offset + symbol_len + payload))
/// - 20 bytes => first 20 bytes of hash of chunk's first hop recipient
///   - we set this even if broadcast bit is not set so that it's known if a message was intended
///     to be sent to self
/// - 1 byte => Chunk's merkle leaf idx
/// - 1 byte => reserved
/// - 2 bytes (u16) => This chunk's id
/// - rest => data
#[repr(C, packed)]
#[derive(FromBytes, IntoBytes, Immutable, KnownLayout, Clone, Copy)]
pub struct RaptorcastChunkHeaderV0 {
    recipient_hash: [u8; MERKLE_HASH_SIZE],
    merkle_leaf_idx: u8,
    reserved: u8,
    chunk_id: U16<LE>,
}

impl RaptorcastChunkHeaderV0 {
    const SIZE: usize = MERKLE_HASH_SIZE + 1 + 1 + 2;
}

/// Raptorcast packet V1 versioned header layout (follows common header):
/// - 2 bits => broadcast mode
/// - 2 bits => unused
/// - 4 bits => Merkle tree depth
/// - 1 byte (u8) => encoding scheme variant
/// - 8 bytes (u64) => Round #
/// - 8 bytes (u64) => Epoch #
/// - 8 bytes (u64) => Unix timestamp
/// - 20 bytes => global merkle tree root for the full message
/// - 4 bytes (u32) => Serialized AppMessage length (bytes)
#[repr(C, packed)]
#[derive(FromBytes, IntoBytes, Immutable, KnownLayout, Clone, Copy)]
pub struct RaptorcastHeaderV1 {
    broadcast_tree_depth: u8,
    encoding_scheme_variant: u8,
    round: U64<LE>,
    epoch: U64<LE>,
    unix_ts_ms: U64<LE>,
    global_merkle_root: [u8; MERKLE_HASH_SIZE],
    app_message_len: U32<LE>,
}

/// Combined header size for V1 (common header + versioned header)
const RAPTORCAST_HEADER_V1_SIZE: usize = RaptorcastHeader::SIZE + RaptorcastHeaderV1::SIZE;

impl RaptorcastHeaderV1 {
    const SIZE: usize = 1 + 1 + 8 + 8 + 8 + MERKLE_HASH_SIZE + 4;

    pub fn broadcast_mode(&self) -> Result<BroadcastMode, MalformedPacket> {
        match (self.broadcast_tree_depth & 0b1100_0000) >> 6 {
            0b10 => Ok(BroadcastMode::Primary),
            bits => Err(MalformedPacket::InvalidBroadcastBits(bits)),
        }
    }

    pub fn encoding_scheme(&self) -> Result<EncodingScheme, MalformedPacket> {
        let round = Round(self.round.get());
        match self.encoding_scheme_variant {
            0x1 => Ok(EncodingScheme::Deterministic25(round)),
            v => Err(MalformedPacket::InvalidEncodingScheme(v)),
        }
    }

    #[allow(clippy::manual_range_contains)]
    pub fn tree_depth(&self) -> Result<u8, MalformedPacket> {
        let depth = self.broadcast_tree_depth & 0b0000_1111;
        ensure!(
            (deterministic::MIN_MERKLE_TREE_DEPTH..=deterministic::MAX_MERKLE_TREE_DEPTH)
                .contains(&depth),
            MalformedPacket::InvalidTreeDepth(depth)
        );

        Ok(depth)
    }

    pub fn group_id(&self) -> Result<GroupId, MalformedPacket> {
        let epoch = Epoch(self.epoch.get());
        match self.broadcast_mode()? {
            BroadcastMode::Primary => Ok(GroupId::Primary(epoch)),
            // broadcast_mode() only returns Primary for v1
            _broadcast_mode => unreachable!(),
        }
    }
}

/// Raptorcast V1 chunk header layout (follows header and merkle proof):
/// - 20 bytes * (merkle_tree_depth - 1) => merkle proof (leaves include everything that follows,
///   eg hash(chunk_recipient + chunk_byte_offset + symbol_len + payload))
/// - 20 bytes => first 20 bytes of hash of chunk's first hop recipient
///   - we set this even if broadcast bit is not set so that it's known if a message was intended
///     to be sent to self
/// - 2 bytes => reserved
/// - 2 bytes (u16) => This chunk's id, also used as the merkle leaf index
/// - rest => data
#[repr(C, packed)]
#[derive(FromBytes, IntoBytes, Immutable, KnownLayout, Clone, Copy)]
pub struct RaptorcastChunkHeaderV1 {
    recipient_hash: [u8; MERKLE_HASH_SIZE],
    reserved: U16<LE>,
    chunk_id: U16<LE>,
}

impl RaptorcastChunkHeaderV1 {
    const SIZE: usize = MERKLE_HASH_SIZE + 2 + 2;

    fn merkle_leaf_index(&self) -> u16 {
        self.chunk_id.get()
    }
}

/// header + merkle root, used as cache key for signatures
#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct SignedOverDataV0([u8; RAPTORCAST_HEADER_V0_SIZE + MERKLE_HASH_SIZE]);

impl SignedOverDataV0 {
    pub fn new(
        common_header: &Ref<&[u8], RaptorcastHeader>,
        versioned_header: &Ref<&[u8], RaptorcastHeaderV0>,
        merkle_root: &MerkleHash,
    ) -> Self {
        use zerocopy::IntoBytes;
        let mut data = [0u8; RAPTORCAST_HEADER_V0_SIZE + MERKLE_HASH_SIZE];
        data[..RaptorcastHeader::SIZE].copy_from_slice(common_header.as_bytes());
        data[RaptorcastHeader::SIZE..RAPTORCAST_HEADER_V0_SIZE]
            .copy_from_slice(versioned_header.as_bytes());
        data[RAPTORCAST_HEADER_V0_SIZE..].copy_from_slice(merkle_root);
        Self(data)
    }

    fn signed_message(&self) -> &[u8] {
        &self.0[SIGNATURE_SIZE..]
    }

    fn signature(&self) -> &[u8] {
        &self.0[..SIGNATURE_SIZE]
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct SignedOverDataV1([u8; RAPTORCAST_HEADER_V1_SIZE]);

impl SignedOverDataV1 {
    pub fn new(
        common_header: &Ref<&[u8], RaptorcastHeader>,
        versioned_header: &Ref<&[u8], RaptorcastHeaderV1>,
    ) -> Self {
        use zerocopy::IntoBytes;
        let mut data = [0u8; RAPTORCAST_HEADER_V1_SIZE];
        data[..RaptorcastHeader::SIZE].copy_from_slice(common_header.as_bytes());
        data[RaptorcastHeader::SIZE..].copy_from_slice(versioned_header.as_bytes());
        Self(data)
    }

    pub fn signed_message(&self) -> &[u8] {
        &self.0[SIGNATURE_SIZE..]
    }

    fn signature(&self) -> &[u8] {
        &self.0[..SIGNATURE_SIZE]
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub enum SignedOverData {
    V0(SignedOverDataV0),
    V1(SignedOverDataV1),
}

impl SignedOverData {
    pub fn signature(&self) -> &[u8] {
        match self {
            SignedOverData::V0(v0) => v0.signature(),
            SignedOverData::V1(v1) => v1.signature(),
        }
    }

    pub fn signed_message(&self) -> &[u8] {
        match self {
            SignedOverData::V0(v0) => v0.signed_message(),
            SignedOverData::V1(v1) => v1.signed_message(),
        }
    }
}

pub struct RaptorcastPacketV0<'a> {
    header: Ref<&'a [u8], RaptorcastHeaderV0>,
    merkle_proof: Ref<&'a [u8], [MerkleHash]>,
    chunk_header: Ref<&'a [u8], RaptorcastChunkHeaderV0>,
    chunk_header_and_payload: &'a [u8],
    payload_offset: usize,
}

impl<'a> RaptorcastPacketV0<'a> {
    pub fn parse(rest: &'a [u8]) -> Result<Self, MalformedPacket> {
        ensure!(
            rest.len() >= RaptorcastHeaderV0::SIZE,
            MalformedPacket::TooShort
        );
        let (header_bytes, rest) = rest.split_at(RaptorcastHeaderV0::SIZE);
        let header: Ref<&[u8], RaptorcastHeaderV0> =
            Ref::from_bytes(header_bytes).map_err(|_| MalformedPacket::TooShort)?;

        let tree_depth = header.tree_depth();
        ensure!(
            (regular::MIN_MERKLE_TREE_DEPTH..=regular::MAX_MERKLE_TREE_DEPTH).contains(&tree_depth),
            MalformedPacket::InvalidTreeDepth(tree_depth)
        );

        let merkle_proof_len = header.merkle_proof_size();
        ensure!(rest.len() >= merkle_proof_len, MalformedPacket::TooShort);
        let (merkle_proof_bytes, chunk_header_and_payload) = rest.split_at(merkle_proof_len);
        let merkle_proof: Ref<&[u8], [MerkleHash]> =
            Ref::from_bytes(merkle_proof_bytes).map_err(|_| MalformedPacket::TooShort)?;

        ensure!(
            chunk_header_and_payload.len() >= RaptorcastChunkHeaderV0::SIZE,
            MalformedPacket::TooShort
        );
        let (chunk_header_bytes, payload) =
            chunk_header_and_payload.split_at(RaptorcastChunkHeaderV0::SIZE);
        let chunk_header: Ref<&[u8], RaptorcastChunkHeaderV0> =
            Ref::from_bytes(chunk_header_bytes).map_err(|_| MalformedPacket::TooShort)?;

        ensure!(!payload.is_empty(), MalformedPacket::TooShort);

        let payload_offset =
            RAPTORCAST_HEADER_V0_SIZE + merkle_proof_len + RaptorcastChunkHeaderV0::SIZE;

        Ok(Self {
            header,
            merkle_proof,
            chunk_header,
            chunk_header_and_payload,
            payload_offset,
        })
    }

    pub fn split_chunk(&self, message: &Bytes) -> Result<Bytes, MalformedPacket> {
        ensure!(
            message.len() >= self.payload_offset,
            MalformedPacket::TooShort
        );
        Ok(message.slice(self.payload_offset..))
    }

    pub fn payload(&self) -> &[u8] {
        &self.chunk_header_and_payload[RaptorcastChunkHeaderV0::SIZE..]
    }

    fn compute_merkle_root(&self) -> Result<MerkleHash, InvalidChunk> {
        let proof = MerkleProof::new_from_leaf_idx(
            self.merkle_proof.to_vec(),
            self.chunk_header.merkle_leaf_idx as u16,
        )
        .ok_or(InvalidChunk::InvalidMerkleProof)?;

        let mut hasher = HasherType::new();
        hasher.update(self.chunk_header_and_payload);
        let leaf_hash = hasher.hash();

        proof
            .compute_root(&leaf_hash)
            .ok_or(InvalidChunk::InvalidMerkleProof)
    }

    // Validates all the header fields to be consistent and within
    // expected bounds. Does not check the signature yet.
    fn validate_chunk_meta(
        &self,
        common_header: &Ref<&[u8], RaptorcastHeader>,
        max_age_ms: u64,
    ) -> Result<ChunkMeta, InvalidChunk> {
        let app_message_len = self.header.app_message_len.get() as usize;
        ensure!(
            app_message_len > 0 && app_message_len <= MAX_MESSAGE_SIZE,
            InvalidChunk::InvalidAppMessageLen(app_message_len)
        );

        let timestamp = self.header.unix_ts_ms.get();
        ensure_valid_timestamp(timestamp, max_age_ms)?;

        let merkle_root = self.compute_merkle_root()?;
        let chunk_len = self.payload().len();
        ensure!(
            // a chunk is either a full message, or at least the minimum chunk length
            chunk_len >= regular::MIN_CHUNK_LENGTH || chunk_len == app_message_len,
            InvalidChunk::InvalidChunkLen
        );

        let num_source_symbols = app_message_len.div_ceil(chunk_len);
        ensure!(
            // defensive: should be unreachable given the validated
            // range for app_message_len and chunk_len.
            (monad_raptor::SOURCE_SYMBOLS_MIN..=monad_raptor::SOURCE_SYMBOLS_MAX)
                .contains(&num_source_symbols),
            InvalidChunk::InvalidAppMessageLen(app_message_len)
        );
        let mut chunk_id_cap = regular::MAX_REDUNDANCY
            .scale(num_source_symbols)
            .ok_or(InvalidChunk::InvalidChunkLen)?;

        let broadcast_mode = self.header.broadcast_mode()?;
        if matches!(broadcast_mode, BroadcastMode::Primary) {
            // TODO: use more accurate estimation for number of rounding chunks
            chunk_id_cap = chunk_id_cap.saturating_add(MAX_VALIDATOR_SET_SIZE);
        }

        let chunk_id = self.chunk_header.chunk_id.get() as usize;
        ensure!(chunk_id < chunk_id_cap, InvalidChunk::InvalidChunkId);

        let group_id = self.header.group_id()?;
        let signed_over_data = self.signed_over_data(common_header, &merkle_root);
        let app_message_hash = Some(HexBytes(self.header.app_message_hash));
        let recipient_hash = HexBytes(self.chunk_header.recipient_hash);

        Ok(ChunkMeta {
            app_message_len,
            chunk_id,
            num_source_symbols,
            encoded_symbol_capacity: chunk_id_cap,
            timestamp,
            broadcast_mode,
            encoding_scheme: EncodingScheme::Unspecified,
            group_id,
            app_message_hash,
            merkle_root: HexBytes(merkle_root),
            recipient_hash,
            signed_over_data,
        })
    }

    pub fn signed_over_data(
        &self,
        common_header: &Ref<&[u8], RaptorcastHeader>,
        merkle_root: &MerkleHash,
    ) -> SignedOverData {
        SignedOverData::V0(SignedOverDataV0::new(
            common_header,
            &self.header,
            merkle_root,
        ))
    }
}

pub struct RaptorcastPacketV1<'a> {
    pub header: Ref<&'a [u8], RaptorcastHeaderV1>,
    pub merkle_proof: Ref<&'a [u8], [MerkleHash]>,
    pub chunk_header: Ref<&'a [u8], RaptorcastChunkHeaderV1>,
    pub chunk_header_and_payload: &'a [u8],
    pub payload: &'a [u8],
    pub payload_offset: usize,
}

impl<'a> RaptorcastPacketV1<'a> {
    pub fn parse(rest: &'a [u8]) -> Result<Self, MalformedPacket> {
        // Deterministic raptorcast uses a globally fixed segment size.
        let expected_min_len = deterministic::MIN_SEGMENT_LEN - RaptorcastHeader::SIZE;
        let expected_max_len = deterministic::MAX_SEGMENT_LEN - RaptorcastHeader::SIZE;
        ensure!(rest.len() >= expected_min_len, MalformedPacket::TooShort);
        ensure!(rest.len() <= expected_max_len, MalformedPacket::TooLong);

        // Defensive: Should be unreachable given the asserted segment size.
        ensure!(
            rest.len() >= RaptorcastChunkHeaderV1::SIZE,
            MalformedPacket::TooShort
        );

        let (header_bytes, rest) = rest.split_at(RaptorcastHeaderV1::SIZE);
        let header: Ref<&[u8], RaptorcastHeaderV1> =
            Ref::from_bytes(header_bytes).map_err(|_| MalformedPacket::TooShort)?;

        let tree_depth = header.tree_depth()?;
        let merkle_proof_len = MERKLE_HASH_SIZE * (tree_depth - 1) as usize;
        ensure!(rest.len() > merkle_proof_len, MalformedPacket::TooShort);

        let (merkle_proof_bytes, chunk_header_and_payload) = rest.split_at(merkle_proof_len);
        let merkle_proof: Ref<&[u8], [MerkleHash]> =
            Ref::from_bytes(merkle_proof_bytes).map_err(|_| MalformedPacket::TooShort)?;

        ensure!(
            chunk_header_and_payload.len() >= RaptorcastChunkHeaderV1::SIZE,
            MalformedPacket::TooShort
        );
        let (chunk_header_bytes, payload) =
            chunk_header_and_payload.split_at(RaptorcastChunkHeaderV1::SIZE);
        let chunk_header: Ref<&[u8], RaptorcastChunkHeaderV1> =
            Ref::from_bytes(chunk_header_bytes).map_err(|_| MalformedPacket::TooShort)?;

        ensure!(!payload.is_empty(), MalformedPacket::TooShort);

        let payload_offset =
            RAPTORCAST_HEADER_V1_SIZE + merkle_proof_len + RaptorcastChunkHeaderV1::SIZE;

        Ok(Self {
            header,
            merkle_proof,
            chunk_header,
            chunk_header_and_payload,
            payload,
            payload_offset,
        })
    }

    pub fn split_chunk(&self, message: &Bytes) -> Result<Bytes, MalformedPacket> {
        ensure!(
            message.len() >= self.payload_offset,
            MalformedPacket::TooShort
        );
        Ok(message.slice(self.payload_offset..))
    }

    pub fn payload(&self) -> &[u8] {
        &self.chunk_header_and_payload[RaptorcastChunkHeaderV1::SIZE..]
    }

    pub fn signed_over_data(&self, common_header: &Ref<&[u8], RaptorcastHeader>) -> SignedOverData {
        SignedOverData::V1(SignedOverDataV1::new(common_header, &self.header))
    }

    // Validates all the header fields to be consistent and within
    // expected bounds. Does not check the signature yet.
    fn validate_chunk_meta(
        &self,
        common_header: &Ref<&'a [u8], RaptorcastHeader>,
        max_age_ms: u64,
    ) -> Result<ChunkMeta, InvalidChunk> {
        let encoding_scheme = self.header.encoding_scheme()?;
        // ensure encoding scheme is recognized before any other validation

        let app_message_len = self.ensure_app_message_length()?;
        let timestamp = self.header.unix_ts_ms.get();
        ensure_valid_timestamp(timestamp, max_age_ms)?;

        let symbol_len = self.payload().len();
        ensure!(
            symbol_len >= deterministic::MIN_SYMBOL_LEN || symbol_len == app_message_len,
            InvalidChunk::InvalidChunkLen
        );
        ensure!(
            symbol_len <= deterministic::MAX_SYMBOL_LEN,
            InvalidChunk::InvalidChunkLen
        );

        let num_source_symbols = app_message_len.div_ceil(symbol_len);
        ensure!(
            (monad_raptor::SOURCE_SYMBOLS_MIN..=monad_raptor::SOURCE_SYMBOLS_MAX)
                .contains(&num_source_symbols),
            InvalidChunk::InvalidAppMessageLen(app_message_len)
        );

        let broadcast_mode = self.header.broadcast_mode()?;
        let chunk_id_cap = stake_partition_num_chunks_hint(
            num_source_symbols,
            deterministic::DEFAULT_REDUNDANCY,
            MAX_VALIDATOR_SET_SIZE,
        )
        .ok_or(InvalidChunk::InvalidChunkId)?;
        let chunk_id = self.chunk_header.chunk_id.get() as usize;
        ensure!(chunk_id < chunk_id_cap, InvalidChunk::InvalidChunkId);

        let group_id = self.header.group_id()?;
        let signed_over_data = self.signed_over_data(common_header);
        let merkle_root = self.validate_merkle_root()?;
        let recipient_hash = HexBytes(self.chunk_header.recipient_hash);

        Ok(ChunkMeta {
            app_message_len,
            chunk_id,
            num_source_symbols,
            encoded_symbol_capacity: chunk_id_cap,
            timestamp,
            broadcast_mode,
            encoding_scheme,
            group_id,
            app_message_hash: None,
            merkle_root: HexBytes(merkle_root),
            recipient_hash,
            signed_over_data,
        })
    }

    fn ensure_app_message_length(&self) -> Result<usize, InvalidChunk> {
        let app_message_len = self.header.app_message_len.get() as usize;
        ensure!(
            app_message_len > 0 && app_message_len <= MAX_MESSAGE_SIZE,
            InvalidChunk::InvalidAppMessageLen(app_message_len)
        );
        Ok(app_message_len)
    }

    fn validate_merkle_root(&self) -> Result<MerkleHash, InvalidChunk> {
        let proof = MerkleProof::new_from_leaf_idx(
            self.merkle_proof.to_vec(),
            self.chunk_header.merkle_leaf_index(),
        )
        .ok_or(InvalidChunk::InvalidMerkleProof)?;

        let mut hasher = HasherType::new();
        hasher.update(self.chunk_header_and_payload);
        let leaf_hash = hasher.hash();

        let root = proof
            .compute_root(&leaf_hash)
            .ok_or(InvalidChunk::InvalidMerkleProof)?;

        if root != self.header.global_merkle_root {
            return Err(InvalidChunk::InvalidMerkleProof);
        }

        Ok(root)
    }
}

pub enum RaptorcastPacketVersioned<'a> {
    V0(RaptorcastPacketV0<'a>),
    V1(RaptorcastPacketV1<'a>),
}

pub type CommonRaptorcastHeader<'a> = Ref<&'a [u8], RaptorcastHeader>;

pub struct ChunkValidationEnv<'a, ST: CertificateSignatureRecoverable, F> {
    // signature verification cache & rate limiter
    pub signature_verifier: &'a mut ChunkSignatureVerifier<ST>,
    // bypass sig verification rate limiter if validator is sender
    pub bypass_rate_limiter: F,
    // replay protection
    pub max_age_ms: u64,
    // for loopback detection
    pub self_id: &'a NodeId<CertificateSignaturePubKey<ST>>,
}

pub struct RaptorcastPacket<'a> {
    pub common_header: CommonRaptorcastHeader<'a>,
    pub versioned: RaptorcastPacketVersioned<'a>,
}

impl<'a> RaptorcastPacket<'a> {
    pub fn parse(data: &'a [u8]) -> Result<Self, MalformedPacket> {
        ensure!(
            data.len() >= RaptorcastHeader::SIZE,
            MalformedPacket::TooShort
        );
        let (header_bytes, rest) = data.split_at(RaptorcastHeader::SIZE);
        let header: Ref<&[u8], RaptorcastHeader> =
            Ref::from_bytes(header_bytes).map_err(|_| MalformedPacket::TooShort)?;

        let versioned = match header.version.get() {
            0 => RaptorcastPacketVersioned::V0(RaptorcastPacketV0::parse(rest)?),
            1 => RaptorcastPacketVersioned::V1(RaptorcastPacketV1::parse(rest)?),
            v => return Err(MalformedPacket::UnknownVersion(v)),
        };

        Ok(Self {
            common_header: header,
            versioned,
        })
    }

    fn validate_chunk_meta(&self, max_age_ms: u64) -> Result<ChunkMeta, InvalidChunk> {
        match &self.versioned {
            RaptorcastPacketVersioned::V0(v0) => {
                v0.validate_chunk_meta(&self.common_header, max_age_ms)
            }
            RaptorcastPacketVersioned::V1(v1) => {
                v1.validate_chunk_meta(&self.common_header, max_age_ms)
            }
        }
    }

    pub fn validate_chunk<ST, F>(
        &self,
        message: &Bytes,
        env: ChunkValidationEnv<'_, ST, F>,
    ) -> Result<ValidatedChunk<CertificateSignaturePubKey<ST>>, InvalidChunk>
    where
        ST: CertificateSignatureRecoverable,
        F: FnOnce(Epoch) -> bool,
    {
        let meta = self.validate_chunk_meta(env.max_age_ms)?;
        let author = verify_signature(env.signature_verifier, &meta, env.bypass_rate_limiter)?;
        let chunk = match &self.versioned {
            RaptorcastPacketVersioned::V0(v0) => v0.split_chunk(message)?,
            RaptorcastPacketVersioned::V1(v1) => v1.split_chunk(message)?,
        };

        ensure!(author != *env.self_id, InvalidChunk::Loopback);

        Ok(ValidatedChunk {
            chunk,
            message: message.clone(),
            author,
            group_id: meta.group_id,
            unix_ts_ms: meta.timestamp,
            app_message_hash: meta.app_message_hash,
            merkle_root: meta.merkle_root,
            app_message_len: meta.app_message_len as u32,
            encoding_scheme: meta.encoding_scheme,
            broadcast_mode: meta.broadcast_mode,
            recipient_hash: meta.recipient_hash,
            chunk_id: meta.chunk_id as u16,
            num_source_symbols: meta.num_source_symbols,
            encoded_symbol_capacity: meta.encoded_symbol_capacity,
        })
    }
}

fn ensure_valid_timestamp(packet_ts_ms: u64, max_age_ms: u64) -> Result<(), InvalidChunk> {
    let current_ts_ms = if let Ok(current_time_elapsed) = std::time::UNIX_EPOCH.elapsed() {
        current_time_elapsed.as_millis() as u64
    } else {
        warn!("system time is before unix epoch, ignoring timestamp");
        return Ok(());
    };

    let delta_abs = current_ts_ms.abs_diff(packet_ts_ms);
    if delta_abs > max_age_ms {
        return Err(InvalidChunk::InvalidTimestamp {
            packet_ts_ms,
            current_ts_ms,
        });
    }

    Ok(())
}

fn verify_signature<ST, F>(
    signature_verifier: &mut ChunkSignatureVerifier<ST>,
    chunk_meta: &ChunkMeta,
    bypass_rate_limiter: F,
) -> Result<NodeId<CertificateSignaturePubKey<ST>>, InvalidChunk>
where
    ST: CertificateSignatureRecoverable,
    F: FnOnce(Epoch) -> bool,
{
    let signed_over_data = &chunk_meta.signed_over_data;
    if let Some(author) = signature_verifier.load_cached(signed_over_data) {
        return Ok(author); // cache hit
    };

    let signature = signed_over_data.signature();
    let signature = <ST as CertificateSignature>::deserialize(signature)
        .map_err(|_| InvalidChunk::InvalidSignature)?;

    // bypass signature verification rate limiter if sender is
    // validator.
    let bypass = chunk_meta.get_epoch().is_some_and(bypass_rate_limiter);
    let author = if bypass {
        signature_verifier.verify_force(signature, signed_over_data.signed_message())
    } else {
        signature_verifier.verify(signature, signed_over_data.signed_message())
    }?;
    signature_verifier.save_cache(*signed_over_data, author);

    Ok(author)
}
