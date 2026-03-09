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
use monad_types::{Epoch, Round};
use tracing::warn;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Ref, LE, U16, U32, U64};

use crate::{
    message::MAX_MESSAGE_SIZE,
    packet::{self, deterministic, regular},
    udp::{
        ChunkSignatureVerifier, GroupId, MessageIdentifier, MessageValidationError,
        SignatureCacheKey, ValidatedMessage, MAX_VALIDATOR_SET_SIZE,
    },
    util::{BroadcastMode, HexBytes, Redundancy},
    SIGNATURE_SIZE,
};

const MERKLE_HASH_SIZE: usize = 20;

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

/// Raptorcast packet V1 versioned header layout (follows common header):
/// - 2 bits => broadcast mode
/// - 2 bits => unused
/// - 4 bits => Merkle tree depth
/// - 8 bytes (u64) => Round #
/// - 8 bytes (u64) => Epoch #
/// - 8 bytes (u64) => Unix timestamp
/// - 20 bytes => global merkle tree root for the full message
/// - 4 bytes (u32) => Serialized AppMessage length (bytes)
#[repr(C, packed)]
#[derive(FromBytes, IntoBytes, Immutable, KnownLayout, Clone, Copy)]
pub struct RaptorcastHeaderV1 {
    broadcast_tree_depth: u8,
    round: U64<LE>,
    epoch: U64<LE>,
    unix_ts_ms: U64<LE>,
    global_merkle_root: [u8; MERKLE_HASH_SIZE],
    app_message_len: U32<LE>,
}

impl RaptorcastHeaderV1 {
    const SIZE: usize = 1 + 8 + 8 + 8 + MERKLE_HASH_SIZE + 4;

    pub fn broadcast_mode(&self) -> Result<BroadcastMode, MessageValidationError> {
        let round = Round(self.round.get());
        match (self.broadcast_tree_depth & 0b1100_0000) >> 6 {
            0b10 => Ok(BroadcastMode::DeterministicPrimary(round)),
            bits => Err(MessageValidationError::InvalidBroadcastBits(bits)),
        }
    }

    #[allow(clippy::manual_range_contains)]
    pub fn tree_depth(&self) -> Result<u8, MessageValidationError> {
        let depth = self.broadcast_tree_depth & 0b0000_1111;
        if depth < packet::deterministic::MIN_MERKLE_TREE_DEPTH
            || depth > packet::deterministic::MAX_MERKLE_TREE_DEPTH
        {
            return Err(MessageValidationError::InvalidTreeDepth);
        }

        Ok(depth)
    }

    pub fn group_id(&self) -> Result<GroupId, MessageValidationError> {
        let epoch = Epoch(self.epoch.get());
        match self.broadcast_mode()? {
            BroadcastMode::DeterministicPrimary(_) => Ok(GroupId::Primary(epoch)),
            _broadcast_mode => unreachable!(), // broadcast_mode is always DeterministicPrimary for V1
        }
    }
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

    pub fn broadcast_mode(&self) -> Result<BroadcastMode, MessageValidationError> {
        match (self.broadcast_tree_depth & 0b1100_0000) >> 6 {
            0b10 => Ok(BroadcastMode::Primary),
            0b01 => Ok(BroadcastMode::Secondary),
            0b00 => Ok(BroadcastMode::Unspecified),
            bits => Err(MessageValidationError::InvalidBroadcastBits(bits)),
        }
    }

    #[allow(clippy::manual_range_contains)]
    pub fn tree_depth(&self) -> Result<u8, MessageValidationError> {
        let depth = self.broadcast_tree_depth & 0b0000_1111;
        if depth < packet::regular::MIN_MERKLE_TREE_DEPTH
            || depth > packet::regular::MAX_MERKLE_TREE_DEPTH
        {
            return Err(MessageValidationError::InvalidTreeDepth);
        }

        Ok(depth)
    }

    pub fn group_id(&self) -> Result<GroupId, MessageValidationError> {
        match self.broadcast_mode()? {
            BroadcastMode::Primary | BroadcastMode::Unspecified => {
                Ok(GroupId::Primary(Epoch(self.group_id.get())))
            }
            BroadcastMode::Secondary => Ok(GroupId::Secondary(Round(self.group_id.get()))),
            // broadcast_mode can only be Primary, Secondary, or Unspecified for V0
            _broadcast_mode => unreachable!(),
        }
    }
}

/// Combined header size for V0 (common header + versioned header)
const RAPTORCAST_HEADER_V0_SIZE: usize = RaptorcastHeader::SIZE + RaptorcastHeaderV0::SIZE;

const _: () = assert!(
    RAPTORCAST_HEADER_V0_SIZE == crate::packet::regular::PacketLayout::HEADER_LEN,
    "RaptorcastHeader size must match HEADER_LEN"
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

const _: () = assert!(
    RaptorcastChunkHeaderV0::SIZE == crate::packet::regular::PacketLayout::CHUNK_HEADER_LEN,
    "RaptorcastChunkHeader size must match CHUNK_HEADER_LEN"
);

/// Combined header size for V1 (common header + versioned header)
const RAPTORCAST_HEADER_V1_SIZE: usize = RaptorcastHeader::SIZE + RaptorcastHeaderV1::SIZE;

const _: () = assert!(
    RAPTORCAST_HEADER_V1_SIZE == crate::packet::deterministic::PacketLayout::HEADER_LEN,
    "RaptorcastHeader size must match HEADER_LEN"
);

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

const _: () = assert!(
    RaptorcastChunkHeaderV1::SIZE == crate::packet::deterministic::PacketLayout::CHUNK_HEADER_LEN,
    "RaptorcastChunkHeader size must match CHUNK_HEADER_LEN"
);

/// header + merkle root, used as cache key for signatures
#[repr(transparent)]
#[derive(Clone, PartialEq, Eq, Hash)]
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

    pub fn signed_message(&self) -> &[u8] {
        &self.0[SIGNATURE_SIZE..]
    }
}

#[derive(Clone, PartialEq, Eq, Hash)]
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
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub enum SignedOverData {
    V0(SignedOverDataV0),
    V1(SignedOverDataV1),
}

impl SignedOverData {
    pub fn signed_message(&self) -> &[u8] {
        match self {
            SignedOverData::V0(v0) => v0.signed_message(),
            SignedOverData::V1(v1) => v1.signed_message(),
        }
    }
}

pub struct RaptorcastPacketV0<'a> {
    pub header: Ref<&'a [u8], RaptorcastHeaderV0>,
    pub merkle_proof: Ref<&'a [u8], [MerkleHash]>,
    pub chunk_header: Ref<&'a [u8], RaptorcastChunkHeaderV0>,
    pub chunk_header_and_payload: &'a [u8],
    pub payload: &'a [u8],
    pub payload_offset: usize,
}

impl<'a> RaptorcastPacketV0<'a> {
    pub fn parse(rest: &'a [u8]) -> Result<Self, MessageValidationError> {
        if rest.len() < RaptorcastHeaderV0::SIZE {
            return Err(MessageValidationError::TooShort);
        }
        let (header_bytes, rest) = rest.split_at(RaptorcastHeaderV0::SIZE);
        let header: Ref<&[u8], RaptorcastHeaderV0> =
            Ref::from_bytes(header_bytes).map_err(|_| MessageValidationError::TooShort)?;

        let tree_depth = header.tree_depth()?;
        let merkle_proof_len = MERKLE_HASH_SIZE * (tree_depth - 1) as usize;
        if rest.len() < merkle_proof_len {
            return Err(MessageValidationError::TooShort);
        }
        let (merkle_proof_bytes, chunk_header_and_payload) = rest.split_at(merkle_proof_len);
        let merkle_proof: Ref<&[u8], [MerkleHash]> =
            Ref::from_bytes(merkle_proof_bytes).map_err(|_| MessageValidationError::TooShort)?;

        if chunk_header_and_payload.len() < RaptorcastChunkHeaderV0::SIZE {
            return Err(MessageValidationError::TooShort);
        }
        let (chunk_header_bytes, payload) =
            chunk_header_and_payload.split_at(RaptorcastChunkHeaderV0::SIZE);
        let chunk_header: Ref<&[u8], RaptorcastChunkHeaderV0> =
            Ref::from_bytes(chunk_header_bytes).map_err(|_| MessageValidationError::TooShort)?;

        if payload.is_empty() {
            return Err(MessageValidationError::TooShort);
        }

        let payload_offset =
            RAPTORCAST_HEADER_V0_SIZE + merkle_proof_len + RaptorcastChunkHeaderV0::SIZE;

        Ok(Self {
            header,
            merkle_proof,
            chunk_header,
            chunk_header_and_payload,
            payload,
            payload_offset,
        })
    }

    pub fn split_chunk(&self, message: &Bytes) -> Result<Bytes, MessageValidationError> {
        if message.len() < self.payload_offset {
            return Err(MessageValidationError::TooShort);
        }
        Ok(message.slice(self.payload_offset..))
    }

    pub fn payload(&self) -> &[u8] {
        &self.chunk_header_and_payload[RaptorcastChunkHeaderV0::SIZE..]
    }

    pub fn compute_merkle_root(&self) -> Result<MerkleHash, MessageValidationError> {
        let proof = MerkleProof::new_from_leaf_idx(
            self.merkle_proof.to_vec(),
            self.chunk_header.merkle_leaf_idx as u16,
        )
        .ok_or(MessageValidationError::InvalidMerkleProof)?;

        let mut hasher = HasherType::new();
        hasher.update(self.chunk_header_and_payload);
        let leaf_hash = hasher.hash();

        proof
            .compute_root(&leaf_hash)
            .ok_or(MessageValidationError::InvalidMerkleProof)
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
    pub fn parse(rest: &'a [u8]) -> Result<Self, MessageValidationError> {
        if rest.len() < RaptorcastHeaderV1::SIZE {
            return Err(MessageValidationError::TooShort);
        }
        let (header_bytes, rest) = rest.split_at(RaptorcastHeaderV1::SIZE);
        let header: Ref<&[u8], RaptorcastHeaderV1> =
            Ref::from_bytes(header_bytes).map_err(|_| MessageValidationError::TooShort)?;

        let app_message_len = header.app_message_len.get() as usize;
        if app_message_len > MAX_MESSAGE_SIZE {
            return Err(MessageValidationError::TooLong);
        }
        let tree_depth = header.tree_depth()?;
        let merkle_proof_len = MERKLE_HASH_SIZE * (tree_depth - 1) as usize;
        if rest.len() < merkle_proof_len {
            return Err(MessageValidationError::TooShort);
        }
        let (merkle_proof_bytes, chunk_header_and_payload) = rest.split_at(merkle_proof_len);
        let merkle_proof: Ref<&[u8], [MerkleHash]> =
            Ref::from_bytes(merkle_proof_bytes).map_err(|_| MessageValidationError::TooShort)?;

        if chunk_header_and_payload.len() < RaptorcastChunkHeaderV1::SIZE {
            return Err(MessageValidationError::TooShort);
        }
        let (chunk_header_bytes, payload) =
            chunk_header_and_payload.split_at(RaptorcastChunkHeaderV1::SIZE);
        let chunk_header: Ref<&[u8], RaptorcastChunkHeaderV1> =
            Ref::from_bytes(chunk_header_bytes).map_err(|_| MessageValidationError::TooShort)?;

        if payload.is_empty() {
            return Err(MessageValidationError::TooShort);
        }

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

    pub fn split_chunk(&self, message: &Bytes) -> Result<Bytes, MessageValidationError> {
        if message.len() < self.payload_offset {
            return Err(MessageValidationError::TooShort);
        }
        Ok(message.slice(self.payload_offset..))
    }

    pub fn payload(&self) -> &[u8] {
        &self.chunk_header_and_payload[RaptorcastChunkHeaderV1::SIZE..]
    }

    pub fn validate_merkle_proof(&self) -> Result<(), MessageValidationError> {
        let proof = MerkleProof::new_from_leaf_idx(
            self.merkle_proof.to_vec(),
            self.chunk_header.merkle_leaf_index(),
        )
        .ok_or(MessageValidationError::InvalidMerkleProof)?;

        let mut hasher = HasherType::new();
        hasher.update(self.chunk_header_and_payload);
        let leaf_hash = hasher.hash();

        let root = proof
            .compute_root(&leaf_hash)
            .ok_or(MessageValidationError::InvalidMerkleProof)?;
        if self.header.global_merkle_root != root {
            return Err(MessageValidationError::InvalidMerkleProof);
        }
        Ok(())
    }

    // Loosely validate the tree depth without the knowledge of the
    // actual size of the validator set.
    //
    // SAFETY: the caller must ensure the preconditions hold
    pub fn validate_tree_depth(&self) -> Result<(), MessageValidationError> {
        let app_message_len = self.header.app_message_len.get() as usize;
        debug_assert!(app_message_len <= MAX_MESSAGE_SIZE);

        let min_depth = deterministic::PacketLayout::optimal_tree_depth(app_message_len, 1);
        let max_depth = deterministic::PacketLayout::optimal_tree_depth(
            app_message_len,
            MAX_VALIDATOR_SET_SIZE,
        );
        let actual_depth = self.header.tree_depth()?;
        if actual_depth < min_depth || actual_depth > max_depth {
            return Err(MessageValidationError::InvalidTreeDepth);
        }
        Ok(())
    }

    pub fn signed_over_data(&self, common_header: &Ref<&[u8], RaptorcastHeader>) -> SignedOverData {
        SignedOverData::V1(SignedOverDataV1::new(common_header, &self.header))
    }
}

pub enum RaptorcastPacketVersioned<'a> {
    V0(RaptorcastPacketV0<'a>),
    V1(RaptorcastPacketV1<'a>),
}

pub type CommonRaptorcastHeader<'a> = Ref<&'a [u8], RaptorcastHeader>;

pub struct RaptorcastPacket<'a> {
    pub common_header: CommonRaptorcastHeader<'a>,
    pub versioned: RaptorcastPacketVersioned<'a>,
}

impl<'a> RaptorcastPacket<'a> {
    pub fn parse(data: &'a [u8]) -> Result<Self, MessageValidationError> {
        if data.len() < RaptorcastHeader::SIZE {
            return Err(MessageValidationError::TooShort);
        }

        let (header_bytes, rest) = data.split_at(RaptorcastHeader::SIZE);
        let header: Ref<&[u8], RaptorcastHeader> =
            Ref::from_bytes(header_bytes).map_err(|_| MessageValidationError::TooShort)?;

        let versioned = match header.version.get() {
            0 => RaptorcastPacketVersioned::V0(RaptorcastPacketV0::parse(rest)?),
            1 => RaptorcastPacketVersioned::V1(RaptorcastPacketV1::parse(rest)?),
            v => return Err(MessageValidationError::UnknownVersion(v)),
        };

        Ok(Self {
            common_header: header,
            versioned,
        })
    }
}

pub fn validate_message_v0<ST, F>(
    signature_verifier: &mut ChunkSignatureVerifier<ST>,
    common_header: &Ref<&[u8], RaptorcastHeader>,
    packet: &RaptorcastPacketV0,
    message: &Bytes,
    max_age_ms: u64,
    bypass_rate_limiter: F,
) -> Result<ValidatedMessage<CertificateSignaturePubKey<ST>>, MessageValidationError>
where
    ST: CertificateSignatureRecoverable,
    F: FnOnce(Epoch) -> bool,
{
    let app_message_len = packet.header.app_message_len.get() as usize;
    if app_message_len > MAX_MESSAGE_SIZE {
        return Err(MessageValidationError::TooLong);
    }

    ensure_valid_timestamp(packet.header.unix_ts_ms.get(), max_age_ms)?;

    let broadcast_mode = packet.header.broadcast_mode()?;
    let group_id = packet.header.group_id()?;

    match broadcast_mode {
        BroadcastMode::Unspecified | BroadcastMode::Secondary => {
            validate_chunk_id(packet, regular::MAX_REDUNDANCY, 0)?
        }
        BroadcastMode::Primary => {
            validate_chunk_id(packet, regular::MAX_REDUNDANCY, MAX_VALIDATOR_SET_SIZE)?
        }
        BroadcastMode::DeterministicPrimary(_) => unreachable!(),
    };

    let signature = <ST as CertificateSignature>::deserialize(&common_header.signature)
        .map_err(|_| MessageValidationError::InvalidSignature)?;

    let merkle_root = packet.compute_merkle_root()?;

    let signed_over: SignatureCacheKey = packet.signed_over_data(common_header, &merkle_root);

    let author = if let Some(author) = signature_verifier.load_cached(&signed_over) {
        author
    } else {
        let new_author = match group_id {
            GroupId::Primary(epoch) if bypass_rate_limiter(epoch) => {
                signature_verifier.verify_force(signature, signed_over.signed_message())?
            }
            _ => signature_verifier.verify(signature, signed_over.signed_message())?,
        };
        signature_verifier.save_cache(signed_over, new_author);
        new_author
    };

    Ok(ValidatedMessage {
        message: message.clone(),
        author,
        group_id,
        unix_ts_ms: packet.header.unix_ts_ms.get(),
        message_id: MessageIdentifier::AppMessageHash(HexBytes(packet.header.app_message_hash)),
        app_message_len: packet.header.app_message_len.get(),
        broadcast_mode,
        recipient_hash: HexBytes(packet.chunk_header.recipient_hash),
        chunk_id: packet.chunk_header.chunk_id.get(),
        chunk: packet.split_chunk(message)?,
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

pub fn validate_message_v1<ST, F>(
    signature_verifier: &mut ChunkSignatureVerifier<ST>,
    common_header: &Ref<&[u8], RaptorcastHeader>,
    packet: &RaptorcastPacketV1,
    message: &Bytes,
    max_age_ms: u64,
    bypass_rate_limiter: F,
) -> Result<ValidatedMessage<CertificateSignaturePubKey<ST>>, MessageValidationError>
where
    ST: CertificateSignatureRecoverable,
    F: FnOnce(Epoch) -> bool,
{
    // deterministic raptorcast requires a universal segment size
    if message.len() < deterministic::PacketLayout::SEGMENT_LEN {
        return Err(MessageValidationError::TooShort);
    } else if message.len() > deterministic::PacketLayout::SEGMENT_LEN {
        return Err(MessageValidationError::TooLong);
    }

    let signature = <ST as CertificateSignature>::deserialize(&common_header.signature)
        .map_err(|_| MessageValidationError::InvalidSignature)?;
    ensure_valid_timestamp(packet.header.unix_ts_ms.get(), max_age_ms)?;

    let app_message_len = packet.header.app_message_len.get() as usize;
    if app_message_len > MAX_MESSAGE_SIZE {
        return Err(MessageValidationError::TooLong);
    }

    let group_id = packet.header.group_id()?;
    let broadcast_mode = packet.header.broadcast_mode()?;
    let chunk_id = packet.chunk_header.chunk_id.get() as usize;
    validate_chunk_id_v1(chunk_id, app_message_len)?;

    packet.validate_tree_depth()?;
    packet.validate_merkle_proof()?;
    let global_merkle_root = HexBytes(packet.header.global_merkle_root);
    let message_id = MessageIdentifier::GlobalMerkleRoot(global_merkle_root);
    let signed_over: SignatureCacheKey = packet.signed_over_data(common_header);

    let author = if let Some(author) = signature_verifier.load_cached(&signed_over) {
        author
    } else {
        let new_author = match group_id {
            GroupId::Primary(epoch) if bypass_rate_limiter(epoch) => {
                signature_verifier.verify_force(signature, signed_over.signed_message())?
            }
            _ => signature_verifier.verify(signature, signed_over.signed_message())?,
        };
        signature_verifier.save_cache(signed_over, new_author);
        new_author
    };

    Ok(ValidatedMessage {
        message: message.clone(),
        author,
        group_id,
        unix_ts_ms: packet.header.unix_ts_ms.get(),
        message_id,
        app_message_len: packet.header.app_message_len.get(),
        broadcast_mode,
        recipient_hash: HexBytes(packet.chunk_header.recipient_hash),
        chunk_id: packet.chunk_header.chunk_id.get(),
        chunk: packet.split_chunk(message)?,
    })
}

fn validate_chunk_id_v1(
    chunk_id: usize,
    app_message_len: usize,
) -> Result<(), MessageValidationError> {
    if app_message_len == 0 {
        return Err(MessageValidationError::TooShort);
    } else if app_message_len > MAX_MESSAGE_SIZE {
        return Err(MessageValidationError::TooLong);
    }

    // SAFETY: app_message_len is within sanity bounds
    let max_layout = deterministic::PacketLayout::new(app_message_len, MAX_VALIDATOR_SET_SIZE);
    let chunk_id_cap_max = max_layout.encoded_symbol_capacity();

    if chunk_id >= chunk_id_cap_max {
        return Err(MessageValidationError::InvalidChunkId);
    }

    Ok(())
}

pub(crate) fn validate_chunk_id(
    packet: &RaptorcastPacketV0,
    max_redundancy: Redundancy,
    max_rounding_chunks: usize,
) -> Result<(), MessageValidationError> {
    let symbol_len = packet.payload().len();
    if symbol_len == 0 {
        return Err(MessageValidationError::TooShort);
    }
    let base_chunks = (packet.header.app_message_len.get() as usize).div_ceil(symbol_len);
    let num_chunks = max_redundancy
        .scale(base_chunks)
        .ok_or(MessageValidationError::TooLong)?
        + max_rounding_chunks;

    let chunk_id = packet.chunk_header.chunk_id.get() as usize;

    if chunk_id >= num_chunks {
        return Err(MessageValidationError::InvalidChunkId);
    }
    Ok(())
}
