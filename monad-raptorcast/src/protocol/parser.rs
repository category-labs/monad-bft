use bytes::Bytes;
use monad_crypto::{
    certificate_signature::{
        CertificateSignature, CertificateSignaturePubKey, CertificateSignatureRecoverable,
    },
    hasher::{Hasher as _, HasherType},
    signing_domain,
};
use monad_merkle::{MerkleHash, MerkleProof};
use monad_types::{Epoch, Round};
use tracing::warn;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Ref, LE, U16, U32, U64};

use crate::{
    message::MAX_MESSAGE_SIZE,
    udp::{
        GroupId, MessageValidationError, ValidatedMessage, MAX_MERKLE_TREE_DEPTH, MIN_CHUNK_LENGTH,
        MIN_MERKLE_TREE_DEPTH,
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

    pub fn broadcast_mode(&self) -> Result<BroadcastMode, MessageValidationError> {
        match (self.broadcast(), self.secondary_broadcast()) {
            (true, false) => Ok(BroadcastMode::Primary),
            (false, true) => Ok(BroadcastMode::Secondary),
            (false, false) => Ok(BroadcastMode::Unspecified),
            (true, true) => Err(MessageValidationError::InvalidBroadcastBits(0b11)),
        }
    }

    pub fn group_id(&self) -> Result<GroupId, MessageValidationError> {
        match self.broadcast_mode()? {
            BroadcastMode::Primary | BroadcastMode::Unspecified => {
                // Note: GroupId is set to Epoch for
                // validator-to-validator broadcast.
                Ok(GroupId::Primary(Epoch(self.group_id.get())))
            }
            BroadcastMode::Secondary => Ok(GroupId::Secondary(Round(self.group_id.get()))),
        }
    }
}

/// Combined header size for V0 (common header + versioned header)
const RAPTORCAST_HEADER_V0_SIZE: usize = RaptorcastHeader::SIZE + RaptorcastHeaderV0::SIZE;

const _: () = assert!(
    RAPTORCAST_HEADER_V0_SIZE == crate::packet::assembler::HEADER_LEN,
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
    RaptorcastChunkHeaderV0::SIZE == crate::packet::assembler::CHUNK_HEADER_LEN,
    "RaptorcastChunkHeader size must match CHUNK_HEADER_LEN"
);

#[repr(transparent)]
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct SignedOverDataV0([u8; RAPTORCAST_HEADER_V0_SIZE + MERKLE_HASH_SIZE]);

impl SignedOverDataV0 {
    fn new(
        common_header: &Ref<&[u8], RaptorcastHeader>,
        versioned_header: &Ref<&[u8], RaptorcastHeaderV0>,
        merkle_root: &MerkleHash,
    ) -> Self {
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
}

/// V0 packet structure (everything after the common header)
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

        let tree_depth = header.tree_depth();
        if !(MIN_MERKLE_TREE_DEPTH..=MAX_MERKLE_TREE_DEPTH).contains(&tree_depth) {
            return Err(MessageValidationError::InvalidTreeDepth);
        }

        let merkle_proof_len = header.merkle_proof_size();
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

    pub fn payload(&self) -> &'a [u8] {
        self.payload
    }

    pub fn split_chunk(&self, message: &Bytes) -> Result<Bytes, MessageValidationError> {
        if message.len() < self.payload_offset {
            return Err(MessageValidationError::TooShort);
        }
        Ok(message.slice(self.payload_offset..))
    }

    pub fn compute_merkle_root(&self) -> Result<MerkleHash, MessageValidationError> {
        let proof = MerkleProof::new_from_leaf_idx(
            self.merkle_proof.to_vec(),
            self.chunk_header.merkle_leaf_idx,
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
    ) -> SignedOverDataV0 {
        SignedOverDataV0::new(common_header, &self.header, merkle_root)
    }
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub enum SignedOverData {
    V0(SignedOverDataV0),
}

impl SignedOverData {
    fn signed_message(&self) -> &[u8] {
        match self {
            SignedOverData::V0(v0) => v0.signed_message(),
        }
    }
}

pub enum RaptorcastPacketVersioned<'a> {
    V0(RaptorcastPacketV0<'a>),
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
            v => return Err(MessageValidationError::UnknownVersion(v)),
        };

        Ok(Self {
            common_header: header,
            versioned,
        })
    }
}

use super::signature_verifier::SignatureVerifier;

pub fn validate_message_v0<ST>(
    signature_verifier: &mut SignatureVerifier<ST, SignedOverData, signing_domain::RaptorcastChunk>,
    common_header: &Ref<&[u8], RaptorcastHeader>,
    packet: &RaptorcastPacketV0,
    message: &Bytes,
    max_age_ms: u64,
    bypass_rate_limiter: bool,
) -> Result<ValidatedMessage<CertificateSignaturePubKey<ST>>, MessageValidationError>
where
    ST: CertificateSignatureRecoverable,
{
    // V0 protocol validation
    let app_message_len = packet.header.app_message_len.get() as usize;
    if app_message_len > MAX_MESSAGE_SIZE {
        return Err(MessageValidationError::TooLong);
    }

    // Enforce a minimum chunk size for messages consisting of multiple source chunks.
    let chunk_len = packet.payload.len();
    if chunk_len < MIN_CHUNK_LENGTH && app_message_len > chunk_len {
        return Err(MessageValidationError::ChunkTooSmall(chunk_len));
    }

    ensure_valid_timestamp(packet.header.unix_ts_ms.get(), max_age_ms)?;

    let signature = <ST as CertificateSignature>::deserialize(&common_header.signature)
        .map_err(|_| MessageValidationError::InvalidSignature)?;

    let merkle_root = packet.compute_merkle_root()?;

    let signed_over = SignedOverData::V0(packet.signed_over_data(common_header, &merkle_root));

    let author = if let Some(author) = signature_verifier.try_cached(&signed_over) {
        author
    } else {
        let new_author = signature_verifier.verify(
            signature,
            signed_over.signed_message(),
            bypass_rate_limiter,
        )?;
        signature_verifier.save_cache(signed_over, new_author);
        new_author
    };

    Ok(ValidatedMessage {
        message: message.clone(),
        author,
        group_id: packet.header.group_id()?,
        unix_ts_ms: packet.header.unix_ts_ms.get(),
        app_message_hash: HexBytes(packet.header.app_message_hash),
        app_message_len: packet.header.app_message_len.get(),
        broadcast_mode: packet.header.broadcast_mode()?,
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

pub(crate) fn validate_chunk_id(
    packet: &RaptorcastPacketV0,
    max_redundancy: Redundancy,
    max_rounding_chunks: usize,
) -> Result<(), MessageValidationError> {
    let symbol_len = packet.payload.len();
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
