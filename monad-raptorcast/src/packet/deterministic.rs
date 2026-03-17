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

use bytes::{Bytes, BytesMut};
use monad_crypto::{
    certificate_signature::{
        CertificateSignature, CertificateSignaturePubKey, CertificateSignatureRecoverable, PubKey,
    },
    hasher::Hasher as _,
    signing_domain,
};
use monad_dataplane::udp::segment_size_for_mtu;
use monad_merkle::{MerkleHash, MerkleTree};
use monad_raptor::Encoder;
use monad_types::{NodeId, Round, Stake, ETHERNET_MTU};
use monad_wireauth::messages::DataPacketHeader;

use super::{assigner::StakeBasedWithRC, Chunk, ChunkAssigner as _};
use crate::{
    message::MAX_MESSAGE_SIZE,
    packet::BuildError,
    udp::GroupId,
    util::{
        ensure, BroadcastMode, Collector, EncodingScheme, PrimaryBroadcastGroup, Redundancy,
        UdpMessage,
    },
    SIGNATURE_SIZE,
};

// Deterministic RaptorCast requires the parameters (segment_len,
// redundancy) to be fixed for all nodes in order to provide
// deterministic encoding guarantee.

const VERSION: u16 = 0x1;

// default parameter for Deterministic25 encoding scheme
pub const DEFAULT_REDUNDANCY: Redundancy = Redundancy::from_fract(2, 50);
pub const DEFAULT_SEGMENT_LEN: usize =
    segment_size_for_mtu(ETHERNET_MTU - DataPacketHeader::SIZE as u16) as usize;

// The smallest message requires 4 chunks (from redundancy=2.5 +
// min_validator_set_size=1), which is accommodated by a merkle tree
// of depth 3 with 4 leaves.
pub const MIN_MERKLE_TREE_DEPTH: u8 = 3;
pub const MAX_MERKLE_TREE_DEPTH: u8 = 15;

pub const MIN_SEGMENT_LEN: usize = DEFAULT_SEGMENT_LEN;
pub const MAX_SEGMENT_LEN: usize = DEFAULT_SEGMENT_LEN;

pub const MIN_SYMBOL_LEN: usize = DEFAULT_SEGMENT_LEN - PacketLayout::MAX_HEADER_LEN;
pub const MAX_SYMBOL_LEN: usize = DEFAULT_SEGMENT_LEN - PacketLayout::MIN_HEADER_LEN;

#[derive(Debug, Clone, Copy)]
pub(crate) struct PacketLayout {
    segment_len: usize,
    merkle_tree_depth: u8,
}

#[allow(clippy::identity_op)]
impl PacketLayout {
    pub const HEADER_LEN: usize = 0
        + SIGNATURE_SIZE // Sender signature (65 bytes)
        + 2  // Version
        + 1  // 2 bits for Broadcast Mode, 2 bits reserved, 4 bits for Merkle Tree Depth
        + 1  // Encoding scheme variant
        + 8  // Round #
        + 8  // Epoch #
        + 8  // Unix timestamp
        + 20 // Global merkle root
        + 4; // App message length

    pub const CHUNK_HEADER_LEN: usize = 0
        + 20 // Chunk recipient hash
        + 2  // Reserved
        + 2; // Chunk idx

    // the size of individual merkle hash
    pub const MERKLE_HASH_LEN: usize = 20;

    pub const MAX_HEADER_LEN: usize = PacketLayout::HEADER_LEN
        + PacketLayout::CHUNK_HEADER_LEN
        + PacketLayout::MERKLE_HASH_LEN * (MAX_MERKLE_TREE_DEPTH as usize - 1);
    pub const MIN_HEADER_LEN: usize = PacketLayout::HEADER_LEN
        + PacketLayout::CHUNK_HEADER_LEN
        + PacketLayout::MERKLE_HASH_LEN * (MIN_MERKLE_TREE_DEPTH as usize - 1);

    pub fn new(segment_len: usize, merkle_tree_depth: u8) -> Self {
        let header_len = Self::HEADER_LEN + Self::CHUNK_HEADER_LEN;
        let proof_len = Self::MERKLE_HASH_LEN * (merkle_tree_depth as usize - 1);
        let symbol_len = segment_len.saturating_sub(header_len + proof_len);
        assert!(symbol_len > 0);

        Self {
            segment_len,
            merkle_tree_depth,
        }
    }

    pub fn segment_len(&self) -> usize {
        self.segment_len
    }

    pub const fn symbol_len(&self) -> usize {
        self.segment_len - Self::HEADER_LEN - self.merkle_proof_len() - Self::CHUNK_HEADER_LEN
    }

    pub const fn num_symbols(&self, app_message_len: usize, redundancy: Redundancy) -> usize {
        let base_num_symbols = app_message_len.div_ceil(self.symbol_len());
        redundancy
            .scale(base_num_symbols)
            .expect("should never overflow")
    }

    pub const fn merkle_proof_len(&self) -> usize {
        Self::MERKLE_HASH_LEN * (self.merkle_tree_depth as usize - 1)
    }

    pub const fn header_len(&self) -> usize {
        Self::HEADER_LEN
    }

    const fn symbol_offset(&self) -> usize {
        Self::HEADER_LEN + self.merkle_proof_len() + Self::CHUNK_HEADER_LEN
    }

    pub fn merkle_tree_depth(&self) -> u8 {
        self.merkle_tree_depth
    }

    pub fn merkle_proof_range(&self) -> std::ops::Range<usize> {
        Self::HEADER_LEN..(Self::HEADER_LEN + self.merkle_proof_len())
    }

    pub fn chunk_header_range(&self) -> std::ops::Range<usize> {
        let start = Self::HEADER_LEN + self.merkle_proof_len();
        start..(start + Self::CHUNK_HEADER_LEN)
    }

    pub fn merkle_hashed_range(&self) -> std::ops::Range<usize> {
        // chunk header + symbol
        let start = Self::HEADER_LEN + self.merkle_proof_len();
        start..self.segment_len
    }

    pub fn symbol_range(&self) -> std::ops::Range<usize> {
        self.symbol_offset()..self.segment_len
    }

    pub fn symbol_mut<'a, PT: PubKey>(&self, chunk: &'a mut Chunk<PT>) -> &'a mut [u8] {
        &mut chunk.payload_mut()[self.symbol_range()]
    }

    pub fn write_header<PT: PubKey>(
        &self,
        chunk: &mut Chunk<PT>,
        header: &[u8], // including signature
    ) {
        chunk.payload_mut()[..self.header_len()].copy_from_slice(header);
    }

    pub fn write_chunk_header<PT: PubKey>(&self, chunk: &mut Chunk<PT>) -> Result<(), BuildError> {
        let recipient_hash = *chunk.recipient().node_hash();
        let chunk_id: [u8; 2] = u16::try_from(chunk.chunk_id())
            .map_err(|_| BuildError::ChunkIdOverflow)?
            .to_le_bytes();

        let buffer = &mut chunk.payload_mut()[self.chunk_header_range()];
        debug_assert_eq!(buffer.len(), Self::CHUNK_HEADER_LEN);

        buffer[0..20].copy_from_slice(&recipient_hash); // node_id hash
        buffer[20] = 0; // reserved
        buffer[21] = 0; // reserved
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

    pub fn chunk_hash<PT: PubKey>(&self, chunk: &Chunk<PT>) -> monad_crypto::hasher::Hash {
        let mut hasher = monad_crypto::hasher::HasherType::new();
        hasher.update(&chunk.payload()[self.merkle_hashed_range()]);
        monad_crypto::hasher::Hasher::hash(hasher)
    }
}

fn build_header<ST>(
    key: &ST::KeyPairType,
    broadcast_mode: BroadcastMode,
    encoding_scheme: EncodingScheme,
    merkle_tree_depth: u8,
    group_id: GroupId,
    unix_ts_ms: u64,
    global_merkle_root: &MerkleHash,
    app_message_len: usize,
) -> Result<Bytes, BuildError>
where
    ST: CertificateSignatureRecoverable,
{
    // 65 // Signature
    // 2  // Version
    // 1  // Broadcast mode bits (2 bits)
    //       2 unused bits,
    //       4 bits for Merkle tree depth
    // 1  // Encoding scheme variant
    // 8  // Round
    // 8  // Epoch
    // 8  // Unix timestamp
    // 20 // Global merkle root
    // 4  // App message length
    let mut buffer = BytesMut::zeroed(PacketLayout::HEADER_LEN);
    let cursor = &mut buffer[SIGNATURE_SIZE..];

    let (cursor_version, cursor) = cursor.split_at_mut_checked(2).expect("header too short");
    cursor_version.copy_from_slice(&VERSION.to_le_bytes());

    let (cursor_broadcast_merkle_depth, cursor) =
        cursor.split_at_mut_checked(1).expect("header too short");

    let broadcast_bits: u8 = match broadcast_mode {
        BroadcastMode::Primary => 0b10 << 6,
        _ => return Err(BuildError::InvalidBroadcastMode(broadcast_mode)),
    };
    if (merkle_tree_depth & 0b1111_0000) != 0 {
        return Err(BuildError::MerkleTreeTooDeep);
    }
    cursor_broadcast_merkle_depth[0] = broadcast_bits | (merkle_tree_depth & 0b0000_1111);

    let (cursor_encoding_scheme, cursor) =
        cursor.split_at_mut_checked(1).expect("header too short");
    let EncodingScheme::Deterministic25(round) = encoding_scheme else {
        return Err(BuildError::InvalidEncodingScheme);
    };
    cursor_encoding_scheme[0] = 0x1; // Deterministic25

    let GroupId::Primary(epoch) = group_id else {
        return Err(BuildError::InvalidGroupId(group_id));
    };
    let (cursor_round, cursor) = cursor.split_at_mut_checked(8).expect("header too short");
    cursor_round.copy_from_slice(&round.0.to_le_bytes());

    let (cursor_epoch, cursor) = cursor.split_at_mut_checked(8).expect("header too short");
    cursor_epoch.copy_from_slice(&epoch.0.to_le_bytes());

    let (cursor_unix_ts_ms, cursor) = cursor.split_at_mut_checked(8).expect("header too short");
    cursor_unix_ts_ms.copy_from_slice(&unix_ts_ms.to_le_bytes());

    let (cursor_global_merkle_root, cursor) =
        cursor.split_at_mut_checked(20).expect("header too short");
    cursor_global_merkle_root.copy_from_slice(global_merkle_root);

    let (cursor_app_message_len, cursor) =
        cursor.split_at_mut_checked(4).expect("header too short");
    let app_message_len: u32 = app_message_len
        .try_into()
        .map_err(|_| BuildError::AppMessageTooLarge)?;
    cursor_app_message_len.copy_from_slice(&app_message_len.to_le_bytes());

    // should have consumed the whole buffer
    debug_assert_eq!(cursor.len(), 0);

    let signed_over = &buffer[SIGNATURE_SIZE..];
    let signature = <ST as CertificateSignature>::serialize(&ST::sign::<
        signing_domain::RaptorcastChunk,
    >(signed_over, key));
    debug_assert_eq!(signature.len(), SIGNATURE_SIZE);

    buffer[..SIGNATURE_SIZE].copy_from_slice(&signature);

    Ok(buffer.freeze())
}

pub(super) struct GlobalMerkleTree<'a, PT: PubKey> {
    chunks: &'a mut [Chunk<PT>],
    tree: MerkleTree,
    layout: PacketLayout,
}

impl<'a, PT: PubKey> GlobalMerkleTree<'a, PT> {
    fn build(chunks: &'a mut [Chunk<PT>], layout: PacketLayout) -> Result<Self, BuildError> {
        chunks.sort_by_key(|c| c.chunk_id());

        let mut hashes = Vec::with_capacity(chunks.len());
        let depth = layout.merkle_tree_depth();
        debug_assert!(chunks.len() <= 2usize.pow((depth - 1) as u32));

        for (leaf_index, chunk) in chunks.iter_mut().enumerate() {
            if chunk.chunk_id() != leaf_index {
                return Err(BuildError::NonContiguousChunkIds);
            }

            layout.write_chunk_header(chunk)?;
            let hash = layout.chunk_hash(chunk);
            hashes.push(hash);
        }

        let tree = MerkleTree::new_with_depth(&hashes, depth);
        Ok(Self {
            chunks,
            tree,
            layout,
        })
    }

    fn write_header_and_proofs(&mut self, header: &[u8]) -> Result<(), BuildError> {
        for (leaf_idx, chunk) in self.chunks.iter_mut().enumerate() {
            // for deterministic rc, leaf_idx === chunk_id
            debug_assert_eq!(leaf_idx, chunk.chunk_id());

            // write header
            self.layout.write_header(chunk, header);

            // write merkle proof
            let leaf_idx: u16 = leaf_idx
                .try_into()
                .map_err(|_| BuildError::ChunkIdOverflow)?;
            let proof = self.tree.proof(leaf_idx);
            self.layout.write_merkle_proof(chunk, proof.siblings());
        }
        Ok(())
    }

    fn root(&self) -> &MerkleHash {
        self.tree.root()
    }
}

pub(crate) struct PrimaryEncoding<PT: PubKey> {
    layout: PacketLayout,
    app_message_len: usize,
    redundancy: Redundancy,
    // already shuffled
    validator_set: Vec<(NodeId<PT>, Stake)>,
}

impl<PT: PubKey> PrimaryEncoding<PT> {
    pub fn new<'a>(
        encoding_scheme: EncodingScheme,
        group: &'a PrimaryBroadcastGroup<'a, PT>,
        app_message_len: usize,
        unix_ts_ms: u64,
    ) -> Result<Self, BuildError> {
        let EncodingScheme::Deterministic25(round) = encoding_scheme else {
            return Err(BuildError::InvalidEncodingScheme);
        };

        ensure!(app_message_len > 0, BuildError::AppMessageEmpty);
        ensure!(
            app_message_len <= MAX_MESSAGE_SIZE,
            BuildError::AppMessageTooLarge
        );

        // Encoding scheme for Deterministic25
        let redundancy = DEFAULT_REDUNDANCY;
        let segment_len = DEFAULT_SEGMENT_LEN;

        // Search for optimal tree depth.
        let num_validators = group.len().into();
        let depth = optimal_merkle_tree_depth(|d| {
            let layout = PacketLayout::new(segment_len, d);
            let num_symbols = layout.num_symbols(app_message_len, redundancy);
            num_symbols.saturating_add(num_validators) // rounding chunks
        })
        .ok_or(BuildError::MerkleTreeTooDeep)?;
        let layout = PacketLayout::new(segment_len, depth);

        // Shuffle the validator set
        let author = group.author();
        let mut validator_set = group
            .iter()
            .map(|(node_id, stake)| (*node_id, *stake))
            .filter(|(node_id, _)| *node_id != *author)
            .collect::<Vec<_>>();
        let seed = derive_seed(author, round, unix_ts_ms);
        StakeBasedWithRC::shuffle_validators(&mut validator_set, seed);

        Ok(Self {
            app_message_len,
            redundancy,
            layout,
            validator_set,
        })
    }

    pub fn layout(&self) -> PacketLayout {
        self.layout
    }

    pub fn make_chunks(&self) -> Result<Vec<Chunk<PT>>, BuildError> {
        let num_symbols = self
            .layout
            .num_symbols(self.app_message_len, self.redundancy);
        let assigner = StakeBasedWithRC::from_validator_set(self.validator_set.clone());

        let assignment = assigner.assign_chunks(num_symbols, None)?;
        let chunks = assignment.generate(self.layout.segment_len());

        Ok(chunks)
    }
}

pub fn calc_tree_depth(
    encoding_scheme: EncodingScheme,
    app_message_len: usize,
    num_validators: usize,
) -> Option<u8> {
    let EncodingScheme::Deterministic25(_) = encoding_scheme else {
        // we only support Deterministic25 for now.
        return None;
    };

    let redundancy = DEFAULT_REDUNDANCY;
    let segment_len = DEFAULT_SEGMENT_LEN;

    let depth = optimal_merkle_tree_depth(|d| {
        let layout = PacketLayout::new(segment_len, d);
        let num_symbols = layout.num_symbols(app_message_len, redundancy);
        num_symbols.saturating_add(num_validators) // rounding chunks
    })?;

    Some(depth)
}

#[cfg(test)]
pub(crate) fn build_with_given_header<PT, C>(
    header: &[u8],
    app_message: &[u8],
    group: &PrimaryBroadcastGroup<'_, PT>,
    encoding_scheme: EncodingScheme,
    unix_ts_ms: u64,
    collector: &mut C,
) -> Result<(), BuildError>
where
    PT: PubKey,
    C: Collector<UdpMessage<PT>>,
{
    // step 1: chunk assignment
    let encoding = PrimaryEncoding::new(encoding_scheme, group, app_message.len(), unix_ts_ms)?;
    let layout = encoding.layout();
    let mut chunks = encoding.make_chunks()?;

    // step 2: write the encoded symbols into chunk's payload
    encode_unique_symbols(app_message, &mut chunks, layout)?;

    // step 3: build merkle tree & write chunk header
    let mut merkle_tree = GlobalMerkleTree::build(&mut chunks[..], layout)?;
    merkle_tree.write_header_and_proofs(header)?;

    // step 4: send out the chunks
    collector.reserve(chunks.len());
    for chunk in chunks.into_iter() {
        collector.push(chunk.into());
    }

    Ok(())
}

pub(crate) fn build<ST, C>(
    key: &ST::KeyPairType,
    unix_ts_ms: u64,
    app_message: &[u8],
    group: &PrimaryBroadcastGroup<'_, CertificateSignaturePubKey<ST>>,
    encoding_scheme: EncodingScheme,
    collector: &mut C,
) -> Result<(), BuildError>
where
    ST: CertificateSignatureRecoverable,
    C: Collector<UdpMessage<CertificateSignaturePubKey<ST>>>,
{
    // step 1: chunk assignment
    let encoding = PrimaryEncoding::new(encoding_scheme, group, app_message.len(), unix_ts_ms)?;
    let layout = encoding.layout();
    let mut chunks = encoding.make_chunks()?;

    // step 2: write the encoded symbols into chunk's payload
    encode_unique_symbols(app_message, &mut chunks, layout)?;

    // step 3: build merkle tree & write chunk header
    let mut merkle_tree = GlobalMerkleTree::build(&mut chunks[..], layout)?;

    // step 4: write header
    let group_id = group.group_id();
    let header = build_header::<ST>(
        key,
        BroadcastMode::Primary,
        encoding_scheme,
        layout.merkle_tree_depth(),
        group_id,
        unix_ts_ms,
        merkle_tree.root(),
        app_message.len(),
    )?;
    merkle_tree.write_header_and_proofs(&header)?;

    // step 5: send out the chunks
    collector.reserve(chunks.len());
    for chunk in chunks.into_iter() {
        collector.push(chunk.into());
    }

    Ok(())
}

// Calculate the deterministic global merkle root of a given app
// message without actually building the packets.
pub(crate) fn calc_global_merkle_root<PT: PubKey>(
    app_message: &[u8],
    group: &PrimaryBroadcastGroup<'_, PT>,
    encoding_scheme: EncodingScheme,
    unix_ts_ms: u64,
) -> Result<MerkleHash, BuildError> {
    // step 1: chunk assignment
    let encoding = PrimaryEncoding::new(encoding_scheme, group, app_message.len(), unix_ts_ms)?;
    let layout = encoding.layout();
    let mut chunks = encoding.make_chunks()?;

    // step 2: write the encoded symbols into chunk's payload
    encode_unique_symbols(app_message, &mut chunks, layout)?;

    let merkle_tree = GlobalMerkleTree::build(&mut chunks[..], layout)?;

    Ok(*merkle_tree.root())
}

fn encode_unique_symbols<PT: PubKey>(
    app_message: &[u8],
    chunks: &mut [Chunk<PT>],
    layout: PacketLayout,
) -> Result<(), BuildError> {
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

// Derive the seed used to shuffling validator set for deterministic raptorcast.
// Layout: round (8) || floor(unix_ts_ms / 2048) (8) || author_pk[..16] (16) = 32 bytes
pub fn derive_seed<PT: PubKey>(author: &NodeId<PT>, round: Round, unix_ts_ms: u64) -> [u8; 32] {
    let author_bytes = author.pubkey().bytes();
    let coarse_ts = unix_ts_ms / 2048; // ~2s resolution
    let mut seed = [0u8; 32];
    seed[..8].copy_from_slice(&round.0.to_le_bytes());
    seed[8..16].copy_from_slice(&coarse_ts.to_le_bytes());
    seed[16..].copy_from_slice(&author_bytes[..16]);
    seed
}

fn optimal_merkle_tree_depth(calc_total_symbols: impl Fn(u8) -> usize) -> Option<u8> {
    for depth in MIN_MERKLE_TREE_DEPTH..=MAX_MERKLE_TREE_DEPTH {
        let leaf_count = 2usize.pow((depth - 1) as u32);
        if calc_total_symbols(depth) <= leaf_count {
            return Some(depth);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use monad_merkle::MerkleTree;

    use super::{
        optimal_merkle_tree_depth, PacketLayout, DEFAULT_REDUNDANCY, DEFAULT_SEGMENT_LEN,
        MAX_MERKLE_TREE_DEPTH, MAX_SYMBOL_LEN, MIN_MERKLE_TREE_DEPTH, MIN_SYMBOL_LEN,
    };
    use crate::{
        message::MAX_MESSAGE_SIZE,
        udp::{MAX_NUM_PACKETS, MAX_VALIDATOR_SET_SIZE},
        util::Redundancy,
    };

    // Statically asserted properties
    const _: () = assert!(
        super::MAX_MERKLE_TREE_DEPTH <= 0b1111,
        "MAX_MERKLE_TREE_DEPTH must fit in 4 bits"
    );
    const _: () = assert!(
        MIN_MERKLE_TREE_DEPTH <= MAX_MERKLE_TREE_DEPTH,
        "MIN_MERKLE_TREE_DEPTH must be less than MAX_MERKLE_TREE_DEPTH"
    );
    const _: () = assert!(
        MIN_SYMBOL_LEN >= crate::packet::regular::MIN_CHUNK_LENGTH,
        "MIN_SYMBOL_LEN must be no smaller than regular MIN_CHUNK_LENGTH"
    );

    // Statically asserted fixed value for constants
    //   1500 (ethernet mtu)
    // - 20   (ip header)
    // - 8    (udp header)
    // - 32   (wireauth header)
    // = 1440
    const _: () = assert!(
        DEFAULT_SEGMENT_LEN == 1440,
        "SEGMENT_SIZE must be set to 1440"
    );
    const _: () = assert!(MIN_SYMBOL_LEN == 1019);
    const _: () = assert!(MAX_SYMBOL_LEN == 1259);

    fn validate_layout(
        segment_len: usize,
        redundancy: Redundancy,
        app_msg_len: usize,
        val_set_size: usize,
    ) {
        let depth = optimal_merkle_tree_depth(|d| {
            let layout = PacketLayout::new(segment_len, d);
            let num_symbols = layout.num_symbols(app_msg_len, redundancy);
            num_symbols.saturating_add(val_set_size) // rounding chunks
        })
        .expect("should be valid for some depth in the allowed range");

        assert!((MIN_MERKLE_TREE_DEPTH..=MAX_MERKLE_TREE_DEPTH).contains(&depth));

        let layout = PacketLayout::new(segment_len, depth);
        let num_total_symbols = layout
            .num_symbols(app_msg_len, redundancy)
            .saturating_add(val_set_size); // rounding chunks

        // within valid ranges
        assert!((1..=MerkleTree::MAX_NUM_LEAVES).contains(&num_total_symbols));
        assert!((1..=MAX_NUM_PACKETS).contains(&num_total_symbols));

        // depth optimal
        assert!(2usize.pow((depth - 1) as u32) >= num_total_symbols);
        assert!(2usize.pow((depth - 2) as u32) < num_total_symbols);
    }

    #[test]
    fn test_no_panic_on_valid_ranges() {
        for app_msg_len in 1..=MAX_MESSAGE_SIZE {
            for val_set_size in [1, 100, MAX_VALIDATOR_SET_SIZE] {
                validate_layout(
                    DEFAULT_SEGMENT_LEN,
                    DEFAULT_REDUNDANCY,
                    app_msg_len,
                    val_set_size,
                );
            }
        }
    }

    #[test]
    #[ignore]
    fn test_no_panic_on_valid_ranges_slow() {
        for app_msg_len in 1..=MAX_MESSAGE_SIZE {
            for val_set_size in 1..=MAX_VALIDATOR_SET_SIZE {
                validate_layout(
                    DEFAULT_SEGMENT_LEN,
                    DEFAULT_REDUNDANCY,
                    app_msg_len,
                    val_set_size,
                );
            }
        }
    }
}
