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
use monad_types::ETHERNET_MTU;
use monad_wireauth::messages::DataPacketHeader;

use super::{
    assigner::{ChunkAssignment, StakeBasedWithRC},
    Chunk, ChunkAssigner as _,
};
use crate::{
    message::MAX_MESSAGE_SIZE,
    packet::BuildError,
    udp::{GroupId, MAX_VALIDATOR_SET_SIZE},
    util::{
        compute_app_message_hash, BroadcastMode, Collector, PrimaryBroadcastGroup, Redundancy,
        UdpMessage,
    },
    SIGNATURE_SIZE,
};

// Deterministic RaptorCast requires the parameters (segment_len,
// redundancy) to be fixed for all nodes in order to provide
// deterministic encoding guarantee.

const VERSION: u16 = 0x1;

#[derive(Debug, Clone, Copy)]
pub(crate) struct PacketLayout {
    app_message_len: usize,
    merkle_tree_depth: u8,
    num_validators: usize,
}

// Constant redundancy
pub const REDUNDANCY: Redundancy = Redundancy::from_u8(3);

// The smallest message requires 4 chunks (from redundancy=3 +
// min_validator_set_size=1), which is accommodated by a merkle tree
// of depth 3 with 4 leaves.
pub const MIN_MERKLE_TREE_DEPTH: u8 = 3;

pub const MAX_MERKLE_TREE_DEPTH: u8 = 15;

#[allow(clippy::identity_op)]
impl PacketLayout {
    pub const HEADER_LEN: usize = 0
        + SIGNATURE_SIZE // Sender signature (65 bytes)
        + 2  // Version
        + 1  // 2 bits for Broadcast Mode, 2 bits reserved, 4 bits for Merkle Tree Depth
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

    // Segment size is fixed in deterministic raptorcast
    pub const SEGMENT_LEN: usize =
        segment_size_for_mtu(ETHERNET_MTU - DataPacketHeader::SIZE as u16) as usize;

    // SAFETY: the caller must ensure the asserted preconditions hold
    pub fn new(app_message_len: usize, num_validators: usize) -> Self {
        debug_assert!(app_message_len <= MAX_MESSAGE_SIZE);
        debug_assert!(num_validators <= MAX_VALIDATOR_SET_SIZE);
        let merkle_tree_depth = Self::optimal_tree_depth(app_message_len, num_validators);
        Self {
            merkle_tree_depth,
            app_message_len,
            num_validators,
        }
    }

    // SAFETY: the caller must ensure the asserted preconditions hold
    pub fn optimal_tree_depth(app_message_len: usize, num_validators: usize) -> u8 {
        debug_assert!(app_message_len <= MAX_MESSAGE_SIZE);
        debug_assert!(num_validators <= MAX_VALIDATOR_SET_SIZE);
        for depth in MIN_MERKLE_TREE_DEPTH..=MAX_MERKLE_TREE_DEPTH {
            let symbol_len = Self::SEGMENT_LEN
                - Self::HEADER_LEN
                - Self::MERKLE_HASH_LEN * (depth as usize - 1)
                - Self::CHUNK_HEADER_LEN;
            let base_num_symbols = app_message_len.div_ceil(symbol_len).max(1);
            let Some(num_symbols) = REDUNDANCY.scale(base_num_symbols) else {
                continue;
            };
            let leaf_count = 2usize.pow((depth - 1) as u32);
            if num_symbols.saturating_add(num_validators) <= leaf_count {
                return depth;
            }
        }

        // SAFETY: verified by test_no_panic_on_valid_ranges as long as
        // the preconditions hold.
        unreachable!()
    }

    // does not account for rounding chunks
    pub fn calc_num_symbols(&self) -> usize {
        let num_base_symbols = self.app_message_len.div_ceil(self.symbol_len()).max(1);
        // SAFETY: verified in tests::test_no_panic_on_valid_ranges
        REDUNDANCY.scale(num_base_symbols).expect("scale overflow")
    }

    // accounts for rounding chunks
    pub fn encoded_symbol_capacity(&self) -> usize {
        self.calc_num_symbols() + self.num_validators
    }

    pub const fn symbol_len(&self) -> usize {
        Self::SEGMENT_LEN - Self::HEADER_LEN - self.merkle_proof_len() - Self::CHUNK_HEADER_LEN
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
        start..Self::SEGMENT_LEN
    }

    pub fn symbol_range(&self) -> std::ops::Range<usize> {
        self.symbol_offset()..Self::SEGMENT_LEN
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

    let BroadcastMode::DeterministicPrimary(round) = broadcast_mode else {
        return Err(BuildError::InvalidBroadcastMode(broadcast_mode));
    };
    let mut broadcast_byte: u8 = 0b10 << 6; // primary broadcast
    if (merkle_tree_depth & 0b1111_0000) != 0 {
        return Err(BuildError::MerkleTreeTooDeep);
    }
    broadcast_byte |= merkle_tree_depth & 0b0000_1111;
    cursor_broadcast_merkle_depth[0] = broadcast_byte;

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

pub(crate) fn build_into<ST, C>(
    key: &ST::KeyPairType,
    unix_ts_ms: u64,
    app_message: &[u8],
    group: &PrimaryBroadcastGroup<'_, CertificateSignaturePubKey<ST>>,
    round: monad_types::Round,
    collector: &mut C,
) -> Result<(), BuildError>
where
    ST: CertificateSignatureRecoverable,
    C: Collector<UdpMessage<CertificateSignaturePubKey<ST>>>,
{
    if app_message.len() > MAX_MESSAGE_SIZE {
        return Err(BuildError::AppMessageTooLarge);
    }

    // deterministic chunk assignment
    let app_message_hash = compute_app_message_hash(app_message).0;
    let seed = derive_seed(&app_message_hash, round);
    let mut validators = group
        .iter()
        .map(|(node_id, stake)| (*node_id, *stake))
        .collect::<Vec<_>>();
    StakeBasedWithRC::shuffle_validators(&mut validators, seed);

    // valid validators.len() is guaranteed through ValidatorSet type invariance.
    let layout = PacketLayout::new(app_message.len(), validators.len());
    let assigner = StakeBasedWithRC::from_validator_set(validators);
    let num_base_symbols = layout.calc_num_symbols();
    let assignment = assigner.assign_chunks(num_base_symbols, None)?;

    // header fields
    let broadcast_mode = BroadcastMode::DeterministicPrimary(round);
    let group_id = group.group_id();

    // build header & assemble the chunks
    build::<ST>(
        key,
        layout,
        broadcast_mode,
        group_id,
        unix_ts_ms,
        app_message,
        assignment,
        collector,
    )
}

#[allow(clippy::too_many_arguments)]
fn build<ST>(
    key: &ST::KeyPairType,
    layout: PacketLayout,
    broadcast_mode: BroadcastMode,
    group_id: GroupId,
    unix_ts_ms: u64,
    app_message: &[u8],
    assignment: ChunkAssignment<'_, CertificateSignaturePubKey<ST>>,
    collector: &mut impl Collector<UdpMessage<CertificateSignaturePubKey<ST>>>,
) -> Result<(), BuildError>
where
    ST: CertificateSignatureRecoverable,
{
    // step 1. generate the chunks
    let mut chunks = assignment.generate(PacketLayout::SEGMENT_LEN);

    // step 2. encode and write raptor symbols to each chunk
    encode_unique_symbols(app_message, &mut chunks, layout)?;

    // step 3. build a global merkle tree (and write chunk header)
    let mut merkle_tree = GlobalMerkleTree::build(&mut chunks[..], layout)?;

    // step 4. make header
    let header = build_header::<ST>(
        key,
        broadcast_mode,
        layout.merkle_tree_depth(),
        group_id,
        unix_ts_ms,
        merkle_tree.root(),
        app_message.len(),
    )?;

    // step 5. write header and merkle proofs to each chunk
    merkle_tree.write_header_and_proofs(&header)?;

    // step 6. send out the chunks
    collector.reserve(chunks.len());
    for chunk in chunks.into_iter() {
        collector.push(chunk.into());
    }

    Ok(())
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
        debug_assert!(chunk_id < layout.encoded_symbol_capacity());
        let symbol_buffer = layout.symbol_mut(chunk);
        encoder.encode_symbol(symbol_buffer, chunk_id);
    }
    Ok(())
}

// Derive the seed used to shuffling validator set for deterministic raptorcast
pub fn derive_seed(app_message_hash: &[u8; 20], round: monad_types::Round) -> [u8; 32] {
    let mut padded_seed = [0u8; 32];
    padded_seed[..20].copy_from_slice(app_message_hash);
    padded_seed[20..28].copy_from_slice(&round.0.to_le_bytes());
    padded_seed
}

#[cfg(test)]
mod tests {
    use monad_merkle::MerkleTree;

    use super::{PacketLayout, MAX_MERKLE_TREE_DEPTH, MIN_MERKLE_TREE_DEPTH};
    use crate::{
        message::MAX_MESSAGE_SIZE,
        udp::{MAX_NUM_PACKETS, MAX_VALIDATOR_SET_SIZE},
    };

    //   1500 (ethernet mtu)
    // - 20   (ip header)
    // - 8    (udp header)
    // - 32   (wireauth header)
    // = 1440
    const _: () = assert!(
        PacketLayout::SEGMENT_LEN == 1440,
        "SEGMENT_SIZE must be set to 1440"
    );

    const _: () = assert!(
        super::MAX_MERKLE_TREE_DEPTH <= 0b1111,
        "MAX_MERKLE_TREE_DEPTH must fit in 4 bits"
    );
    const _: () = assert!(
        MIN_MERKLE_TREE_DEPTH <= MAX_MERKLE_TREE_DEPTH,
        "MIN_MERKLE_TREE_DEPTH must be less than MAX_MERKLE_TREE_DEPTH"
    );

    fn validate_layout(app_msg_len: usize, val_set_size: usize) {
        // no panicking
        let layout = PacketLayout::new(app_msg_len, val_set_size);
        let _ = layout.calc_num_symbols();

        // within valid ranges
        let max_num_symbols = layout.encoded_symbol_capacity();
        assert!((1..=MerkleTree::MAX_NUM_LEAVES).contains(&max_num_symbols));
        assert!((1..=MAX_NUM_PACKETS).contains(&max_num_symbols));
        let depth = layout.merkle_tree_depth();
        assert!((MIN_MERKLE_TREE_DEPTH..=MAX_MERKLE_TREE_DEPTH).contains(&depth));

        // depth optimal
        assert!(2usize.pow((depth - 1) as u32) >= max_num_symbols);
        assert!(2usize.pow((depth - 2) as u32) < max_num_symbols);
    }

    #[test]
    fn test_no_panic_on_valid_ranges() {
        for app_msg_len in 0..=MAX_MESSAGE_SIZE {
            for val_set_size in [1, 100, MAX_VALIDATOR_SET_SIZE] {
                validate_layout(app_msg_len, val_set_size);
            }
        }
    }

    #[test]
    #[ignore]
    fn test_no_panic_on_valid_ranges_slow() {
        for app_msg_len in 0..=MAX_MESSAGE_SIZE {
            for val_set_size in 1..=MAX_VALIDATOR_SET_SIZE {
                validate_layout(app_msg_len, val_set_size);
            }
        }
    }
}
