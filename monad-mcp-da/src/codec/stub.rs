use std::sync::Arc;

use bytes::Bytes;
use monad_crypto::hasher::{Hash, Hasher as _, HasherType};
use monad_mcp_chorus::spec::ProposalHeader as _;
use monad_merkle::{MerkleProof, MerkleTree};

use super::{
    Chunk, ChunkAssignment, ChunkId, ChunkStore, Context, DecodingOutcome, InvalidChunk,
    RaptorcastCodec,
    assignment::{Assigner as _, D25StakePartition},
    layout::PacketLayout,
    types::{MerkleHash, MerkleRoot, NodeId, ProposalHeader, Slot},
};

#[derive(Clone)]
pub(crate) struct StubChunk {
    // shared by all chunks of the same message
    slot: Slot,
    layout: PacketLayout,
    header: ProposalHeader,
    author: Arc<NodeId>,

    // chunk dependent
    chunk_id: ChunkId,
    merkle_proof: Vec<MerkleHash>,
    symbol: Bytes,
}

impl StubChunk {
    // private on purpose, use RaptorcastCodec::construct_chunk.
    fn reconstruct(
        proposal_header: &ProposalHeader,
        layout: PacketLayout,
        chunk_id: ChunkId,
        merkle_proof: Vec<MerkleHash>,
        symbol: Bytes,
        // contextual data
        slot: Slot,
        author: &NodeId,
    ) -> Result<Self, InvalidChunk> {
        let chunk = Self {
            slot,
            layout,
            header: proposal_header.clone(),
            author: Arc::new(*author),
            chunk_id,
            merkle_proof,
            symbol,
        };

        Ok(chunk)
    }
}

impl Chunk for StubChunk {
    fn layout(&self) -> PacketLayout {
        self.layout
    }

    fn proposal_header(&self) -> &ProposalHeader {
        &self.header
    }

    fn slot(&self) -> Slot {
        self.slot
    }

    fn author(&self) -> &NodeId {
        &self.author
    }

    fn chunk_id(&self) -> ChunkId {
        self.chunk_id
    }

    fn proof(&self) -> &[MerkleHash] {
        &self.merkle_proof
    }

    fn symbol(&self) -> &Bytes {
        &self.symbol
    }
}

pub(crate) struct StubRaptorcastCodec {
    slot: Slot,
    header: ProposalHeader,
    author: NodeId,
    layout: PacketLayout,
    assignment: ChunkAssignment,
}

#[derive(Clone, Debug, thiserror::Error)]
pub(crate) enum StubRaptorcastDecodingError {}

impl RaptorcastCodec for StubRaptorcastCodec {
    type Chunk = StubChunk;
    type DecodingError = StubRaptorcastDecodingError;
    type Assigner = D25StakePartition;

    fn new_decoder(context: &Context, chunk: &StubChunk) -> Self {
        let layout = chunk.layout();
        let assignment = D25StakePartition::from_layout(&layout, chunk.author(), context);

        Self {
            slot: chunk.slot(),
            header: chunk.proposal_header().clone(),
            author: *chunk.author(),
            layout,
            assignment,
        }
    }

    // todo: add build error
    fn encode(
        message: &[u8],
        layout: PacketLayout,
        assignment: &ChunkAssignment,
        store: &mut ChunkStore,
    ) -> Option<MerkleRoot> {
        let num_chunks = assignment.num_chunks();
        let depth = layout.merkle_tree_depth();

        if message.is_empty() || num_chunks == 0 {
            return None;
        }
        if depth == 0 || depth > MerkleTree::MAX_DEPTH {
            return None;
        }
        if num_chunks > 1usize << (depth - 1) {
            return None;
        }

        let digest = message_digest(message);
        let leaves = (0..num_chunks as u16)
            .map(|id| leaf_hash(ChunkId::from(id), &digest))
            .collect::<Vec<_>>();
        let tree = MerkleTree::new_with_depth(&leaves, depth);

        let symbol = Bytes::copy_from_slice(message);
        for id in 0..num_chunks as u16 {
            let proof = tree
                .proof(id)
                .siblings()
                .iter()
                .map(|hash| MerkleHash(*hash))
                .collect::<Vec<_>>();
            store.insert_raw(ChunkId::from(id), &proof, symbol.clone());
        }

        Some(MerkleRoot(MerkleHash(*tree.root())))
    }

    fn ingest_chunk(&mut self, chunk: &Self::Chunk) -> Result<(), InvalidChunk> {
        assert!(chunk.slot() == self.slot);
        assert!(chunk.proposal_header() == &self.header);
        assert!(chunk.author() == &self.author);

        let chunk_id = chunk.chunk_id();
        if usize::from(chunk_id) >= self.assignment.num_chunks() {
            return Err(InvalidChunk::InvalidChunkId);
        }

        // merkle proof validation. todo: this should be checked in
        // chunk parsing, so a Chunk always hold valid merkle proof.
        let leaf = leaf_hash(chunk_id, &message_digest(chunk.symbol()));
        let siblings = chunk.proof().iter().map(|hash| hash.0).collect();
        let proof = MerkleProof::new_from_leaf_idx(siblings, u16::from(chunk_id))
            .ok_or(InvalidChunk::BadProof)?;
        let root = proof.compute_root(&leaf).ok_or(InvalidChunk::BadProof)?;
        if &MerkleRoot(MerkleHash(root)) != self.header.root() {
            return Err(InvalidChunk::BadProof);
        }

        // the stub implementation doesn't mutate internal state for
        // decoding. for prod implementation we may decide to update
        // decoder state (e.g. for r10 managed decoder).
        Ok(())
    }

    fn try_decode(&mut self, store: &mut ChunkStore) -> DecodingOutcome<Self::DecodingError> {
        // decode only after receiving >= f+1 chunks
        if store.len() < self.assignment.num_source_chunks() + 1 {
            return DecodingOutcome::Pending;
        }

        // the actual decoding. for stub, all symbols are just
        // Arc<OrigMessage>.
        let Some(message) = store.symbols().next().cloned() else {
            return DecodingOutcome::Pending;
        };

        // reencoding
        let mut reencoded = ChunkStore::new(self.assignment.num_chunks());
        let Some(root) = Self::encode(&message, self.layout, &self.assignment, &mut reencoded)
        else {
            return DecodingOutcome::BadEncoding;
        };
        if &root != self.header.root() {
            return DecodingOutcome::BadEncoding;
        }

        // re-encoding successful, populate store with re-encoded chunks
        *store = reencoded;
        DecodingOutcome::Decoded(message)
    }

    fn construct_chunk(
        chunk_id: ChunkId,
        symbol: Bytes,
        merkle_proof: Vec<MerkleHash>,
        header: &ProposalHeader,
        layout: &PacketLayout,
        slot: Slot,
        author: &NodeId,
    ) -> Option<Self::Chunk> {
        StubChunk::reconstruct(
            header,
            *layout,
            chunk_id,
            merkle_proof,
            symbol,
            slot,
            author,
        )
        .ok()
    }
}

fn message_digest(message: &[u8]) -> Hash {
    let mut hasher = HasherType::new();
    hasher.update(message);
    hasher.hash()
}

// the leaf commits to (chunk_id, message)
fn leaf_hash(chunk_id: ChunkId, message_digest: &Hash) -> Hash {
    let mut hasher = HasherType::new();
    hasher.update(u16::from(chunk_id).to_le_bytes());
    hasher.update(message_digest.0);
    hasher.hash()
}
