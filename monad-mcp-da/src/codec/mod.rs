mod stub;

use std::sync::Arc;

use bytes::Bytes;

use super::{
    assignment::{self, Assigner, ChunkAssignment, ChunkId},
    chunk::Chunk,
    chunk_store::ChunkStore,
    layout::{self, PacketLayout},
    types::{
        self, HeaderAuth, MerkleHash, MerkleRoot, NodeId, ProposalHeader, ProposalKeyPair, Slot,
        ValidatorData,
    },
};

// todo: give this type a more proper name. it encodes the concept of
// slot-scoped config, the identity and validator set. think about if
// we can further refine this concept to have a clearer boundary.
#[derive(Clone)]
pub struct Context {
    pub self_id: NodeId,
    pub num_proposals: usize,
    pub validator_data: Arc<ValidatorData>,
    pub key_pair: Arc<ProposalKeyPair>,
    pub header_auth: Arc<HeaderAuth>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InvalidChunk {
    SlotOutOfRange,
    InvalidChunkId,
    UnexpectedChunk,
    InvalidProposer,
    BadSignature,
    BadProof,
}

pub enum DecodingOutcome<E> {
    Pending,
    Decoded(Bytes),
    // merkle root doesn't match the decoded-then-re-encoded message
    BadEncoding,
    InternalError(E),
}

pub trait RaptorcastCodec {
    type Chunk: Chunk + Clone;
    type DecodingError: std::error::Error + Clone;
    type Assigner: Assigner;

    // Context { keypair, validator_data } is required for
    // re-encoding. The first chunk carries necessary data. It is NOT
    // ingested here.
    fn new_decoder(context: &Context, chunk: &Self::Chunk) -> Self;

    // fill store with symbols
    fn encode(
        message: &[u8],
        layout: PacketLayout,
        assignment: &ChunkAssignment,
        store: &mut ChunkStore,
    ) -> Option<MerkleRoot>;

    // perform necessary validation & ingest chunk to update its internal decoding state
    fn ingest_chunk(&mut self, chunk: &Self::Chunk) -> Result<(), InvalidChunk>;

    // try decode, then re-encode and verify merkle root. populate
    // store with all re-encoded chunks.
    fn try_decode(&mut self, store: &mut ChunkStore) -> DecodingOutcome<Self::DecodingError>;

    // the caller must ensure chunk_id < num_chunks
    fn construct_chunk(
        chunk_id: ChunkId,
        symbol: Bytes,
        merkle_proof: Vec<MerkleHash>,
        header: &ProposalHeader,
        layout: &PacketLayout,
        slot: Slot,
        author: &NodeId,
    ) -> Option<Self::Chunk>;
}
