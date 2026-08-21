use bytes::Bytes;

use super::{
    assignment::ChunkId,
    layout::PacketLayout,
    types::{MerkleHash, NodeId, ProposalHeader, Slot},
};

// todo: encode that a chunk is isomorphic to a well-formed (encoding
// scheme, proposal header, chunk id, merkle proof, symbol), nothing
// more than that.
pub trait Chunk {
    // proposal data
    fn layout(&self) -> PacketLayout;
    fn proposal_header(&self) -> &ProposalHeader;
    fn slot(&self) -> Slot;
    fn author(&self) -> &NodeId;

    // chunk-specific data
    fn chunk_id(&self) -> ChunkId;
    fn proof(&self) -> &[MerkleHash];
    fn symbol(&self) -> &Bytes;
}
