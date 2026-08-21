use monad_mcp_chorus::spec::validator::ValidatorData as _;

use super::{Assigner, ChunkAssignment, Context, NodeId, PacketLayout, StakePartition};

pub(crate) struct D25StakePartition;

// D25 encoding scheme:
// - 2.5x redundancy
// - author excluded in assignment
// - stake partition with round up chunks
// - valset pre-shuffled based on (slot, ...)
// - assigned round-robin
impl D25StakePartition {
    const REDUNDANCY: f32 = 2.5;
}

impl Assigner for D25StakePartition {
    fn from_layout(layout: &PacketLayout, author: &NodeId, context: &Context) -> ChunkAssignment {
        let mut weights = vec![];
        for node_id in context.validator_data.nodes() {
            if node_id == author {
                // author excluded
                continue;
            }
            let stake = context.validator_data.get_stake(node_id);
            weights.push((*node_id, *stake));
        }

        let partition = StakePartition::new(weights);
        let num_source_chunks = layout.num_source_chunks();
        partition.assign(num_source_chunks, Self::REDUNDANCY)
    }
}
