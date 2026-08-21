use super::types::{ProposalIndex, Slot, NodeId};

pub trait ProposerElection {
    fn proposer_indices(&self) -> impl Iterator<Item = ProposalIndex> + '_;

    // invariant: get_proposer(s, i) == Some(n) iff get_index(s, n) == Some(i)
    // corollary: for each slot, a node can occupy at most one slot

    // returns None if index has no proposer
    fn get_proposer(&self, slot: Slot, index: ProposalIndex) -> Option<&NodeId>;

    // returns None if node is not a valid proposer for the slot
    fn get_index(&self, slot: Slot, node: &NodeId) -> Option<ProposalIndex>;
}
