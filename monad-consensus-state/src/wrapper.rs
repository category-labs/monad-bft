use monad_consensus_types::signature_collection::SignatureCollection;

use crate::ConsensusState;

struct ConsensusStateWrapper<SCT: SignatureCollection, TV, SV> {
    consensus_state: ConsensusState<SCT, TV, SV>,
}

impl<SCT: SignatureCollection, TV, SV> Drop for ConsensusStateWrapper<SCT, TV, SV> {
    fn drop(&mut self) {
        eprintln!("{:?}", self);
    }
}

impl<SC: SignatureCollection, TV, SV> std::fmt::Debug for ConsensusStateWrapper<SC, TV, SV> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConsensusState")
            .field(
                "pending_block_tree",
                &self.consensus_state.pending_block_tree,
            )
            .field("vote_state", &self.consensus_state.vote_state)
            .field("high_qc", &self.consensus_state.high_qc)
            .field("pacemaker", &self.consensus_state.pacemaker)
            .field("safety", &self.consensus_state.safety)
            .field("nodeid", &self.consensus_state.nodeid)
            .finish_non_exhaustive()
    }
}
