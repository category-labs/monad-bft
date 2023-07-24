use monad_consensus_types::signature::SignatureCollection;
use monad_crypto::Signature;

use crate::ConsensusState;

struct ConsensusStateWrapper<S: Signature, SC: SignatureCollection, TV> {
    consensus_state: ConsensusState<S, SC, TV>,
}

impl<S: Signature, SC: SignatureCollection, TV> Drop for ConsensusStateWrapper<S, SC, TV> {
    fn drop(&mut self) {
        eprintln!("{:?}", self);
    }
}

impl<S: Signature, SC: SignatureCollection, TV> std::fmt::Debug
    for ConsensusStateWrapper<S, SC, TV>
{
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
