use monad_consensus_types::{signature::SignatureCollection, transaction::TransactionCollection};
use monad_crypto::Signature;

use crate::ConsensusState;

struct ConsensusStateWrapper<S: Signature, SC: SignatureCollection, T: TransactionCollection> {
    consensus_state: ConsensusState<S, SC, T>,
}

impl<S: Signature, SC: SignatureCollection, T: TransactionCollection> Drop
    for ConsensusStateWrapper<S, SC, T>
{
    fn drop(&mut self) {
        eprintln!("{:?}", self);
    }
}

impl<S: Signature, SC: SignatureCollection, T: TransactionCollection> std::fmt::Debug
    for ConsensusStateWrapper<S, SC, T>
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
