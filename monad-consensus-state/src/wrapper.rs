use monad_consensus_types::signature_collection::SignatureCollection;
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};

use crate::ConsensusState;

struct ConsensusStateWrapper<ST, SCT, TV, SV>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    consensus_state: ConsensusState<ST, SCT, TV, SV>,
}

impl<ST, SCT, TV, SV> Drop for ConsensusStateWrapper<ST, SCT, TV, SV>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    fn drop(&mut self) {
        eprintln!("{:?}", self);
    }
}

impl<ST, SC, TV, SV> std::fmt::Debug for ConsensusStateWrapper<ST, SC, TV, SV>
where
    ST: CertificateSignatureRecoverable,
    SC: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
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
