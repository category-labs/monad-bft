use monad_consensus_types::{
    block::Block,
    signature::SignatureCollection,
    timeout::{TimeoutCertificate, TimeoutInfo},
    validation::{Hashable, Hasher},
    voting::Vote,
};
use monad_crypto::Signature;

#[derive(PartialEq, Eq)]
pub struct VoteMessage<SCT: SignatureCollection> {
    pub vote: Vote,
    pub sig: SCT::SignatureType,
}

impl<SCT: SignatureCollection> Copy for VoteMessage<SCT> {}

impl<SCT: SignatureCollection> Clone for VoteMessage<SCT> {
    fn clone(&self) -> Self {
        Self {
            vote: self.vote,
            sig: self.sig,
        }
    }
}

impl<SCT: SignatureCollection> std::fmt::Debug for VoteMessage<SCT> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VoteMessageSigned")
            .field("vote", &self.vote)
            .field("sig", &self.sig)
            .finish()
    }
}

impl<SCT: SignatureCollection> Hashable for VoteMessage<SCT> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.vote.hash(state);
        self.sig.hash(state);
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TimeoutMessage<S, T> {
    pub tminfo: TimeoutInfo<T>,
    pub last_round_tc: Option<TimeoutCertificate<S>>,
}

impl<S: Signature, T: SignatureCollection> Hashable for TimeoutMessage<S, T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.update(self.tminfo.round);
        state.update(self.tminfo.high_qc.info.vote.round);
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProposalMessage<S, T> {
    pub block: Block<T>,
    pub last_round_tc: Option<TimeoutCertificate<S>>,
}

impl<S: Signature, T: SignatureCollection> Hashable for ProposalMessage<S, T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.block.hash(state);
    }
}
