use monad_crypto::Signature;

use crate::validation::hashing::{Hashable, Hasher};

use super::{
    block::Block,
    ledger::LedgerCommitInfo,
    signature::SignatureCollection,
    timeout::{TimeoutCertificate, TimeoutInfo},
    voting::VoteInfo,
};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct VoteMessage {
    pub vote_info: VoteInfo,
    pub ledger_commit_info: LedgerCommitInfo,
}

impl Hashable for VoteMessage {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.ledger_commit_info.hash(state)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TimeoutMessage<S, T> {
    pub tminfo: TimeoutInfo<T>,
    pub last_round_tc: Option<TimeoutCertificate<S>>,
}

impl<S: Signature, T: SignatureCollection> Hashable for TimeoutMessage<S, T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.update(&self.tminfo.round);
        state.update(&self.tminfo.high_qc.info.vote.round);
    }
}

#[derive(Clone, Debug)]
pub struct ProposalMessage<S, T> {
    pub block: Block<T>,
    pub last_round_tc: Option<TimeoutCertificate<S>>,
}

impl<S: Signature, T: SignatureCollection> Hashable for ProposalMessage<S, T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        (&self.block).hash(state);
    }
}
