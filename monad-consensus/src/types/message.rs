use crate::validation::hashing::{Hashable, Hasher};

use super::{
    block::Block,
    ledger::LedgerCommitInfo,
    signature::SignatureCollection,
    timeout::{TimeoutCertificate, TimeoutInfo},
    voting::VoteInfo,
};

#[derive(Clone, Debug, Default)]
pub struct VoteMessage {
    pub vote_info: VoteInfo,
    pub ledger_commit_info: LedgerCommitInfo,
}

#[derive(Debug, Clone)]
pub struct TimeoutMessage<T> {
    pub tminfo: TimeoutInfo<T>,
    pub last_round_tc: Option<TimeoutCertificate>,
}

impl<T: SignatureCollection> Hashable for &TimeoutMessage<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.update(&self.tminfo.round);
        state.update(&self.tminfo.high_qc.info.vote.round);
    }
}

#[derive(Clone, Debug)]
pub struct ProposalMessage<T> {
    pub block: Block<T>,
    pub last_round_tc: Option<TimeoutCertificate>,
}

impl<T: SignatureCollection> Hashable for &ProposalMessage<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        (&self.block).hash(state);
    }
}
