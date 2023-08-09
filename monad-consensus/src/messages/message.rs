use monad_consensus_types::{
    block::Block,
    ledger::LedgerCommitInfo,
    message_signature::MessageSignature,
    signature_collection::SignatureCollection,
    timeout::{TimeoutCertificate, TimeoutInfo},
    validation::{Hashable, Hasher},
    voting::VoteInfo,
};

#[derive(Copy, Clone, PartialEq, Eq)]
pub struct VoteMessage {
    pub vote_info: VoteInfo,
    pub ledger_commit_info: LedgerCommitInfo,
}

impl std::fmt::Debug for VoteMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VoteMessage")
            .field("info", &self.vote_info)
            .field("lc", &self.ledger_commit_info)
            .finish()
    }
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

impl<S: MessageSignature, T: SignatureCollection> Hashable for TimeoutMessage<S, T> {
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

impl<S: MessageSignature, T: SignatureCollection> Hashable for ProposalMessage<S, T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.block.hash(state);
    }
}
