use crate::validation::hashing::{Hashable, Hasher};
use crate::validation::signing::{Signable, Signed, Unverified};
use crate::*;

use super::{
    block::Block,
    ledger::LedgerCommitInfo,
    signature::{ConsensusSignature, SignatureCollection},
    timeout::{TimeoutCertificate, TimeoutInfo},
    voting::VoteInfo,
};

#[derive(Clone, Debug, Default)]
pub struct VoteMessage {
    pub vote_info: VoteInfo,
    pub ledger_commit_info: LedgerCommitInfo,
}

impl Signable for VoteMessage {
    type Output = Unverified<VoteMessage>;

    fn signed_object(self, author: NodeId, author_signature: ConsensusSignature) -> Self::Output {
        Unverified(Signed {
            obj: self,
            author,
            author_signature,
        })
    }
}

impl Hashable for &VoteMessage {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.update(&self.ledger_commit_info.vote_info_hash);
        if let Some(x) = self.ledger_commit_info.commit_state_hash.as_ref() {
            state.update(x);
        }
    }
}

#[derive(Clone)]
pub struct TimeoutMessage<T>
where
    T: SignatureCollection,
{
    pub tminfo: TimeoutInfo<T>,
    pub last_round_tc: Option<TimeoutCertificate>,
}

impl<T> Signable for TimeoutMessage<T>
where
    T: SignatureCollection,
{
    type Output = Unverified<TimeoutMessage<T>>;

    fn signed_object(self, author: NodeId, author_signature: ConsensusSignature) -> Self::Output {
        Unverified(Signed {
            obj: self,
            author,
            author_signature,
        })
    }
}

impl<T: SignatureCollection> Hashable for &TimeoutMessage<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.update(&self.tminfo.round);
        state.update(&self.tminfo.high_qc.info.vote.round);
    }
}

#[derive(Clone, Debug)]
pub struct ProposalMessage<T>
where
    T: SignatureCollection,
{
    pub block: Block<T>,
    pub last_round_tc: Option<TimeoutCertificate>,
}

impl<T: SignatureCollection> Hashable for &ProposalMessage<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        (&self.block).hash(state);
    }
}

impl<T> Signable for ProposalMessage<T>
where
    T: SignatureCollection,
{
    type Output = Unverified<ProposalMessage<T>>;

    fn signed_object(self, author: NodeId, author_signature: ConsensusSignature) -> Self::Output {
        Unverified(Signed {
            obj: self,
            author,
            author_signature,
        })
    }
}
