use crate::*;

use super::{ledger::*, signature::ConsensusSignature, voting::*};

#[non_exhaustive]
#[derive(Clone, Debug)]
pub struct QuorumCertificate<T>
where
    T: VotingQuorum,
{
    pub vote_info: VoteInfo,
    pub ledger_commit_info: LedgerCommitInfo,
    pub signatures: T,
    pub author: NodeId,
    pub author_signature: Option<ConsensusSignature>, // TODO: will make a signable trait
}

impl<T: VotingQuorum> QuorumCertificate<T> {
    pub fn new(vote_info: VoteInfo, ledger_commit_info: LedgerCommitInfo) -> Self {
        QuorumCertificate {
            vote_info,
            ledger_commit_info,
            signatures: Default::default(),
            author: Default::default(),
            author_signature: None,
        }
    }
}

impl<T: VotingQuorum> PartialEq for QuorumCertificate<T> {
    fn eq(&self, other: &Self) -> bool {
        self.vote_info.round == other.vote_info.round
    }
}

impl<T: VotingQuorum> Eq for QuorumCertificate<T> {}

impl<T: VotingQuorum> Ord for QuorumCertificate<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.vote_info.round.0.cmp(&other.vote_info.round.0)
    }
}

impl<T: VotingQuorum> PartialOrd for QuorumCertificate<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod tests {
    use crate::types::ledger::LedgerCommitInfo;
    use crate::types::signature::ConsensusSignature;
    use crate::types::voting::{VoteInfo, VotingQuorum};

    use crate::*;

    use super::QuorumCertificate;

    #[derive(Clone, Default, Debug)]
    struct MockSignatures();
    impl VotingQuorum for MockSignatures {
        fn verify_quorum(&self) -> bool {
            true
        }

        fn current_voting_power(&self) -> i64 {
            0
        }

        fn get_hash(&self) -> crate::Hash {
            Default::default()
        }

        fn add_signature(&mut self, _s: ConsensusSignature) {}
    }

    #[test]
    fn comparison() {
        let ci = LedgerCommitInfo::default();

        let mut vi_1 = VoteInfo::default();
        vi_1.round = Round(2);

        let mut vi_2 = VoteInfo::default();
        vi_2.round = Round(3);

        let qc_1 = QuorumCertificate::<MockSignatures>::new(vi_1, ci);
        let mut qc_2 = QuorumCertificate::<MockSignatures>::new(vi_2, ci);

        assert!(qc_1 < qc_2);
        assert!(qc_2 > qc_1);

        qc_2.vote_info.round = Round(2);

        assert_eq!(qc_1, qc_2);
    }
}
