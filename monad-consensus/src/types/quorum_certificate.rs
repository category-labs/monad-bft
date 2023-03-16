use crate::*;

use super::{ledger::*, signature::ConsensusSignature, voting::*};

#[non_exhaustive]
#[derive(Clone, Debug)]
pub struct QuorumCertificate<T>
where
    T: VotingQuorum,
{
    pub info: QcInfo,
    pub signatures: T,
    pub author: NodeId,
    pub author_signature: Option<ConsensusSignature>, // TODO: will make a signable trait
}

#[derive(Copy, Clone, Debug)]
pub struct QcInfo {
    pub vote: VoteInfo,
    pub ledger_commit: LedgerCommitInfo,
}

#[derive(Copy, Clone, Debug)]
pub struct Rank(QcInfo);

impl PartialEq for Rank {
    fn eq(&self, other: &Self) -> bool {
        self.0.vote.round == other.0.vote.round
    }
}

impl Eq for Rank {}

impl PartialOrd for Rank {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Rank {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.vote.round.0.cmp(&other.0.vote.round.0)
    }
}

impl<T: VotingQuorum> QuorumCertificate<T> {
    pub fn new(info: QcInfo) -> Self {
        QuorumCertificate {
            info,
            signatures: Default::default(),
            author: Default::default(),
            author_signature: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::types::ledger::LedgerCommitInfo;
    use crate::types::signature::ConsensusSignature;
    use crate::types::voting::{VoteInfo, VotingQuorum};

    use crate::*;

    use super::QcInfo;
    use super::QuorumCertificate;
    use super::Rank;

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

        let qc_1 = QuorumCertificate::<MockSignatures>::new(QcInfo {
            vote: vi_1,
            ledger_commit: ci,
        });
        let mut qc_2 = QuorumCertificate::<MockSignatures>::new(QcInfo {
            vote: vi_2,
            ledger_commit: ci,
        });

        assert!(Rank(qc_1.info) < Rank(qc_2.info));
        assert!(Rank(qc_2.info) > Rank(qc_1.info));

        qc_2.info.vote.round = Round(2);

        assert_eq!(Rank(qc_1.info), Rank(qc_2.info));
    }
}
