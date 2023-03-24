use crate::validation::signing::{Hashable, Signable, Signed};
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
    pub signature_hash: Hash,
}

pub struct QcIter<'a, T>
where
    T: VotingQuorum,
{
    pub qc: &'a QuorumCertificate<T>,
    pub index: usize,
}

impl<'a, T> Iterator for QcIter<'a, T>
where
    T: VotingQuorum,
{
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        let result = if self.index == 0 {
            Some(self.qc.signature_hash.as_bytes())
        } else {
            None
        };
        self.index += 1;
        result
    }
}

impl<'a, T> Hashable<'a> for &'a QuorumCertificate<T>
where
    T: VotingQuorum,
{
    type DataIter = QcIter<'a, T>;

    fn msg_parts(&self) -> Self::DataIter {
        QcIter { qc: self, index: 0 }
    }
}

impl<T> Signable for QuorumCertificate<T>
where
    T: VotingQuorum,
{
    type Output = Signed<QuorumCertificate<T>>;

    fn signed_object(self, author: NodeId, author_signature: ConsensusSignature) -> Self::Output {
        Self::Output {
            obj: self,
            author,
            author_signature,
        }
    }
}

#[derive(Copy, Clone, Debug, Default)]
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
    pub fn new(info: QcInfo, signatures: T) -> Self {
        let hash = signatures.get_hash();
        QuorumCertificate {
            info,
            signatures,
            signature_hash: hash,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::mock_types::mock_signature::MockSignatures;
    use crate::types::ledger::LedgerCommitInfo;
    use crate::types::voting::VoteInfo;

    use crate::*;

    use super::QcInfo;
    use super::QuorumCertificate;
    use super::Rank;

    #[test]
    fn comparison() {
        let ci = LedgerCommitInfo::default();

        let mut vi_1 = VoteInfo::default();
        vi_1.round = Round(2);

        let mut vi_2 = VoteInfo::default();
        vi_2.round = Round(3);

        let qc_1 = QuorumCertificate::<MockSignatures>::new(
            QcInfo {
                vote: vi_1,
                ledger_commit: ci,
            },
            MockSignatures(),
        );
        let mut qc_2 = QuorumCertificate::<MockSignatures>::new(
            QcInfo {
                vote: vi_2,
                ledger_commit: ci,
            },
            MockSignatures(),
        );

        assert!(Rank(qc_1.info) < Rank(qc_2.info));
        assert!(Rank(qc_2.info) > Rank(qc_1.info));

        qc_2.info.vote.round = Round(2);

        assert_eq!(Rank(qc_1.info), Rank(qc_2.info));
    }
}
