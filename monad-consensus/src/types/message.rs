use crate::validation::signing::{Hashable, Signable, Signed};
use crate::*;

use super::block::BlockIter;
use super::{
    block::Block,
    ledger::LedgerCommitInfo,
    signature::ConsensusSignature,
    timeout::{TimeoutCertificate, TimeoutInfo},
    voting::{VoteInfo, VotingQuorum},
};

#[derive(Clone, Debug, Default)]
pub struct VoteMessage {
    pub vote_info: VoteInfo,
    pub ledger_commit_info: LedgerCommitInfo,
}

pub struct VoteMessageIter<'a> {
    vm: &'a VoteMessage,
    index: usize,
}

impl<'a> Iterator for VoteMessageIter<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        let result = match self.index {
            0 => Some(&self.vm.ledger_commit_info.vote_info_hash),
            1 => self.vm.ledger_commit_info.commit_state_hash.as_ref(),
            _ => None,
        };

        self.index += 1;
        result.map(|s| s.as_bytes())
    }
}

impl Signable for VoteMessage {
    type Output = Signed<VoteMessage>;

    fn signed_object(self, author: NodeId, author_signature: ConsensusSignature) -> Self::Output {
        Self::Output {
            obj: self,
            author,
            author_signature,
        }
    }
}

impl<'a> Hashable<'a> for &'a VoteMessage {
    type DataIter = VoteMessageIter<'a>;

    fn msg_parts(&self) -> Self::DataIter {
        VoteMessageIter { vm: self, index: 0 }
    }
}

#[derive(Clone)]
pub struct TimeoutMessage<T>
where
    T: VotingQuorum,
{
    pub tminfo: TimeoutInfo<T>,
    pub last_round_tc: Option<TimeoutCertificate>,
}

pub struct TimeoutMessageIter<'a, T>
where
    T: VotingQuorum,
{
    tm: &'a TimeoutMessage<T>,
    index: usize,
}

impl<'a, T> Iterator for TimeoutMessageIter<'a, T>
where
    T: VotingQuorum,
{
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        let result = match self.index {
            0 => Some(&self.tm.tminfo.round),
            1 => Some(&self.tm.tminfo.high_qc.info.vote.round),
            _ => None,
        };

        self.index += 1;
        result.map(|s| s.as_bytes())
    }
}

impl<T> Signable for TimeoutMessage<T>
where
    T: VotingQuorum,
{
    type Output = Signed<TimeoutMessage<T>>;

    fn signed_object(self, author: NodeId, author_signature: ConsensusSignature) -> Self::Output {
        Self::Output {
            obj: self,
            author,
            author_signature,
        }
    }
}

impl<'a, T> Hashable<'a> for &'a TimeoutMessage<T>
where
    T: VotingQuorum,
{
    type DataIter = TimeoutMessageIter<'a, T>;

    fn msg_parts(&self) -> Self::DataIter {
        TimeoutMessageIter { tm: self, index: 0 }
    }
}

#[derive(Clone, Debug)]
pub struct ProposalMessage<T>
where
    T: VotingQuorum,
{
    pub block: Block<T>,
    pub last_round_tc: Option<TimeoutCertificate>,
}

impl<'a, T> Hashable<'a> for &'a ProposalMessage<T>
where
    T: VotingQuorum,
{
    type DataIter = BlockIter<'a, T>;

    fn msg_parts(&self) -> Self::DataIter {
        BlockIter {
            b: &self.block,
            index: 0,
        }
    }
}

impl<T> Signable for ProposalMessage<T>
where
    T: VotingQuorum,
{
    type Output = Signed<ProposalMessage<T>>;

    fn signed_object(self, author: NodeId, author_signature: ConsensusSignature) -> Self::Output {
        Self::Output {
            obj: self,
            author,
            author_signature,
        }
    }
}

#[cfg(test)]
mod tests {
    use test_case::test_case;

    use super::VoteMessage;
    use crate::mock_types::mock_signature::MockSignatures;
    use crate::types::block::{Block, TransactionList};
    use crate::types::message::{ProposalMessage, TimeoutMessage};
    use crate::types::quorum_certificate::{QcInfo, QuorumCertificate};
    use crate::types::timeout::TimeoutInfo;
    use crate::*;
    use crate::{types::ledger::LedgerCommitInfo, validation::signing::Hashable, Hash};
    use sha2::Digest;

    #[test_case(None ; "None commit_state")]
    #[test_case(Some(Default::default()) ; "Some commit_state")]
    fn vote_msg_hash(cs: Option<Hash>) {
        let lci = LedgerCommitInfo {
            commit_state_hash: cs,
            vote_info_hash: Default::default(),
        };

        let vm = VoteMessage {
            vote_info: Default::default(),
            ledger_commit_info: lci,
        };

        let mut hasher = sha2::Sha256::new();
        hasher.update(vm.ledger_commit_info.vote_info_hash);
        if vm.ledger_commit_info.commit_state_hash.is_some() {
            hasher.update(vm.ledger_commit_info.commit_state_hash.as_ref().unwrap());
        }
        let h1 = hasher.finalize_reset();

        for v in (&vm).msg_parts() {
            hasher.update(v);
        }
        let h2 = hasher.finalize();

        assert_eq!(h1, h2);
    }

    #[test]
    fn timeout_msg_hash() {
        let ti = TimeoutInfo {
            round: Round(10),
            high_qc: QuorumCertificate::<MockSignatures>::new(
                QcInfo {
                    vote: Default::default(),
                    ledger_commit: Default::default(),
                },
                MockSignatures(),
            ),
        };

        let tm = TimeoutMessage {
            tminfo: ti,
            last_round_tc: None,
        };

        let mut hasher = sha2::Sha256::new();
        hasher.update(tm.tminfo.round);
        hasher.update(tm.tminfo.high_qc.info.vote.round);
        let h1 = hasher.finalize_reset();

        for m in (&tm).msg_parts() {
            hasher.update(m);
        }
        let h2 = hasher.finalize();

        assert_eq!(h1, h2);
    }

    #[test]
    fn proposal_msg_hash() {
        use crate::types::block::tests::hash;

        let txns = TransactionList(vec![1, 2, 3, 4]);
        let author = NodeId(12);
        let round = Round(234);
        let qc = QuorumCertificate::<MockSignatures>::new(Default::default(), MockSignatures());

        let block = Block::<MockSignatures>::new(author, round, &txns, &qc);

        let proposal = ProposalMessage {
            block: block.clone(),
            last_round_tc: None,
        };

        let mut hasher = sha2::Sha256::new();
        for p in (&proposal).msg_parts() {
            hasher.update(p);
        }
        let h1 = hasher.finalize_reset();
        let h2 = hash(&block);

        assert_eq!(h1, h2.into());
    }
}
