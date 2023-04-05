use crate::validation::hashing::Hashable;
use crate::validation::signing::{Signable, Signed, Unverified};
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
    type Output = Unverified<VoteMessage>;

    fn signed_object(self, author: NodeId, author_signature: ConsensusSignature) -> Self::Output {
        Unverified(Signed {
            obj: self,
            author,
            author_signature,
        })
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
    type Output = Unverified<TimeoutMessage<T>>;

    fn signed_object(self, author: NodeId, author_signature: ConsensusSignature) -> Self::Output {
        Unverified(Signed {
            obj: self,
            author,
            author_signature,
        })
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
    type Output = Unverified<ProposalMessage<T>>;

    fn signed_object(self, author: NodeId, author_signature: ConsensusSignature) -> Self::Output {
        Unverified(Signed {
            obj: self,
            author,
            author_signature,
        })
    }
}
