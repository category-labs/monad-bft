use crate::*;

use super::{
    block::Block,
    ledger::LedgerCommitInfo,
    signature::ConsensusSignature,
    timeout::{TimeoutCertificate, TimeoutInfo},
    voting::{VoteInfo, VotingQuorum},
};

pub struct VoteMessage {
    pub vote_info: VoteInfo,
    pub ledger_commit_info: LedgerCommitInfo,
    pub author: NodeId,
    pub author_signature: ConsensusSignature,
}

pub struct TimeoutMessage<T>
where
    T: VotingQuorum,
{
    pub tminfo: TimeoutInfo<T>,
    // TC for tminfo.round - 1 if tminfo.high_qc.round != tminfo.round - 1, else None
    pub last_round_tc: Option<TimeoutCertificate>,
    //TODO does this need a signature
}

pub struct ProposalMessage<T>
where
    T: VotingQuorum,
{
    pub block: Block<T>,
    // TC for block.round - 1 if block.qc.vote_info.round != block.round - 1, else None
    pub last_round_tc: Option<TimeoutCertificate>,
    pub signature: ConsensusSignature,
}
