use std::cmp;

use monad_consensus_types::{
    block::{Block, BlockType},
    ledger::LedgerCommitInfo,
    quorum_certificate::{QcInfo, QuorumCertificate},
    signature_collection::SignatureCollection,
    timeout::{TimeoutCertificate, TimeoutInfo},
    voting::{Vote, VoteInfo},
};
use monad_crypto::hasher::Hasher;
use monad_types::*;

#[cfg_attr(feature = "monad_test", derive(PartialEq, Eq))]
#[derive(Debug)]
pub struct Safety {
    highest_vote_round: Round,
    highest_qc_round: Round,
}

impl Default for Safety {
    fn default() -> Self {
        Self {
            highest_vote_round: Round(0),
            highest_qc_round: Round(0),
        }
    }
}

impl Safety {
    fn update_highest_vote_round(&mut self, r: Round) {
        self.highest_vote_round = cmp::max(r, self.highest_vote_round);
    }

    fn update_highest_qc_round(&mut self, r: Round) {
        self.highest_qc_round = cmp::max(r, self.highest_qc_round);
    }

    fn safe_to_vote<SCT>(
        &self,
        block_round: Round,
        qc_round: Round,
        tc: &Option<TimeoutCertificate<SCT>>,
    ) -> bool {
        if block_round <= cmp::max(self.highest_vote_round, qc_round) {
            return false;
        }

        consecutive(block_round, qc_round) || safe_to_extend(block_round, qc_round, tc)
    }

    fn safe_to_timeout<SCT>(
        &self,
        round: Round,
        qc_round: Round,
        tc: &Option<TimeoutCertificate<SCT>>,
    ) -> bool {
        if qc_round < self.highest_qc_round
            || round + Round(1) <= self.highest_vote_round
            || round <= qc_round
        {
            return false;
        }

        let consecutive_tc = match tc {
            Some(t) => consecutive(round, t.round),
            None => false,
        };

        consecutive(round, qc_round) || consecutive_tc
    }

    pub fn make_timeout<SCT: SignatureCollection>(
        &mut self,
        round: Round,
        high_qc: QuorumCertificate<SCT>,
        last_tc: &Option<TimeoutCertificate<SCT>>,
    ) -> Option<TimeoutInfo<SCT>> {
        let qc_round = high_qc.info.vote.round;
        if self.safe_to_timeout(round, qc_round, last_tc) {
            self.update_highest_vote_round(round);
            Some(TimeoutInfo { round, high_qc })
        } else {
            None
        }
    }

    pub fn make_vote<SCT: SignatureCollection, H: Hasher>(
        &mut self,
        block: &Block<SCT>,
        last_tc: &Option<TimeoutCertificate<SCT>>,
    ) -> Option<Vote> {
        let qc_round = block.qc.info.vote.round;
        if self.safe_to_vote(block.round, qc_round, last_tc) {
            self.update_highest_qc_round(qc_round);
            self.update_highest_vote_round(block.round);

            let vote_info = VoteInfo {
                id: block.get_id(),
                round: block.round,
                parent_id: block.qc.info.vote.id,
                parent_round: block.qc.info.vote.round,
                seq_num: block.get_seq_num(),
            };

            let commit_hash = if commit_condition(block.round, block.qc.info) {
                Some(H::hash_object(block))
            } else {
                None
            };

            let ledger_commit_info = LedgerCommitInfo::new::<H>(commit_hash, &vote_info);

            return Some(Vote {
                vote_info,
                ledger_commit_info,
            });
        }

        None
    }
}

fn consecutive(block_round: Round, round: Round) -> bool {
    block_round == round + Round(1)
}

fn safe_to_extend<SCT>(
    block_round: Round,
    qc_round: Round,
    tc: &Option<TimeoutCertificate<SCT>>,
) -> bool {
    match tc {
        Some(t) => consecutive(block_round, t.round) && qc_round >= t.max_round(),
        None => false,
    }
}

fn commit_condition(block_round: Round, qc: QcInfo) -> bool {
    consecutive(block_round, qc.vote.round)
}
