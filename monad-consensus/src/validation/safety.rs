use std::cmp;

use monad_types::*;

use crate::types::block::Block;
use crate::types::ledger::LedgerCommitInfo;
use crate::types::message::VoteMessage;
use crate::types::quorum_certificate::{QcInfo, QuorumCertificate};
use crate::types::signature::SignatureCollection;
use crate::types::timeout::{TimeoutCertificate, TimeoutInfo};
use crate::types::voting::VoteInfo;
use crate::validation::hashing::Hasher;

pub struct Safety {
    highest_vote_round: Round,
    highest_qc_round: Round,
}

impl Safety {
    pub fn new() -> Self {
        Safety {
            highest_vote_round: Round(0),
            highest_qc_round: Round(0),
        }
    }

    fn update_highest_vote_round(&mut self, r: Round) {
        self.highest_vote_round = cmp::max(r, self.highest_vote_round);
    }

    fn update_highest_qc_round(&mut self, r: Round) {
        self.highest_qc_round = cmp::max(r, self.highest_qc_round);
    }

    fn safe_to_vote<S>(
        &self,
        block_round: Round,
        qc_round: Round,
        tc: &Option<TimeoutCertificate<S>>,
    ) -> bool {
        if block_round <= cmp::max(self.highest_vote_round, qc_round) {
            return false;
        }

        return consecutive(block_round, qc_round) || safe_to_extend(block_round, qc_round, tc);
    }

    fn safe_to_timeout<S>(
        &self,
        round: Round,
        qc_round: Round,
        tc: &Option<TimeoutCertificate<S>>,
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

        return consecutive(round, qc_round) || consecutive_tc;
    }

    pub fn make_timeout<S, T: SignatureCollection>(
        &mut self,
        round: Round,
        high_qc: QuorumCertificate<T>,
        last_tc: &Option<TimeoutCertificate<S>>,
    ) -> Option<TimeoutInfo<T>> {
        let qc_round = high_qc.info.vote.round;
        if self.safe_to_timeout(round, qc_round, &last_tc) {
            self.update_highest_vote_round(round);
            Some(TimeoutInfo { round, high_qc })
        } else {
            None
        }
    }

    pub fn make_vote<S, T: SignatureCollection, H: Hasher>(
        &mut self,
        block: &Block<T>,
        last_tc: &Option<TimeoutCertificate<S>>,
    ) -> Option<VoteMessage> {
        let qc_round = block.qc.info.vote.round;
        if self.safe_to_vote(block.round, qc_round, last_tc) {
            self.update_highest_qc_round(qc_round);
            self.update_highest_vote_round(block.round);

            let vote_info = VoteInfo {
                id: block.get_id(),
                round: block.round,
                parent_id: block.qc.info.vote.id,
                parent_round: block.qc.info.vote.round,
            };

            let commit_hash = if commit_condition(block.round, block.qc.info) {
                Some(H::hash_object(block))
            } else {
                None
            };

            let ledger_commit_info = LedgerCommitInfo::new::<H>(commit_hash, &vote_info);

            return Some(VoteMessage {
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

fn safe_to_extend<S>(
    block_round: Round,
    qc_round: Round,
    tc: &Option<TimeoutCertificate<S>>,
) -> bool {
    match tc {
        Some(t) => consecutive(block_round, t.round) && qc_round >= t.max_round(),
        None => false,
    }
}

fn commit_condition(block_round: Round, qc: QcInfo) -> bool {
    consecutive(block_round, qc.vote.round)
}
