use std::cmp;

use crate::types::quorum_certificate::{QcInfo, QuorumCertificate};
use crate::types::signature::SignatureCollection;
use crate::types::timeout::{TimeoutCertificate, TimeoutInfo};
use crate::*;

pub struct Safety {
    highest_vote_round: Round,
    highest_qc_round: Round,
}

impl Safety {
    fn update_highest_vote_round(&mut self, r: Round) {
        self.highest_vote_round = cmp::max(r, self.highest_vote_round);
    }

    fn update_highest_qc_round(&mut self, r: Round) {
        self.highest_qc_round = cmp::max(r, self.highest_qc_round);
    }

    fn safe_to_vote(
        &self,
        block_round: Round,
        qc_round: Round,
        tc: &Option<TimeoutCertificate>,
    ) -> bool {
        if block_round <= cmp::max(self.highest_vote_round, qc_round) {
            return false;
        }

        return consecutive(block_round, qc_round) || safe_to_extend(block_round, qc_round, tc);
    }

    fn safe_to_timeout(
        &self,
        round: Round,
        qc_round: Round,
        tc: &Option<TimeoutCertificate>,
    ) -> bool {
        if qc_round < self.highest_qc_round
            || round <= cmp::max(self.highest_qc_round - Round(1), qc_round)
        {
            return false;
        }

        let consecutive_tc = match tc {
            Some(t) => consecutive(round, t.round),
            None => false,
        };

        return consecutive(round, qc_round) || consecutive_tc;
    }

    pub fn make_timeout<T: SignatureCollection>(
        &mut self,
        round: Round,
        high_qc: QuorumCertificate<T>,
        last_tc: &Option<TimeoutCertificate>,
    ) -> Option<TimeoutInfo<T>> {
        let qc_round = high_qc.info.vote.round;
        if self.safe_to_timeout(round, qc_round, &last_tc) {
            self.update_highest_vote_round(round);
            Some(TimeoutInfo { round, high_qc })
        } else {
            None
        }
    }
}

fn consecutive(block_round: Round, round: Round) -> bool {
    round + Round(1) == block_round
}

fn safe_to_extend(block_round: Round, qc_round: Round, tc: &Option<TimeoutCertificate>) -> bool {
    match tc {
        Some(t) => consecutive(block_round, t.round) && qc_round >= t.max_round(),
        None => false,
    }
}

fn commit_condition(block_round: Round, qc: QcInfo) -> bool {
    consecutive(block_round, qc.vote.round)
}
