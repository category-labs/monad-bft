use crate::*;

use crate::types::message::{ProposalMessage, TimeoutMessage};
use crate::types::timeout::TimeoutCertificate;
use crate::types::voting::VotingQuorum;

pub fn well_formed_proposal<T: VotingQuorum>(p: &ProposalMessage<T>) -> bool {
    well_formed(p.block.round, p.block.qc.info.vote.round, &p.last_round_tc)
}

pub fn well_formed_timeout<T: VotingQuorum>(t: &TimeoutMessage<T>) -> bool {
    well_formed(
        t.tminfo.round,
        t.tminfo.high_qc.info.vote.round,
        &t.last_round_tc,
    )
}

// (DiemBFT v4, p.12)
// https://developers.diem.com/papers/diem-consensus-state-machine-replication-in-the-diem-blockchain/2021-08-17.pdf
fn well_formed(round: Round, qc_round: Round, tc: &Option<TimeoutCertificate>) -> bool {
    let prev_round = round - Round(1);
    if qc_round != prev_round {
        match tc {
            Some(t) => t.round == prev_round,
            None => false,
        }
    } else {
        tc.is_none()
    }
}
