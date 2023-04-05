use crate::*;

use crate::types::message::{ProposalMessage, TimeoutMessage};
use crate::types::timeout::TimeoutCertificate;
use crate::types::voting::VotingQuorum;
use crate::validation::error::Error;
use crate::validation::signing::Unverified;

pub fn well_formed_proposal<T: VotingQuorum>(
    p: &Unverified<ProposalMessage<T>>,
) -> Result<(), Error> {
    well_formed(
        p.0.obj.block.round,
        p.0.obj.block.qc.info.vote.round,
        &p.0.obj.last_round_tc,
    )
}

pub fn well_formed_timeout<T: VotingQuorum>(
    t: &Unverified<TimeoutMessage<T>>,
) -> Result<(), Error> {
    well_formed(
        t.0.obj.tminfo.round,
        t.0.obj.tminfo.high_qc.info.vote.round,
        &t.0.obj.last_round_tc,
    )
}

// (DiemBFT v4, p.12)
// https://developers.diem.com/papers/diem-consensus-state-machine-replication-in-the-diem-blockchain/2021-08-17.pdf
fn well_formed(
    round: Round,
    qc_round: Round,
    tc: &Option<TimeoutCertificate>,
) -> Result<(), Error> {
    let prev_round = round - Round(1);
    if qc_round == prev_round {
        if tc.is_none() {
            Ok(())
        } else {
            Err(Error::NotWellFormed)
        }
    } else {
        tc.as_ref().map_or(Err(Error::NotWellFormed), |t| {
            if t.round == prev_round {
                Ok(())
            } else {
                Err(Error::NotWellFormed)
            }
        })
    }
}
