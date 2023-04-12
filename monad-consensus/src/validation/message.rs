use monad_types::*;

use crate::types::message::{ProposalMessage, TimeoutMessage};
use crate::types::signature::SignatureCollection;
use crate::types::timeout::TimeoutCertificate;
use crate::validation::error::Error;
use crate::validation::signing::Unverified;

pub fn well_formed_proposal<T: SignatureCollection>(
    p: &Unverified<ProposalMessage<T>>,
) -> Result<(), Error> {
    well_formed(
        p.obj.block.round,
        p.obj.block.qc.info.vote.round,
        &p.obj.last_round_tc,
    )
}

pub fn well_formed_timeout<T: SignatureCollection>(
    t: &Unverified<TimeoutMessage<T>>,
) -> Result<(), Error> {
    well_formed(
        t.obj.tminfo.round,
        t.obj.tminfo.high_qc.info.vote.round,
        &t.obj.last_round_tc,
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
    let valid_qc_round = qc_round == prev_round;

    match tc {
        Some(tc) => {
            // if there is a TC, the qc round must be invalid and the tc round must be valid
            if !valid_qc_round && tc.round == prev_round {
                Ok(())
            } else {
                Err(Error::NotWellFormed)
            }
        }
        None => {
            // If no TC, the qc round must be valid
            if valid_qc_round {
                Ok(())
            } else {
                Err(Error::NotWellFormed)
            }
        }
    }
}
