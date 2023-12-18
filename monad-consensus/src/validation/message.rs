use monad_consensus_types::{timeout::TimeoutCertificate, validation::Error};
use monad_types::*;

// (DiemBFT v4, p.12)
// https://developers.diem.com/papers/diem-consensus-state-machine-replication-in-the-diem-blockchain/2021-08-17.pdf
/// A timeout or proposal message of round r is well-formed if
/// 1. it contains a QC of round r-1
/// 2. it contains a TC of round r-1 if the QC is not from round r-1
pub fn well_formed<SCT>(
    round: Round,
    qc_round: Round,
    tc: &Option<TimeoutCertificate<SCT>>,
) -> Result<(), Error> {
    let prev_round = round - Round(1);
    let valid_qc_round = qc_round == prev_round;

    // ignore tc if qc is from r-1
    if valid_qc_round {
        return Ok(());
    }
    // otherwise check tc comes from r-1
    if let Some(tc) = tc {
        if tc.round == prev_round {
            return Ok(());
        }
    }
    Err(Error::NotWellFormed)
}
