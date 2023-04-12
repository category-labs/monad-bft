use zerocopy::AsBytes;

use monad_types::*;

use crate::validation::{
    hashing::{Hashable, Hasher},
    signing::Unverified,
};

use super::quorum_certificate::QuorumCertificate;

#[derive(Clone, Debug)]
pub struct TimeoutInfo<T> {
    pub round: Round,
    pub high_qc: QuorumCertificate<T>,
}

#[derive(Clone, Copy, Debug)]
pub struct HighQcRound {
    pub qc_round: Round,
}

impl Hashable for &HighQcRound {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.update(self.qc_round.as_bytes());
    }
}

#[derive(Clone, Debug)]
pub struct TimeoutCertificate {
    pub round: Round,
    pub high_qc_rounds: Vec<Unverified<HighQcRound>>,
}

impl TimeoutCertificate {
    pub fn max_round(&self) -> Round {
        self.high_qc_rounds
            .iter()
            .map(|v| v.obj.qc_round)
            .max()
            // TODO can we unwrap here?
            .unwrap_or(Round(0))
    }
}
