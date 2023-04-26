use zerocopy::AsBytes;

use monad_types::*;

use crate::validation::hashing::{Hashable, Hasher};

use super::quorum_certificate::QuorumCertificate;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TimeoutInfo<T> {
    pub round: Round,
    pub high_qc: QuorumCertificate<T>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HighQcRound {
    pub qc_round: Round,
}

impl Hashable for HighQcRound {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.update(self.qc_round.as_bytes());
    }
}

impl AsRef<[u8]> for HighQcRound {
    fn as_ref(&self) -> &[u8] {
        self.qc_round.as_ref()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HighQcRoundSigTuple<S> {
    pub high_qc_round: HighQcRound,
    pub author_signature: S,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TimeoutCertificate<S> {
    pub round: Round,
    pub high_qc_rounds: Vec<HighQcRoundSigTuple<S>>,
}

impl<S> TimeoutCertificate<S> {
    pub fn max_round(&self) -> Round {
        self.high_qc_rounds
            .iter()
            .map(|v| v.high_qc_round.qc_round)
            .max()
            // TODO can we unwrap here?
            .unwrap_or(Round(0))
    }
}
