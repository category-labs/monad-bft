use crate::validation::hashing::{Hashable, Hasher};
use crate::validation::signing::{Signable, Signed, Unverified};
use crate::*;

use super::{
    quorum_certificate::QuorumCertificate, signature::ConsensusSignature,
    signature::SignatureCollection,
};

#[derive(Clone, Debug)]
pub struct TimeoutInfo<T>
where
    T: SignatureCollection,
{
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

impl Signable for HighQcRound {
    type Output = Unverified<HighQcRound>;

    fn signed_object(self, author: NodeId, author_signature: ConsensusSignature) -> Self::Output {
        Unverified(Signed {
            obj: self,
            author,
            author_signature,
        })
    }
}

#[derive(Clone, Debug)]
pub struct TimeoutCertificate {
    pub round: Round,
    pub high_qc_rounds: Vec<Unverified<HighQcRound>>,
}

impl TimeoutCertificate {
    pub fn max_round(&self) -> Round {
        self.high_qc_rounds.iter().fold(Round(0), |acc, r| {
            if acc >= r.0.obj.qc_round {
                acc
            } else {
                r.0.obj.qc_round
            }
        })
    }
}
