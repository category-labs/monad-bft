use crate::*;

use super::{
    quorum_certificate::QuorumCertificate, signature::ConsensusSignature, voting::VotingQuorum,
};

#[derive(Clone, Debug)]
pub struct TimeoutInfo<T>
where
    T: VotingQuorum,
{
    pub round: Round,
    pub high_qc: QuorumCertificate<T>,
}

#[derive(Clone, Debug)]
pub struct HighQcRound {
    pub qc_round: Round,
    pub signature: Option<ConsensusSignature>,
}

#[derive(Clone, Debug)]
pub struct TimeoutCertificate {
    pub round: Round,
    pub high_qc_rounds: Vec<HighQcRound>,
}

impl TimeoutCertificate {
    pub fn max_round(&self) -> Round {
        self.high_qc_rounds.iter().fold(
            Round(0),
            |acc, r| {
                if acc >= r.qc_round {
                    acc
                } else {
                    r.qc_round
                }
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::*;

    use super::{HighQcRound, TimeoutCertificate};

    #[test]
    fn max_high_qc() {
        let high_qc_rounds = vec![
            HighQcRound {
                qc_round: Round(1),
                signature: Default::default(),
            },
            HighQcRound {
                qc_round: Round(3),
                signature: Default::default(),
            },
            HighQcRound {
                qc_round: Round(1),
                signature: Default::default(),
            },
        ];

        let tc = TimeoutCertificate {
            round: Round(2),
            high_qc_rounds: high_qc_rounds,
        };

        assert_eq!(tc.max_round(), Round(3));
    }
}
