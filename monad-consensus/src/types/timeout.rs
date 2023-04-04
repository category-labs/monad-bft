use crate::validation::hashing::Hashable;
use crate::validation::signing::{Signable, Signed};
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

#[derive(Clone, Copy, Debug)]
pub struct HighQcRound {
    pub qc_round: Round,
}

pub struct HighQcRoundIter<'a> {
    pub hqc: &'a HighQcRound,
    pub index: usize,
}

impl<'a> Iterator for HighQcRoundIter<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        let result = if self.index == 0 {
            Some(self.hqc.qc_round.as_bytes())
        } else {
            None
        };
        self.index += 1;
        result
    }
}

impl<'a> Hashable<'a> for &'a HighQcRound {
    type DataIter = HighQcRoundIter<'a>;

    fn msg_parts(&self) -> Self::DataIter {
        Self::DataIter {
            hqc: self,
            index: 0,
        }
    }
}

impl Signable for HighQcRound {
    type Output = Signed<HighQcRound>;

    fn signed_object(self, author: NodeId, author_signature: ConsensusSignature) -> Self::Output {
        Self::Output {
            obj: self,
            author,
            author_signature,
        }
    }
}

#[derive(Clone, Debug)]
pub struct TimeoutCertificate {
    pub round: Round,
    pub high_qc_rounds: Vec<Signed<HighQcRound>>,
}

impl TimeoutCertificate {
    pub fn max_round(&self) -> Round {
        self.high_qc_rounds.iter().fold(Round(0), |acc, r| {
            if acc >= r.obj.qc_round {
                acc
            } else {
                r.obj.qc_round
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::*;

    use super::{HighQcRound, TimeoutCertificate};
    use crate::validation::hashing::*;
    use monad_testutil::signing::get_key;
    use monad_testutil::signing::Signer;

    #[test]
    fn max_high_qc() {
        let high_qc_rounds = vec![
            HighQcRound { qc_round: Round(1) },
            HighQcRound { qc_round: Round(3) },
            HighQcRound { qc_round: Round(1) },
        ]
        .iter()
        .map(|x| {
            let hasher = Sha256Hash;
            let msg = hasher.hash_object(x);
            let keypair = get_key("a");

            Signer::sign_object(*x, &msg, keypair)
        })
        .collect();

        let tc = TimeoutCertificate {
            round: Round(2),
            high_qc_rounds,
        };

        assert_eq!(tc.max_round(), Round(3));
    }
}
