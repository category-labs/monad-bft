use crate::validation::hashing::Hashable;
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
