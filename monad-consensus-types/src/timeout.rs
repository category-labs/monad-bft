use std::collections::HashMap;

use monad_crypto::hasher::{Hash, Hashable, Hasher};
use monad_types::*;
use zerocopy::AsBytes;

use super::quorum_certificate::QuorumCertificate;
use crate::{
    signature_collection::{
        SignatureCollection, SignatureCollectionError, SignatureCollectionKeyPairType,
    },
    voting::ValidatorMapping,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Timeout<SCT: SignatureCollection> {
    pub tminfo: TimeoutInfo<SCT>,
    pub last_round_tc: Option<TimeoutCertificate<SCT>>,
}

impl<SCT: SignatureCollection> Hashable for Timeout<SCT> {
    fn hash(&self, state: &mut impl Hasher) {
        // similar to ProposalMessage, not hashing over last_round_tc
        self.tminfo.hash(state);
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TimeoutInfo<SCT> {
    pub round: Round,
    pub high_qc: QuorumCertificate<SCT>,
}

impl<SCT: SignatureCollection> Hashable for TimeoutInfo<SCT> {
    fn hash(&self, state: &mut impl Hasher) {
        state.update(self.round);
        state.update(self.high_qc.info.vote.id.0.as_bytes());
        state.update(self.high_qc.get_hash());
    }
}

impl<SCT: SignatureCollection> TimeoutInfo<SCT> {
    pub fn timeout_digest<H: Hasher>(&self) -> Hash {
        let mut hasher = H::new();
        hasher.update(self.round.as_bytes());
        hasher.update(self.high_qc.info.vote.round.as_bytes());
        hasher.hash()
    }
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub struct HighQcRound {
    pub qc_round: Round,
}

impl Hashable for HighQcRound {
    fn hash(&self, state: &mut impl Hasher) {
        state.update(self.qc_round.as_bytes());
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HighQcRoundSigColTuple<SCT> {
    pub high_qc_round: HighQcRound,
    pub sigs: SCT,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TimeoutCertificate<SCT> {
    pub round: Round,
    pub high_qc_rounds: Vec<HighQcRoundSigColTuple<SCT>>,
}

impl<SCT: SignatureCollection> TimeoutCertificate<SCT> {
    pub fn new<H: Hasher>(
        round: Round,
        high_qc_round_sig_tuple: &[(NodeId, TimeoutInfo<SCT>, SCT::SignatureType)],
        validator_mapping: &ValidatorMapping<SignatureCollectionKeyPairType<SCT>>,
    ) -> Result<Self, SignatureCollectionError<SCT::SignatureType>> {
        let mut sigs = HashMap::new();
        for (node_id, tmo_info, sig) in high_qc_round_sig_tuple {
            let high_qc_round = HighQcRound {
                qc_round: tmo_info.high_qc.info.vote.round,
            };
            let tminfo_digest = tmo_info.timeout_digest::<H>();
            let entry = sigs
                .entry(high_qc_round)
                .or_insert((tminfo_digest, Vec::new()));
            assert_eq!(entry.0, tminfo_digest);
            entry.1.push((*node_id, *sig));
        }
        let mut high_qc_rounds = Vec::new();
        for (high_qc_round, (tminfo_digest, sigs)) in sigs.into_iter() {
            let sct = SCT::new(sigs, validator_mapping, tminfo_digest.as_ref())?;
            high_qc_rounds.push(HighQcRoundSigColTuple::<SCT> {
                high_qc_round,
                sigs: sct,
            });
        }
        Ok(Self {
            round,
            high_qc_rounds,
        })
    }
}

impl<SCT> TimeoutCertificate<SCT> {
    pub fn max_round(&self) -> Round {
        self.high_qc_rounds
            .iter()
            .map(|v| v.high_qc_round.qc_round)
            .max()
            // TODO can we unwrap here?
            .unwrap_or(Round(0))
    }
}
