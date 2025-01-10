use std::collections::HashMap;

use alloy_rlp::{RlpDecodable, RlpEncodable};
use monad_types::*;

use super::quorum_certificate::QuorumCertificate;
use crate::{
    signature_collection::{
        SignatureCollection, SignatureCollectionError, SignatureCollectionKeyPairType,
    },
    voting::ValidatorMapping,
};

/// Timeout message to broadcast to other nodes after a local timeout
#[derive(Clone, Debug, PartialEq, Eq, RlpDecodable, RlpEncodable)]
#[rlp(trailing)]
pub struct Timeout<SCT: SignatureCollection> {
    pub tminfo: TimeoutInfo<SCT>,
    /// if the high qc round != tminfo.round-1, then this must be the
    /// TC for tminfo.round-1. Otherwise it must be None
    pub last_round_tc: Option<TimeoutCertificate<SCT>>,
}

/// Data to include in a timeout
#[derive(Clone, Debug, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct TimeoutInfo<SCT> {
    /// Epoch where the timeout happens
    pub epoch: Epoch,
    /// The round that timed out
    pub round: Round,
    /// The node's highest known qc
    pub high_qc: QuorumCertificate<SCT>,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct TimeoutDigest {
    pub epoch: Epoch,
    pub round: Round,
    pub high_qc_round: Round,
}

impl<SCT: SignatureCollection> TimeoutInfo<SCT> {
    pub fn timeout_digest(&self) -> TimeoutDigest {
        TimeoutDigest {
            epoch: self.epoch,
            round: self.round,
            high_qc_round: self.high_qc.get_round(),
        }
    }
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct HighQcRound {
    pub qc_round: Round,
}

#[derive(Clone, Debug, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct HighQcRoundSigColTuple<SCT> {
    pub high_qc_round: HighQcRound,
    pub sigs: SCT,
}

/// TimeoutCertificate is used to advance rounds when a QC is unable to
/// form for a round
/// A collection of Timeout messages is the basis for building a TC
#[derive(Clone, Debug, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct TimeoutCertificate<SCT> {
    /// The epoch where the TC is created
    pub epoch: Epoch,
    /// The Timeout messages must have been for the same round
    /// to create a TC
    pub round: Round,
    /// signatures over the round of the TC and the high qc round,
    /// proving that the supermajority of the network is locked on the
    /// same high_qc
    pub high_qc_rounds: Vec<HighQcRoundSigColTuple<SCT>>,
}

impl<SCT: SignatureCollection> TimeoutCertificate<SCT> {
    pub fn new(
        epoch: Epoch,
        round: Round,
        high_qc_round_sig_tuple: &[(
            NodeId<SCT::NodeIdPubKey>,
            TimeoutInfo<SCT>,
            SCT::SignatureType,
        )],
        validator_mapping: &ValidatorMapping<
            SCT::NodeIdPubKey,
            SignatureCollectionKeyPairType<SCT>,
        >,
    ) -> Result<Self, SignatureCollectionError<SCT::NodeIdPubKey, SCT::SignatureType>> {
        let mut sigs = HashMap::new();
        for (node_id, tmo_info, sig) in high_qc_round_sig_tuple {
            let high_qc_round = HighQcRound {
                qc_round: tmo_info.high_qc.get_round(),
            };
            let tminfo_digest = tmo_info.timeout_digest();
            let entry = sigs
                .entry(high_qc_round)
                .or_insert((tminfo_digest, Vec::new()));
            assert_eq!(entry.0, tminfo_digest);
            entry.1.push((*node_id, *sig));
        }
        let mut high_qc_rounds = Vec::new();
        for (high_qc_round, (tminfo_digest, sigs)) in sigs.into_iter() {
            let tminfo_digest_enc = alloy_rlp::encode(tminfo_digest);
            let sct = SCT::new(sigs, validator_mapping, tminfo_digest_enc.as_ref())?;
            high_qc_rounds.push(HighQcRoundSigColTuple::<SCT> {
                high_qc_round,
                sigs: sct,
            });
        }
        Ok(Self {
            epoch,
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
            .expect("verification of received TimeoutCertificates should have rejected any with empty high_qc_rounds")
    }
}
