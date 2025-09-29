// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::collections::HashMap;

use alloy_rlp::{Decodable, Encodable, RlpDecodable, RlpEncodable};
use monad_crypto::{
    certificate_signature::{
        CertificateSignature, CertificateSignaturePubKey, CertificateSignatureRecoverable,
    },
    signing_domain,
};
use monad_types::*;
use monad_validator::{
    signature_collection::{
        deserialize_signature_collection, serialize_signature_collection, SignatureCollection,
        SignatureCollectionError, SignatureCollectionKeyPairType,
    },
    validator_mapping::ValidatorMapping,
};
use serde::{Deserialize, Serialize};

use crate::{
    quorum_certificate::QuorumCertificate, tip::ConsensusTip, voting::Vote, RoundCertificate,
};

/// Timeout message to broadcast to other nodes after a local timeout
#[derive(Clone, Debug, PartialEq, Eq, RlpDecodable, RlpEncodable, Serialize)]
#[rlp(trailing)]
pub struct Timeout<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    pub tminfo: TimeoutInfo,
    pub timeout_signature: SCT::SignatureType,

    pub high_extend: HighExtendVote<ST, SCT, EPT>,

    /// if the high_extend.qc round != tminfo.round-1, then this must be the
    /// TC or QC from r-1
    pub last_round_certificate: Option<RoundCertificate<ST, SCT, EPT>>,
}

impl<ST, SCT, EPT> Timeout<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    pub fn new(
        cert_keypair: &SignatureCollectionKeyPairType<SCT>,
        timeout: TimeoutInfo,
        high_extend: HighExtend<ST, SCT, EPT>,
        safe_to_vote: bool,
        last_round_certificate: Option<RoundCertificate<ST, SCT, EPT>>,
    ) -> Self {
        let timeout_digest = alloy_rlp::encode(&timeout);
        let timeout_signature = <SCT::SignatureType as CertificateSignature>::sign::<
            signing_domain::Timeout,
        >(&timeout_digest, cert_keypair);

        let high_extend = match high_extend {
            HighExtend::Qc(qc) => HighExtendVote::Qc(qc),
            HighExtend::Tip(tip) => {
                let maybe_vote_signature = safe_to_vote.then(|| {
                    let vote_digest = alloy_rlp::encode(Vote {
                        round: timeout.round,
                        epoch: timeout.epoch,
                        id: tip.block_header.get_id(),
                    });
                    <SCT::SignatureType as CertificateSignature>::sign::<signing_domain::Vote>(
                        &vote_digest,
                        cert_keypair,
                    )
                });
                HighExtendVote::Tip(tip, maybe_vote_signature)
            }
        };

        Self {
            tminfo: timeout,
            high_extend,
            last_round_certificate,

            timeout_signature,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(bound(serialize = "", deserialize = ""))]
pub enum HighExtend<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    Tip(ConsensusTip<ST, SCT, EPT>),
    Qc(QuorumCertificate<SCT>),
}

impl<ST, SCT, EPT> PartialOrd for HighExtend<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (&self, other) {
            (Self::Qc(qc_1), Self::Qc(qc_2)) => {
                if qc_1.get_round() == qc_2.get_round() {
                    None
                } else {
                    Some(qc_1.get_round().cmp(&qc_2.get_round()))
                }
            }
            (Self::Tip(tip_1), Self::Tip(tip_2)) => {
                if tip_1.block_header.block_round == tip_2.block_header.block_round {
                    None
                } else {
                    Some(
                        tip_1
                            .block_header
                            .block_round
                            .cmp(&tip_2.block_header.block_round),
                    )
                }
            }
            (Self::Qc(qc_1), Self::Tip(tip_2)) => {
                if qc_1.get_round() == tip_2.block_header.block_round {
                    // QC takes precedence
                    Some(std::cmp::Ordering::Greater)
                } else {
                    Some(qc_1.get_round().cmp(&tip_2.block_header.block_round))
                }
            }
            (Self::Tip(tip_1), Self::Qc(qc_2)) => {
                if tip_1.block_header.block_round == qc_2.get_round() {
                    // QC takes precedence
                    Some(std::cmp::Ordering::Less)
                } else {
                    Some(tip_1.block_header.block_round.cmp(&qc_2.get_round()))
                }
            }
        }
    }
}

impl<ST, SCT, EPT> HighExtend<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    pub fn qc(&self) -> &QuorumCertificate<SCT> {
        match &self {
            Self::Tip(tip) => &tip.block_header.qc,
            Self::Qc(qc) => qc,
        }
    }
}

impl<ST, SCT, EPT> Encodable for HighExtend<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn encode(&self, out: &mut dyn bytes::BufMut) {
        match &self {
            Self::Tip(tip) => {
                let enc: [&dyn Encodable; 2] = [&1u8, tip];
                alloy_rlp::encode_list::<_, dyn Encodable>(&enc, out);
            }
            Self::Qc(qc) => {
                let enc: [&dyn Encodable; 2] = [&2u8, qc];
                alloy_rlp::encode_list::<_, dyn Encodable>(&enc, out);
            }
        }
    }
}

impl<ST, SCT, EPT> Decodable for HighExtend<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let mut payload = alloy_rlp::Header::decode_bytes(buf, true)?;
        match u8::decode(&mut payload)? {
            1 => {
                let tip = ConsensusTip::decode(&mut payload)?;
                Ok(Self::Tip(tip))
            }
            2 => {
                let qc = QuorumCertificate::decode(&mut payload)?;
                Ok(Self::Qc(qc))
            }
            _ => Err(alloy_rlp::Error::Custom(
                "failed to decode unknown HighExtend",
            )),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub enum HighExtendVote<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    Tip(
        ConsensusTip<ST, SCT, EPT>,
        Option<SCT::SignatureType>, // vote
    ),
    Qc(QuorumCertificate<SCT>),
}

impl<ST, SCT, EPT> HighExtendVote<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    pub fn qc(&self) -> &QuorumCertificate<SCT> {
        match &self {
            Self::Tip(tip, _) => &tip.block_header.qc,
            Self::Qc(qc) => qc,
        }
    }
}

impl<ST, SCT, EPT> Encodable for HighExtendVote<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn encode(&self, out: &mut dyn bytes::BufMut) {
        match &self {
            Self::Tip(tip, maybe_vote_signature) => match maybe_vote_signature {
                Some(vote_signature) => {
                    let enc: [&dyn Encodable; 3] = [&1u8, tip, vote_signature];
                    alloy_rlp::encode_list::<_, dyn Encodable>(&enc, out);
                }
                None => {
                    let enc: [&dyn Encodable; 2] = [&1u8, tip];
                    alloy_rlp::encode_list::<_, dyn Encodable>(&enc, out);
                }
            },
            Self::Qc(qc) => {
                let enc: [&dyn Encodable; 2] = [&2u8, qc];
                alloy_rlp::encode_list::<_, dyn Encodable>(&enc, out);
            }
        }
    }
}

impl<ST, SCT, EPT> Decodable for HighExtendVote<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let mut payload = alloy_rlp::Header::decode_bytes(buf, true)?;
        match u8::decode(&mut payload)? {
            1 => {
                let tip = ConsensusTip::decode(&mut payload)?;
                let maybe_vote_signature = if !payload.is_empty() {
                    Some(SCT::SignatureType::decode(&mut payload)?)
                } else {
                    None
                };
                Ok(Self::Tip(tip, maybe_vote_signature))
            }
            2 => {
                let qc = QuorumCertificate::decode(&mut payload)?;
                Ok(Self::Qc(qc))
            }
            _ => Err(alloy_rlp::Error::Custom(
                "failed to decode unknown HighExtend",
            )),
        }
    }
}

impl<ST, SCT, EPT> From<HighExtendVote<ST, SCT, EPT>> for HighExtend<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn from(high_extend: HighExtendVote<ST, SCT, EPT>) -> Self {
        match high_extend {
            HighExtendVote::Qc(qc) => Self::Qc(qc),
            HighExtendVote::Tip(tip, _) => Self::Tip(tip),
        }
    }
}

/// Data to include in a timeout
#[derive(Clone, Debug, PartialEq, Eq, Hash, RlpEncodable, RlpDecodable, Serialize)]
pub struct TimeoutInfo {
    /// Epoch where the timeout happens
    pub epoch: Epoch,
    /// The round that timed out
    pub round: Round,
    /// The node's highest voted tip
    pub high_qc_round: Round,
    /// The node's highest voted tip, if greater than high_qc_round
    /// Otherwise, is zero (GENESIS_ROUND)
    pub high_tip_round: Round,
}

#[derive(Clone, Debug, PartialEq, Eq, RlpEncodable, RlpDecodable, Serialize, Deserialize)]
#[serde(bound(
    serialize = "SCT: SignatureCollection",
    deserialize = "SCT: SignatureCollection"
))]
pub struct HighTipRoundSigColTuple<SCT> {
    pub high_qc_round: Round,
    pub high_tip_round: Round,
    #[serde(serialize_with = "serialize_signature_collection::<_, SCT>")]
    #[serde(deserialize_with = "deserialize_signature_collection::<_, SCT>")]
    pub sigs: SCT,
}

/// TimeoutCertificate is used to advance rounds when a QC is unable to
/// form for a round
/// A collection of Timeout messages is the basis for building a TC
#[derive(Clone, Debug, PartialEq, Eq, RlpEncodable, RlpDecodable, Serialize, Deserialize)]
#[serde(bound(serialize = "", deserialize = ""))]
pub struct TimeoutCertificate<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    /// The epoch where the TC is created
    pub epoch: Epoch,
    /// The Timeout messages must have been for the same round
    /// to create a TC
    pub round: Round,
    /// signatures over the round of the TC and the high tip round,
    pub tip_rounds: Vec<HighTipRoundSigColTuple<SCT>>,

    // corresponds to the highest tip (or qc if no tip) in tip_rounds
    pub high_extend: HighExtend<ST, SCT, EPT>,
}

impl<ST, SCT, EPT> TimeoutCertificate<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    pub fn new(
        epoch: Epoch,
        round: Round,
        high_tip_round_sig_tuple: &[(NodeId<SCT::NodeIdPubKey>, Timeout<ST, SCT, EPT>)],
        validator_mapping: &ValidatorMapping<
            SCT::NodeIdPubKey,
            SignatureCollectionKeyPairType<SCT>,
        >,
    ) -> Result<Self, SignatureCollectionError<SCT::NodeIdPubKey, SCT::SignatureType>> {
        let mut highest_extend = HighExtend::Qc(QuorumCertificate::genesis_qc());
        let mut sigs: HashMap<TimeoutInfo, Vec<_>> = HashMap::new();
        for (node_id, timeout) in high_tip_round_sig_tuple {
            let entry = sigs.entry(timeout.tminfo.clone()).or_default();
            entry.push((*node_id, timeout.timeout_signature));

            let timeout_high_extend: HighExtend<_, _, _> = timeout.high_extend.clone().into();
            if timeout_high_extend > highest_extend {
                highest_extend = timeout_high_extend;
            }
        }

        let mut tip_rounds = Vec::new();
        for (timeout_info, sigs) in sigs.into_iter() {
            let tminfo_digest_enc = alloy_rlp::encode(&timeout_info);
            let sct = SCT::new::<signing_domain::Timeout>(
                sigs,
                validator_mapping,
                tminfo_digest_enc.as_ref(),
            )?;
            tip_rounds.push(HighTipRoundSigColTuple {
                high_qc_round: timeout_info.high_qc_round,
                high_tip_round: timeout_info.high_tip_round,
                sigs: sct,
            });
        }

        Ok(Self {
            epoch,
            round,
            tip_rounds,
            high_extend: highest_extend,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, RlpEncodable, RlpDecodable, Serialize)]
pub struct NoTipCertificate<SCT>
where
    SCT: SignatureCollection,
{
    pub epoch: Epoch,
    pub round: Round,
    pub tip_rounds: Vec<HighTipRoundSigColTuple<SCT>>,

    pub high_qc: QuorumCertificate<SCT>,
}
