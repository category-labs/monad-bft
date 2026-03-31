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

use std::fmt::Debug;

use alloy_rlp::{encode_list, Decodable, Encodable, Header, RlpDecodable, RlpEncodable};
use monad_consensus_types::{checkpoint::RootInfo, RoundCertificate};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_types::{ExecutionProtocol, Round};
use monad_validator::signature_collection::SignatureCollection;
use serde::Serialize;

use crate::{
    messages::message::{
        AdvanceRoundMessage, NoEndorsementMessage, ProposalMessage, RoundRecoveryMessage,
        TimeoutMessage, VoteMessage,
    },
    validation::signing::{Validated, Verified},
};

const PROTOCOL_MESSAGE_NAME: &str = "ProtocolMessage";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PrefilterError {
    OutdatedProposal,
    OutdatedVote,
    OutdatedTimeout,
    OutdatedRoundRecovery,
    OutdatedNoEndorsement,
    OutdatedAdvanceRoundQc,
    OutdatedAdvanceRoundTc,
}

/// Consensus protocol messages
#[derive(Clone, PartialEq, Eq, Serialize)]
pub enum ProtocolMessage<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    /// Consensus protocol proposal message
    Proposal(ProposalMessage<ST, SCT, EPT>),

    /// Consensus protocol vote message
    Vote(VoteMessage<SCT>),

    /// Consensus protocol timeout message
    Timeout(TimeoutMessage<ST, SCT, EPT>),

    RoundRecovery(RoundRecoveryMessage<ST, SCT, EPT>),
    NoEndorsement(NoEndorsementMessage<SCT>),

    /// This message is broadcasted upon locally constructing QC(r)
    /// This helps other nodes advance their round faster
    AdvanceRound(AdvanceRoundMessage<ST, SCT, EPT>),
}

impl<ST, SCT, EPT> Debug for ProtocolMessage<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProtocolMessage::Proposal(p) => f.debug_tuple("").field(&p).finish(),
            ProtocolMessage::Vote(v) => f.debug_tuple("").field(&v).finish(),
            ProtocolMessage::Timeout(t) => f.debug_tuple("").field(&t).finish(),
            ProtocolMessage::RoundRecovery(r) => f.debug_tuple("").field(&r).finish(),
            ProtocolMessage::NoEndorsement(n) => f.debug_tuple("").field(&n).finish(),
            ProtocolMessage::AdvanceRound(l) => f.debug_tuple("").field(&l).finish(),
        }
    }
}

impl<ST, SCT, EPT> ProtocolMessage<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    // NOTE: Keep this in sync with `Self::prefilter()`. Verification epoch lookup and the
    // obsolete-message prefilter should use the same effective round semantics.
    pub fn get_round(&self) -> Round {
        match self {
            ProtocolMessage::Proposal(p) => p.proposal_round,
            ProtocolMessage::Vote(v) => v.vote.round,
            ProtocolMessage::Timeout(t) => t.0.tminfo.round,
            ProtocolMessage::RoundRecovery(r) => r.round,
            ProtocolMessage::NoEndorsement(n) => n.msg.round,
            ProtocolMessage::AdvanceRound(n) => n.last_round_certificate.round(),
        }
    }

    pub fn prefilter(
        &self,
        root_info: Option<&RootInfo>,
        current_round: Round,
    ) -> Result<(), PrefilterError> {
        match self {
            ProtocolMessage::Proposal(p) => {
                // proposals are compared against root to permit out of order proposals
                if let Some(root_info) = root_info {
                    if p.proposal_round <= root_info.round {
                        Err(PrefilterError::OutdatedProposal)
                    } else {
                        Ok(())
                    }
                } else {
                    Ok(())
                }
            }
            ProtocolMessage::Vote(v) => {
                if v.vote.round < current_round {
                    Err(PrefilterError::OutdatedVote)
                } else {
                    Ok(())
                }
            }
            ProtocolMessage::Timeout(t) => {
                if t.0.tminfo.round < current_round {
                    Err(PrefilterError::OutdatedTimeout)
                } else {
                    Ok(())
                }
            }
            ProtocolMessage::RoundRecovery(r) => {
                if r.round < current_round {
                    Err(PrefilterError::OutdatedRoundRecovery)
                } else {
                    Ok(())
                }
            }
            ProtocolMessage::NoEndorsement(n) => {
                if n.msg.round < current_round {
                    Err(PrefilterError::OutdatedNoEndorsement)
                } else {
                    Ok(())
                }
            }
            ProtocolMessage::AdvanceRound(msg) => {
                if msg.last_round_certificate.round() < current_round {
                    match &msg.last_round_certificate {
                        RoundCertificate::Qc(_) => Err(PrefilterError::OutdatedAdvanceRoundQc),
                        RoundCertificate::Tc(_) => Err(PrefilterError::OutdatedAdvanceRoundTc),
                    }
                } else {
                    Ok(())
                }
            }
        }
    }
}

// FIXME-2:
// it can be confusing as we are hashing only part of the message
// in the signature refactoring, we might want a clean split between:
//      integrity sig: sign over the entire serialized struct
//      protocol sig: signatures outlined in the protocol
impl<ST, SCT, EPT> Encodable for ProtocolMessage<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn encode(&self, out: &mut dyn alloy_rlp::BufMut) {
        let name = PROTOCOL_MESSAGE_NAME;
        match self {
            ProtocolMessage::Proposal(m) => {
                let enc: [&dyn Encodable; 3] = [&name, &1u8, &m];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
            ProtocolMessage::Vote(m) => {
                let enc: [&dyn Encodable; 3] = [&name, &2u8, &m];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
            ProtocolMessage::Timeout(m) => {
                let enc: [&dyn Encodable; 3] = [&name, &3u8, &m];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
            ProtocolMessage::RoundRecovery(m) => {
                let enc: [&dyn Encodable; 3] = [&name, &4u8, &m];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
            ProtocolMessage::NoEndorsement(m) => {
                let enc: [&dyn Encodable; 3] = [&name, &5u8, &m];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
            ProtocolMessage::AdvanceRound(m) => {
                let enc: [&dyn Encodable; 3] = [&name, &6u8, &m];
                encode_list::<_, dyn Encodable>(&enc, out);
            }
        }
    }
}

impl<ST, SCT, EPT> Decodable for ProtocolMessage<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let mut payload = Header::decode_bytes(buf, true)?;
        let name = String::decode(&mut payload)?;
        if name != PROTOCOL_MESSAGE_NAME {
            return Err(alloy_rlp::Error::Custom(
                "expected to decode type ProtocolMessage",
            ));
        }

        let result = match u8::decode(&mut payload)? {
            1 => ProtocolMessage::Proposal(ProposalMessage::decode(&mut payload)?),
            2 => ProtocolMessage::Vote(VoteMessage::decode(&mut payload)?),
            3 => ProtocolMessage::Timeout(TimeoutMessage::decode(&mut payload)?),
            4 => ProtocolMessage::RoundRecovery(RoundRecoveryMessage::decode(&mut payload)?),
            5 => ProtocolMessage::NoEndorsement(NoEndorsementMessage::decode(&mut payload)?),
            6 => ProtocolMessage::AdvanceRound(AdvanceRoundMessage::decode(&mut payload)?),
            _ => {
                return Err(alloy_rlp::Error::Custom(
                    "failed to decode unknown ProtocolMessage",
                ));
            }
        };
        if !payload.is_empty() {
            return Err(alloy_rlp::Error::UnexpectedLength);
        }
        Ok(result)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, RlpEncodable, RlpDecodable, Serialize)]
pub struct ConsensusMessage<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    pub version: u32,
    pub message: ProtocolMessage<ST, SCT, EPT>,
}

impl<ST, SCT, EPT> ConsensusMessage<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    #[tracing::instrument(
        level = "debug", 
        name = "consesnsus_sign"
        skip_all,
    )]
    pub fn sign(
        self,
        keypair: &ST::KeyPairType,
    ) -> Verified<ST, Validated<ConsensusMessage<ST, SCT, EPT>>>
    where
        ST: CertificateSignatureRecoverable,
        SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    {
        Verified::new(Validated::new(self), keypair)
    }

    pub fn get_round(&self) -> Round {
        self.message.get_round()
    }

    pub fn prefilter(
        &self,
        root_info: Option<&RootInfo>,
        current_round: Round,
    ) -> Result<(), PrefilterError> {
        self.message.prefilter(root_info, current_round)
    }
}

#[cfg(test)]
mod tests {
    use monad_consensus_types::{
        block::{
            ConsensusBlockHeader, MockExecutionBody, MockExecutionProposedHeader,
            MockExecutionProtocol, GENESIS_TIMESTAMP,
        },
        no_endorsement::NoEndorsement,
        payload::{ConsensusBlockBody, ConsensusBlockBodyInner, RoundSignature},
        quorum_certificate::QuorumCertificate,
        timeout::{HighExtend, TimeoutCertificate, TimeoutInfo},
        tip::ConsensusTip,
        voting::Vote,
    };
    use monad_crypto::{certificate_signature::CertificateKeyPair, NopSignature};
    use monad_multi_sig::MultiSig;
    use monad_testutil::signing::get_certificate_key;
    use monad_types::{
        BlockId, Epoch, Hash, NodeId, Round, SeqNum, GENESIS_BLOCK_ID, GENESIS_ROUND,
    };

    use super::*;
    use crate::messages::message::{
        AdvanceRoundMessage, NoEndorsementMessage, ProposalMessage, RoundRecoveryMessage,
        TimeoutMessage, VoteMessage,
    };

    const BASE_FEE: u64 = 100_000_000_000;
    const BASE_FEE_TREND: u64 = 0;
    const BASE_FEE_MOMENT: u64 = 0;

    type SignatureType = NopSignature;
    type SignatureCollectionType = MultiSig<SignatureType>;
    type ExecutionProtocolType = MockExecutionProtocol;

    fn root_info(round: Round) -> RootInfo {
        RootInfo {
            round,
            seq_num: SeqNum(round.as_u64()),
            epoch: Epoch(1),
            block_id: if round == Round::MIN {
                GENESIS_BLOCK_ID
            } else {
                BlockId(Hash([round.as_u64() as u8; 32]))
            },
            timestamp_ns: GENESIS_TIMESTAMP,
        }
    }

    fn timeout_certificate(
        round: Round,
    ) -> TimeoutCertificate<SignatureType, SignatureCollectionType, ExecutionProtocolType> {
        TimeoutCertificate {
            epoch: Epoch(1),
            round,
            tip_rounds: vec![].into(),
            high_extend: HighExtend::Qc(QuorumCertificate::genesis_qc()),
        }
    }

    fn proposal_message(
        proposal_round: Round,
    ) -> ProtocolMessage<SignatureType, SignatureCollectionType, ExecutionProtocolType> {
        let key = get_certificate_key::<SignatureCollectionType>(22354);
        let block_body = ConsensusBlockBody::new(ConsensusBlockBodyInner {
            execution_body: MockExecutionBody::default(),
        });
        let header = ConsensusBlockHeader::new(
            NodeId::new(key.pubkey()),
            Epoch(1),
            proposal_round,
            Vec::new(),
            MockExecutionProposedHeader::default(),
            block_body.get_id(),
            QuorumCertificate::genesis_qc(),
            SeqNum(proposal_round.as_u64().max(1)),
            0,
            RoundSignature::new(proposal_round, &key),
            BASE_FEE,
            BASE_FEE_TREND,
            BASE_FEE_MOMENT,
        );

        ProtocolMessage::Proposal(ProposalMessage {
            proposal_round,
            proposal_epoch: Epoch(1),
            tip: ConsensusTip::new(&key, header, None),
            block_body,
            last_round_tc: None,
        })
    }

    fn vote_message(
        round: Round,
    ) -> ProtocolMessage<SignatureType, SignatureCollectionType, ExecutionProtocolType> {
        let key = get_certificate_key::<SignatureCollectionType>(22355);
        ProtocolMessage::Vote(VoteMessage::new(
            Vote {
                id: BlockId(Hash([round.as_u64() as u8; 32])),
                epoch: Epoch(1),
                round,
            },
            &key,
        ))
    }

    fn timeout_message(
        round: Round,
    ) -> ProtocolMessage<SignatureType, SignatureCollectionType, ExecutionProtocolType> {
        let key = get_certificate_key::<SignatureCollectionType>(22356);
        ProtocolMessage::Timeout(TimeoutMessage::new(
            &key,
            TimeoutInfo {
                epoch: Epoch(1),
                round,
                high_qc_round: GENESIS_ROUND,
                high_tip_round: GENESIS_ROUND,
            },
            HighExtend::Qc(QuorumCertificate::genesis_qc()),
            true,
            None,
        ))
    }

    fn round_recovery_message(
        round: Round,
    ) -> ProtocolMessage<SignatureType, SignatureCollectionType, ExecutionProtocolType> {
        ProtocolMessage::RoundRecovery(RoundRecoveryMessage {
            round,
            epoch: Epoch(1),
            tc: timeout_certificate(round - Round(1)),
        })
    }

    fn no_endorsement_message(
        round: Round,
    ) -> ProtocolMessage<SignatureType, SignatureCollectionType, ExecutionProtocolType> {
        let key = get_certificate_key::<SignatureCollectionType>(22357);
        ProtocolMessage::NoEndorsement(NoEndorsementMessage::new(
            NoEndorsement {
                epoch: Epoch(1),
                round,
                tip_qc_round: GENESIS_ROUND,
            },
            &key,
        ))
    }

    fn advance_round_qc_message(
        round: Round,
    ) -> ProtocolMessage<SignatureType, SignatureCollectionType, ExecutionProtocolType> {
        let mut qc = QuorumCertificate::genesis_qc();
        qc.info.round = round;
        ProtocolMessage::AdvanceRound(AdvanceRoundMessage {
            last_round_certificate: RoundCertificate::Qc(qc),
        })
    }

    fn advance_round_tc_message(
        round: Round,
    ) -> ProtocolMessage<SignatureType, SignatureCollectionType, ExecutionProtocolType> {
        ProtocolMessage::AdvanceRound(AdvanceRoundMessage {
            last_round_certificate: RoundCertificate::Tc(timeout_certificate(round)),
        })
    }

    #[test]
    fn protocol_message_get_round_matches_variant() {
        assert_eq!(proposal_message(Round(7)).get_round(), Round(7));
        assert_eq!(vote_message(Round(8)).get_round(), Round(8));
        assert_eq!(timeout_message(Round(9)).get_round(), Round(9));
        assert_eq!(round_recovery_message(Round(10)).get_round(), Round(10));
        assert_eq!(no_endorsement_message(Round(11)).get_round(), Round(11));
        assert_eq!(advance_round_qc_message(Round(12)).get_round(), Round(12));
        assert_eq!(advance_round_tc_message(Round(13)).get_round(), Round(13));
    }

    #[test]
    fn prefilter_drops_obsolete_proposals_but_keeps_out_of_order_ones() {
        assert_eq!(
            proposal_message(Round(3)).prefilter(Some(&root_info(Round(3))), Round(10)),
            Err(PrefilterError::OutdatedProposal)
        );
        assert_eq!(
            proposal_message(Round(5)).prefilter(Some(&root_info(Round(3))), Round(10)),
            Ok(())
        );
        assert_eq!(
            proposal_message(Round(3)).prefilter(None, Round(10)),
            Ok(())
        );
    }

    #[test]
    fn prefilter_drops_non_proposals_below_current_round() {
        let current_round = Round(10);
        let root_info = root_info(Round(3));

        assert_eq!(
            vote_message(Round(9)).prefilter(Some(&root_info), current_round),
            Err(PrefilterError::OutdatedVote)
        );
        assert_eq!(
            timeout_message(Round(9)).prefilter(Some(&root_info), current_round),
            Err(PrefilterError::OutdatedTimeout)
        );
        assert_eq!(
            round_recovery_message(Round(9)).prefilter(Some(&root_info), current_round),
            Err(PrefilterError::OutdatedRoundRecovery)
        );
        assert_eq!(
            no_endorsement_message(Round(9)).prefilter(Some(&root_info), current_round),
            Err(PrefilterError::OutdatedNoEndorsement)
        );
        assert_eq!(
            vote_message(Round(10)).prefilter(Some(&root_info), current_round),
            Ok(())
        );
        assert_eq!(
            vote_message(Round(9)).prefilter(None, current_round),
            Err(PrefilterError::OutdatedVote)
        );
    }

    #[test]
    fn prefilter_distinguishes_advance_round_certificate_kind() {
        let current_round = Round(10);
        let root_info = root_info(Round(3));

        assert_eq!(
            advance_round_qc_message(Round(9)).prefilter(Some(&root_info), current_round),
            Err(PrefilterError::OutdatedAdvanceRoundQc)
        );
        assert_eq!(
            advance_round_tc_message(Round(9)).prefilter(Some(&root_info), current_round),
            Err(PrefilterError::OutdatedAdvanceRoundTc)
        );
        assert_eq!(
            advance_round_qc_message(Round(10)).prefilter(Some(&root_info), current_round),
            Ok(())
        );
    }
}
