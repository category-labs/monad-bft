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

use std::{fmt::Debug, ops::Deref};

use alloy_rlp::{RlpDecodable, RlpDecodableWrapper, RlpEncodable, RlpEncodableWrapper};
use monad_consensus_types::{
    no_endorsement::NoEndorsement,
    payload::ConsensusBlockBody,
    timeout::{HighExtend, Timeout, TimeoutCertificate, TimeoutInfo},
    tip::ConsensusTip,
    voting::Vote,
    RoundCertificate,
};
use monad_crypto::{
    certificate_signature::{
        CertificateSignature, CertificateSignaturePubKey, CertificateSignatureRecoverable,
    },
    signing_domain,
};
use monad_types::{Epoch, ExecutionProtocol, Round};
use monad_validator::signature_collection::{SignatureCollection, SignatureCollectionKeyPairType};
use serde::Serialize;

/// Consensus protocol vote message
///
/// The signature is a protocol signature, can be collected into the
/// corresponding SignatureCollection type, used to create QC from the votes
#[derive(PartialEq, Eq, Clone, RlpEncodable, RlpDecodable, Serialize)]
pub struct VoteMessage<SCT: SignatureCollection> {
    pub vote: Vote,
    pub sig: SCT::SignatureType,
}

/// Explicitly implementing Copy because derive macro can't resolve that
/// SCT::SignatureType is actually Copy
impl<SCT: SignatureCollection> Copy for VoteMessage<SCT> {}

impl<SCT: SignatureCollection> std::fmt::Debug for VoteMessage<SCT> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VoteMessage")
            .field("vote", &self.vote)
            .field("sig", &self.sig)
            .finish()
    }
}

impl<SCT: SignatureCollection> VoteMessage<SCT> {
    pub fn new(vote: Vote, key: &SignatureCollectionKeyPairType<SCT>) -> Self {
        let vote_enc = alloy_rlp::encode(vote);
        let sig = <SCT::SignatureType as CertificateSignature>::sign::<signing_domain::Vote>(
            vote_enc.as_ref(),
            key,
        );

        Self { vote, sig }
    }
}

/// Wrapper for Timeout to keep validation in the same crate
#[derive(Clone, Debug, PartialEq, Eq, RlpDecodableWrapper, RlpEncodableWrapper, Serialize)]
pub struct TimeoutMessage<ST, SCT, EPT>(pub Timeout<ST, SCT, EPT>)
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol;

impl<ST, SCT, EPT> TimeoutMessage<ST, SCT, EPT>
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
        TimeoutMessage(Timeout::new(
            cert_keypair,
            timeout,
            high_extend,
            safe_to_vote,
            last_round_certificate,
        ))
    }
}

impl<ST, SCT, EPT> AsRef<Timeout<ST, SCT, EPT>> for TimeoutMessage<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn as_ref(&self) -> &Timeout<ST, SCT, EPT> {
        &self.0
    }
}

impl<ST, SCT, EPT> Deref for TimeoutMessage<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    type Target = Timeout<ST, SCT, EPT>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<ST, SCT, EPT> From<Timeout<ST, SCT, EPT>> for TimeoutMessage<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn from(timeout: Timeout<ST, SCT, EPT>) -> Self {
        Self(timeout)
    }
}

/// Consensus protocol proposal message
#[derive(Clone, Debug, PartialEq, Eq, RlpEncodable, RlpDecodable, Serialize)]
#[rlp(trailing)]
pub struct ProposalMessage<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    pub proposal_round: Round,
    pub proposal_epoch: Epoch,

    pub tip: ConsensusTip<ST, SCT, EPT>,
    pub block_body: ConsensusBlockBody<EPT>,

    pub last_round_tc: Option<TimeoutCertificate<ST, SCT, EPT>>,
}

/// This message is broadcasted upon locally constructing QC(r)
/// This helps other nodes advance their round faster
#[derive(Clone, Debug, PartialEq, Eq, RlpEncodable, RlpDecodable, Serialize)]
pub struct AdvanceRoundMessage<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    pub last_round_certificate: RoundCertificate<ST, SCT, EPT>,
}

#[derive(Clone, Debug, PartialEq, Eq, RlpEncodable, RlpDecodable, Serialize)]
pub struct RoundRecoveryMessage<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    pub round: Round,
    pub epoch: Epoch,
    pub tc: TimeoutCertificate<ST, SCT, EPT>,
}

#[derive(PartialEq, Eq, Clone, Debug, RlpEncodable, RlpDecodable, Serialize)]
pub struct NoEndorsementMessage<SCT: SignatureCollection> {
    pub msg: NoEndorsement,

    pub signature: SCT::SignatureType,
}

impl<SCT: SignatureCollection> NoEndorsementMessage<SCT> {
    pub fn new(no_endorsement: NoEndorsement, key: &SignatureCollectionKeyPairType<SCT>) -> Self {
        let no_endorsement_enc = alloy_rlp::encode(&no_endorsement);
        let signature = <SCT::SignatureType as CertificateSignature>::sign::<
            signing_domain::NoEndorsement,
        >(no_endorsement_enc.as_ref(), key);

        Self {
            msg: no_endorsement,
            signature,
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy_rlp::Encodable;
    use monad_bls::BlsSignatureCollection;
    use monad_consensus_types::{
        block::{ConsensusBlockHeader, MockExecutionProposedHeader, MockExecutionProtocol},
        payload::{ConsensusBlockBodyId, RoundSignature},
        quorum_certificate::QuorumCertificate,
        timeout::{HighExtend, HighExtendVote, TimeoutInfo},
    };
    use monad_crypto::{
        certificate_signature::{CertificateKeyPair, CertificateSignaturePubKey},
        NopSignature,
    };
    use monad_multi_sig::MultiSig;
    use monad_secp::SecpSignature;
    use monad_testutil::signing::get_certificate_key;
    use monad_types::{Epoch, Hash, NodeId, Round, SeqNum, GENESIS_ROUND};

    use super::*;

    type SignatureType = SecpSignature;
    type SignatureCollectionType =
        BlsSignatureCollection<CertificateSignaturePubKey<SignatureType>>;
    type ExecutionProtocolType = MockExecutionProtocol;

    type MockSigType = NopSignature;
    type MockSignatureCollectionType = MultiSig<MockSigType>;

    #[test]
    fn timeout_message_serdes_1() {
        let key = get_certificate_key::<SignatureCollectionType>(22354);
        let genesis_qc = QuorumCertificate::<SignatureCollectionType>::genesis_qc();

        let timeout = TimeoutInfo {
            epoch: Epoch(12),
            round: Round(123),
            high_qc_round: genesis_qc.get_round(),
            high_tip_round: GENESIS_ROUND,
        };

        let msg: TimeoutMessage<SignatureType, SignatureCollectionType, ExecutionProtocolType> =
            TimeoutMessage::new(&key, timeout, HighExtend::Qc(genesis_qc), true, None);

        let b = alloy_rlp::encode(msg);
        let c: TimeoutMessage<SignatureType, SignatureCollectionType, ExecutionProtocolType> =
            alloy_rlp::decode_exact(b).unwrap();

        assert_eq!(c.tminfo.epoch, Epoch(12));
        assert_eq!(c.tminfo.round, Round(123));
        assert_eq!(
            c.high_extend,
            HighExtendVote::Qc(QuorumCertificate::genesis_qc())
        );
        assert!(c.last_round_certificate.is_none());
    }

    #[test]
    fn timeout_message_serdes_2() {
        let key = get_certificate_key::<MockSignatureCollectionType>(22354);
        let genesis_qc = QuorumCertificate::<MockSignatureCollectionType>::genesis_qc();

        let timeout = TimeoutInfo {
            epoch: Epoch(12),
            round: Round(123),
            high_qc_round: genesis_qc.get_round(),
            high_tip_round: GENESIS_ROUND,
        };

        let msg: TimeoutMessage<MockSigType, MockSignatureCollectionType, ExecutionProtocolType> =
            TimeoutMessage::new(&key, timeout, HighExtend::Qc(genesis_qc), true, None);

        let b = alloy_rlp::encode(msg);
        let c: TimeoutMessage<MockSigType, MockSignatureCollectionType, ExecutionProtocolType> =
            alloy_rlp::decode_exact(b).unwrap();

        assert_eq!(c.tminfo.epoch, Epoch(12));
        assert_eq!(c.tminfo.round, Round(123));
        assert_eq!(
            c.high_extend,
            HighExtendVote::Qc(QuorumCertificate::genesis_qc())
        );
        assert!(c.last_round_certificate.is_none());
    }

    #[test]
    fn high_extend_vote_serde() {
        const BASE_FEE: u64 = 100_000_000_000;
        const BASE_FEE_TREND: u64 = 0;
        const BASE_FEE_MOMENT: u64 = 0;

        let key = get_certificate_key::<MockSignatureCollectionType>(22354);

        let header: ConsensusBlockHeader<
            MockSigType,
            MockSignatureCollectionType,
            ExecutionProtocolType,
        > = ConsensusBlockHeader::new(
            NodeId::new(key.pubkey()),
            Epoch(1),
            Round(10),
            Vec::new(), // delayed_execution_results
            MockExecutionProposedHeader {},
            ConsensusBlockBodyId(Hash::default()),
            QuorumCertificate::genesis_qc(),
            SeqNum(1),
            0,
            RoundSignature::new(Round(10), &key),
            Some(BASE_FEE),
            Some(BASE_FEE_TREND),
            Some(BASE_FEE_MOMENT),
        );
        let tip = ConsensusTip::new(&key, header, None);

        let high_extend_tip_no_vote = HighExtendVote::Tip(tip.clone(), None);
        let high_extend_tip_no_vote_encoded = {
            let mut buf = Vec::new();
            alloy_rlp::encode_list::<_, dyn Encodable>(
                &[&1_u8 as &dyn Encodable, &tip as &dyn Encodable],
                &mut buf,
            );
            buf
        };
        assert_eq!(
            high_extend_tip_no_vote_encoded,
            alloy_rlp::encode(&high_extend_tip_no_vote)
        );
        assert_eq!(
            alloy_rlp::decode_exact(&high_extend_tip_no_vote_encoded),
            Ok(high_extend_tip_no_vote),
        );

        let vote_sig = NopSignature::sign::<signing_domain::Vote>(b"asdf", &key);
        let high_extend_tip_vote = HighExtendVote::Tip(tip.clone(), Some(vote_sig));
        let high_extend_tip_vote_encoded = {
            let mut buf = Vec::new();
            alloy_rlp::encode_list::<_, dyn Encodable>(
                &[
                    &1_u8 as &dyn Encodable,
                    &tip as &dyn Encodable,
                    &vote_sig as &dyn Encodable,
                ],
                &mut buf,
            );
            buf
        };
        assert_eq!(
            high_extend_tip_vote_encoded,
            alloy_rlp::encode(&high_extend_tip_vote)
        );
        assert_eq!(
            alloy_rlp::decode_exact(&high_extend_tip_vote_encoded),
            Ok(high_extend_tip_vote),
        );
    }
}
