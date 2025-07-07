use std::fmt::Debug;

use alloy_rlp::{RlpDecodable, RlpEncodable};
use monad_consensus_types::{
    payload::ConsensusBlockBody,
    signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
    timeout::{Timeout, TimeoutCertificate},
    tip::ConsensusTip,
    voting::Vote,
};
use monad_crypto::{
    certificate_signature::{
        CertificateSignature, CertificateSignaturePubKey, CertificateSignatureRecoverable,
    },
    signing_domain,
};
use monad_types::{Epoch, ExecutionProtocol, Round};

/// Consensus protocol vote message
///
/// The signature is a protocol signature, can be collected into the
/// corresponding SignatureCollection type, used to create QC from the votes
#[derive(PartialEq, Eq, Clone, RlpEncodable, RlpDecodable)]
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

pub type TimeoutMessage<ST, SCT, EPT> = Timeout<ST, SCT, EPT>;

/// Consensus protocol proposal message
#[derive(Clone, Debug, PartialEq, Eq, RlpEncodable, RlpDecodable)]
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

#[derive(Clone, Debug, PartialEq, Eq, RlpEncodable, RlpDecodable)]
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

#[cfg(test)]
mod tests {
    use monad_bls::BlsSignatureCollection;
    use monad_consensus_types::{
        block::MockExecutionProtocol,
        quorum_certificate::QuorumCertificate,
        timeout::{HighExtend, HighExtendVote, TimeoutInfo},
    };
    use monad_crypto::{certificate_signature::CertificateSignaturePubKey, NopSignature};
    use monad_multi_sig::MultiSig;
    use monad_secp::SecpSignature;
    use monad_testutil::signing::get_certificate_key;
    use monad_types::{Epoch, Round, GENESIS_ROUND};

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
            TimeoutMessage::new(&key, timeout, HighExtend::Qc(genesis_qc), None);

        let b = alloy_rlp::encode(msg);
        let c: TimeoutMessage<SignatureType, SignatureCollectionType, ExecutionProtocolType> =
            alloy_rlp::decode_exact(b).unwrap();

        assert_eq!(c.tminfo.epoch, Epoch(12));
        assert_eq!(c.tminfo.round, Round(123));
        assert_eq!(
            c.high_extend,
            HighExtendVote::Qc(QuorumCertificate::genesis_qc())
        );
        assert!(c.last_round_tc.is_none());
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
            TimeoutMessage::new(&key, timeout, HighExtend::Qc(genesis_qc), None);

        let b = alloy_rlp::encode(msg);
        let c: TimeoutMessage<MockSigType, MockSignatureCollectionType, ExecutionProtocolType> =
            alloy_rlp::decode_exact(b).unwrap();

        assert_eq!(c.tminfo.epoch, Epoch(12));
        assert_eq!(c.tminfo.round, Round(123));
        assert_eq!(
            c.high_extend,
            HighExtendVote::Qc(QuorumCertificate::genesis_qc())
        );
        assert!(c.last_round_tc.is_none());
    }
}
