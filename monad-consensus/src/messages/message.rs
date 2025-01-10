use std::fmt::Debug;

use alloy_rlp::{RlpDecodable, RlpEncodable};
use monad_consensus_types::{
    block::ConsensusBlockHeader,
    payload::ConsensusBlockBody,
    signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
    timeout::{Timeout, TimeoutCertificate},
    voting::Vote,
};
use monad_crypto::{
    certificate_signature::{
        CertificateSignature, CertificateSignaturePubKey, CertificateSignatureRecoverable,
    },
    hasher::{Hashable, Hasher},
};
use monad_types::ExecutionProtocol;

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

/// An integrity hash over all the fields
impl<SCT: SignatureCollection> Hashable for VoteMessage<SCT> {
    fn hash(&self, state: &mut impl Hasher) {
        state.update(alloy_rlp::encode(self));
    }
}

impl<SCT: SignatureCollection> VoteMessage<SCT> {
    pub fn new(vote: Vote, key: &SignatureCollectionKeyPairType<SCT>) -> Self {
        let vote_enc = alloy_rlp::encode(vote);
        let sig = <SCT::SignatureType as CertificateSignature>::sign(vote_enc.as_ref(), key);

        Self { vote, sig }
    }
}

/// Consensus protocol timeout message
///
/// The signature is a protocol signature,can be collected into the
/// corresponding SignatureCollection type, used to create TC from the timeouts
#[derive(Clone, Debug, PartialEq, Eq, RlpDecodable, RlpEncodable)]
pub struct TimeoutMessage<SCT: SignatureCollection> {
    pub timeout: Timeout<SCT>,
    pub sig: SCT::SignatureType,
}

impl<SCT: SignatureCollection> TimeoutMessage<SCT> {
    pub fn new(timeout: Timeout<SCT>, key: &SignatureCollectionKeyPairType<SCT>) -> Self {
        let tmo_enc = alloy_rlp::encode(timeout.tminfo.timeout_digest());
        let sig = <SCT::SignatureType as CertificateSignature>::sign(tmo_enc.as_ref(), key);

        Self { timeout, sig }
    }
}

/// An integrity hash over all the fields
impl<SCT: SignatureCollection> Hashable for TimeoutMessage<SCT> {
    fn hash(&self, state: &mut impl Hasher) {
        state.update(alloy_rlp::encode(self));
    }
}

/// Consensus protocol proposal message
#[derive(Clone, Debug, PartialEq, Eq, RlpEncodable, RlpDecodable)]
#[rlp(trailing)]
pub struct ProposalMessage<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    pub block_header: ConsensusBlockHeader<ST, SCT, EPT>,
    pub block_body: ConsensusBlockBody<EPT>,
    pub last_round_tc: Option<TimeoutCertificate<SCT>>,
}

/// The last_round_tc can be independently verified. The message hash is over
/// the block only
impl<ST, SCT, EPT> Hashable for ProposalMessage<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn hash(&self, state: &mut impl Hasher) {
        state.update(alloy_rlp::encode(self));
    }
}

#[cfg(test)]
mod tests {
    use monad_bls::{BlsSignature, BlsSignatureCollection};
    use monad_consensus_types::{quorum_certificate::QuorumCertificate, timeout::TimeoutInfo};
    use monad_crypto::{certificate_signature::CertificateSignaturePubKey, NopSignature};
    use monad_multi_sig::MultiSig;
    use monad_testutil::signing::get_certificate_key;
    use monad_types::{Epoch, Round};

    use super::*;

    type SignatureType = BlsSignature;
    type SignatureCollectionType =
        BlsSignatureCollection<CertificateSignaturePubKey<SignatureType>>;

    type MockSigType = NopSignature;
    type MockSignatureCollectionType = MultiSig<MockSigType>;

    #[test]
    fn timeout_message_serdes_1() {
        let key = get_certificate_key::<SignatureCollectionType>(22354);
        let genesis_qc = QuorumCertificate::<SignatureCollectionType>::genesis_qc();

        let tminfo = TimeoutInfo {
            epoch: Epoch(12),
            round: Round(123),
            high_qc: genesis_qc.clone(),
        };

        let tm = Timeout {
            tminfo,
            last_round_tc: None,
        };

        let msg = TimeoutMessage::new(tm, &key);

        let b = alloy_rlp::encode(msg);
        let c: TimeoutMessage<SignatureCollectionType> = alloy_rlp::decode_exact(b).unwrap();

        assert_eq!(c.timeout.tminfo.epoch, Epoch(12));
        assert_eq!(c.timeout.tminfo.round, Round(123));
        assert_eq!(c.timeout.tminfo.high_qc, genesis_qc);
        assert!(c.timeout.last_round_tc.is_none());
    }

    #[test]
    fn timeout_message_serdes_2() {
        let key = get_certificate_key::<MockSignatureCollectionType>(22354);
        let genesis_qc = QuorumCertificate::<MockSignatureCollectionType>::genesis_qc();

        let tminfo = TimeoutInfo {
            epoch: Epoch(12),
            round: Round(123),
            high_qc: genesis_qc.clone(),
        };

        let tm = Timeout {
            tminfo,
            last_round_tc: None,
        };

        let msg = TimeoutMessage::new(tm, &key);

        let b = alloy_rlp::encode(msg);
        let c: TimeoutMessage<MockSignatureCollectionType> = alloy_rlp::decode_exact(b).unwrap();

        assert_eq!(c.timeout.tminfo.epoch, Epoch(12));
        assert_eq!(c.timeout.tminfo.round, Round(123));
        assert_eq!(c.timeout.tminfo.high_qc, genesis_qc);
        assert!(c.timeout.last_round_tc.is_none());
    }
}
