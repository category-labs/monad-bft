use monad_consensus_types::{
    signature_collection::SignatureCollection, tip::ConsensusTip, RoundCertificate,
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_types::{ExecutionProtocol, *};

/// The safety module is responsible for all *stateful* safety checks.
/// This makes sure that we don't double vote, double propose, etc.
///
/// Stateless validity checks must be done in the validation module.
///
/// For example, validating that a proposal correctly extends included
/// certificates is outside the scope of this module.
#[derive(PartialEq, Eq, Debug)]
pub struct Safety<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    maybe_high_tip: Option<ConsensusTip<ST, SCT, EPT>>,

    highest_vote: Round,
    highest_no_endorse: Round,
    highest_propose: Round,
    highest_recovery_request: Round,
}

impl<ST, SCT, EPT> Default for Safety<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn default() -> Self {
        Self {
            maybe_high_tip: None,
            highest_vote: GENESIS_ROUND,
            highest_no_endorse: GENESIS_ROUND,
            highest_propose: GENESIS_ROUND,
            highest_recovery_request: GENESIS_ROUND,
        }
    }
}

impl<ST, SCT, EPT> Safety<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    pub fn new(
        high_certificate: RoundCertificate<ST, SCT, EPT>,
        maybe_high_tip: Option<ConsensusTip<ST, SCT, EPT>>,
    ) -> Self {
        let current_round = high_certificate.round() + Round(1);
        Self {
            maybe_high_tip,

            highest_vote: current_round,
            highest_no_endorse: current_round,
            highest_propose: current_round,
            highest_recovery_request: current_round,
        }
    }

    pub fn maybe_high_tip(&self) -> Option<&ConsensusTip<ST, SCT, EPT>> {
        self.maybe_high_tip.as_ref()
    }

    pub(crate) fn process_certificate(&mut self, certificate: &RoundCertificate<ST, SCT, EPT>) {
        if let Some(tip) = &self.maybe_high_tip {
            if certificate.qc().get_round() >= tip.block_header.block_round {
                self.maybe_high_tip = None;
            }
        }
    }

    pub fn is_safe_to_vote(&self, round: Round) -> bool {
        round > self.highest_vote
    }

    pub fn vote(&mut self, round: Round, tip: ConsensusTip<ST, SCT, EPT>) {
        assert!(self.is_safe_to_vote(round));
        self.highest_vote = round;
        self.maybe_high_tip = Some(tip);
    }

    pub fn is_safe_to_no_endorse(&self, round: Round) -> bool {
        round > self.highest_no_endorse.max(self.highest_vote)
    }

    pub fn no_endorse(&mut self, round: Round) {
        assert!(self.is_safe_to_no_endorse(round));
        self.highest_no_endorse = round;
    }

    pub fn timeout(&mut self, round: Round) {
        assert!(round >= self.highest_vote);
        self.highest_vote = round;
    }

    pub fn is_safe_to_propose(&self, round: Round) -> bool {
        round > self.highest_propose.max(self.highest_vote)
    }

    pub fn propose(&mut self, round: Round) {
        assert!(self.is_safe_to_propose(round));
        self.highest_propose = round;
    }

    pub fn is_safe_to_recovery_request(&mut self, round: Round) -> bool {
        round > self.highest_recovery_request
    }

    pub fn recovery_request(&mut self, round: Round) {
        assert!(self.is_safe_to_recovery_request(round));
        self.highest_recovery_request = round;
    }
}
