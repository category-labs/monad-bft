use std::marker::PhantomData;

use monad_consensus_types::signature_collection::SignatureCollection;
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
    highest_vote: Round,
    highest_propose: Round,

    _phantom: PhantomData<(ST, SCT, EPT)>,
}

impl<ST, SCT, EPT> Default for Safety<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn default() -> Self {
        Self {
            highest_vote: GENESIS_ROUND,
            highest_propose: GENESIS_ROUND,

            _phantom: PhantomData,
        }
    }
}

impl<ST, SCT, EPT> Safety<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    pub fn new(highest_vote: Round) -> Self {
        Self {
            highest_vote,
            highest_propose: highest_vote,

            _phantom: PhantomData,
        }
    }

    pub fn is_safe_to_vote(&self, round: Round) -> bool {
        round > self.highest_vote
    }

    pub fn vote(&mut self, round: Round) {
        assert!(self.is_safe_to_vote(round));
        self.highest_vote = round;
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
}
