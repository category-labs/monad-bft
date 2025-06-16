use std::{cmp, marker::PhantomData};

use monad_consensus_types::{
    block::BlockPolicy,
    quorum_certificate::QuorumCertificate,
    signature_collection::SignatureCollection,
    timeout::{TimeoutCertificate, TimeoutInfo},
    voting::Vote,
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_state_backend::StateBackend;
use monad_types::{ExecutionProtocol, *};

#[derive(PartialEq, Eq, Debug)]
pub struct Safety<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    highest_vote_round: Round,
    highest_qc_round: Round,

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
            highest_vote_round: GENESIS_ROUND,
            highest_qc_round: GENESIS_ROUND,

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
    pub fn new(highest_vote_round: Round, highest_qc_round: Round) -> Self {
        Self {
            highest_vote_round,
            highest_qc_round,

            _phantom: PhantomData,
        }
    }

    fn update_highest_vote_round(&mut self, r: Round) {
        self.highest_vote_round = cmp::max(r, self.highest_vote_round);
    }

    fn update_highest_qc_round(&mut self, r: Round) {
        self.highest_qc_round = cmp::max(r, self.highest_qc_round);
    }

    /// A block is safe to vote on if it's strictly higher than the highest
    /// voted round, and it must be correctly extending a QC or TC from the
    /// previous round
    fn safe_to_vote(
        &self,
        block_round: Round,
        qc_round: Round,
        tc: &Option<TimeoutCertificate<ST, SCT, EPT>>,
    ) -> bool {
        if block_round <= cmp::max(self.highest_vote_round, qc_round) {
            return false;
        }

        consecutive(block_round, qc_round) || Self::safe_to_extend(block_round, qc_round, tc)
    }

    /// A round is safe to timeout if there's no QC formed for that round, and
    /// we haven't voted for a higher round, which implies a QC/TC is formed for
    /// the round.
    fn safe_to_timeout(
        &self,
        round: Round,
        qc_round: Round,
        tc: &Option<TimeoutCertificate<ST, SCT, EPT>>,
    ) -> bool {
        if qc_round < self.highest_qc_round
            || round + Round(1) <= self.highest_vote_round
            || round <= qc_round
        {
            return false;
        }

        let consecutive_tc = match tc {
            Some(t) => consecutive(round, t.round),
            None => false,
        };

        consecutive(round, qc_round) || consecutive_tc
    }

    /// Make a TimeoutInfo if it's safe to timeout the current round
    pub fn make_timeout(
        &mut self,
        epoch: Epoch,
        round: Round,
        high_qc: QuorumCertificate<SCT>,
        last_tc: &Option<TimeoutCertificate<ST, SCT, EPT>>,
    ) -> Option<TimeoutInfo<SCT>> {
        let qc_round = high_qc.get_round();
        if self.safe_to_timeout(round, qc_round, last_tc) {
            self.update_highest_vote_round(round);
            Some(TimeoutInfo {
                epoch,
                round,
                high_qc,
            })
        } else {
            None
        }
    }

    /// Make a Vote if it's safe to vote in the round.
    pub fn make_vote<BPT, SBT>(
        &mut self,
        block: &BPT::ValidatedBlock,
        last_tc: &Option<TimeoutCertificate<ST, SCT, EPT>>,
    ) -> Option<Vote>
    where
        BPT: BlockPolicy<ST, SCT, EPT, SBT>,
        SBT: StateBackend,
    {
        let qc_round = block.get_parent_round();
        if self.safe_to_vote(block.get_round(), qc_round, last_tc) {
            self.update_highest_qc_round(qc_round);
            self.update_highest_vote_round(block.get_round());

            let vote = Vote {
                id: block.get_id(),
                epoch: block.get_epoch(),
                round: block.get_round(),
                parent_id: block.get_parent_id(),
                parent_round: block.get_parent_round(),
            };

            return Some(vote);
        }

        None
    }

    fn safe_to_extend(
        block_round: Round,
        qc_round: Round,
        tc: &Option<TimeoutCertificate<ST, SCT, EPT>>,
    ) -> bool {
        match tc {
            Some(t) => consecutive(block_round, t.round) && qc_round >= t.max_round(),
            None => false,
        }
    }
}

pub(crate) fn consecutive(block_round: Round, round: Round) -> bool {
    block_round == round + Round(1)
}
