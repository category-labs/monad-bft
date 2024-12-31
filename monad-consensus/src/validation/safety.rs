use std::cmp;

use monad_consensus_types::{
    block::{BlockPolicy, ExecutionProtocol},
    quorum_certificate::QuorumCertificate,
    signature_collection::SignatureCollection,
    timeout::{TimeoutCertificate, TimeoutInfo},
    voting::Vote,
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_state_backend::StateBackend;
use monad_types::*;

#[derive(PartialEq, Eq, Debug)]
pub struct Safety {
    highest_vote_round: Round,
    highest_qc_round: Round,
}

impl Default for Safety {
    fn default() -> Self {
        Self {
            highest_vote_round: Round(0),
            highest_qc_round: Round(0),
        }
    }
}

impl Safety {
    pub fn new(highest_vote_round: Round, highest_qc_round: Round) -> Self {
        Safety {
            highest_vote_round,
            highest_qc_round,
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
    fn safe_to_vote<SCT>(
        &self,
        block_round: Round,
        qc_round: Round,
        tc: &Option<TimeoutCertificate<SCT>>,
    ) -> bool {
        if block_round <= cmp::max(self.highest_vote_round, qc_round) {
            return false;
        }

        consecutive(block_round, qc_round) || safe_to_extend(block_round, qc_round, tc)
    }

    /// A round is safe to timeout if there's no QC formed for that round, and
    /// we haven't voted for a higher round, which implies a QC/TC is formed for
    /// the round.
    fn safe_to_timeout<SCT>(
        &self,
        round: Round,
        qc_round: Round,
        tc: &Option<TimeoutCertificate<SCT>>,
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
    pub fn make_timeout<SCT: SignatureCollection>(
        &mut self,
        epoch: Epoch,
        round: Round,
        high_qc: QuorumCertificate<SCT>,
        last_tc: &Option<TimeoutCertificate<SCT>>,
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
    pub fn make_vote<ST, SCT, EPT, BPT, SBT>(
        &mut self,
        block: &BPT::ValidatedBlock,
        last_tc: &Option<TimeoutCertificate<SCT>>,
    ) -> Option<Vote>
    where
        ST: CertificateSignatureRecoverable,
        SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
        EPT: ExecutionProtocol,
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
            };

            return Some(vote);
        }

        None
    }
}

pub(crate) fn consecutive(block_round: Round, round: Round) -> bool {
    block_round == round + Round(1)
}

fn safe_to_extend<SCT>(
    block_round: Round,
    qc_round: Round,
    tc: &Option<TimeoutCertificate<SCT>>,
) -> bool {
    match tc {
        Some(t) => consecutive(block_round, t.round) && qc_round >= t.max_round(),
        None => false,
    }
}
