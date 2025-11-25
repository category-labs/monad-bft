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

use std::collections::BTreeSet;

use monad_consensus_types::{tip::ConsensusTip, RoundCertificate};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_types::{ExecutionProtocol, *};
use monad_validator::signature_collection::SignatureCollection;

/// Max number of handled proposal_rounds to keep track of
/// This does not affect correctness; it just helps reduce DOS
///
/// In practice, there will only be <10 live rounds past root.round
const HANDLED_PROPOSAL_CACHE_SIZE: usize = 100;

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
    // maybe_high_tip only exists if contains QC >= high_certificate_qc_round
    // this ensures that timeout.high_tip_round > timeout.high_qc_round
    high_certificate_qc_round: Round,

    highest_vote: Round,
    highest_no_endorse: HighNoEndorse,
    highest_propose: Round,
    highest_recovery_request: Round,

    // used to ensure we only process at max 1 proposal per proposal_round
    // this is to prevent DOS
    //
    // keeps track of the highest HANDLED_PROPOSAL_CACHE_SIZE proposals
    handled_proposals: BTreeSet<Round>,
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
            high_certificate_qc_round: GENESIS_ROUND,
            highest_vote: GENESIS_ROUND,
            highest_no_endorse: HighNoEndorse {
                round: GENESIS_ROUND,
                tip: GENESIS_BLOCK_ID,
            },
            highest_propose: GENESIS_ROUND,
            highest_recovery_request: GENESIS_ROUND,

            handled_proposals: Default::default(),
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
            high_certificate_qc_round: high_certificate.qc().get_round(),

            // we never vote/ne/propose the current round
            highest_vote: current_round,
            highest_no_endorse: HighNoEndorse {
                round: current_round,
                tip: GENESIS_BLOCK_ID,
            },
            highest_propose: current_round,
            highest_recovery_request: current_round,

            // this is just for DOS prevention
            handled_proposals: Default::default(),
        }
    }

    pub fn maybe_high_tip(&self) -> Option<&ConsensusTip<ST, SCT, EPT>> {
        self.maybe_high_tip.as_ref()
    }

    pub(crate) fn process_certificate(&mut self, certificate: &RoundCertificate<ST, SCT, EPT>) {
        if certificate.qc().get_round() > self.high_certificate_qc_round {
            self.high_certificate_qc_round = certificate.qc().get_round();
        }
        if self
            .maybe_high_tip
            .as_ref()
            .is_some_and(|tip| tip.block_header.block_round <= self.high_certificate_qc_round)
        {
            self.maybe_high_tip = None;
        }
    }

    pub fn is_safe_to_vote(&self, round: Round, last_round_tc_tip: Option<BlockId>) -> bool {
        if round == self.highest_no_endorse.round
            && last_round_tc_tip != Some(self.highest_no_endorse.tip)
        {
            // once a TC is NE'd, we must only vote on proposals including that TC
            //
            // otherwise, a QC and NEC can be formed in the same round between
            // conflicting tips
            return false;
        }
        round > self.highest_vote
    }

    pub fn vote(
        &mut self,
        round: Round,
        last_round_tc_tip: Option<BlockId>,
        tip: ConsensusTip<ST, SCT, EPT>,
    ) {
        assert!(self.is_safe_to_vote(round, last_round_tc_tip));
        self.highest_vote = round;
        if tip.block_header.block_round > self.high_certificate_qc_round {
            // we should only update high_tip if the tip we're voting on
            // is higher than our high_certificate.qc()

            // this ensures that any timeouts we send have timeout.high_tip_round > timeout.high_qc_round
            self.maybe_high_tip = Some(tip);
        } else {
            self.maybe_high_tip = None;
        }
    }

    pub fn is_safe_to_no_endorse(&self, round: Round) -> bool {
        // has not sent NE in round
        round > self.highest_no_endorse.round.max(self.highest_vote)
    }

    pub fn no_endorse(&mut self, round: Round, tip: BlockId) {
        assert!(self.is_safe_to_no_endorse(round));
        self.highest_no_endorse = HighNoEndorse { round, tip }
    }

    /// returns true if it's safe to include a vote for high_tip
    pub fn timeout(&mut self, round: Round) -> bool {
        assert!(round >= self.highest_vote);
        self.highest_vote = round;

        // We should only include Vote(r) in Timeout(r) if either:
        // 1. We haven't sent NE(r)
        // 2. We've sent NE(r) AND high_tip.round == r
        // This is necessary to ensure that NEC(r) and QC(r) don't form for conflicting tips in the same round.
        self.highest_no_endorse.round < round
            || self
                .maybe_high_tip()
                .is_some_and(|tip| tip.block_header.block_round == round)
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

    pub fn is_safe_to_handle_proposal(&mut self, round: Round) -> bool {
        !self.handled_proposals.contains(&round)
    }

    pub fn handle_proposal(&mut self, round: Round) {
        assert!(self.is_safe_to_handle_proposal(round));
        let _ = self.handled_proposals.insert(round);
        while self.handled_proposals.len() > HANDLED_PROPOSAL_CACHE_SIZE {
            self.handled_proposals.pop_first();
        }
    }
}

#[derive(PartialEq, Eq, Debug)]
struct HighNoEndorse {
    round: Round,
    tip: BlockId,
}
