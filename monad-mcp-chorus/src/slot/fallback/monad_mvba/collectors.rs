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

use std::{cell::OnceCell, collections::BTreeMap};

use super::{
    super::{
        super::types::{IsVote, NodeId, Slot, StrongQc, ValidatorData, VoteMsg, VotePool},
        FallbackView, ValidateCert, Votable,
    },
    certificates::{FallbackCommitQc, PrepareQc, TimeoutCertificate},
    messages::{
        CommitVoteMsg, FallbackCommitVote, PrePrepareMsg, PrepareVote, PrepareVoteMsg, TimeoutMsg,
        TimeoutVote,
    },
};
use crate::spec::{Stake as _, validator::ValidatorData as _};

/// A vote pool whose strong quorum, once formed, is final: no later vote can
/// change what a supermajority over the scope certified, so the aggregation
/// runs once
///
/// Only sound for votes where every strong certificate over the scope
/// certifies the same fact, which prepare and commit votes are (two strong
/// quorums on different verdicts need a third of the stake to equivocate)
#[derive(Clone)]
pub(crate) struct SealingVotePool<V: IsVote> {
    votes: VotePool<V>,
    /// Set by the first successful aggregation; a `&self` cache so formation
    /// stays a read
    sealed: OnceCell<StrongQc<V>>,
}

impl<V: IsVote> SealingVotePool<V> {
    fn new(scope: V::Scope) -> Self {
        Self {
            votes: VotePool::new(scope),
            sealed: OnceCell::new(),
        }
    }

    fn add_vote(&mut self, node_id: NodeId, msg: VoteMsg<V>) {
        if self.sealed.get().is_some() {
            return;
        }
        self.votes.add_vote(node_id, msg);
    }

    fn try_form_strong_qc(&self, validator_data: &ValidatorData) -> Option<&StrongQc<V>> {
        if let Some(qc) = self.sealed.get() {
            return Some(qc);
        }
        let qc = self.votes.try_form_strong_qc(validator_data)?;
        Some(self.sealed.get_or_init(|| qc))
    }
}

/// Everything this instance has buffered for one view
#[derive(Clone)]
pub(crate) struct ViewCollectors<V: Votable, C: ValidateCert> {
    slot: Slot,
    view: FallbackView,

    /// The first pre-prepare seen for the view
    pre_prepare: Option<PrePrepareMsg<V, C>>,
    prepare_votes: SealingVotePool<PrepareVote<V>>,
    commit_votes: SealingVotePool<FallbackCommitVote<V>>,
    timeout_votes: VotePool<TimeoutVote>,
    /// Prepare certificates carried in by timeouts, keyed by their own view
    prep_qcs: BTreeMap<FallbackView, PrepareQc<V>>,

    /// A timeout certificate for this view harvested from another message,
    /// kept even when that message is refused
    harvested_tc: Option<TimeoutCertificate<V>>,
}

impl<V: Votable, C: ValidateCert> ViewCollectors<V, C> {
    pub(crate) fn new(slot: Slot, view: FallbackView) -> Self {
        Self {
            slot,
            view,
            pre_prepare: None,
            prepare_votes: SealingVotePool::new((slot, view)),
            commit_votes: SealingVotePool::new((slot, view)),
            timeout_votes: VotePool::new((slot, view)),
            prep_qcs: BTreeMap::new(),
            harvested_tc: None,
        }
    }

    /// First write wins
    pub(crate) fn store_pre_prepare(&mut self, msg: PrePrepareMsg<V, C>) {
        debug_assert_eq!((msg.slot, msg.view), (self.slot, self.view));

        self.pre_prepare.get_or_insert(msg);
    }

    pub(crate) fn pre_prepare(&self) -> Option<&PrePrepareMsg<V, C>> {
        self.pre_prepare.as_ref()
    }

    pub(crate) fn store_prepare_vote(&mut self, sender: NodeId, msg: PrepareVoteMsg<V>) {
        self.prepare_votes.add_vote(sender, msg);
    }

    pub(crate) fn store_commit_vote(&mut self, sender: NodeId, msg: CommitVoteMsg<V>) {
        self.commit_votes.add_vote(sender, msg);
    }

    /// `PrepQC_{slot, view}`, if a supermajority voted to prepare the same
    /// entries here. Aggregated once; a formed certificate is final
    pub(crate) fn try_form_prepare_qc(
        &self,
        validator_data: &ValidatorData,
    ) -> Option<PrepareQc<V>> {
        self.prepare_votes
            .try_form_strong_qc(validator_data)
            .cloned()
    }

    /// `CommitQC_{slot, view}`, likewise
    pub(crate) fn try_form_commit_qc(
        &self,
        validator_data: &ValidatorData,
    ) -> Option<FallbackCommitQc<V>> {
        self.commit_votes
            .try_form_strong_qc(validator_data)
            .cloned()
    }

    pub(crate) fn store_timeout(&mut self, sender: NodeId, msg: TimeoutMsg<V>) {
        debug_assert_eq!((msg.slot(), msg.view()), (self.slot, self.view));

        if let Some(qc) = msg.high_prep_qc {
            self.prep_qcs.entry(qc.scope.1).or_insert(qc);
        }

        // last write wins: sender may update its lock when periodically timing
        // out
        self.timeout_votes.add_or_replace_vote(sender, msg.vote);
    }

    /// Whether f+1 stake has timed out here
    pub(crate) fn has_honest_timeout(&self, validator_data: &ValidatorData) -> bool {
        validator_data.sum_stake(self.timeout_votes.all_voters())
            > validator_data.total_stake().honest_threshold()
    }

    /// First write wins: every valid certificate for a view certifies the same
    /// thing
    pub(crate) fn store_tc(&mut self, tc: TimeoutCertificate<V>) {
        debug_assert_eq!((tc.slot, tc.view), (self.slot, self.view));

        self.harvested_tc.get_or_insert(tc);
    }

    pub(crate) fn harvested_tc(&self) -> Option<&TimeoutCertificate<V>> {
        self.harvested_tc.as_ref()
    }

    /// `TC_{slot, view}` once a supermajority has timed out here. The highest
    /// claim comes from the *formed* groups, which is what
    /// [`TimeoutCertificate::verify`] re-checks
    pub(crate) fn try_form_tc(
        &self,
        validator_data: &ValidatorData,
    ) -> Option<TimeoutCertificate<V>> {
        let target_stake = validator_data.total_stake().supermajority_threshold();
        let groups = self
            .timeout_votes
            .try_form_vote_groups(target_stake, validator_data)?;

        let highest_claim = groups
            .iter()
            .map(|(vote, _)| vote.high_prep_view)
            .max()
            // view 0 is the "no lock" claim: nothing was carried for it
            .filter(|view| *view != FallbackView::GENESIS);

        let high_prep_qc = highest_claim.map(|view| {
            self.prep_qcs
                .get(&view)
                .expect("a claimed view was carried by the timeout that claimed it")
                .clone()
        });

        let groups = groups
            .into_iter()
            .map(|(vote, sigcol)| (vote.clone(), sigcol))
            .collect();

        Some(TimeoutCertificate {
            slot: self.slot,
            view: self.view,
            groups,
            high_prep_qc,
        })
    }
}
