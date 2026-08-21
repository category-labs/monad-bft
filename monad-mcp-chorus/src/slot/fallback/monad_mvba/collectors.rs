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

//! Where a view's messages accumulate until they form a certificate.
//!
//! One [`ViewCollectors`] per view holds everything buffered for it: the
//! proposal, the votes of each round, and the timeouts. Grouping them by view
//! is what makes garbage collection a single truncation and keeps the four
//! kinds of buffered message from drifting out of step.
//!
//! Collectors are dumb stores: they never decide anything and never look at
//! the state machine's phase. They answer "is there a certificate here yet?",
//! and the state machine asks whenever anything arrives -- which is what lets
//! a quorum that completed before its proposal arrived fire the moment the
//! proposal lands.

use std::collections::{BTreeMap, HashMap};

use super::{
    super::{
        super::types::{NodeId, Signature, SignatureCollection, Slot, ValidatorData, VotePool},
        FallbackView,
    },
    certificates::{FallbackCommitQc, PrepareQc, TimeoutCertificate},
    messages::{
        CommitVoteMsg, FallbackCommitVote, PrePrepareMsg, PrepareVote, PrepareVoteMsg, TimeoutMsg,
        TimeoutVote,
    },
};
use crate::spec::{
    Stake as _,
    validator::ValidatorData as _,
    vote::{Signature as _, SignatureCollection as _},
};

/// Everything this instance has buffered for one view.
///
/// Each round of votes gets its own [`VotePool`], which already gives
/// first-write-wins per sender and buckets votes by the value voted for, so a
/// supermajority on one value is all that can ever form. Timeouts cannot live
/// in a pool: every timeout carries its own prepare certificate, which the pool
/// has no room for, and forming a timeout certificate needs a supermajority
/// across *all* buckets rather than within one.
#[derive(Clone)]
pub(crate) struct ViewCollectors {
    slot: Slot,
    view: FallbackView,

    /// The first pre-prepare seen for the view. First write wins, so a
    /// Byzantine leader cannot displace the proposal a validator is already
    /// working on with a second one.
    pre_prepare: Option<PrePrepareMsg>,
    prepare_votes: VotePool<PrepareVote>,
    commit_votes: VotePool<FallbackCommitVote>,
    /// First timeout seen per sender; a later one from the same sender is
    /// equivocation and is ignored.
    /// FIXME: timeout can be modified to fit IsVote shape. The scope of a timeout is the slot number and fallback view number. The view number is the view that everyone is timing out on. The actual verdict itself is the high-prepared QC view number. The timeout message is that kind of a vote message plus the high QC in plain text. The high QC plain text, we should try to harvest it as soon as possible, and the rest of it is buffered and inserted into timeout. I think we can just use the same vote pool concept without a monad, without manual tracking here.
    timeouts: BTreeMap<NodeId, TimeoutMsg>,
}

impl ViewCollectors {
    pub(crate) fn new(slot: Slot, view: FallbackView) -> Self {
        Self {
            slot,
            view,
            pre_prepare: None,
            prepare_votes: VotePool::new((slot, view)),
            commit_votes: VotePool::new((slot, view)),
            timeouts: BTreeMap::new(),
        }
    }

    /// Store the view's proposal, which the caller must have validated in full.
    /// First write wins.
    pub(crate) fn store_pre_prepare(&mut self, msg: PrePrepareMsg) {
        debug_assert_eq!((msg.slot, msg.view), (self.slot, self.view));

        self.pre_prepare.get_or_insert(msg);
    }

    pub(crate) fn pre_prepare(&self) -> Option<&PrePrepareMsg> {
        self.pre_prepare.as_ref()
    }

    /// Store a prepare vote. The caller must have authenticated the sender and
    /// checked that it is in the validator set.
    pub(crate) fn add_prepare_vote(&mut self, sender: NodeId, msg: PrepareVoteMsg) {
        self.prepare_votes.add_vote(sender, msg);
    }

    /// Store a commit vote, under the same obligation on the caller.
    pub(crate) fn add_commit_vote(&mut self, sender: NodeId, msg: CommitVoteMsg) {
        self.commit_votes.add_vote(sender, msg);
    }

    /// `PrepQC_{slot, view}`, if a supermajority has voted to prepare the same
    /// entries here.
    pub(crate) fn try_form_prepare_qc(&self, validator_data: &ValidatorData) -> Option<PrepareQc> {
        self.prepare_votes.try_form_strong_qc(validator_data)
    }

    /// `CommitQC_{slot, view}`, likewise.
    pub(crate) fn try_form_commit_qc(
        &self,
        validator_data: &ValidatorData,
    ) -> Option<FallbackCommitQc> {
        self.commit_votes.try_form_strong_qc(validator_data)
    }

    /// Store a timeout whose claim has already been checked against what it
    /// carries ([`TimeoutMsg::is_valid`]).
    pub(crate) fn add_timeout(&mut self, sender: NodeId, msg: TimeoutMsg) {
        debug_assert_eq!((msg.slot(), msg.view()), (self.slot, self.view));

        if !msg.vote.signature.is_well_formed() {
            return;
        }

        // first write wins: a sender gets one timeout per view.
        self.timeouts.entry(sender).or_insert(msg);
    }

    /// Whether f+1 stake has timed out here, which obliges this validator to
    /// send its own timeout even though its timer has not fired.
    pub(crate) fn has_echo(&self, validator_data: &ValidatorData) -> bool {
        validator_data.sum_stake(self.timeouts.keys())
            > validator_data.total_stake().honest_threshold()
    }

    /// `TC_{slot, view}` once a supermajority has timed out here.
    ///
    /// Senders are grouped by the digest they signed -- the view of the
    /// prepare certificate each carries -- since only identical digests
    /// aggregate. The highest certificate any of them carried is attached, so
    /// the next leader learns the lock it is bound to; the block behind it is
    /// not here and is fetched separately.
    pub(crate) fn try_form_tc(&self, validator_data: &ValidatorData) -> Option<TimeoutCertificate> {
        let stake = validator_data.sum_stake(self.timeouts.keys());
        if stake <= validator_data.total_stake().supermajority_threshold() {
            return None;
        }

        let mut sig_groups: BTreeMap<FallbackView, HashMap<&NodeId, &Signature>> = BTreeMap::new();
        for (sender, msg) in &self.timeouts {
            sig_groups
                .entry(msg.vote.vote.high_prep_view)
                .or_default()
                .insert(sender, &msg.vote.signature);
        }

        let groups = sig_groups
            .into_iter()
            .map(|(high_prep_view, sig_map)| {
                let sigcol = SignatureCollection::aggregate(&sig_map, validator_data);
                (TimeoutVote { high_prep_view }, sigcol)
            })
            .collect();

        // the highest prepare certificate carried. `is_valid` at ingress
        // guarantees each one matches the view its sender signed, so this is
        // also the highest view any of the aggregated timeouts claims.
        let high_prep_qc = self
            .timeouts
            .values()
            .filter_map(|msg| msg.high_prep_qc.as_ref())
            .max_by_key(|qc| qc.scope.1)
            .cloned();

        Some(TimeoutCertificate {
            slot: self.slot,
            view: self.view,
            groups,
            high_prep_qc,
        })
    }
}
