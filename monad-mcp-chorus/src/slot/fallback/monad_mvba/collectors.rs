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

//! Where votes and timeouts accumulate until they form a certificate.
//!
//! Collectors are dumb stores: they never decide anything and never look at
//! the state machine's phase. They answer "is there a certificate here yet?",
//! and the state machine asks whenever anything arrives -- which is what lets
//! a quorum that completed before its proposal arrived fire the moment the
//! proposal lands.

use std::collections::{BTreeMap, HashMap};

use super::{
    super::{
        super::types::{
            IsVote, NodeId, Signature, SignatureCollection, Slot, StrongQc, ValidatorData, VoteMsg,
            VotePool,
        },
        FallbackView,
    },
    certificates::TimeoutCertificate,
    messages::{TimeoutMsg, TimeoutVote},
};
use crate::spec::{
    Stake as _,
    validator::ValidatorData as _,
    vote::{Signature as _, SignatureCollection as _},
};

/// Per-view pools for one kind of vote (prepare or commit).
///
/// Each view gets its own [`VotePool`], which already gives first-write-wins
/// per sender and buckets votes by the value voted for, so a supermajority on
/// one value is all that can ever form.
#[derive(Clone)]
pub(crate) struct VoteCollector<V>
where
    V: IsVote<Scope = (Slot, FallbackView)>,
{
    slot: Slot,
    pools: BTreeMap<FallbackView, VotePool<V>>,
}

impl<V> VoteCollector<V>
where
    V: IsVote<Scope = (Slot, FallbackView)>,
{
    pub(crate) fn new(slot: Slot) -> Self {
        Self {
            slot,
            pools: BTreeMap::new(),
        }
    }

    /// Store a vote. The caller must have authenticated the sender and checked
    /// that it is in the validator set.
    pub(crate) fn add(&mut self, sender: NodeId, msg: VoteMsg<V>) {
        let (slot, view) = msg.scope;
        debug_assert_eq!(slot, self.slot);

        self.pools
            .entry(view)
            .or_insert_with(|| VotePool::new((slot, view)))
            .add_vote(sender, msg);
    }

    /// The certificate for `view`, if a supermajority has voted for the same
    /// value there.
    pub(crate) fn try_form_qc(
        &self,
        view: FallbackView,
        validator_data: &ValidatorData,
    ) -> Option<StrongQc<V>> {
        self.pools.get(&view)?.try_form_strong_qc(validator_data)
    }

    /// Drop everything below `view`: state for views below the current one can
    /// no longer contribute a certificate this instance would act on.
    pub(crate) fn gc_below(&mut self, view: FallbackView) {
        self.pools = self.pools.split_off(&view);
    }
}

/// Per-view timeouts.
///
/// A [`VotePool`] cannot hold these: every timeout carries its own prepare
/// certificate and metablock, which the pool has no room for, and forming a
/// timeout certificate needs a supermajority across *all* buckets rather than
/// within one.
#[derive(Clone)]
pub(crate) struct TimeoutCollector {
    slot: Slot,
    /// First timeout seen per sender per view; a later one from the same
    /// sender is equivocation and is ignored.
    views: BTreeMap<FallbackView, BTreeMap<NodeId, TimeoutMsg>>,
}

impl TimeoutCollector {
    pub(crate) fn new(slot: Slot) -> Self {
        Self {
            slot,
            views: BTreeMap::new(),
        }
    }

    /// Store a timeout whose claim has already been checked against what it
    /// carries ([`TimeoutMsg::is_valid`]).
    pub(crate) fn add(&mut self, sender: NodeId, msg: TimeoutMsg) {
        debug_assert_eq!(msg.slot(), self.slot);

        if !msg.vote.signature.is_well_formed() {
            return;
        }

        self.views
            .entry(msg.view())
            .or_default()
            // first write wins: a sender gets one timeout per view.
            .entry(sender)
            .or_insert(msg);
    }

    /// Whether f+1 stake has timed out in `view`, which obliges this validator
    /// to send its own timeout even though its timer has not fired.
    pub(crate) fn has_echo(&self, view: FallbackView, validator_data: &ValidatorData) -> bool {
        let Some(timeouts) = self.views.get(&view) else {
            return false;
        };

        validator_data.sum_stake(timeouts.keys()) > validator_data.total_stake().honest_threshold()
    }

    /// `TC_{slot, view}` once a supermajority has timed out in `view`.
    ///
    /// Senders are grouped by the digest they signed -- the view of the
    /// prepare certificate each carries -- since only identical digests
    /// aggregate. The highest certificate any of them carried is attached,
    /// together with the metablock it locks, so the next leader can honour the
    /// lock without fetching the block from a signer.
    pub(crate) fn try_form_tc(
        &self,
        view: FallbackView,
        validator_data: &ValidatorData,
    ) -> Option<TimeoutCertificate> {
        let timeouts = self.views.get(&view)?;

        let stake = validator_data.sum_stake(timeouts.keys());
        if stake <= validator_data.total_stake().supermajority_threshold() {
            return None;
        }

        let mut sig_groups: BTreeMap<FallbackView, HashMap<&NodeId, &Signature>> = BTreeMap::new();
        for (sender, msg) in timeouts {
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

        // the highest prepare certificate carried, together with the block
        // that goes with it. `is_valid` at ingress guarantees the two agree.
        let high_prepare = timeouts
            .values()
            .filter_map(|msg| msg.high_prepare.as_ref())
            .max_by_key(|high| high.qc.scope.1)
            .cloned();

        Some(TimeoutCertificate {
            slot: self.slot,
            view,
            groups,
            high_prepare,
        })
    }

    pub(crate) fn gc_below(&mut self, view: FallbackView) {
        self.views = self.views.split_off(&view);
    }
}
