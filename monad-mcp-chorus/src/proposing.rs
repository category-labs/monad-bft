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

//! The proposal-creation component: decides *when* this node seals and
//! submits its proposals.
//!
//! Proposing is driven by the slot deadline, not by eligibility: a proposal
//! only counts if it is disseminated and decoded by a supermajority before
//! the slot's deadline `D_s`, and a proposer benefits from sealing as late
//! as viable (payload economics). Everything else is a *constraint*, never
//! the trigger.
//!
//! The constraints come from the MCP specification (`main.tex`), which
//! builds the proposal of round `r` against the translated block `B_{r−y}`
//! and the executed state `state^{r−x}`, and requires that *"a party that
//! does not yet have `B_{r−y}` and `state^{r−x}` does not propose in round
//! `r`. This is what keeps consensus from running ahead of execution."*
//! Here `x` (the execution delay window) and `y ≤ x` (the observation
//! cutoff) are protocol constants — hard upper bounds on the actual lags,
//! enforced as back-pressure: a node whose chaining or execution lags the
//! bound stops proposing, costing throughput but never consensus progress.
//! The *blind window* of round `r` is the sliding window of rounds
//! `r−y+1 ..= r`, whose proposals may not be known in round `r`.
//!
//! The planner enforces the `B_{r−y}` half as a seal prerequisite — the
//! *chaining gate*: the proposal for slot `r` is sealed only once slot
//! `r − y` is *chained*, part of the contiguous finalized prefix reported
//! by the conductor (the stand-in for the translated `B_{r−y}` until
//! sequencing/translation exists as a component). The chain's first `y`
//! slots have no prefix to wait for and are exempt. The rest of the
//! blind-window knowledge a proposer needs — the in-flight proposals at
//! its own index — it has by construction: mid-phase they are its own
//! proposals, and at a phase handoff the schedule's rotation vacancy of
//! `y` slots guarantees there are none (see [`super::proposers`]); the
//! planner sees vacancy simply as "not scheduled". The `state^{r−x}` half
//! arrives with the component that owns execution progress; it slots in as
//! an additional prerequisite next to the chaining gate.
//!
//! The timing policy itself is deliberately simple for now: a fixed,
//! configurable [`PlannerConfig::lead`] before the deadline, covering
//! dissemination latency plus clock skew. Refinements (adaptive leads from
//! DA feedback, payload-economic strategies) replace the policy inside this
//! component without touching consensus: the planner's interface to the
//! outside — facts in ([`ProposalPlanner::handle_slot_open`],
//! [`ProposalPlanner::handle_cap_advance`]), the next due time and the
//! seals out ([`ProposalPlanner::next_due`], [`ProposalPlanner::poll`]) —
//! is the seam.
//!
//! The planner is deliberately not part of the consensus core: it holds no
//! consensus state, and a missed or skipped seal only costs the proposal
//! (the index finalizes empty); safety and liveness are unaffected.

use std::{collections::BTreeMap, sync::Arc};

use tracing::{info, warn};

use super::types::{NodeId, ProposalIndex, ProposerSchedule, Slot, Timestamp, TimestampDelta};

/// Timing parameters of the proposal planner. Node-local policy, not
/// protocol: a bad value costs proposals, never consensus.
#[derive(Debug, Clone)]
pub struct PlannerConfig {
    /// How long before a slot's deadline the proposal is sealed and handed
    /// to the DA layer. Must cover dissemination-plus-decode latency at a
    /// supermajority, plus clock skew; a conservative constant for now.
    pub lead: TimestampDelta,
    /// Delta: a seal this close to the deadline is withheld rather than
    /// disseminated. A proposal that cannot decode everywhere before the
    /// deadline splits the batch votes and costs the slot its fast path,
    /// where an index nobody received is agreed-negative and costs only
    /// itself.
    pub min_lead: TimestampDelta,
    /// `y`: the observation cutoff — the proposal for slot `r` may only be
    /// sealed once slot `r − y` is chained. A protocol constant; must equal
    /// the proposer schedule's value (it also sets the rotation vacancy
    /// there), both mirroring the same Cadence deployment parameter.
    pub observation_cutoff: u64,
}

struct PendingSeal {
    index: ProposalIndex,
    deadline: Timestamp,
    /// `deadline − lead`, clamped to the open time: never in the past.
    due: Timestamp,
}

/// See the module docs.
pub struct ProposalPlanner {
    me: NodeId,
    schedule: Arc<dyn ProposerSchedule + Send + Sync>,
    config: PlannerConfig,

    /// The chained prefix frontier: every slot strictly below is finalized.
    chained_cap: Slot,
    /// Slots (still) to propose for, with this node's proposal index.
    pending: BTreeMap<Slot, PendingSeal>,
}

impl ProposalPlanner {
    pub fn new(
        me: NodeId,
        schedule: Arc<dyn ProposerSchedule + Send + Sync>,
        config: PlannerConfig,
    ) -> Self {
        // a lead at or inside the withhold window means no proposal ever ships
        if config.min_lead >= config.lead {
            warn!(
                lead = ?config.lead,
                min_lead = ?config.min_lead,
                "proposal lead is within delta of the deadline; every proposal will be withheld"
            );
        }
        Self {
            me,
            schedule,
            config,
            chained_cap: Slot::FIRST,
            pending: BTreeMap::new(),
        }
    }

    /// Fact: the conductor opened `slot` with `deadline`. Registers a seal
    /// due at `deadline − lead` if this node's proposer set includes it.
    pub fn handle_slot_open(&mut self, now: Timestamp, slot: Slot, deadline: Timestamp) {
        let index = match self.schedule.proposer_index_at(slot, &self.me) {
            Ok(Some(index)) => index,
            Ok(None) => return,
            Err(error) => {
                warn!(%error, ?slot, "proposer schedule query failed; not proposing");
                return;
            }
        };

        let due = deadline.saturating_sub_delta(self.config.lead).max(now);
        self.pending.insert(
            slot,
            PendingSeal {
                index,
                deadline,
                due,
            },
        );
    }

    /// Fact: the chained prefix advanced to `cap` (exclusive). Drops slots
    /// the chain already passed; seals gated on it surface via `next_due`.
    pub fn handle_cap_advance(&mut self, now: Timestamp, cap: Slot) {
        if cap <= self.chained_cap {
            return;
        }
        self.chained_cap = cap;

        // A pending slot below the cap finalized without our proposal.
        let stale: Vec<Slot> = self.pending.range(..cap).map(|(slot, _)| *slot).collect();
        for slot in stale {
            info!(
                ?slot,
                "skipping proposal: slot finalized before it was sealed"
            );
            self.pending.remove(&slot);
        }
        self.drop_late(now);
    }

    /// When `poll` may next seal. May already be in the past (a seal
    /// released by a cap advance): the caller wakes at once.
    pub fn next_due(&self) -> Option<Timestamp> {
        self.pending
            .iter()
            .filter(|(slot, _)| self.chained(**slot))
            .map(|(_, pending)| pending.due)
            .min()
    }

    /// The lowest due slot whose chaining gate is satisfied, to seal now and
    /// submit to the DA layer. Slots within `min_lead` of their deadline are
    /// dropped first, so a seal is never disseminated too late to decode.
    pub fn poll(&mut self, now: Timestamp) -> Option<(Slot, ProposalIndex)> {
        self.drop_late(now);
        let slot = self
            .pending
            .iter()
            .find(|(slot, pending)| pending.due <= now && self.chained(**slot))
            .map(|(slot, _)| *slot)?;
        let pending = self.pending.remove(&slot).expect("found above");
        Some((slot, pending.index))
    }

    /// Whether slot `r − y` is chained, for `slot = r` and the observation
    /// cutoff `y`. The chain's first `y` slots are exempt — with `cap ≥ 0`
    /// the inequality holds for them unconditionally.
    fn chained(&self, slot: Slot) -> bool {
        self.config.observation_cutoff == 0
            || slot.get()
                < self
                    .chained_cap
                    .get()
                    .saturating_add(self.config.observation_cutoff)
    }

    // `gated`: the chaining gate, not a late alarm, is what cost the proposal
    fn drop_late(&mut self, now: Timestamp) {
        let late: Vec<(Slot, bool)> = self
            .pending
            .iter()
            .filter_map(|(slot, pending)| {
                let withhold = now >= pending.deadline.saturating_sub_delta(self.config.min_lead);
                withhold.then_some((*slot, now >= pending.deadline))
            })
            .collect();
        for (slot, passed) in late {
            let gated = !self.chained(slot);
            if passed {
                info!(
                    ?slot,
                    gated, "skipping proposal: deadline passed before the seal"
                );
            } else {
                info!(
                    ?slot,
                    gated, "withholding proposal: sealed within delta of the deadline"
                );
            }
            self.pending.remove(&slot);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        super::types::{FixedProposerSchedule, NodeId},
        *,
    };

    const LEAD: TimestampDelta = TimestampDelta::from_millis(60);
    const MIN_LEAD: TimestampDelta = TimestampDelta::from_millis(20);
    const INTERVAL: u64 = 100;

    fn planner(observation_cutoff: u64) -> ProposalPlanner {
        let me = NodeId::dummy(0);
        let other = NodeId::dummy(1);
        let schedule = Arc::new(FixedProposerSchedule::new(vec![other, me]));
        ProposalPlanner::new(
            me,
            schedule,
            PlannerConfig {
                lead: LEAD,
                min_lead: MIN_LEAD,
                observation_cutoff,
            },
        )
    }

    fn deadline(slot: u64) -> Timestamp {
        Timestamp::from_millis(INTERVAL * (slot + 1))
    }

    fn due(slot: u64) -> Timestamp {
        deadline(slot).saturating_sub_delta(LEAD)
    }

    fn before_deadline(slot: u64, millis: u64) -> Timestamp {
        deadline(slot).saturating_sub_delta(TimestampDelta::from_millis(millis))
    }

    // opened at genesis, so no due time is clamped
    fn open(planner: &mut ProposalPlanner, slots: impl IntoIterator<Item = u64>) {
        for slot in slots {
            planner.handle_slot_open(Timestamp::GENESIS, Slot(slot), deadline(slot));
        }
    }

    #[test]
    fn next_due_is_lead_before_the_earliest_deadline() {
        let mut planner = planner(0);
        open(&mut planner, [0, 1]);

        assert_eq!(planner.next_due(), Some(due(0)));
        assert_eq!(planner.poll(due(0)), Some((Slot(0), 1)));
        assert_eq!(planner.next_due(), Some(due(1)));
        assert_eq!(planner.poll(due(1)), Some((Slot(1), 1)));
        assert_eq!(planner.next_due(), None);
    }

    #[test]
    fn passed_lead_clamps_to_now() {
        let mut planner = planner(0);
        let now = Timestamp::from_millis(50);
        planner.handle_slot_open(now, Slot(0), Timestamp::from_millis(80));

        assert_eq!(planner.next_due(), Some(now));
        assert_eq!(planner.poll(now), Some((Slot(0), 1)));
    }

    #[test]
    fn poll_before_due_seals_nothing() {
        let mut planner = planner(0);
        open(&mut planner, [0]);

        assert_eq!(
            planner.poll(due(0).saturating_sub_delta(TimestampDelta::from_millis(1))),
            None
        );
        assert_eq!(planner.poll(due(0)), Some((Slot(0), 1)));
    }

    #[test]
    fn sealing_is_one_shot() {
        let mut planner = planner(0);
        open(&mut planner, [0]);

        assert_eq!(planner.poll(due(0)), Some((Slot(0), 1)));
        assert_eq!(planner.poll(due(0)), None);
        assert_eq!(planner.next_due(), None);
    }

    #[test]
    fn non_proposer_slots_are_ignored() {
        let me = NodeId::dummy(0);
        let others = vec![NodeId::dummy(1), NodeId::dummy(2)];
        let schedule = Arc::new(FixedProposerSchedule::new(others));
        let mut planner = ProposalPlanner::new(
            me,
            schedule,
            PlannerConfig {
                lead: LEAD,
                min_lead: MIN_LEAD,
                observation_cutoff: 0,
            },
        );

        open(&mut planner, [0, 1, 2]);
        assert_eq!(planner.next_due(), None);
        assert_eq!(planner.poll(deadline(2)), None);
    }

    #[test]
    fn genesis_slots_are_exempt_from_the_gate() {
        let mut planner = planner(2);
        open(&mut planner, [0, 1]);

        assert_eq!(planner.poll(due(0)), Some((Slot(0), 1)));
        assert_eq!(planner.poll(due(1)), Some((Slot(1), 1)));
    }

    #[test]
    fn gated_slot_has_no_due_time() {
        let mut planner = planner(2);
        open(&mut planner, [5]);

        assert_eq!(planner.next_due(), None);
        assert_eq!(planner.poll(due(5)), None);
    }

    #[test]
    fn gated_seal_is_released_by_cap_advance() {
        let mut planner = planner(2);
        open(&mut planner, [5]);

        // slot 5 needs slot 3 chained: cap >= 4
        assert_eq!(planner.poll(due(5)), None);

        planner.handle_cap_advance(deadline(4), Slot(3));
        assert_eq!(planner.next_due(), None);

        planner.handle_cap_advance(deadline(4), Slot(4));
        assert_eq!(planner.next_due(), Some(due(5)));
        assert_eq!(planner.poll(due(5)), Some((Slot(5), 1)));
    }

    #[test]
    fn a_seal_within_min_lead_of_the_deadline_is_withheld() {
        let mut planner = planner(0);
        open(&mut planner, [0]);

        // 19 ms of lead left: too late to decode everywhere before D
        assert_eq!(planner.poll(before_deadline(0, 19)), None);
        assert_eq!(planner.next_due(), None);
    }

    // delta is the arrival bound, so a seal exactly delta out lands on the
    // deadline itself, racing the batch vote: withheld, and one ms more ships
    #[test]
    fn a_seal_exactly_min_lead_from_the_deadline_is_withheld() {
        let mut planner = planner(0);
        open(&mut planner, [0]);

        assert_eq!(planner.poll(before_deadline(0, 20)), None);
    }

    #[test]
    fn a_seal_beyond_min_lead_is_sent() {
        let mut planner = planner(0);
        open(&mut planner, [0]);

        assert_eq!(planner.poll(before_deadline(0, 21)), Some((Slot(0), 1)));
    }

    #[test]
    fn a_gate_release_within_min_lead_is_withheld() {
        let mut planner = planner(2);
        open(&mut planner, [5]);

        assert_eq!(planner.poll(due(5)), None);

        // the cap clears the gate with less than delta before the deadline
        planner.handle_cap_advance(before_deadline(5, 10), Slot(4));
        assert_eq!(planner.next_due(), None);
        assert_eq!(planner.poll(before_deadline(5, 10)), None);
    }

    #[test]
    fn late_gate_clearance_skips_the_proposal() {
        let mut planner = planner(2);
        open(&mut planner, [5]);

        assert_eq!(planner.poll(due(5)), None);
        planner.handle_cap_advance(deadline(5), Slot(4));
        assert_eq!(planner.next_due(), None);
        assert_eq!(planner.poll(deadline(5)), None);
    }

    #[test]
    fn late_poll_skips_the_proposal() {
        let mut planner = planner(0);
        open(&mut planner, [0]);

        assert_eq!(planner.poll(deadline(0)), None);
        assert_eq!(planner.next_due(), None);
    }

    #[test]
    fn cap_passing_a_pending_slot_drops_it() {
        let mut planner = planner(2);
        open(&mut planner, [5]);

        planner.handle_cap_advance(deadline(5), Slot(6));
        assert_eq!(planner.next_due(), None);
        assert_eq!(planner.poll(due(5)), None);

        // no resurrection by a later advance
        planner.handle_cap_advance(deadline(6), Slot(7));
        assert_eq!(planner.poll(due(5)), None);
    }

    #[test]
    fn cap_advance_keeps_the_slot_at_the_cap() {
        let mut planner = planner(0);
        open(&mut planner, [4, 5]);

        planner.handle_cap_advance(Timestamp::GENESIS, Slot(5));
        assert_eq!(planner.next_due(), Some(due(5)));
        assert_eq!(planner.poll(due(5)), Some((Slot(5), 1)));
    }

    #[test]
    fn cap_advance_is_monotone() {
        let mut planner = planner(2);
        planner.handle_cap_advance(Timestamp::GENESIS, Slot(6));
        planner.handle_cap_advance(Timestamp::GENESIS, Slot(4));

        open(&mut planner, [7]);
        assert_eq!(planner.poll(due(7)), Some((Slot(7), 1)));
    }
}
