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
//! its own lane — it has by construction: mid-phase they are its own
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
//! (the lane finalizes empty); safety and liveness are unaffected.

use std::{collections::BTreeMap, sync::Arc};

use tracing::{debug, info, trace, warn};

use super::{
    adaptive_lead::{AdaptiveConfig, LeadModel},
    slot::chorus::FinalizationPath,
    types::{
        Lane, Lanes, NodeId, ProposerSchedule, Slot, Timestamp, TimestampDelta, ValidatorData,
        ValidatorIndex, ValidatorSet,
    },
};

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
    /// where a lane nobody received is agreed-negative and costs only
    /// itself.
    pub min_lead: TimestampDelta,
    /// `y`: the observation cutoff — the proposal for slot `r` may only be
    /// sealed once slot `r − y` is chained. A protocol constant; must equal
    /// the proposer schedule's value (it also sets the rotation vacancy
    /// there), both mirroring the same Cadence deployment parameter.
    pub observation_cutoff: u64,
}

/// The adaptive lead (`docs/0005` §5), on top of [`PlannerConfig`]: the
/// static lead stays the cold-start value until enough slots have been
/// observed.
#[derive(Debug, Clone, Copy)]
pub struct AdaptiveSetup {
    pub model: AdaptiveConfig,
    /// The payload size the lead is planned for, bytes: the proposing
    /// source's own size limit. The payload is drawn only at the seal, after
    /// the due time is set.
    pub payload_bytes: usize,
    /// Slots with batch-vote arrivals before the adaptive lead replaces the
    /// static one.
    pub warmup_slots: u32,
}

impl Default for AdaptiveSetup {
    fn default() -> Self {
        Self {
            model: AdaptiveConfig::default(),
            payload_bytes: 0,
            warmup_slots: 20,
        }
    }
}

// slots below the cap whose late batch votes still count: votes arrive up
// to about D + Δ + the farthest one-way latency, a few slots after the cap
// passes a fast slot
const OBSERVATION_SLOTS: u64 = 32;

struct Adaptive {
    setup: AdaptiveSetup,
    model: LeadModel,
    validators: Arc<ValidatorData>,
    me: Option<ValidatorIndex>,
    // recent slots: deadline and who has been heard from
    slots: BTreeMap<Slot, Observed>,
    // own sealed slots, whose votes calibrate the model
    own: BTreeMap<Slot, OwnSlot>,
    // slots with at least one arrival sample
    observed_slots: u32,
}

struct Observed {
    deadline: Timestamp,
    voters: ValidatorSet,
}

// an own sealed slot: what its votes are learned against
struct OwnSlot {
    lane: Lane,
    // the lead actually used, ms
    lead: f64,
    // encode-plus-upload per validator index, ms, under the order used
    extra: Vec<f64>,
    // the first-hop send order handed to DA
    order: Vec<NodeId>,
    report: OwnSlotReport,
}

/// What the adaptive lead planned for an own slot, and what came of it: the
/// planner's observability, logged once per own slot as one structured line
/// (`adaptive lead: own slot`) once its late votes are in.
#[derive(Debug, Clone, PartialEq)]
pub struct OwnSlotReport {
    pub slot: Slot,
    pub lane: Lane,
    /// `L*` as planned at the seal, and the lead actually used (shorter when
    /// a gate released the seal late), ms
    pub planned_lead: f64,
    pub lead: f64,
    /// false: no lead up to `L_max` met the target, `L_max` was planned
    pub feasible: bool,
    /// `M` and `M + z√V` at the lead used
    pub predicted_negative: f64,
    pub predicted_bound: f64,
    /// stake of the send order's core set
    pub core_stake: f64,
    /// stake that voted no on the lane, voted yes, and sent no batch vote
    pub negative: f64,
    pub positive: f64,
    pub missing: f64,
    /// how the slot finalized, and whether the lane committed
    pub path: Option<FinalizationPath>,
    pub committed: Option<bool>,
}

// own slots are reported this many slots below the cap: by then the slot
// and its successors finalized, and the late batch votes are in
const REPORT_SLOTS: u64 = 4;

impl Adaptive {
    fn new(setup: AdaptiveSetup, validators: Arc<ValidatorData>, me: &NodeId) -> Self {
        use monad_mcp_common::spec::Stake as _;

        use crate::spec::validator::ValidatorData as _;

        let stakes: Vec<u64> = validators
            .nodes()
            .map(|node| validators.get_stake(node).amount())
            .collect();
        let me = validators.index_of(me);
        Self {
            model: LeadModel::new(setup.model, &stakes, me.map(ValidatorIndex::get)),
            setup,
            validators,
            me,
            slots: BTreeMap::new(),
            own: BTreeMap::new(),
            observed_slots: 0,
        }
    }

    fn warm(&self) -> bool {
        self.observed_slots >= self.setup.warmup_slots
    }

    // encode-plus-upload per validator for the planned payload, in the
    // current send order
    fn extra(&self) -> Vec<f64> {
        let order = self.model.send_order();
        self.model.extra_ms(&order, self.setup.payload_bytes)
    }

    // L*, once warm
    fn lead(&self, slot: Slot) -> Option<TimestampDelta> {
        if !self.warm() {
            return None;
        }
        let plan = self.model.plan(&self.extra());
        if !plan.feasible {
            warn!(
                ?slot,
                lead_ms = plan.lead,
                bound = plan.bound,
                "adaptive lead: target infeasible, using the max lead"
            );
        }
        Some(delta_of_ms(plan.lead))
    }

    fn withhold(&self, lead: TimestampDelta) -> bool {
        self.model.withhold(ms_of(lead), &self.extra())
    }

    // cold seals go out in DA's own order, which the upload term does not
    // model: only warm seals are recorded and calibrate the model
    fn sealed(&mut self, slot: Slot, lane: Lane, lead: TimestampDelta) {
        use crate::spec::validator::ValidatorData as _;

        if !self.warm() {
            return;
        }
        let (order, core_stake) = self.model.send_order_with_core();
        let extra = self.model.extra_ms(&order, self.setup.payload_bytes);
        let plan = self.model.plan(&extra);
        let (predicted_negative, predicted_bound) = self.model.bound(ms_of(lead), &extra);
        let report = OwnSlotReport {
            slot,
            lane,
            planned_lead: plan.lead,
            lead: ms_of(lead),
            feasible: plan.feasible,
            predicted_negative,
            predicted_bound,
            core_stake,
            negative: 0.0,
            positive: 0.0,
            missing: 0.0,
            path: None,
            committed: None,
        };
        let order = order
            .into_iter()
            .filter_map(ValidatorIndex::new)
            .map(|v| *self.validators.node_id(v))
            .collect();
        self.own.insert(
            slot,
            OwnSlot {
                lane,
                lead: ms_of(lead),
                extra,
                order,
                report,
            },
        );
    }

    fn batch_vote(&mut self, slot: Slot, voter: &NodeId, at: Timestamp, positive: &Lanes<bool>) {
        use crate::spec::validator::ValidatorData as _;

        let Some(voter) = self.validators.index_of(voter) else {
            return;
        };
        if Some(voter) == self.me {
            return;
        }
        let Some(observed) = self.slots.get_mut(&slot) else {
            return;
        };
        // first arrival only: the repeater re-sends
        if observed.voters.is_empty() {
            self.observed_slots = self.observed_slots.saturating_add(1);
        }
        if !observed.voters.insert(voter) {
            return;
        }
        let since = at.as_nanos() as i128 - observed.deadline.as_nanos() as i128;
        let v = voter.get();
        self.model.observe_arrival(v, since as f64 / 1e6);

        if let Some(own) = self.own.get_mut(&slot)
            && let Some(&positive) = positive.get(usize::from(own.lane))
        {
            let stake = self.model.stake(v);
            if positive {
                own.report.positive += stake;
            } else {
                own.report.negative += stake;
            }
            self.model.observe_vote(v, positive, own.lead, own.extra[v]);
        }
    }

    fn prune(&mut self, cap: Slot) {
        let keep = Slot(cap.get().saturating_sub(OBSERVATION_SLOTS));
        self.slots = self.slots.split_off(&keep);
        let kept = self
            .own
            .split_off(&Slot(cap.get().saturating_sub(REPORT_SLOTS)));
        for (_, own) in std::mem::replace(&mut self.own, kept) {
            self.report(own.report);
        }
    }
}

impl Adaptive {
    fn finalized(&mut self, slot: Slot, committed: &Lanes<bool>, path: FinalizationPath) {
        if let Some(own) = self.own.get_mut(&slot) {
            own.report.path = Some(path);
            own.report.committed = committed.get(usize::from(own.lane)).copied();
        }
    }

    fn report(&self, mut report: OwnSlotReport) {
        let others: f64 = (0..self.model.len()).map(|v| self.model.stake(v)).sum();
        report.missing = (others - report.negative - report.positive).max(0.0);
        info!(
            slot = report.slot.get(),
            lane = usize::from(report.lane),
            planned_lead_ms = report.planned_lead,
            lead_ms = report.lead,
            feasible = report.feasible,
            predicted_negative = report.predicted_negative,
            predicted_bound = report.predicted_bound,
            core_stake = report.core_stake,
            negative = report.negative,
            positive = report.positive,
            missing = report.missing,
            path = ?report.path,
            committed = ?report.committed,
            "adaptive lead: own slot"
        );
        debug!(
            slot = report.slot.get(),
            r_hat = ?(0..self.model.len()).map(|v| self.model.r_hat(v)).collect::<Vec<_>>(),
            "adaptive lead: per-validator estimates"
        );
    }
}

fn ms_of(delta: TimestampDelta) -> f64 {
    delta.as_nanos() as f64 / 1e6
}

fn delta_of_ms(ms: f64) -> TimestampDelta {
    TimestampDelta::from_nanos((ms * 1e6).round() as u64)
}

struct PendingSeal {
    lane: Lane,
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
    /// Slots (still) to propose for, with this node's lane.
    pending: BTreeMap<Slot, PendingSeal>,

    adaptive: Option<Adaptive>,
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
            adaptive: None,
        }
    }

    /// The adaptive lead over `validators`, the epoch's validator data.
    pub fn with_adaptive(mut self, setup: AdaptiveSetup, validators: Arc<ValidatorData>) -> Self {
        self.adaptive = Some(Adaptive::new(setup, validators, &self.me));
        self
    }

    /// Fact: the conductor opened `slot` with `deadline`. Registers a seal
    /// due at `deadline − lead` if this node's proposer set includes it.
    pub fn handle_slot_open(&mut self, now: Timestamp, slot: Slot, deadline: Timestamp) {
        if let Some(adaptive) = &mut self.adaptive {
            let observed = Observed {
                deadline,
                voters: ValidatorSet::new(),
            };
            adaptive.slots.insert(slot, observed);
        }
        let lane = match self.schedule.lane_at(slot, &self.me) {
            Ok(Some(lane)) => lane,
            Ok(None) => return,
            Err(error) => {
                warn!(%error, ?slot, "proposer schedule query failed; not proposing");
                return;
            }
        };

        let lead = self
            .adaptive
            .as_ref()
            .and_then(|adaptive| adaptive.lead(slot))
            .unwrap_or(self.config.lead);
        let due = deadline.saturating_sub_delta(lead).max(now);
        self.pending.insert(
            slot,
            PendingSeal {
                lane,
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
        if let Some(adaptive) = &mut self.adaptive {
            adaptive.prune(cap);
        }

        // A pending slot below the cap finalized without our proposal.
        let stale: Vec<Slot> = self.pending.range(..cap).map(|(slot, _)| *slot).collect();
        for slot in stale {
            info!(
                ?slot,
                "skipping proposal: slot finalized before it was sealed"
            );
            self.pending.remove(&slot);
        }
        self.refresh_dues(now);
        self.drop_late(now);
    }

    // Slots open a deadline-agreement window at a time, so the lead set at
    // the open can be a window old: once per cap advance, seals not yet due
    // move to the current L*.
    fn refresh_dues(&mut self, now: Timestamp) {
        let Some(adaptive) = &self.adaptive else {
            return;
        };
        let waiting = self.pending.iter().find(|(_, pending)| pending.due > now);
        let Some((slot, _)) = waiting else {
            return;
        };
        let Some(lead) = adaptive.lead(*slot) else {
            return;
        };
        for pending in self.pending.values_mut() {
            if pending.due > now {
                pending.due = pending.deadline.saturating_sub_delta(lead).max(now);
            }
        }
    }

    /// Fact: `voter`'s batch vote for `slot` arrived at `at`, with `positive`
    /// its entry per lane. Every copy is reported (the repeater re-sends);
    /// only the first is a sample. A validator that saw a fast block before
    /// its deadline sends no batch vote: missing, not negative. Only reported
    /// with the adaptive lead on.
    pub fn handle_batch_vote(
        &mut self,
        slot: Slot,
        voter: &NodeId,
        at: Timestamp,
        positive: &Lanes<bool>,
    ) {
        trace!(?slot, ?voter, ?at, ?positive, "batch vote observed");
        if let Some(adaptive) = &mut self.adaptive {
            adaptive.batch_vote(slot, voter, at, positive);
        }
    }

    /// Fact: `slot` finalized, committing the `committed` lanes over `path`.
    pub fn handle_finalized(
        &mut self,
        slot: Slot,
        committed: &Lanes<bool>,
        path: FinalizationPath,
    ) {
        trace!(?slot, ?committed, ?path, "slot finalized");
        if let Some(adaptive) = &mut self.adaptive {
            adaptive.finalized(slot, committed, path);
        }
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
    pub fn poll(&mut self, now: Timestamp) -> Option<(Slot, Lane)> {
        self.drop_late(now);
        let slot = self
            .pending
            .iter()
            .find(|(slot, pending)| pending.due <= now && self.chained(**slot))
            .map(|(slot, _)| *slot)?;
        let pending = self.pending.remove(&slot).expect("found above");
        if let Some(adaptive) = &mut self.adaptive {
            let lead = pending
                .deadline
                .duration_since(now)
                .unwrap_or(TimestampDelta::ZERO);
            adaptive.sealed(slot, pending.lane, lead);
        }
        Some((slot, pending.lane))
    }

    /// The first-hop send order for `slot`, sealed by the last `poll`: the
    /// adaptive lead's `target_far` order (`docs/0005` §5.5), once warm.
    /// None: DA's own order.
    pub fn send_order(&self, slot: Slot) -> Option<&[NodeId]> {
        let own = self.adaptive.as_ref()?.own.get(&slot)?;
        Some(&own.order)
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

    // `gated`: the chaining gate, not a late alarm, is what cost the proposal.
    // Warm, the adaptive model decides the withhold (docs/0005 §5.7) for a
    // seal that is due: whether sending with the time left risks a split.
    fn drop_late(&mut self, now: Timestamp) {
        let model = self.adaptive.as_ref().filter(|adaptive| adaptive.warm());
        let late: Vec<(Slot, bool)> = self
            .pending
            .iter()
            .filter_map(|(slot, pending)| {
                let passed = now >= pending.deadline;
                let withhold = match model {
                    _ if passed => true,
                    Some(model) => {
                        pending.due <= now && model.withhold(pending.deadline.duration_since(now)?)
                    }
                    None => now >= pending.deadline.saturating_sub_delta(self.config.min_lead),
                };
                withhold.then_some((*slot, passed))
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
        assert_eq!(
            planner.poll(due(0)),
            Some((Slot(0), Lane::new_unchecked(1)))
        );
        assert_eq!(planner.next_due(), Some(due(1)));
        assert_eq!(
            planner.poll(due(1)),
            Some((Slot(1), Lane::new_unchecked(1)))
        );
        assert_eq!(planner.next_due(), None);
    }

    #[test]
    fn passed_lead_clamps_to_now() {
        let mut planner = planner(0);
        let now = Timestamp::from_millis(50);
        planner.handle_slot_open(now, Slot(0), Timestamp::from_millis(80));

        assert_eq!(planner.next_due(), Some(now));
        assert_eq!(planner.poll(now), Some((Slot(0), Lane::new_unchecked(1))));
    }

    #[test]
    fn poll_before_due_seals_nothing() {
        let mut planner = planner(0);
        open(&mut planner, [0]);

        assert_eq!(
            planner.poll(due(0).saturating_sub_delta(TimestampDelta::from_millis(1))),
            None
        );
        assert_eq!(
            planner.poll(due(0)),
            Some((Slot(0), Lane::new_unchecked(1)))
        );
    }

    #[test]
    fn sealing_is_one_shot() {
        let mut planner = planner(0);
        open(&mut planner, [0]);

        assert_eq!(
            planner.poll(due(0)),
            Some((Slot(0), Lane::new_unchecked(1)))
        );
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

        assert_eq!(
            planner.poll(due(0)),
            Some((Slot(0), Lane::new_unchecked(1)))
        );
        assert_eq!(
            planner.poll(due(1)),
            Some((Slot(1), Lane::new_unchecked(1)))
        );
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
        assert_eq!(
            planner.poll(due(5)),
            Some((Slot(5), Lane::new_unchecked(1)))
        );
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

        assert_eq!(
            planner.poll(before_deadline(0, 21)),
            Some((Slot(0), Lane::new_unchecked(1)))
        );
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
        assert_eq!(
            planner.poll(due(5)),
            Some((Slot(5), Lane::new_unchecked(1)))
        );
    }

    #[test]
    fn cap_advance_is_monotone() {
        let mut planner = planner(2);
        planner.handle_cap_advance(Timestamp::GENESIS, Slot(6));
        planner.handle_cap_advance(Timestamp::GENESIS, Slot(4));

        open(&mut planner, [7]);
        assert_eq!(
            planner.poll(due(7)),
            Some((Slot(7), Lane::new_unchecked(1)))
        );
    }

    mod adaptive {
        use monad_mcp_common::spec::KeyPair as _;

        use super::{super::super::types::Stake, *};

        const VALIDATORS: u64 = 4;
        // every peer's batch vote arrives this long after the deadline
        const ARRIVAL_MS: u64 = 10;

        fn validators() -> Arc<ValidatorData> {
            let valset = (0..VALIDATORS)
                .map(|id| (NodeId::dummy(id), Stake::from(1)))
                .collect();
            let mapping = (0..VALIDATORS)
                .map(|id| (NodeId::dummy(id), NodeId::dummy(id).keypair().pubkey()))
                .collect();
            Arc::new(ValidatorData::new(valset, mapping))
        }

        // validator 0 holds lane 1 of every slot
        fn adaptive_planner(min_lead: TimestampDelta) -> ProposalPlanner {
            let me = NodeId::dummy(0);
            let schedule = Arc::new(FixedProposerSchedule::new(vec![NodeId::dummy(1), me]));
            let config = PlannerConfig {
                lead: LEAD,
                min_lead,
                observation_cutoff: 0,
            };
            let setup = AdaptiveSetup {
                payload_bytes: 1_000,
                ..AdaptiveSetup::default()
            };
            ProposalPlanner::new(me, schedule, config).with_adaptive(setup, validators())
        }

        fn vote(planner: &mut ProposalPlanner, slot: u64, voter: u64, after_ms: u64) {
            let at = deadline(slot)
                .checked_add_delta(TimestampDelta::from_millis(after_ms))
                .unwrap();
            let positive = Lanes::new(2, |_| true);
            planner.handle_batch_vote(Slot(slot), &NodeId::dummy(voter), at, &positive);
        }

        // `slots` slots observed, every peer heard from in each
        fn warm_up(planner: &mut ProposalPlanner, slots: u64) {
            for slot in 0..slots {
                open(planner, [slot]);
                for voter in 1..VALIDATORS {
                    vote(planner, slot, voter, ARRIVAL_MS);
                }
            }
            // the warm-up slots were ours too: past their deadlines now
            let cap = Slot(slots);
            planner.handle_cap_advance(deadline(slots), cap);
        }

        fn adaptive_lead(planner: &ProposalPlanner) -> TimestampDelta {
            let adaptive = planner.adaptive.as_ref().unwrap();
            adaptive.lead(Slot(0)).expect("warm")
        }

        #[test]
        fn the_static_lead_holds_until_warm() {
            let mut planner = adaptive_planner(MIN_LEAD);
            warm_up(&mut planner, 19);
            open(&mut planner, [30]);
            assert_eq!(planner.next_due(), Some(due(30)));
        }

        #[test]
        fn warm_arrivals_replace_the_static_lead() {
            let mut planner = adaptive_planner(MIN_LEAD);
            warm_up(&mut planner, 20);
            let lead = adaptive_lead(&planner);
            // 10 ms away with a 5 ms spread, a 1 kB payload: well inside 60 ms,
            // down to the 20 ms clamp (one peer is θ = 1/4 of the stake)
            assert!(lead >= TimestampDelta::from_millis(20), "{lead:?}");
            assert!(lead < LEAD, "{lead:?}");

            open(&mut planner, [30]);
            let due = deadline(30).saturating_sub_delta(lead);
            assert_eq!(planner.next_due(), Some(due));
            assert_eq!(planner.poll(due), Some((Slot(30), Lane::new_unchecked(1))));
        }

        #[test]
        fn a_warm_seal_carries_the_target_far_order() {
            let mut planner = adaptive_planner(MIN_LEAD);
            open(&mut planner, [0]);
            assert!(planner.poll(due(0)).is_some());
            assert_eq!(planner.send_order(Slot(0)), None, "cold: DA's own order");

            warm_up(&mut planner, 20);
            // validator 2 is now farther away than 1 and 3 (before the open,
            // which fixes the due time)
            for voter in 1..VALIDATORS {
                let after = if voter == 2 { 60 } else { ARRIVAL_MS };
                for _ in 0..60 {
                    let adaptive = planner.adaptive.as_mut().unwrap();
                    adaptive.model.observe_arrival(voter as usize, after as f64);
                }
            }
            open(&mut planner, [30]);
            let due = planner.next_due().unwrap();
            assert!(planner.poll(due).is_some());
            let order = planner.send_order(Slot(30)).expect("warm");
            // the core set farthest first; everyone but ourselves
            assert_eq!(order.len(), 3);
            assert_eq!(order[0], NodeId::dummy(2));
            assert!(!order.contains(&NodeId::dummy(0)));
        }

        #[test]
        fn a_cap_advance_moves_a_waiting_seal_to_the_current_lead() {
            let mut planner = adaptive_planner(MIN_LEAD);
            warm_up(&mut planner, 20);
            open(&mut planner, [40]);
            let opened_due = planner.next_due().unwrap();

            // every peer moves 30 ms farther away before slot 40 is due
            for voter in 1..VALIDATORS {
                for _ in 0..200 {
                    let adaptive = planner.adaptive.as_mut().unwrap();
                    adaptive.model.observe_arrival(voter as usize, 40.0);
                }
            }
            planner.handle_cap_advance(deadline(21), Slot(21));
            let due = planner.next_due().unwrap();
            assert!(due < opened_due, "{due:?} vs {opened_due:?}");
            assert_eq!(
                due,
                deadline(40).saturating_sub_delta(adaptive_lead(&planner))
            );
        }

        #[test]
        fn only_the_first_copy_of_a_vote_is_a_sample() {
            let mut planner = adaptive_planner(MIN_LEAD);
            open(&mut planner, [0]);
            vote(&mut planner, 0, 1, ARRIVAL_MS);
            vote(&mut planner, 0, 1, 90);
            let model = &planner.adaptive.as_ref().unwrap().model;
            assert_eq!(model.r_hat(1), Some(ARRIVAL_MS as f64));
        }

        #[test]
        fn votes_on_the_own_lane_calibrate_the_model() {
            let mut planner = adaptive_planner(MIN_LEAD);
            warm_up(&mut planner, 20);
            open(&mut planner, [30]);
            let due = planner.next_due().unwrap();
            assert!(planner.poll(due).is_some());

            // validator 3 votes no on our lane, despite its early arrival
            let at = deadline(30)
                .checked_add_delta(TimestampDelta::from_millis(ARRIVAL_MS))
                .unwrap();
            let no_on_ours = Lanes::new(2, |lane| usize::from(lane) == 0);
            planner.handle_batch_vote(Slot(30), &NodeId::dummy(3), at, &no_on_ours);
            vote(&mut planner, 30, 1, ARRIVAL_MS);

            let model = &planner.adaptive.as_ref().unwrap().model;
            let r_3 = model.r_hat(3).unwrap();
            let r_1 = model.r_hat(1).unwrap();
            assert!(r_3 > r_1 + 1.0, "{r_3} vs {r_1}");
        }

        #[test]
        fn an_own_slot_report_splits_no_from_missing() {
            let mut planner = adaptive_planner(MIN_LEAD);
            warm_up(&mut planner, 20);
            open(&mut planner, [30]);
            let due = planner.next_due().unwrap();
            assert!(planner.poll(due).is_some());

            let at = deadline(30)
                .checked_add_delta(TimestampDelta::from_millis(ARRIVAL_MS))
                .unwrap();
            let no_on_ours = Lanes::new(2, |lane| usize::from(lane) == 0);
            planner.handle_batch_vote(Slot(30), &NodeId::dummy(3), at, &no_on_ours);
            vote(&mut planner, 30, 1, ARRIVAL_MS);
            // validator 2 saw a fast block first: no batch vote
            let committed = Lanes::new(2, |_| true);
            planner.handle_finalized(Slot(30), &committed, FinalizationPath::Fast);

            let adaptive = planner.adaptive.as_ref().unwrap();
            let report = &adaptive.own[&Slot(30)].report;
            assert_eq!(report.negative, 0.25);
            assert_eq!(report.positive, 0.25);
            assert_eq!(report.path, Some(FinalizationPath::Fast));
            assert_eq!(report.committed, Some(true));
            assert!(report.feasible);
            assert!((report.lead - report.planned_lead).abs() < 1e-6);
            assert!(report.core_stake > 0.0);

            // reported and dropped once the cap is far enough past
            planner.handle_cap_advance(deadline(35), Slot(35));
            assert!(planner.adaptive.as_ref().unwrap().own.is_empty());
        }

        // docs/0005 §5.7: a fixed window above the needed lead blocks every
        // proposal; the model sends
        #[test]
        fn the_model_replaces_the_fixed_withhold_window() {
            let mut planner = adaptive_planner(TimestampDelta::from_millis(50));
            warm_up(&mut planner, 20);
            open(&mut planner, [30]);
            let due = planner.next_due().unwrap();
            assert!(deadline(30).duration_since(due).unwrap() < TimestampDelta::from_millis(50));
            assert_eq!(planner.poll(due), Some((Slot(30), Lane::new_unchecked(1))));
        }

        #[test]
        fn the_model_withholds_a_seal_too_late_to_arrive() {
            let mut planner = adaptive_planner(TimestampDelta::ZERO);
            warm_up(&mut planner, 20);
            open(&mut planner, [30]);
            // 12 ms left for validators 10 ms away with a 5 ms spread
            assert_eq!(planner.poll(before_deadline(30, 12)), None);
            assert_eq!(planner.next_due(), None);
        }
    }
}
