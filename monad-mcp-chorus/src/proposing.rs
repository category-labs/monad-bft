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
//! outside — facts in ([`ProposalPlanner::handle_slots_opened`],
//! [`ProposalPlanner::handle_chain_advance`]), wake requests and seal
//! commands out ([`PlannerOutput`]) — is the seam.
//!
//! The planner is deliberately not part of the consensus core: it holds no
//! consensus state, and a missed or skipped seal only costs the proposal
//! (the index finalizes empty); safety and liveness are unaffected.

use std::{
    collections::{BTreeMap, VecDeque},
    sync::Arc,
};

use tracing::{debug, info, warn};

use super::types::{NodeId, ProposalIndex, ProposerSchedule, Slot, Timestamp, TimestampDelta};

/// Timing parameters of the proposal planner. Node-local policy, not
/// protocol: a bad value costs proposals, never consensus.
#[derive(Debug, Clone)]
pub struct PlannerConfig {
    /// How long before a slot's deadline the proposal is sealed and handed
    /// to the DA layer. Must cover dissemination-plus-decode latency at a
    /// supermajority, plus clock skew; a conservative constant for now.
    pub lead: TimestampDelta,
    /// `y`: the observation cutoff — the proposal for slot `r` may only be
    /// sealed once slot `r − y` is chained. A protocol constant; must equal
    /// the proposer schedule's value (it also sets the rotation vacancy
    /// there), both mirroring the same Cadence deployment parameter.
    pub observation_cutoff: u64,
}

/// An effect requested by the planner, executed by the node wiring.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum PlannerOutput {
    /// Wake the planner ([`ProposalPlanner::handle_wake`]) for `slot` at
    /// `at`, clamped to now if `at` already passed.
    ScheduleWake { at: Timestamp, slot: Slot },
    /// All prerequisites hold: seal this node's proposal for
    /// `(slot, index)` now and submit it to the DA layer.
    Seal { slot: Slot, index: ProposalIndex },
}

struct PendingSeal {
    index: ProposalIndex,
    deadline: Timestamp,
    /// The seal alarm fired but the chaining gate was not yet satisfied;
    /// the seal is released by the next sufficient chain advance.
    gated: bool,
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

    outputs: VecDeque<PlannerOutput>,
}

impl ProposalPlanner {
    pub fn new(
        me: NodeId,
        schedule: Arc<dyn ProposerSchedule + Send + Sync>,
        config: PlannerConfig,
    ) -> Self {
        Self {
            me,
            schedule,
            config,
            chained_cap: Slot::FIRST,
            pending: BTreeMap::new(),
            outputs: VecDeque::new(),
        }
    }

    /// Fact: the conductor opened `slots` with their deadlines. Requests a
    /// seal wake at `deadline − lead` for every slot whose proposer set
    /// includes this node.
    pub fn handle_slots_opened(&mut self, slots: &BTreeMap<Slot, Timestamp>) {
        for (&slot, &deadline) in slots {
            let index = match self.schedule.proposer_index_at(slot, &self.me) {
                Ok(Some(index)) => index,
                Ok(None) => continue,
                Err(error) => {
                    warn!(%error, ?slot, "proposer schedule query failed; not proposing");
                    continue;
                }
            };

            let at = deadline.saturating_sub_delta(self.config.lead);
            self.pending.insert(
                slot,
                PendingSeal {
                    index,
                    deadline,
                    gated: false,
                },
            );
            self.outputs
                .push_back(PlannerOutput::ScheduleWake { at, slot });
        }
    }

    /// The seal alarm for `slot` fired: seal now, or hold the seal until
    /// the chaining gate clears.
    pub fn handle_wake(&mut self, now: Timestamp, slot: Slot) {
        let Some(pending) = self.pending.get(&slot) else {
            return;
        };

        if now >= pending.deadline {
            info!(
                ?slot,
                "skipping proposal: seal alarm fired past the deadline"
            );
            self.pending.remove(&slot);
            return;
        }

        if self.chained(slot) {
            self.seal(slot);
        } else {
            debug!(?slot, cap = ?self.chained_cap, "proposal gated on chaining");
            self.pending
                .get_mut(&slot)
                .expect("pending presence checked above")
                .gated = true;
        }
    }

    /// Fact: the chained prefix advanced to `cap` (exclusive). Releases
    /// seals that were gated on it and drops slots the chain already passed.
    pub fn handle_chain_advance(&mut self, now: Timestamp, cap: Slot) {
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

        let released: Vec<Slot> = self
            .pending
            .iter()
            .filter(|(slot, pending)| pending.gated && self.chained(**slot))
            .map(|(slot, _)| *slot)
            .collect();
        for slot in released {
            if now >= self.pending[&slot].deadline {
                info!(
                    ?slot,
                    "skipping proposal: chaining gate cleared past the deadline"
                );
                self.pending.remove(&slot);
            } else {
                self.seal(slot);
            }
        }
    }

    pub fn poll(&mut self) -> Option<PlannerOutput> {
        self.outputs.pop_front()
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

    fn seal(&mut self, slot: Slot) {
        let pending = self
            .pending
            .remove(&slot)
            .expect("seal is only called for pending slots");
        self.outputs.push_back(PlannerOutput::Seal {
            slot,
            index: pending.index,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::{
        super::types::{FixedProposerSchedule, NodeId},
        *,
    };

    const LEAD: TimestampDelta = TimestampDelta::from_millis(60);
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
                observation_cutoff,
            },
        )
    }

    fn deadline(slot: u64) -> Timestamp {
        Timestamp::from_millis(INTERVAL * (slot + 1))
    }

    fn open(planner: &mut ProposalPlanner, slots: impl IntoIterator<Item = u64>) {
        let slots = slots
            .into_iter()
            .map(|slot| (Slot(slot), deadline(slot)))
            .collect();
        planner.handle_slots_opened(&slots);
    }

    fn drain(planner: &mut ProposalPlanner) -> Vec<PlannerOutput> {
        std::iter::from_fn(|| planner.poll()).collect()
    }

    #[test]
    fn schedules_wakes_lead_before_the_deadline() {
        let mut planner = planner(0);
        open(&mut planner, [0, 1]);

        assert_eq!(
            drain(&mut planner),
            vec![
                PlannerOutput::ScheduleWake {
                    at: deadline(0).saturating_sub_delta(LEAD),
                    slot: Slot(0),
                },
                PlannerOutput::ScheduleWake {
                    at: deadline(1).saturating_sub_delta(LEAD),
                    slot: Slot(1),
                },
            ]
        );
    }

    #[test]
    fn lead_underflow_saturates_to_genesis() {
        let mut planner = planner(0);
        let slots = [(Slot(0), Timestamp::from_millis(10))]
            .into_iter()
            .collect();
        planner.handle_slots_opened(&slots);

        assert_eq!(
            drain(&mut planner),
            vec![PlannerOutput::ScheduleWake {
                at: Timestamp::GENESIS,
                slot: Slot(0),
            }]
        );
    }

    #[test]
    fn wake_seals_with_a_zero_observation_cutoff() {
        let mut planner = planner(0);
        open(&mut planner, [0]);
        let _ = drain(&mut planner);

        planner.handle_wake(deadline(0).saturating_sub_delta(LEAD), Slot(0));
        assert_eq!(
            drain(&mut planner),
            vec![PlannerOutput::Seal {
                slot: Slot(0),
                index: 1,
            }]
        );

        // sealing is one-shot
        planner.handle_wake(deadline(0), Slot(0));
        assert_eq!(drain(&mut planner), vec![]);
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
                observation_cutoff: 0,
            },
        );

        open(&mut planner, [0, 1, 2]);
        assert_eq!(drain(&mut planner), vec![]);
    }

    #[test]
    fn genesis_slots_are_exempt_from_the_gate() {
        let mut planner = planner(2);
        open(&mut planner, [0, 1]);
        let _ = drain(&mut planner);

        for slot in [0, 1] {
            planner.handle_wake(deadline(slot).saturating_sub_delta(LEAD), Slot(slot));
        }
        assert_eq!(
            drain(&mut planner),
            vec![
                PlannerOutput::Seal {
                    slot: Slot(0),
                    index: 1,
                },
                PlannerOutput::Seal {
                    slot: Slot(1),
                    index: 1,
                },
            ]
        );
    }

    #[test]
    fn gated_seal_is_released_by_chain_advance() {
        let mut planner = planner(2);
        open(&mut planner, [5]);
        let _ = drain(&mut planner);

        // slot 5 needs slot 3 chained: cap >= 4
        planner.handle_wake(deadline(5).saturating_sub_delta(LEAD), Slot(5));
        assert_eq!(drain(&mut planner), vec![]);

        planner.handle_chain_advance(deadline(4), Slot(3));
        assert_eq!(drain(&mut planner), vec![]);

        planner.handle_chain_advance(deadline(4), Slot(4));
        assert_eq!(
            drain(&mut planner),
            vec![PlannerOutput::Seal {
                slot: Slot(5),
                index: 1,
            }]
        );
    }

    #[test]
    fn late_gate_clearance_skips_the_proposal() {
        let mut planner = planner(2);
        open(&mut planner, [5]);
        let _ = drain(&mut planner);

        planner.handle_wake(deadline(5).saturating_sub_delta(LEAD), Slot(5));
        planner.handle_chain_advance(deadline(5), Slot(4));
        assert_eq!(drain(&mut planner), vec![]);
    }

    #[test]
    fn late_wake_skips_the_proposal() {
        let mut planner = planner(0);
        open(&mut planner, [0]);
        let _ = drain(&mut planner);

        planner.handle_wake(deadline(0), Slot(0));
        assert_eq!(drain(&mut planner), vec![]);
    }

    #[test]
    fn chain_passing_a_pending_slot_drops_it() {
        let mut planner = planner(2);
        open(&mut planner, [5]);
        let _ = drain(&mut planner);

        planner.handle_wake(deadline(5).saturating_sub_delta(LEAD), Slot(5));
        planner.handle_chain_advance(deadline(5), Slot(6));
        assert_eq!(drain(&mut planner), vec![]);

        // no resurrection by a later advance
        planner.handle_chain_advance(deadline(6), Slot(7));
        assert_eq!(drain(&mut planner), vec![]);
    }
}
