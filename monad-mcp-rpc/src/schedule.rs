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

//! Where a tx goes: the slot clock `deadline(slot) = genesis + interval · slot`,
//! a target slot one lead ahead of now, and among its proposers the one with
//! the most tenure left, or with a latency matrix the nearest one with enough.
//! Built from the colocated validator's own config.
//! The fixed clock holds while chorus `DeadlineAgreement` proposes each
//! window's natural deadline; its "compute next deadline" TODO would break it.

use std::{cmp::Reverse, collections::HashMap, fmt, sync::Arc, time::Duration};

use monad_mcp_node::{
    NodeProposerSchedule,
    chorus::types::{
        NodeId, ProposalIndex, ProposerConfig, ProposerSchedule, ScheduleError, Slot, Timestamp,
        TimestampDelta,
    },
    config::NodeConfig,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SlotClock {
    pub genesis_deadline: Timestamp,
    pub slot_interval: TimestampDelta,
}

impl SlotClock {
    pub fn new(genesis_deadline: Timestamp, slot_interval: TimestampDelta) -> Self {
        Self {
            genesis_deadline,
            slot_interval,
        }
    }

    pub fn of(node: &NodeConfig) -> Self {
        Self::new(node.genesis_deadline, node.cadence.slot_interval)
    }

    pub fn deadline(&self, slot: Slot) -> Timestamp {
        let offset = u128::from(self.slot_interval.as_nanos()) * u128::from(slot.0);
        Timestamp::from_nanos(self.genesis_deadline.as_nanos().saturating_add(offset))
    }

    // the first slot whose deadline is at or after `at`; slot 0 before genesis
    pub fn slot_at(&self, at: Timestamp) -> Slot {
        let since = at
            .as_nanos()
            .saturating_sub(self.genesis_deadline.as_nanos());
        let interval = u128::from(self.slot_interval.as_nanos()).max(1);
        Slot(u64::try_from(since.div_ceil(interval)).unwrap_or(u64::MAX))
    }
}

// propose_before_deadline + delta + margin; the margin defaults to one slot
pub fn lead(node: &NodeConfig, margin: Option<TimestampDelta>) -> TimestampDelta {
    let margin = margin.unwrap_or(node.cadence.slot_interval);
    TimestampDelta::from_nanos(
        node.proposal
            .propose_before_deadline
            .as_nanos()
            .saturating_add(node.cadence.delta.as_nanos())
            .saturating_add(margin.as_nanos()),
    )
}

// how far past the target slot a send looks for a leader
pub const LOOKAHEAD: TimestampDelta = TimestampDelta::from_millis(5_000);

// K · (y + z): the longest a proposer can hold one lane
pub fn tenure_horizon(config: &ProposerConfig) -> u64 {
    (config.concurrent_proposers as u64).saturating_mul(config.slots_per_rotation())
}

// the proposers at `target` in lane order, each with the slots it keeps its
// lane from `target` on, counted up to `horizon`
fn tenures(
    schedule: &(impl ProposerSchedule + ?Sized),
    target: Slot,
    horizon: u64,
) -> Vec<(ProposalIndex, NodeId, u64)> {
    let Ok(set) = schedule.proposers_at(target) else {
        return Vec::new();
    };
    let mut tenures: Vec<_> = set
        .iter()
        .filter_map(|(lane, proposer)| Some((lane, proposer?, 1)))
        .collect();
    for k in 1..horizon {
        let Some(set) = target
            .0
            .checked_add(k)
            .and_then(|slot| schedule.proposers_at(Slot(slot)).ok())
        else {
            break;
        };
        let mut held = false;
        for (lane, node, tenure) in &mut tenures {
            if *tenure == k && set.proposer(*lane) == Some(*node) {
                *tenure += 1;
                held = true;
            }
        }
        if !held {
            break;
        }
    }
    tenures
}

// the first of the longest tenure, so ties go to the lower lane
fn most_tenured(tenures: &[(ProposalIndex, NodeId, u64)]) -> Option<(ProposalIndex, NodeId)> {
    tenures
        .iter()
        .min_by_key(|(_, _, tenure)| Reverse(*tenure))
        .map(|&(lane, node, _)| (lane, node))
}

// among the proposers at `target`, the one keeping its lane for the most
// slots from `target` on, counted up to `horizon`; ties go to the lower lane
pub fn choose_leader(
    schedule: &(impl ProposerSchedule + ?Sized),
    target: Slot,
    horizon: u64,
) -> Option<(ProposalIndex, NodeId)> {
    most_tenured(&tenures(schedule, target, horizon))
}

// one-way latency from the rpc's validator to each validator
pub type Latency = HashMap<NodeId, Duration>;

// among the proposers at `target` other than `avoid` that keep their lane for
// at least `min_tenure` slots, the nearest; ties go to more tenure, then the
// lower lane. With none, the most tenured other than `avoid`
pub fn choose_nearest(
    schedule: &(impl ProposerSchedule + ?Sized),
    target: Slot,
    horizon: u64,
    latency: &Latency,
    min_tenure: u64,
    avoid: Option<NodeId>,
) -> Option<(ProposalIndex, NodeId)> {
    let mut candidates = tenures(schedule, target, horizon);
    candidates.retain(|(_, node, _)| Some(*node) != avoid);
    candidates
        .iter()
        .filter(|(_, _, tenure)| *tenure >= min_tenure)
        .min_by_key(|(lane, node, tenure)| {
            let latency = latency.get(node).copied().unwrap_or(Duration::MAX);
            (latency, Reverse(*tenure), *lane)
        })
        .map(|&(lane, node, _)| (lane, node))
        .or_else(|| most_tenured(&candidates))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Route {
    pub slot: Slot,
    pub lane: ProposalIndex,
    pub leader: NodeId,
    // one-way, from the latency matrix when one is loaded
    pub latency: Option<Duration>,
}

pub struct Planner {
    clock: SlotClock,
    lead: TimestampDelta,
    schedule: Arc<NodeProposerSchedule>,
    horizon: u64,
    latency: Option<Latency>,
    min_tenure: u64,
}

impl fmt::Debug for Planner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Planner")
            .field("clock", &self.clock)
            .field("lead", &self.lead)
            .field("horizon", &self.horizon)
            .field("latency", &self.latency)
            .field("min_tenure", &self.min_tenure)
            .finish_non_exhaustive()
    }
}

impl Planner {
    // the same schedule the node builds from the same config
    pub fn new(
        node: &NodeConfig,
        lead_margin: Option<TimestampDelta>,
    ) -> Result<Self, ScheduleError> {
        let schedule = node.epoch_handle()?.proposers;
        Ok(Self {
            clock: SlotClock::of(node),
            lead: lead(node, lead_margin),
            horizon: tenure_horizon(schedule.config()),
            schedule,
            latency: None,
            min_tenure: 0,
        })
    }

    // unpinned txs go to the nearest proposer keeping its lane `min_tenure` slots
    pub fn with_latency(self, latency: Latency, min_tenure: u64) -> Self {
        Self {
            latency: Some(latency),
            min_tenure,
            ..self
        }
    }

    pub fn clock(&self) -> SlotClock {
        self.clock
    }

    pub fn lead(&self) -> TimestampDelta {
        self.lead
    }

    pub fn horizon(&self) -> u64 {
        self.horizon
    }

    pub fn schedule(&self) -> &Arc<NodeProposerSchedule> {
        &self.schedule
    }

    // the slot whose deadline is the first at or after now + lead
    pub fn target_slot(&self, now: Timestamp) -> Slot {
        self.clock.slot_at(now + self.lead)
    }

    pub fn latency(&self) -> Option<&Latency> {
        self.latency.as_ref()
    }

    pub fn min_tenure(&self) -> u64 {
        self.min_tenure
    }

    // the first slot from the target to LOOKAHEAD past it with a proposer on
    // `lane`, or on any lane by tenure or latency, and that leader. With a
    // latency matrix an unpinned route skips `avoid` unless no one else leads
    pub fn route(
        &self,
        now: Timestamp,
        lane: Option<ProposalIndex>,
        avoid: Option<NodeId>,
    ) -> Option<Route> {
        let route = self.first_route(now, lane, avoid);
        match avoid {
            Some(_) if route.is_none() => self.first_route(now, lane, None),
            _ => route,
        }
    }

    fn first_route(
        &self,
        now: Timestamp,
        lane: Option<ProposalIndex>,
        avoid: Option<NodeId>,
    ) -> Option<Route> {
        let target = self.target_slot(now).0;
        let last = self.clock.slot_at(now + self.lead + LOOKAHEAD).0;
        let schedule = self.schedule.as_ref();
        (target..=last).map(Slot).find_map(|slot| {
            let (lane, leader) = match (lane, &self.latency) {
                (Some(lane), _) => (lane, schedule.proposers_at(slot).ok()?.proposer(lane)?),
                (None, Some(latency)) => choose_nearest(
                    schedule,
                    slot,
                    self.horizon,
                    latency,
                    self.min_tenure,
                    avoid,
                )?,
                (None, None) => choose_leader(schedule, slot, self.horizon)?,
            };
            let latency = self
                .latency
                .as_ref()
                .and_then(|row| row.get(&leader))
                .copied();
            Some(Route {
                slot,
                lane,
                leader,
                latency,
            })
        })
    }
}
