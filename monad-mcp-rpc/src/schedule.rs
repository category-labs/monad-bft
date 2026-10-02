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
//! the most tenure left. Built from the colocated validator's own config.
//! The fixed clock holds while chorus `DeadlineAgreement` proposes each
//! window's natural deadline; its "compute next deadline" TODO would break it.

use std::{fmt, sync::Arc};

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

// among the proposers at `target`, the one keeping its lane for the most
// slots from `target` on, counted up to `horizon`; ties go to the lower lane
pub fn choose_leader(
    schedule: &(impl ProposerSchedule + ?Sized),
    target: Slot,
    horizon: u64,
) -> Option<(ProposalIndex, NodeId)> {
    let set = schedule.proposers_at(target).ok()?;
    // lane order, so the survivors of the longest run start with the lowest lane
    let mut holding: Vec<(ProposalIndex, NodeId)> = set
        .iter()
        .filter_map(|(lane, proposer)| Some((lane, proposer?)))
        .collect();
    for k in 1..horizon {
        let Some(set) = target
            .0
            .checked_add(k)
            .and_then(|slot| schedule.proposers_at(Slot(slot)).ok())
        else {
            break;
        };
        let still: Vec<_> = holding
            .iter()
            .copied()
            .filter(|(lane, node)| set.proposer(*lane) == Some(*node))
            .collect();
        if still.is_empty() {
            break;
        }
        holding = still;
    }
    holding.first().copied()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Route {
    pub slot: Slot,
    pub lane: ProposalIndex,
    pub leader: NodeId,
}

pub struct Planner {
    clock: SlotClock,
    lead: TimestampDelta,
    schedule: Arc<NodeProposerSchedule>,
    horizon: u64,
}

impl fmt::Debug for Planner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Planner")
            .field("clock", &self.clock)
            .field("lead", &self.lead)
            .field("horizon", &self.horizon)
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
        })
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

    // the first slot from the target to LOOKAHEAD past it with a proposer on
    // `lane`, or on any lane by tenure, and that leader
    pub fn route(&self, now: Timestamp, lane: Option<ProposalIndex>) -> Option<Route> {
        let target = self.target_slot(now).0;
        let last = self.clock.slot_at(now + self.lead + LOOKAHEAD).0;
        (target..=last).map(Slot).find_map(|slot| {
            let (lane, leader) = match lane {
                Some(lane) => (lane, self.schedule.proposers_at(slot).ok()?.proposer(lane)?),
                None => choose_leader(self.schedule.as_ref(), slot, self.horizon)?,
            };
            Some(Route { slot, lane, leader })
        })
    }
}
