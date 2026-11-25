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

use monad_mcp_common::{Lane, Lanes, Slot};

use super::env::NodeId;

pub mod proposer_schedule;
pub mod schedule;

pub use proposer_schedule::ScheduleError;
pub use schedule::{
    CreditLotterySchedule, EpochAnchor, FixedProposerSchedule, LeaderSchedule,
    ProposerScheduleConfig, RotatingProposerSchedule, RoundRobinLeaderSchedule, is_proposer,
};

/// The proposers of one slot: one `Option<NodeId>` per lane
/// `j ∈ 0..K`. A `None` lane is vacant — no proposer may propose for it and
/// its proposals are empty (see the module docs for the vacancy causes).
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ProposerSet {
    slot: Slot,
    /// Global rotation containing `slot`.
    rotation: u64,
    proposers: Box<[Option<NodeId>]>,
}

impl ProposerSet {
    pub fn slot(&self) -> Slot {
        self.slot
    }

    pub fn rotation(&self) -> u64 {
        self.rotation
    }

    /// `K`: the number of lanes per slot.
    pub fn num_lanes(&self) -> usize {
        self.proposers.len()
    }

    /// The proposer at `lane`; `None` when the lane is vacant or out of
    /// range.
    pub fn proposer(&self, lane: Lane) -> Option<NodeId> {
        self.proposers.get(usize::from(lane)).copied().flatten()
    }

    /// The lane `proposer` occupies, if it is a proposer of this slot.
    pub fn lane_of(&self, proposer: &NodeId) -> Option<Lane> {
        self.iter()
            .find(|(_, held)| *held == Some(*proposer))
            .map(|(lane, _)| lane)
    }

    pub fn iter(&self) -> impl Iterator<Item = (Lane, Option<NodeId>)> + '_ {
        let lanes = (0..self.num_lanes()).map(|j| Lane::new_unchecked(j as u8));
        lanes.zip(self.proposers.iter().copied())
    }

    pub fn occupancy(&self) -> Lanes<bool> {
        Lanes::new(self.num_lanes(), |lane| self.proposer(lane).is_some())
    }
}

/// Who may propose at which lane for a given slot.
///
/// Both directions are supported: slot to proposer set, and membership of a
/// node in a slot's proposer set. Dyn-compatible on purpose — this is the
/// mock seam for consensus tests.
pub trait Election {
    /// `K`: the number of lanes per slot.
    fn num_lanes(&self) -> usize;

    /// The proposer occupying each lane at `slot`.
    fn proposers_at(&self, slot: Slot) -> Result<ProposerSet, ScheduleError>;

    /// The lane `node` holds at `slot`, if any.
    fn lane_at(&self, slot: Slot, node: &NodeId) -> Result<Option<Lane>, ScheduleError> {
        Ok(self.proposers_at(slot)?.lane_of(node))
    }
}
