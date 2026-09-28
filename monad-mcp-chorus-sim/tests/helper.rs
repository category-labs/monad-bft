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

use monad_mcp_chorus::stub::types::{NodeId, Slot, Timestamp};
use monad_mcp_chorus_sim::CadenceSwarm;

pub fn expect_finalized<M: Clone + 'static>(
    swarm: &CadenceSwarm<M>,
    node: NodeId,
    slots: impl IntoIterator<Item = u64>,
) {
    let expected: Vec<Slot> = slots.into_iter().map(Slot).collect();
    assert_eq!(swarm.log().get_finalized_slots(node), expected);
}

pub fn expect_finalized_at<M: Clone + 'static>(
    swarm: &CadenceSwarm<M>,
    node: NodeId,
    timestamps: impl IntoIterator<Item = Timestamp>,
) {
    let expected: Vec<Timestamp> = timestamps.into_iter().collect();
    let finalized: Vec<Timestamp> = swarm.log().get_finalization_times(node);
    assert_eq!(finalized, expected);
}
