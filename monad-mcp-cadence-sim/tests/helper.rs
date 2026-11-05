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

use monad_mcp_cadence::stub::{CadenceRuntime, CadenceWireMsg, Conductor, SlotConsensus};
use monad_mcp_cadence_sim::CadenceSwarm;
use monad_mcp_common::{Slot, Timestamp, stub::NodeId};

pub fn expect_finalized<S, C>(
    swarm: &CadenceSwarm<S, C>,
    node: NodeId,
    slots: impl IntoIterator<Item = u64>,
) where
    S: SlotConsensus + 'static,
    C: Conductor + 'static,
    C::Message: Clone,
    CadenceRuntime<S, C>: Send,
    CadenceWireMsg<S, C>: Send,
{
    let expected: Vec<Slot> = slots.into_iter().map(Slot).collect();
    assert_eq!(swarm.log().get_finalized_slots(node), expected);
}

pub fn expect_finalized_at<S, C>(
    swarm: &CadenceSwarm<S, C>,
    node: NodeId,
    timestamps: impl IntoIterator<Item = Timestamp>,
) where
    S: SlotConsensus + 'static,
    C: Conductor + 'static,
    C::Message: Clone,
    CadenceRuntime<S, C>: Send,
    CadenceWireMsg<S, C>: Send,
{
    let expected: Vec<Timestamp> = timestamps.into_iter().collect();
    let finalized: Vec<Timestamp> = swarm.log().get_finalization_times(node);
    assert_eq!(finalized, expected);
}
