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

mod helper;

use std::{sync::Arc, time::Duration};

use helper::expect_finalized_at;
use monad_mcp_chorus::{
    CadenceDriverMsg,
    conductor::dummy::DummyConductor,
    slot::dummy::{DummySlotConsensus, DummySlotConsensusConfig},
    types::{NodeId, Timestamp, TimestampDelta},
};
use monad_mcp_chorus_sim::CadenceSwarmBuilder;

const DEADLINE_OFFSET: u64 = 10;
const SLOTS_PER_WINDOW: u64 = 10;
const SLOT_INTERVAL: u64 = 100;

type DummyMsg = CadenceDriverMsg<DummySlotConsensus, DummyConductor>;

fn add_dummy_node(builder: &mut CadenceSwarmBuilder<DummyMsg>, id: NodeId, quorum: usize) {
    let config = DummySlotConsensusConfig { quorum };
    let conductor = DummyConductor::new(TimestampDelta::new(SLOT_INTERVAL), SLOTS_PER_WINDOW)
        .set_deadline_offset(TimestampDelta::new(DEADLINE_OFFSET));
    let key = Arc::new(id.keypair());
    builder.add_node::<DummySlotConsensus, _>(id, conductor, config, key);
}

#[allow(clippy::erasing_op, clippy::identity_op)] // keep *0, *1 expressions
#[test]
fn all_slots_finalize_on_schedule() {
    const NODES: usize = 3;
    const LATENCY: u64 = 200;

    let mut builder = CadenceSwarmBuilder::new();
    builder.set_latency(Duration::from_millis(LATENCY));

    for i in 0..NODES {
        add_dummy_node(&mut builder, NodeId::dummy(i as u64), NODES);
    }

    let mut swarm = builder.build();
    swarm.run_until(Timestamp::new(SLOT_INTERVAL * 4 + LATENCY));

    for i in 0..NODES {
        let node_id = NodeId::dummy(i as u64);

        expect_finalized_at(
            &swarm,
            node_id,
            [
                SLOT_INTERVAL * 0 + DEADLINE_OFFSET + LATENCY,
                SLOT_INTERVAL * 1 + DEADLINE_OFFSET + LATENCY,
                SLOT_INTERVAL * 2 + DEADLINE_OFFSET + LATENCY,
                SLOT_INTERVAL * 3 + DEADLINE_OFFSET + LATENCY,
            ],
        );
    }
}
