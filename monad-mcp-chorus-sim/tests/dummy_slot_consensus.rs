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

use std::{num::NonZeroU64, sync::Arc};

use chorus::{
    CadenceDriverMsg,
    conductor::{ConductorConfig, MonadConductor, acs::nop::NopAcs},
    slot::dummy::{DummySlotConsensus, DummySlotConsensusConfig},
    types::{NodeId, SlotDeadline, TimestampDelta},
};
use helper::expect_finalized_at;
use monad_mcp_chorus::stub as chorus;
use monad_mcp_chorus_sim::CadenceSwarmBuilder;

const SLOTS_PER_WINDOW: NonZeroU64 = NonZeroU64::new(10).unwrap();
const SYNC_BOUNDARY_SLOTS: NonZeroU64 = NonZeroU64::new(8).unwrap();
const SLOT_INTERVAL: TimestampDelta = TimestampDelta::from_millis(100);
const GENESIS_DEADLINE: SlotDeadline = SlotDeadline::from_millis(100);

type Conductor = MonadConductor<NopAcs<SlotDeadline>>;

type DummyMsg = CadenceDriverMsg<DummySlotConsensus, Conductor>;

fn conductor() -> Conductor {
    let config = ConductorConfig::new(
        SLOTS_PER_WINDOW,
        SYNC_BOUNDARY_SLOTS,
        SLOT_INTERVAL,
        GENESIS_DEADLINE,
    )
    .unwrap();
    MonadConductor::genesis(config, ()).unwrap()
}

fn add_dummy_node(builder: &mut CadenceSwarmBuilder<DummyMsg>, id: NodeId, quorum: usize) {
    let config = DummySlotConsensusConfig { quorum };
    let key = Arc::new(id.keypair());
    builder.add_node::<DummySlotConsensus, _>(id, conductor(), config, key);
}

#[test]
fn all_slots_finalize_on_schedule() {
    const NODES: usize = 3;
    const LATENCY: TimestampDelta = TimestampDelta::from_millis(200);

    let mut builder = CadenceSwarmBuilder::new();
    builder.set_latency(LATENCY.as_duration());

    for i in 0..NODES {
        add_dummy_node(&mut builder, NodeId::dummy(i as u64), NODES);
    }

    let deadline_of_slot = |s: u64| {
        GENESIS_DEADLINE
            .checked_add_deltas(SLOT_INTERVAL, s)
            .unwrap()
    };

    let mut swarm = builder.build();
    swarm.run_until(deadline_of_slot(3) + LATENCY);

    for i in 0..NODES {
        let node_id = NodeId::dummy(i as u64);

        expect_finalized_at(
            &swarm,
            node_id,
            [
                deadline_of_slot(0) + LATENCY,
                deadline_of_slot(1) + LATENCY,
                deadline_of_slot(2) + LATENCY,
                deadline_of_slot(3) + LATENCY,
            ],
        );
    }
}
