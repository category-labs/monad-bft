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

use chorus::{
    CadenceDriverMsg,
    conductor::dummy::DummyConductor,
    slot::chorus::{Chorus, ChorusConfig, ChorusContext},
    types::{DAHandle, NodeId, Stake, Timestamp, TimestampDelta, ValidatorData},
};
use helper::expect_finalized_at;
use monad_mcp_chorus::{spec::KeyPair as _, stub as chorus};
use monad_mcp_chorus_sim::CadenceSwarmBuilder;

const DEADLINE_OFFSET: u64 = 100; // should be >= Delta
const SLOTS_PER_WINDOW: u64 = 10;
const SLOT_INTERVAL: u64 = 100;
const LATENCY: u64 = 50; // expected networking latency (delta)
const DELTA: u64 = 100; // latency bound (Delta)

type DummyMsg = CadenceDriverMsg<Chorus, DummyConductor>;

fn add_node(builder: &mut CadenceSwarmBuilder<DummyMsg>, id: u64, val_data: &Arc<ValidatorData>) {
    let node_id = NodeId::dummy(id);
    let conductor = DummyConductor::new(TimestampDelta::new(SLOT_INTERVAL), SLOTS_PER_WINDOW)
        .set_deadline_offset(TimestampDelta::new(DEADLINE_OFFSET));
    let context = ChorusContext {
        key: Arc::new(node_id.keypair()),
        validator_data: val_data.clone(),
        da_handle: Arc::new(DAHandle),
    };
    let config = ChorusConfig {
        delta: TimestampDelta::new(DELTA),
        num_proposals: 1,
    };

    builder.add_node::<Chorus, _>(node_id, conductor, config, context);
}

fn gen_validator_data(n: u64) -> ValidatorData {
    let validators = (0..n).map(NodeId::dummy).collect::<Vec<_>>();
    let valset = validators.iter().map(|id| (*id, Stake::from(1))).collect();
    let mapping = validators
        .iter()
        .map(|id| (*id, id.keypair().pubkey()))
        .collect();

    ValidatorData::new(valset, mapping)
}

#[test]
fn all_slots_finalize_on_schedule() {
    const NODES: u64 = 4;

    let mut builder = CadenceSwarmBuilder::new();
    builder.set_latency(Duration::from_millis(LATENCY));

    let val_data = Arc::new(gen_validator_data(NODES));
    for i in 0..NODES {
        add_node(&mut builder, i, &val_data);
    }

    let mut swarm = builder.build();
    swarm.run_until(Timestamp::new(
        SLOT_INTERVAL * 3 + DEADLINE_OFFSET + LATENCY * 2,
    ));

    let deadline_of_slot = |s: u64| SLOT_INTERVAL * s + DEADLINE_OFFSET;

    for i in 0..NODES {
        let node_id = NodeId::dummy(i);

        expect_finalized_at(
            &swarm,
            node_id,
            [
                deadline_of_slot(0) + LATENCY * 2,
                deadline_of_slot(1) + LATENCY * 2,
                deadline_of_slot(2) + LATENCY * 2,
                deadline_of_slot(3) + LATENCY * 2,
            ],
        );
    }
}
