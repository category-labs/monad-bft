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
    slot::chorus::{Chorus, ChorusConfig, ChorusContext},
    types::{DAHandle, NodeId, SlotDeadline, Stake, TimestampDelta, ValidatorData},
};
use helper::expect_finalized_at;
use monad_mcp_chorus::{spec::KeyPair as _, stub as chorus};
use monad_mcp_chorus_sim::CadenceSwarmBuilder;

const SLOTS_PER_WINDOW: NonZeroU64 = NonZeroU64::new(10).unwrap();
const SYNC_BOUNDARY_SLOTS: NonZeroU64 = NonZeroU64::new(8).unwrap();
const SLOT_INTERVAL: TimestampDelta = TimestampDelta::from_millis(100);
const GENESIS_DEADLINE: SlotDeadline = SlotDeadline::from_millis(100);
const LATENCY: TimestampDelta = TimestampDelta::from_millis(50); // expected networking latency (delta)
const DELTA: TimestampDelta = TimestampDelta::from_millis(100); // latency bound (Delta)

type Conductor = MonadConductor<NopAcs<SlotDeadline>>;
type DummyMsg = CadenceDriverMsg<Chorus, Conductor>;

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

fn add_node(builder: &mut CadenceSwarmBuilder<DummyMsg>, id: u64, val_data: &Arc<ValidatorData>) {
    let node_id = NodeId::dummy(id);
    let context = ChorusContext {
        node_id,
        key: Arc::new(node_id.keypair()),
        validator_data: val_data.clone(),
        da_handle: Arc::new(DAHandle),
    };
    let config = ChorusConfig {
        delta: DELTA,
        num_proposals: 1,
    };

    builder.add_node::<Chorus, _>(node_id, conductor(), config, context);
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
    builder.set_latency(LATENCY.as_duration());

    let val_data = Arc::new(gen_validator_data(NODES));
    for i in 0..NODES {
        add_node(&mut builder, i, &val_data);
    }

    let deadline_of_slot = |s: u64| {
        GENESIS_DEADLINE
            .checked_add_deltas(SLOT_INTERVAL, s)
            .unwrap()
    };

    let mut swarm = builder.build();
    let finalization_latency = LATENCY.checked_mul(2).unwrap();
    swarm.run_until(deadline_of_slot(3) + finalization_latency);

    for i in 0..NODES {
        let node_id = NodeId::dummy(i);

        expect_finalized_at(
            &swarm,
            node_id,
            [
                deadline_of_slot(0) + finalization_latency,
                deadline_of_slot(1) + finalization_latency,
                deadline_of_slot(2) + finalization_latency,
                deadline_of_slot(3) + finalization_latency,
            ],
        );
    }
}
