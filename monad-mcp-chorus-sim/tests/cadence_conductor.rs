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

//! Exercises the Cadence conductor (window management + ACS-driven window
//! rollover) using a dummy ACS implementation. Two scenarios are tested: one
//! with the production Chorus implementation and when with a dummay slot
//! consensus implementation. Both cases run the same scenario logic
//! (`run_full_window_rollover`).

mod helper;

use std::{num::NonZeroU64, sync::Arc, time::Duration};

use chorus::{
    conductor::{
        acs::dummy::DummyAcs,
        cadence::{CadenceConductor, CadenceConductorConfig},
    },
    slot::{
        chorus::{Chorus, ChorusConfig, ChorusContext},
        dummy::{DummySlotConsensus, DummySlotConsensusConfig},
    },
    types::{DAHandle, NodeId, SlotDeadline, Stake, Timestamp, TimestampDelta, ValidatorData},
};
use helper::{expect_finalized, expect_finalized_at};
use monad_mcp_chorus::{spec::KeyPair as _, stub as chorus};
use monad_mcp_chorus_sim::CadenceSwarmBuilder;

const NODES: u64 = 4;
const SLOTS_PER_WINDOW: u64 = 4; // W
const SYNC_BOUNDARY_SLOTS: u64 = 2; // p; must be < W
const SLOT_INTERVAL: u64 = 100; // tau
const DEADLINE_OFFSET: u64 = 100; // genesis -> first slot deadline
const LATENCY: u64 = 50; // networking latency
const DELTA: u64 = 100; // Chorus latency bound (Delta); Chorus variant only

// The conductor and ACS are the same across both cases; only the inner
// slot consensus changes.
type Cadence = CadenceConductor<DummyAcs<SlotDeadline>>;

fn cadence_config() -> CadenceConductorConfig {
    CadenceConductorConfig {
        genesis: Timestamp::GENESIS,
        deadline_offset: TimestampDelta::new(DEADLINE_OFFSET),
        slot_interval: TimestampDelta::new(SLOT_INTERVAL),
        slots_per_window: NonZeroU64::new(SLOTS_PER_WINDOW).unwrap(),
        sync_boundary_slots: NonZeroU64::new(SYNC_BOUNDARY_SLOTS).unwrap(),
    }
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
fn full_window_rolls_over_with_dummy_slot_consensus() {
    // Dummy: each node votes on its slot deadline and finalizes once the
    // quorum of votes arrives, i.e. one latency after the deadline.
    //   window 0 deadlines 100, 200, 300, 400 -> finalize 150, 250, 350, 450.
    //   Sync boundary p=2 crossed when slot 1 finalizes (t=250); ACS decides
    //   the next window start one latency later (t=300); window 1 opens with
    //   deadlines 450, 550, ... so slots 4, 5 finalize at 500, 600.
    let mut builder = CadenceSwarmBuilder::new();
    builder.set_latency(Duration::from_millis(LATENCY));

    let val_data = Arc::new(gen_validator_data(NODES));
    for i in 0..NODES {
        let id = NodeId::dummy(i);
        let conductor = Cadence::new(cadence_config(), val_data.clone());
        let slot_config = DummySlotConsensusConfig {
            quorum: NODES as usize,
        };
        let key = Arc::new(id.keypair());
        builder.add_node::<DummySlotConsensus, _>(id, conductor, slot_config, key);
    }

    let mut swarm = builder.build();
    swarm.run_until(Timestamp::new(650));

    for i in 0..NODES {
        let node_id = NodeId::dummy(i);
        expect_finalized(&swarm, node_id, [0, 1, 2, 3, 4, 5]);
        expect_finalized_at(&swarm, node_id, [150, 250, 350, 450, 500, 600]);
    }
}

#[test]
fn full_window_rolls_over_with_chorus() {
    // Production Chorus inner: the fast path finalizes two latencies after the
    // slot deadline (batch votes at +1 latency form the fast block, commit
    // votes at +2 latencies form the fast-commit QC).
    //   window 0 deadlines 100, 200, 300, 400 -> finalize 200, 300, 400, 500.
    //   Sync boundary p=2 crossed when slot 1 finalizes (t=300); ACS decides
    //   the next window start one latency later (t=350); window 1 opens with
    //   deadlines 500, 600, ... so slots 4, 5 finalize at 600, 700.
    // Dummy: each node votes on its slot deadline and finalizes once the
    // quorum of votes arrives, i.e. one latency after the deadline.
    //   window 0 deadlines 100, 200, 300, 400 -> finalize 150, 250, 350, 450.
    //   Sync boundary p=2 crossed when slot 1 finalizes (t=250); ACS decides
    //   the next window start one latency later (t=300); window 1 opens with
    //   deadlines 450, 550, ... so slots 4, 5 finalize at 500, 600.
    let mut builder = CadenceSwarmBuilder::new();
    builder.set_latency(Duration::from_millis(LATENCY));

    let val_data = Arc::new(gen_validator_data(NODES));
    for i in 0..NODES {
        let id = NodeId::dummy(i);
        let conductor = Cadence::new(cadence_config(), val_data.clone());
        let slot_config = ChorusConfig {
            delta: TimestampDelta::new(DELTA),
            num_proposals: 1,
        };
        let context = ChorusContext {
            key: Arc::new(id.keypair()),
            validator_data: val_data.clone(),
            da_handle: Arc::new(DAHandle),
        };
        builder.add_node::<Chorus, _>(id, conductor, slot_config, context);
    }

    let mut swarm = builder.build();
    swarm.run_until(Timestamp::new(750));

    for i in 0..NODES {
        let node_id = NodeId::dummy(i);
        expect_finalized(&swarm, node_id, [0, 1, 2, 3, 4, 5]);
        expect_finalized_at(&swarm, node_id, [200, 300, 400, 500, 600, 700]);
    }
}
