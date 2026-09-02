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

//! Exercises the Monad conductor (window management + deadline-agreement
//! driven window rollover) using the no-op ACS, which decides on its own
//! proposal without communicating. Two scenarios are tested: one with the
//! production Chorus implementation and one with a dummy slot consensus
//! implementation.

mod helper;

use std::{num::NonZeroU64, sync::Arc};

use chorus::{
    conductor::{ConductorConfig, MonadConductor, acs::nop::NopAcs},
    slot::{
        chorus::{Chorus, ChorusConfig, ChorusContext},
        dummy::{DummySlotConsensus, DummySlotConsensusConfig},
    },
    types::{
        CreditLotterySchedule, DAHandle, NodeId, ProposerConfig, RotatingProposerSchedule,
        SlotDeadline, Stake, Timestamp, TimestampDelta, ValidatorData,
    },
};
use helper::{expect_finalized, expect_finalized_at};
use monad_mcp_chorus::{spec::KeyPair as _, stub as chorus};
use monad_mcp_chorus_sim::CadenceSwarmBuilder;

const NODES: u64 = 4;
const SLOTS_PER_WINDOW: NonZeroU64 = NonZeroU64::new(4).unwrap(); // W
const SYNC_BOUNDARY_SLOTS: NonZeroU64 = NonZeroU64::new(2).unwrap(); // p; must be <= W
const SLOT_INTERVAL: u64 = 100; // tau
const LATENCY: u64 = 50; // networking latency
const DELTA: u64 = 100; // Chorus latency bound (Delta); Chorus variant only
const GENESIS_DEADLINE: Timestamp = Timestamp::from_millis(100);

// The conductor and ACS are the same across both cases; only the inner
// slot consensus changes. The no-op ACS decides on the natural (genesis
// anchored) deadline schedule, so slot k's deadline is
// GENESIS_DEADLINE + k * tau = 100 + k * 100.
type Conductor = MonadConductor<NopAcs<SlotDeadline>>;

fn conductor() -> Conductor {
    let config = ConductorConfig::new(
        SLOTS_PER_WINDOW,
        SYNC_BOUNDARY_SLOTS,
        TimestampDelta::from_millis(SLOT_INTERVAL),
        GENESIS_DEADLINE,
    )
    .unwrap();
    Conductor::genesis(config, ()).unwrap()
}

// Slot k's deadline under the natural genesis-anchored schedule:
// deadline(k) = GENESIS_DEADLINE + k * tau. The no-op ACS decides this
// schedule for every window, so it also holds across window rollovers.
fn slot_deadline(slot: u64) -> Timestamp {
    GENESIS_DEADLINE
        .checked_add_deltas(TimestampDelta::from_millis(SLOT_INTERVAL), slot)
        .unwrap()
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

fn proposer_schedule(
    val_data: &Arc<ValidatorData>,
) -> Arc<RotatingProposerSchedule<CreditLotterySchedule>> {
    let config = ProposerConfig {
        concurrent_proposers: 1,
        observation_cutoff: 0,
        rotation_slack: 1,
        slots_per_epoch: 1_000, // never crossed in this test
    };
    let algorithm = CreditLotterySchedule::recommended(&config, 0xC0FFEE);
    Arc::new(RotatingProposerSchedule::new(config, algorithm, val_data.clone()).unwrap())
}

#[test]
fn full_window_rolls_over_with_dummy_slot_consensus() {
    // Dummy: each node votes on its slot deadline and finalizes once the
    // quorum of votes arrives, i.e. one latency after the deadline.
    //   window 0 deadlines 100, 200, 300, 400 -> finalize 150, 250, 350, 450.
    //   Sync boundary p=2 crossed when slot 1 finalizes (t=250); the no-op
    //   ACS immediately decides the natural window 1 deadline schedule, so
    //   window 1 opens with deadlines 500, 600, ... and slots 4, 5 finalize
    //   at 550, 650.
    let mut builder = CadenceSwarmBuilder::new();
    builder.set_latency(TimestampDelta::from_millis(LATENCY).as_duration());

    for i in 0..NODES {
        let id = NodeId::dummy(i);
        let slot_config = DummySlotConsensusConfig {
            quorum: NODES as usize,
        };
        let key = Arc::new(id.keypair());
        builder.add_node::<DummySlotConsensus, _>(id, conductor(), slot_config, key);
    }

    let mut swarm = builder.build();
    swarm.run_until(Timestamp::from_millis(700));

    for i in 0..NODES {
        let node_id = NodeId::dummy(i);
        expect_finalized(&swarm, node_id, [0, 1, 2, 3, 4, 5]);
        // finalized(k) = deadline(k) + latency = 150, 250, 350, 450, 550, 650
        expect_finalized_at(
            &swarm,
            node_id,
            [0, 1, 2, 3, 4, 5]
                .map(|slot| slot_deadline(slot) + TimestampDelta::from_millis(LATENCY)),
        );
    }
}

#[test]
fn full_window_rolls_over_with_chorus() {
    // Production Chorus inner: the fast path finalizes two latencies after the
    // slot deadline (batch votes at +1 latency form the fast block, commit
    // votes at +2 latencies form the fast-commit QC).
    //   window 0 deadlines 100, 200, 300, 400 -> finalize 200, 300, 400, 500.
    //   Sync boundary p=2 crossed when slot 1 finalizes (t=300); the no-op
    //   ACS immediately decides the natural window 1 deadline schedule, so
    //   window 1 opens with deadlines 500, 600, ... and slots 4, 5 finalize
    //   at 600, 700.
    let mut builder = CadenceSwarmBuilder::new();
    builder.set_latency(TimestampDelta::from_millis(LATENCY).as_duration());

    let val_data = Arc::new(gen_validator_data(NODES));
    for i in 0..NODES {
        let id = NodeId::dummy(i);
        let slot_config = ChorusConfig {
            delta: TimestampDelta::from_millis(DELTA),
        };
        let context = ChorusContext {
            key: Arc::new(id.keypair()),
            validator_data: val_data.clone(),
            da_handle: Arc::new(DAHandle),
            proposers: proposer_schedule(&val_data),
        };
        builder.add_node::<Chorus, _>(id, conductor(), slot_config, context);
    }

    let mut swarm = builder.build();
    swarm.run_until(Timestamp::from_millis(750));

    for i in 0..NODES {
        let node_id = NodeId::dummy(i);
        expect_finalized(&swarm, node_id, [0, 1, 2, 3, 4, 5]);
        // finalized(k) = deadline(k) + 2 * latency = 200, 300, 400, 500, 600, 700
        expect_finalized_at(
            &swarm,
            node_id,
            [0, 1, 2, 3, 4, 5]
                .map(|slot| slot_deadline(slot) + TimestampDelta::from_millis(2 * LATENCY)),
        );
    }
}
