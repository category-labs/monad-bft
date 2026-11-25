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

use cadence::{
    conductor::{CadenceConductor, ConductorConfig, acs::nop::NopAcs},
    slot::chorus::{Chorus, ChorusConfig},
};
use helper::expect_finalized_at;
use monad_mcp_cadence::stub as cadence;
use monad_mcp_cadence_sim::CadenceSwarmBuilder;
use monad_mcp_common::{
    SlotDeadline, TimestampDelta,
    stub::{NodeCredential, NodeId, ValidatorData},
};
use monad_mcp_epoch::stub::{
    EpochHandle,
    election::{CreditLotterySchedule, ProposerScheduleConfig, RotatingProposerSchedule},
};

const SLOTS_PER_WINDOW: NonZeroU64 = NonZeroU64::new(10).unwrap();
const LAG_THRESHOLD: NonZeroU64 = SLOTS_PER_WINDOW; // one window behind triggers a cap jump
const SYNC_BOUNDARY_SLOTS: NonZeroU64 = NonZeroU64::new(8).unwrap();
const SLOT_INTERVAL: TimestampDelta = TimestampDelta::from_millis(100);
const GENESIS_DEADLINE: SlotDeadline = SlotDeadline::from_millis(100);
const LATENCY: TimestampDelta = TimestampDelta::from_millis(50); // expected networking latency (delta)
const DELTA: TimestampDelta = TimestampDelta::from_millis(100); // latency bound (Delta)

type Conductor = CadenceConductor<NopAcs<SlotDeadline>>;

fn conductor(epoch_handle: &EpochHandle) -> Conductor {
    let config = ConductorConfig::new(
        SLOTS_PER_WINDOW,
        SYNC_BOUNDARY_SLOTS,
        SLOT_INTERVAL,
        GENESIS_DEADLINE,
        LAG_THRESHOLD,
    )
    .unwrap();
    CadenceConductor::genesis(config, epoch_handle.clone(), ()).unwrap()
}

// `num_validators` dummy validators, with one lane under the credit lottery
fn epoch_handle(num_validators: u64) -> EpochHandle {
    let validators = ValidatorData::dummy(num_validators);
    let config = ProposerScheduleConfig {
        concurrent_proposers: 1,
        observation_cutoff: 0,
        rotation_slack: 1,
        slots_per_epoch: 1_000, // never crossed in this test
    };
    let algorithm = CreditLotterySchedule::recommended(&config, 0xC0FFEE);
    let election = RotatingProposerSchedule::new(config, algorithm, &validators).unwrap();
    EpochHandle::new(validators, election)
}

fn add_node(
    builder: &mut CadenceSwarmBuilder<Chorus, Conductor>,
    id: u64,
    epoch_handle: &EpochHandle,
) {
    let config = ChorusConfig {
        delta: DELTA,
        credential: NodeCredential::dummy(id),
        header_auth: Arc::new(|_, _, _| false),
    };

    let conductor = conductor(epoch_handle);
    builder.add_node(NodeId::dummy(id), conductor, config, epoch_handle.clone());
}

#[test]
fn all_slots_finalize_on_schedule() {
    const NODES: u64 = 4;

    let mut builder = CadenceSwarmBuilder::new();
    builder.set_latency(LATENCY.as_duration());

    let epoch_handle = epoch_handle(NODES);
    for i in 0..NODES {
        add_node(&mut builder, i, &epoch_handle);
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
