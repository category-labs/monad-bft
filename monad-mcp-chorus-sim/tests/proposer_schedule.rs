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

//! The stake-weighted proposer schedule driving a Chorus swarm: K = 4
//! concurrent proposal indices over non-uniform stakes, with one rotation
//! per slot and four rotations per epoch, so the run crosses the epoch
//! seams at slots 4 and 8. Every node derives the schedule independently.

mod helper;

use std::{num::NonZeroU64, sync::Arc, time::Duration};

use chorus::{
    conductor::{ConductorConfig, MonadConductor, acs::nop::NopAcs},
    slot::chorus::{Chorus, ChorusConfig, ChorusContext},
    types::{
        CreditLotterySchedule, DAHandle, NodeId, ProposerConfig, ProposerSchedule,
        RotatingProposerSchedule, Slot, SlotDeadline, Stake, Timestamp, TimestampDelta,
        ValidatorData,
    },
};
use helper::{expect_finalized, expect_finalized_at};
use monad_mcp_chorus::{spec::KeyPair as _, stub as chorus};
use monad_mcp_chorus_sim::CadenceSwarmBuilder;

const NODES: u64 = 8;
const SLOTS_PER_WINDOW: NonZeroU64 = NonZeroU64::new(4).unwrap(); // W
const SYNC_BOUNDARY_SLOTS: NonZeroU64 = NonZeroU64::new(2).unwrap(); // p; must be <= W
const SLOT_INTERVAL: u64 = 100; // tau
const LATENCY: u64 = 50; // networking latency
const DELTA: u64 = 100; // Chorus latency bound (Delta)

const CONCURRENT_PROPOSERS: usize = 4; // K
const SLOTS: u64 = 12; // three epochs of four slots
const GENESIS_DEADLINE: Timestamp = Timestamp::from_millis(100);

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

// Non-uniform stakes: the heaviest validator holds 20/90 ≈ 22% of the
// total, below the 1/K = 25% feasibility cap of the no-repeat window.
fn stakes() -> Vec<(NodeId, Stake)> {
    (0..NODES)
        .map(|id| {
            (
                NodeId::dummy(id),
                Stake::from(if id == 7 { 20 } else { 10 }),
            )
        })
        .collect()
}

fn gen_validator_data() -> ValidatorData {
    let stakes = stakes();
    let valset = stakes.iter().copied().collect();
    let mapping = stakes
        .iter()
        .map(|(id, _)| (*id, id.keypair().pubkey()))
        .collect();

    ValidatorData::new(valset, mapping)
}

fn proposer_schedule(
    val_data: &Arc<ValidatorData>,
) -> Arc<RotatingProposerSchedule<CreditLotterySchedule>> {
    let config = ProposerConfig {
        concurrent_proposers: CONCURRENT_PROPOSERS,
        blind_window: 0,
        rotation_slack: 1, // one rotation per slot
        slots_per_epoch: 4,
    };
    let algorithm = CreditLotterySchedule::recommended(&config, 0xC0FFEE);
    Arc::new(RotatingProposerSchedule::new(config, algorithm, val_data.clone()).unwrap())
}

#[test]
fn weighted_schedule_drives_the_swarm_across_epochs() {
    let mut builder = CadenceSwarmBuilder::new();
    builder.set_latency(Duration::from_millis(LATENCY));

    let val_data = Arc::new(gen_validator_data());
    // One scheduler per node: every node derives the schedule independently
    // from the shared (config, seed, stakes) inputs.
    let mut schedules = Vec::new();
    for id in 0..NODES {
        let node_id = NodeId::dummy(id);
        let schedule = proposer_schedule(&val_data);
        schedules.push(schedule.clone());

        let config = ChorusConfig {
            delta: TimestampDelta::from_millis(DELTA),
        };
        let context = ChorusContext {
            key: Arc::new(node_id.keypair()),
            validator_data: val_data.clone(),
            da_handle: Arc::new(DAHandle),
            proposers: schedule,
        };
        builder.add_node::<Chorus, _>(node_id, conductor(), config, context);
    }

    let mut swarm = builder.build();
    // Slot k's deadline is GENESIS_DEADLINE + k * tau = 100 + k * 100; the
    // fast path finalizes two latencies later.
    swarm.run_until(Timestamp::from_millis(
        200 + (SLOTS - 1) * SLOT_INTERVAL + LATENCY,
    ));

    // Liveness: every node finalizes all twelve slots, across both seams.
    for id in 0..NODES {
        expect_finalized(&swarm, NodeId::dummy(id), 0..SLOTS);
    }

    // Cross-node determinism: identical proposer sets everywhere. The
    // schedulers populated their epoch caches lazily during the run.
    for slot in (0..SLOTS).map(Slot) {
        let reference = schedules[0].proposers_at(slot).unwrap();
        for schedule in &schedules[1..] {
            assert_eq!(schedule.proposers_at(slot).unwrap(), reference);
        }
    }

    let schedule = &schedules[0];
    let leaders: Vec<NodeId> = (0..SLOTS)
        .map(|rotation| schedule.leader_of_global_rotation(rotation).unwrap())
        .collect();

    // No proposer appears twice within any K consecutive rotations,
    // including across the epoch seams at rotations 4 and 8.
    for window in leaders.windows(CONCURRENT_PROPOSERS) {
        let mut seen = window.to_vec();
        seen.sort_unstable();
        seen.dedup();
        assert_eq!(
            seen.len(),
            CONCURRENT_PROPOSERS,
            "proposer repeated in {window:?}"
        );
    }

    for slot in 0..SLOTS {
        let set = schedule.proposers_at(Slot(slot)).unwrap();

        // With one rotation per slot, slot s is rotation s: indices fill one
        // per rotation at genesis (blind_window is 0, so no rotating
        // vacancy), and rotation s's leader holds the stable index s mod K.
        assert_eq!(set.rotation(), slot);
        let occupied = set
            .iter()
            .filter(|(_, proposer)| proposer.is_some())
            .count();
        assert_eq!(occupied, (slot as usize + 1).min(CONCURRENT_PROPOSERS));

        let newest = set.proposer(slot as usize % CONCURRENT_PROPOSERS);
        assert_eq!(newest, Some(leaders[slot as usize]));

        // Membership answers the reverse direction consistently.
        for (index, proposer) in set.iter() {
            if let Some(proposer) = proposer {
                assert_eq!(
                    schedule.proposer_index_at(Slot(slot), &proposer).unwrap(),
                    Some(index)
                );
            }
        }
    }
}

#[test]
fn vacant_indices_do_not_delay_the_fast_path() {
    // K = 2 with a blind window of one slot: every rotation start except
    // the chain's first has its incoming index vacant, so half of all slots
    // run with a single proposer. A vacant index cannot carry a valid
    // proposal, so every honest node votes Negative for it immediately and
    // unanimously — no equivocation or vote split is possible there — and
    // finalization must stay on the exact fast-path schedule.
    const SLOTS_RUN: u64 = 8; // two epochs of two rotations

    let config = ProposerConfig {
        concurrent_proposers: 2,
        blind_window: 1,
        rotation_slack: 1, // two slots per rotation
        slots_per_epoch: 4,
    };

    let val_data = Arc::new(gen_validator_data());
    let algorithm = CreditLotterySchedule::recommended(&config, 0xC0FFEE);
    let schedule =
        Arc::new(RotatingProposerSchedule::new(config, algorithm, val_data.clone()).unwrap());

    // Sanity: the vacancies this test is about do occur. Rotation starts
    // (slots 2, 4, 6) run with one index vacant; all other slots — and the
    // genesis rotation, exempt from the blind window — have both occupied.
    for slot in 0..SLOTS_RUN {
        let set = schedule.proposers_at(Slot(slot)).unwrap();
        let occupied = set
            .iter()
            .filter(|(_, proposer)| proposer.is_some())
            .count();
        let rotation_start = slot.is_multiple_of(2) && slot > 0;
        // Slots 0 and 1 also run a single proposer: index 1's first
        // rotation has not started yet (genesis ramp-up).
        let expected = if rotation_start || slot < 2 { 1 } else { 2 };
        assert_eq!(occupied, expected, "slot {slot}");
    }

    let mut builder = CadenceSwarmBuilder::new();
    builder.set_latency(Duration::from_millis(LATENCY));

    for id in 0..NODES {
        let node_id = NodeId::dummy(id);
        let chorus_config = ChorusConfig {
            delta: TimestampDelta::from_millis(DELTA),
        };
        let context = ChorusContext {
            key: Arc::new(node_id.keypair()),
            validator_data: val_data.clone(),
            da_handle: Arc::new(DAHandle),
            proposers: schedule.clone(),
        };
        builder.add_node::<Chorus, _>(node_id, conductor(), chorus_config, context);
    }

    let mut swarm = builder.build();
    swarm.run_until(Timestamp::from_millis(
        200 + (SLOTS_RUN - 1) * SLOT_INTERVAL + LATENCY,
    ));

    // Every slot finalizes exactly two latencies after its deadline — the
    // same fast-path schedule as with fully occupied indices.
    for id in 0..NODES {
        let node_id = NodeId::dummy(id);
        expect_finalized(&swarm, node_id, 0..SLOTS_RUN);
        expect_finalized_at(
            &swarm,
            node_id,
            (0..SLOTS_RUN).map(|k| Timestamp::from_millis(200 + k * SLOT_INTERVAL)),
        );
    }
}
