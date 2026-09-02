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

//! End-to-end proposal flow: the proposal planner seals `lead` before each
//! slot deadline, the mock DA layer disseminates announcements over the
//! simulated network, and the finalized slots carry the scheduled
//! proposers' entries — `Positive` with the proposer's payload root where
//! the schedule assigns a proposer, `Negative` where the index is vacant.

mod helper;

use std::{cell::RefCell, collections::BTreeMap, num::NonZeroU64, rc::Rc, sync::Arc};

use chorus::{
    CadenceDriverMsg,
    conductor::{ConductorConfig, MonadConductor, acs::nop::NopAcs},
    proposing::{PlannerConfig, ProposalPlanner},
    slot::chorus::{Chorus, ChorusConfig, ChorusContext, Entry, SlotFinalization},
    types::{
        NodeId, ProposerConfig, ProposerSchedule, RotatingProposerSchedule,
        RoundRobinLeaderSchedule, Slot, SlotDeadline, Stake, Timestamp, TimestampDelta,
        ValidatorData,
    },
};
use helper::{expect_finalized, expect_finalized_at};
use monad_mcp_chorus::{spec::KeyPair as _, stub as chorus};
use monad_mcp_chorus_sim::{CadenceSwarm, CadenceSwarmBuilder, MockDa, mock_payload, mock_root};

const NODES: u64 = 4;
const SLOTS_PER_WINDOW: NonZeroU64 = NonZeroU64::new(10).unwrap();
const SYNC_BOUNDARY_SLOTS: NonZeroU64 = NonZeroU64::new(8).unwrap();
const SLOT_INTERVAL: TimestampDelta = TimestampDelta::from_millis(100);
const GENESIS_DEADLINE: SlotDeadline = SlotDeadline::from_millis(100);
const LATENCY: TimestampDelta = TimestampDelta::from_millis(50); // networking latency
const DELTA: TimestampDelta = TimestampDelta::from_millis(100); // Chorus latency bound (Delta)
// Seal lead: must cover the announcement's dissemination (one latency) with
// margin; small enough that the chaining gate is really exercised (see the
// staggered-rotation test).
const LEAD: TimestampDelta = TimestampDelta::from_millis(60);

type Conductor = MonadConductor<NopAcs<SlotDeadline>>;
type Msg = CadenceDriverMsg<Chorus, Conductor>;
type Schedule = RotatingProposerSchedule<RoundRobinLeaderSchedule>;

// Per-node record of the finalized entries: (node, slot) -> entries by
// proposal index.
type EntriesLog = Rc<RefCell<BTreeMap<(NodeId, Slot), Vec<Entry>>>>;

fn conductor() -> Conductor {
    let config = ConductorConfig::new(
        SLOTS_PER_WINDOW,
        SYNC_BOUNDARY_SLOTS,
        SLOT_INTERVAL,
        GENESIS_DEADLINE,
    )
    .unwrap();
    Conductor::genesis(config, ()).unwrap()
}

fn deadline_of_slot(slot: u64) -> Timestamp {
    GENESIS_DEADLINE
        .checked_add_deltas(SLOT_INTERVAL, slot)
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

// One shared schedule instance for the whole swarm: the schedule is
// deterministic, so per-node instances would agree anyway, but sharing is
// what production wiring will do.
fn round_robin_schedule(config: ProposerConfig, val_data: &Arc<ValidatorData>) -> Arc<Schedule> {
    let algorithm = RoundRobinLeaderSchedule::new(&config);
    Arc::new(RotatingProposerSchedule::new(config, algorithm, val_data.clone()).unwrap())
}

fn build_swarm(
    proposer_config: &ProposerConfig,
    schedule: &Arc<Schedule>,
    val_data: &Arc<ValidatorData>,
) -> (CadenceSwarm<Msg>, EntriesLog) {
    let entries: EntriesLog = Rc::default();

    let mut builder = CadenceSwarmBuilder::new();
    builder.set_latency(LATENCY.as_duration());

    for i in 0..NODES {
        let id = NodeId::dummy(i);
        let da = Arc::new(MockDa::new());
        let context = ChorusContext {
            key: Arc::new(id.keypair()),
            validator_data: val_data.clone(),
            da_handle: da.clone(),
            proposers: schedule.clone(),
        };
        let planner = ProposalPlanner::new(
            id,
            schedule.clone(),
            PlannerConfig {
                lead: LEAD,
                observation_cutoff: proposer_config.observation_cutoff,
            },
        );
        let log = entries.clone();
        let observer = move |_now: Timestamp, slot: Slot, data: &SlotFinalization| {
            let SlotFinalization::Fast(qc) = data;
            let slot_entries: Vec<Entry> = qc.verdict.entries.clone().into_iter().collect();
            let previous = log.borrow_mut().insert((id, slot), slot_entries);
            assert!(previous.is_none(), "slot finalized twice");
        };

        builder.add_proposer_node::<Chorus, _, _>(
            id,
            conductor(),
            ChorusConfig { delta: DELTA },
            context,
            planner,
            da,
            observer,
        );
    }

    (builder.build(), entries)
}

// The entry expected at (slot, index): the scheduled proposer's payload
// root, or Negative for a vacant index.
fn expected_entry(schedule: &Schedule, slot: Slot, index: usize) -> Entry {
    match schedule.proposers_at(slot).unwrap().proposer(index) {
        Some(proposer) => Entry::Positive {
            root: mock_root(&mock_payload(proposer, slot, index)),
        },
        None => Entry::Negative,
    }
}

fn expect_entries(
    entries: &EntriesLog,
    schedule: &Schedule,
    slots: impl IntoIterator<Item = u64> + Clone,
) {
    let entries = entries.borrow();
    for node in (0..NODES).map(NodeId::dummy) {
        for slot in slots.clone() {
            let slot = Slot(slot);
            let finalized = &entries[&(node, slot)];
            let expected: Vec<Entry> = (0..finalized.len())
                .map(|index| expected_entry(schedule, slot, index))
                .collect();
            assert_eq!(
                finalized, &expected,
                "unexpected entries at {slot:?} on {node:?}"
            );
        }
    }
}

#[test]
fn scheduled_proposals_finalize_positive() {
    // K = 1, observation cutoff 0: every slot has exactly one proposer, rotating
    // every slot. All entries finalize Positive with the scheduled
    // proposer's payload root, on the same schedule as the proposal-less
    // runs (finalization at deadline + 2 latencies).
    let proposer_config = ProposerConfig {
        concurrent_proposers: 1,
        observation_cutoff: 0,
        rotation_slack: 1,
        slots_per_epoch: 1_000, // never crossed in this test
    };
    let val_data = Arc::new(gen_validator_data(NODES));
    let schedule = round_robin_schedule(proposer_config.clone(), &val_data);

    // round robin, one-slot rotations: slot s is proposed by validator
    // s mod NODES
    for slot in 0..8 {
        assert_eq!(
            schedule.proposers_at(Slot(slot)).unwrap().proposer(0),
            Some(NodeId::dummy(slot % NODES)),
        );
    }

    let (mut swarm, entries) = build_swarm(&proposer_config, &schedule, &val_data);
    let finalization_latency = LATENCY.checked_mul(2).unwrap();
    swarm.run_until(deadline_of_slot(3) + finalization_latency);

    for i in 0..NODES {
        let node_id = NodeId::dummy(i);
        expect_finalized(&swarm, node_id, 0..4);
        expect_finalized_at(
            &swarm,
            node_id,
            (0..4).map(|slot| deadline_of_slot(slot) + finalization_latency),
        );
    }
    expect_entries(&entries, &schedule, 0..4);

    // and they really are Positive, not vacuously equal
    for slot in 0..4 {
        assert!(matches!(
            expected_entry(&schedule, Slot(slot), 0),
            Entry::Positive { .. }
        ));
    }
}

#[test]
fn staggered_proposers_with_rotation_vacancy() {
    // K = 2 staggered proposer phases with observation cutoff y = 2 and
    // rotation slack z = 2 (rotations y + z = 4 slots apart). Exercises,
    // across a window rollover:
    //  * the genesis exemption (slots 0, 1 sealed without a chained prefix
    //    to wait for),
    //  * rotation vacancies (a handed-over index is vacant for y slots, so
    //    the incoming proposer knows all in-flight proposals at its index),
    //  * the planner's chaining gate: sealing slot r at deadline − lead
    //    requires slot r − y chained, which happens 40ms before the seal
    //    alarm — late enough that early sealing would vote Negative.
    let proposer_config = ProposerConfig {
        concurrent_proposers: 2,
        observation_cutoff: 2,
        rotation_slack: 2,
        slots_per_epoch: 1_000, // never crossed in this test
    };
    let val_data = Arc::new(gen_validator_data(NODES));
    let schedule = round_robin_schedule(proposer_config.clone(), &val_data);

    // Spot-check the schedule shape (round robin: rotation r is led by
    // validator r mod NODES, occupying index r mod 2 for 4 slots).
    let proposers_of = |slot: u64| {
        let set = schedule.proposers_at(Slot(slot)).unwrap();
        [set.proposer(0), set.proposer(1)]
    };
    let v = |i: u64| Some(NodeId::dummy(i));
    assert_eq!(proposers_of(0), [v(0), None]); // genesis ramp-up at index 1
    assert_eq!(proposers_of(3), [v(0), None]);
    assert_eq!(proposers_of(4), [v(0), None]); // index 1 handed over: vacant
    assert_eq!(proposers_of(6), [v(0), v(1)]); // ... and starts proposing
    assert_eq!(proposers_of(8), [None, v(1)]); // index 0 handed over: vacant
    assert_eq!(proposers_of(10), [v(2), v(1)]);
    assert_eq!(proposers_of(11), [v(2), v(1)]);

    let (mut swarm, entries) = build_swarm(&proposer_config, &schedule, &val_data);
    let finalization_latency = LATENCY.checked_mul(2).unwrap();
    swarm.run_until(deadline_of_slot(11) + finalization_latency);

    for i in 0..NODES {
        let node_id = NodeId::dummy(i);
        expect_finalized(&swarm, node_id, 0..12);
        expect_finalized_at(
            &swarm,
            node_id,
            (0..12).map(|slot| deadline_of_slot(slot) + finalization_latency),
        );
    }
    expect_entries(&entries, &schedule, 0..12);
}
