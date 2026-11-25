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
//! proposers' entries — `Positive` with the pid of the proposer's header where
//! the schedule assigns a proposer, `Negative` where the lane is vacant.

mod helper;

use std::{
    collections::BTreeMap,
    num::NonZeroU64,
    sync::{Arc, Mutex},
};

use cadence::{
    conductor::{CadenceConductor, ConductorConfig, acs::nop::NopAcs},
    slot::chorus::{Chorus, ChorusConfig, Entry, SlotFinalization},
};
use helper::{expect_finalized, expect_finalized_at};
use monad_mcp_cadence::stub as cadence;
use monad_mcp_cadence_sim::{
    CadenceSwarm, CadenceSwarmBuilder, MockDa, header_auth, mock_header, mock_payload,
};
use monad_mcp_common::{
    Lane, Slot, SlotDeadline, Timestamp, TimestampDelta,
    stub::{NodeCredential, NodeId, ValidatorData},
};
use monad_mcp_epoch::stub::{
    EpochHandle,
    election::{ProposerScheduleConfig, RotatingProposerSchedule, RoundRobinLeaderSchedule},
};
use monad_mcp_proposing::stub::{FixedLeadConfig, FixedLeadPlanner};

const NODES: u64 = 4;
const SLOTS_PER_WINDOW: NonZeroU64 = NonZeroU64::new(10).unwrap();
const LAG_THRESHOLD: NonZeroU64 = SLOTS_PER_WINDOW; // one window behind triggers a cap jump
const SYNC_BOUNDARY_SLOTS: NonZeroU64 = NonZeroU64::new(8).unwrap();
const SLOT_INTERVAL: TimestampDelta = TimestampDelta::from_millis(100);
const GENESIS_DEADLINE: SlotDeadline = SlotDeadline::from_millis(100);
const LATENCY: TimestampDelta = TimestampDelta::from_millis(50); // networking latency
const DELTA: TimestampDelta = TimestampDelta::from_millis(100); // Chorus latency bound (Delta)
// Seal lead: must cover the announcement's dissemination (one latency) with
// margin; small enough that the chaining gate is really exercised (see the
// staggered-rotation test).
const LEAD: TimestampDelta = TimestampDelta::from_millis(60);
// below LEAD: the withhold gate is exercised in monad-mcp-proposing, not here
const MIN_LEAD: TimestampDelta = TimestampDelta::from_millis(30);

type Conductor = CadenceConductor<NopAcs<SlotDeadline>>;

// Per-node record of the finalized entries: (node, slot) -> entries by
// lane.
type EntriesLog = Arc<Mutex<BTreeMap<(NodeId, Slot), Vec<Entry>>>>;

fn conductor(epoch_handle: &EpochHandle) -> Conductor {
    let config = ConductorConfig::new(
        SLOTS_PER_WINDOW,
        SYNC_BOUNDARY_SLOTS,
        SLOT_INTERVAL,
        GENESIS_DEADLINE,
        LAG_THRESHOLD,
    )
    .unwrap();
    Conductor::genesis(config, epoch_handle.clone(), ()).unwrap()
}

fn deadline_of_slot(slot: u64) -> Timestamp {
    GENESIS_DEADLINE
        .checked_add_deltas(SLOT_INTERVAL, slot)
        .unwrap()
}

// One epoch handle for the whole swarm: the round-robin schedule is
// deterministic, so per-node instances would agree anyway.
fn epoch_handle(config: ProposerScheduleConfig) -> EpochHandle {
    let validators = ValidatorData::dummy(NODES);
    let algorithm = RoundRobinLeaderSchedule::new(&config);
    let election = RotatingProposerSchedule::new(config, algorithm, &validators).unwrap();
    EpochHandle::new(validators, election)
}

fn build_swarm(
    proposer_config: &ProposerScheduleConfig,
    epoch_handle: &EpochHandle,
) -> (CadenceSwarm<Chorus, Conductor>, EntriesLog) {
    let entries: EntriesLog = Arc::default();

    let mut builder = CadenceSwarmBuilder::<Chorus, _>::new();
    builder.set_latency(LATENCY.as_duration());

    for i in 0..NODES {
        let id = NodeId::dummy(i);
        // One header check and one epoch handle, shared by consensus and
        // the node's DA layer: they cannot disagree about who proposes
        // where.
        let header_auth = header_auth();
        let da = MockDa::new(id, epoch_handle.clone(), header_auth.clone());
        let slot_config = ChorusConfig {
            delta: DELTA,
            credential: NodeCredential::dummy(i),
            header_auth,
        };
        let planner = FixedLeadPlanner::new(
            id,
            epoch_handle.clone(),
            FixedLeadConfig {
                lead: LEAD,
                min_lead: MIN_LEAD,
                observation_cutoff: proposer_config.observation_cutoff,
            },
        );
        let log = entries.clone();
        let observer = move |_now: Timestamp, slot: Slot, data: &SlotFinalization| {
            // the proposal flow is a fast-path test; a fallback
            // finalization would mean the premise broke
            let SlotFinalization::Fast { qc } = data else {
                panic!("slot {slot:?} finalized through the fallback path");
            };
            let slot_entries: Vec<Entry> = qc.verdict.entries.clone().into_iter().collect();
            let previous = log
                .lock()
                .expect("entries log poisoned")
                .insert((id, slot), slot_entries);
            assert!(previous.is_none(), "slot finalized twice");
        };

        builder.add_proposer_node(
            id,
            conductor(epoch_handle),
            slot_config,
            epoch_handle.clone(),
            planner,
            da,
            observer,
        );
    }

    (builder.build(), entries)
}

// The entry expected at (slot, lane): the pid of the scheduled proposer's
// header, or Negative for a vacant lane.
fn expected_entry(epoch_handle: &EpochHandle, slot: Slot, lane: Lane) -> Entry {
    use monad_mcp_cadence::spec::ProposalHeader as _;

    let Some(proposer) = epoch_handle.proposers_at(slot).unwrap().proposer(lane) else {
        return Entry::Negative;
    };
    let payload = mock_payload(proposer, slot, lane);
    Entry::Positive(mock_header(proposer, slot, lane, &payload).pid())
}

fn expect_entries(
    entries: &EntriesLog,
    epoch_handle: &EpochHandle,
    slots: impl IntoIterator<Item = u64> + Clone,
) {
    let entries = entries.lock().expect("entries log poisoned");
    for node in (0..NODES).map(NodeId::dummy) {
        for slot in slots.clone() {
            let slot = Slot(slot);
            let finalized = &entries[&(node, slot)];
            let set = epoch_handle.proposers_at(slot).unwrap();
            let expected: Vec<Entry> = set
                .iter()
                .map(|(lane, _)| expected_entry(epoch_handle, slot, lane))
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
    // proposer's header pid, on the same schedule as the proposal-less
    // runs (finalization at deadline + 2 latencies).
    let proposer_config = ProposerScheduleConfig {
        concurrent_proposers: 1,
        observation_cutoff: 0,
        rotation_slack: 1,
        slots_per_epoch: 1_000, // never crossed in this test
    };
    let epoch_handle = epoch_handle(proposer_config.clone());

    // round robin, one-slot rotations: slot s is proposed by validator
    // s mod NODES
    for slot in 0..8 {
        assert_eq!(
            epoch_handle
                .proposers_at(Slot(slot))
                .unwrap()
                .proposer(Lane::new_unchecked(0)),
            Some(NodeId::dummy(slot % NODES)),
        );
    }

    let (mut swarm, entries) = build_swarm(&proposer_config, &epoch_handle);
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
    expect_entries(&entries, &epoch_handle, 0..4);

    // and they really are Positive, not vacuously equal
    for slot in 0..4 {
        assert!(matches!(
            expected_entry(&epoch_handle, Slot(slot), Lane::new_unchecked(0)),
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
    //  * rotation vacancies (a handed-over lane is vacant for y slots, so
    //    the incoming proposer knows all in-flight proposals at its lane),
    //  * the planner's chaining gate: sealing slot r at deadline − lead
    //    requires slot r − y chained, which happens 40ms before the seal
    //    alarm — late enough that early sealing would vote Negative.
    let proposer_config = ProposerScheduleConfig {
        concurrent_proposers: 2,
        observation_cutoff: 2,
        rotation_slack: 2,
        slots_per_epoch: 1_000, // never crossed in this test
    };
    let epoch_handle = epoch_handle(proposer_config.clone());

    // Spot-check the schedule shape (round robin: rotation r is led by
    // validator r mod NODES, occupying index r mod 2 for 4 slots).
    let proposers_of = |slot: u64| {
        let set = epoch_handle.proposers_at(Slot(slot)).unwrap();
        [
            set.proposer(Lane::new_unchecked(0)),
            set.proposer(Lane::new_unchecked(1)),
        ]
    };
    let v = |i: u64| Some(NodeId::dummy(i));
    assert_eq!(proposers_of(0), [v(0), None]); // genesis ramp-up at lane 1
    assert_eq!(proposers_of(3), [v(0), None]);
    assert_eq!(proposers_of(4), [v(0), None]); // lane 1 handed over: vacant
    assert_eq!(proposers_of(6), [v(0), v(1)]); // ... and starts proposing
    assert_eq!(proposers_of(8), [None, v(1)]); // lane 0 handed over: vacant
    assert_eq!(proposers_of(10), [v(2), v(1)]);
    assert_eq!(proposers_of(11), [v(2), v(1)]);

    let (mut swarm, entries) = build_swarm(&proposer_config, &epoch_handle);
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
    expect_entries(&entries, &epoch_handle, 0..12);
}
