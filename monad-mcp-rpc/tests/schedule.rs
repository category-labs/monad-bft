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

//! The rpc's slot clock and leader choice, against the node's real rotating
//! schedule built from the same config.

mod common;

use std::{collections::BTreeSet, net::SocketAddr};

use common::{horizon_of, most_tenured, node_config, run_of, schedule_of};
use monad_mcp_node::{
    chorus::types::{
        FixedProposerSchedule, NodeId, ProposerSchedule, ProposerSet, ScheduleError, Slot,
        Timestamp, TimestampDelta,
    },
    config::NodeConfig,
};
use monad_mcp_rpc::schedule::{Planner, Route, SlotClock, choose_leader, lead, tenure_horizon};

const GENESIS_MS: u64 = 1_000_000;
const SWARM: u64 = 5;
// two epochs of the default 400 slots
const SLOTS: u64 = 800;

fn at(ms: u64) -> Timestamp {
    Timestamp::from_millis(ms)
}

fn ms(ms: u64) -> TimestampDelta {
    TimestampDelta::from_millis(ms)
}

fn swarm(n: u64) -> NodeConfig {
    let addresses: Vec<SocketAddr> = (0..n)
        .map(|id| SocketAddr::from(([127, 0, 0, 1], 9000 + id as u16)))
        .collect();
    node_config(0, &addresses, at(GENESIS_MS))
}

fn lone() -> NodeConfig {
    NodeConfig::single_node(9000, at(GENESIS_MS))
}

#[test]
fn a_deadline_is_genesis_plus_the_interval_per_slot() {
    let clock = SlotClock::new(at(GENESIS_MS), ms(100));
    assert_eq!(clock.deadline(Slot(0)), at(GENESIS_MS));
    assert_eq!(clock.deadline(Slot(1)), at(GENESIS_MS + 100));
    assert_eq!(clock.deadline(Slot(7_654)), at(GENESIS_MS + 765_400));
    assert_eq!(SlotClock::of(&lone()), clock);
}

#[test]
fn a_time_falls_in_the_slot_of_the_next_deadline() {
    let clock = SlotClock::new(at(GENESIS_MS), ms(100));
    assert_eq!(clock.slot_at(at(0)), Slot(0));
    assert_eq!(clock.slot_at(at(GENESIS_MS - 1)), Slot(0));
    assert_eq!(clock.slot_at(at(GENESIS_MS)), Slot(0));
    let just_after = Timestamp::from_nanos(u128::from(GENESIS_MS) * 1_000_000 + 1);
    assert_eq!(clock.slot_at(just_after), Slot(1));
    assert_eq!(clock.slot_at(at(GENESIS_MS + 100)), Slot(1));
    assert_eq!(clock.slot_at(at(GENESIS_MS + 101)), Slot(2));
    for slot in [0, 1, 99, 12_345] {
        assert_eq!(clock.slot_at(clock.deadline(Slot(slot))), Slot(slot));
    }
}

#[test]
fn the_lead_covers_proposing_the_delta_and_a_margin() {
    // demo: propose 200 ms before the deadline, delta 50, one 100 ms slot of margin
    let demo = lone();
    assert_eq!(lead(&demo, None), ms(350));
    assert_eq!(lead(&demo, Some(ms(40))), ms(290));
    assert_eq!(lead(&demo, Some(TimestampDelta::ZERO)), ms(250));

    let mut defaults = lone();
    defaults.cadence = Default::default();
    defaults.proposal = Default::default();
    // 500 + 150 + 100
    assert_eq!(lead(&defaults, None), ms(750));
}

#[test]
fn the_target_is_the_first_slot_a_lead_away() {
    let planner = Planner::new(&lone(), None).unwrap();
    assert_eq!(planner.lead(), ms(350));
    assert_eq!(planner.clock(), SlotClock::of(&lone()));
    // before genesis the first slot is the target
    assert_eq!(planner.target_slot(at(GENESIS_MS - 1_000)), Slot(0));
    assert_eq!(planner.target_slot(at(GENESIS_MS - 350)), Slot(0));
    assert_eq!(planner.target_slot(at(GENESIS_MS - 349)), Slot(1));
    // genesis + 350 ms lies inside slot 4
    assert_eq!(planner.target_slot(at(GENESIS_MS)), Slot(4));
    assert_eq!(planner.target_slot(at(GENESIS_MS + 50)), Slot(4));
    assert_eq!(planner.target_slot(at(GENESIS_MS + 51)), Slot(5));
    // deadlines are never shifted: an hour on, still the formula
    let hour = 3_600_000;
    assert_eq!(
        planner.target_slot(at(GENESIS_MS + hour)),
        Slot(hour / 100 + 4)
    );

    let margin = Planner::new(&lone(), Some(ms(0))).unwrap();
    assert_eq!(margin.target_slot(at(GENESIS_MS)), Slot(3));
}

#[test]
fn the_horizon_is_k_rotations() {
    let schedule = schedule_of(&swarm(SWARM));
    let cfg = schedule.config();
    // the node's defaults: K = 5, y = 5, z = 3
    assert_eq!(
        (
            cfg.concurrent_proposers,
            cfg.observation_cutoff,
            cfg.rotation_slack
        ),
        (5, 5, 3)
    );
    assert_eq!(tenure_horizon(cfg), 40);
    assert_eq!(Planner::new(&swarm(SWARM), None).unwrap().horizon(), 40);
}

// every slot of two epochs agrees with the reference rule
#[test]
fn the_leader_is_the_proposer_with_the_most_tenure_left() {
    let schedule = schedule_of(&swarm(SWARM));
    let horizon = horizon_of(&schedule);
    for slot in (0..SLOTS).map(Slot) {
        let chosen = choose_leader(schedule.as_ref(), slot, horizon);
        assert_eq!(
            chosen,
            most_tenured(schedule.as_ref(), slot, horizon),
            "{slot:?}"
        );
        let (lane, leader) = chosen.expect("five validators leave no slot empty");
        let set = schedule.proposers_at(slot).unwrap();
        assert_eq!(set.proposer(lane), Some(leader), "{slot:?}");
        let longest = run_of(schedule.as_ref(), slot, lane, leader, horizon);
        for (other, proposer) in set.iter() {
            let Some(proposer) = proposer else {
                continue;
            };
            let run = run_of(schedule.as_ref(), slot, other, proposer, horizon);
            assert!(
                run <= longest,
                "{slot:?}: lane {other} runs {run} > {longest}"
            );
            if run == longest {
                assert!(
                    lane <= other,
                    "{slot:?}: a tie went to lane {lane} over {other}"
                );
            }
        }
    }
}

#[test]
fn a_proposer_about_to_hand_over_loses() {
    let schedule = schedule_of(&swarm(SWARM));
    let horizon = horizon_of(&schedule);
    let mut handovers = 0;
    for slot in (0..SLOTS).map(Slot) {
        let set = schedule.proposers_at(slot).unwrap();
        let (lane, _) = choose_leader(schedule.as_ref(), slot, horizon).unwrap();
        for (index, proposer) in set.iter() {
            let Some(proposer) = proposer else {
                continue;
            };
            // its last slot on this lane
            if run_of(schedule.as_ref(), slot, index, proposer, horizon) == 1 {
                handovers += 1;
                assert_ne!(lane, index, "{slot:?}: chose a proposer leaving its lane");
            }
        }
    }
    assert!(handovers > 0, "no handover in {SLOTS} slots");
}

#[test]
fn a_vacant_lane_is_never_chosen() {
    let schedule = schedule_of(&swarm(SWARM));
    let horizon = horizon_of(&schedule);
    let (mut vacant, mut incoming) = (0, 0);
    for slot in (0..SLOTS).map(Slot) {
        let set = schedule.proposers_at(slot).unwrap();
        let next = schedule.proposers_at(Slot(slot.0 + 1)).unwrap();
        let (lane, _) = choose_leader(schedule.as_ref(), slot, horizon).unwrap();
        assert!(set.proposer(lane).is_some(), "{slot:?}");
        for index in 0..set.num_indices() {
            if set.proposer(index).is_some() {
                continue;
            }
            vacant += 1;
            // the incoming proposer will hold the lane longest, but not yet
            if next.proposer(index).is_some() {
                incoming += 1;
                assert_ne!(lane, index, "{slot:?}: chose a lane still vacant");
            }
        }
    }
    assert!(
        vacant > 0 && incoming > 0,
        "vacant {vacant}, incoming {incoming}"
    );
}

#[test]
fn ties_go_to_the_lower_lane() {
    // a one-slot horizon: every occupied lane runs 1
    let schedule = schedule_of(&swarm(SWARM));
    for slot in (0..SLOTS).map(Slot) {
        let set = schedule.proposers_at(slot).unwrap();
        let lowest = set
            .iter()
            .find_map(|(index, proposer)| Some((index, proposer?)));
        assert_eq!(
            choose_leader(schedule.as_ref(), slot, 1),
            lowest,
            "{slot:?}"
        );
    }
    // no rotation: every lane is held for the whole horizon
    let fixed = FixedProposerSchedule::new(vec![NodeId::dummy(3), NodeId::dummy(1)]);
    assert_eq!(
        choose_leader(&fixed, Slot(42), 40),
        Some((0, NodeId::dummy(3)))
    );
}

// every slot's proposer set is the given one
struct Constant(ProposerSet);

impl ProposerSchedule for Constant {
    fn num_indices(&self) -> usize {
        self.0.num_indices()
    }

    fn proposers_at(&self, _: Slot) -> Result<ProposerSet, ScheduleError> {
        Ok(self.0.clone())
    }
}

#[test]
fn a_lone_validator_leads_every_occupied_slot() {
    let schedule = schedule_of(&lone());
    let horizon = horizon_of(&schedule);
    let occupied = |slot: Slot| {
        let set = schedule.proposers_at(slot).unwrap();
        set.iter().any(|(_, proposer)| proposer.is_some())
    };
    let mut vacant = Vec::new();
    for slot in (0..SLOTS).map(Slot) {
        let chosen = choose_leader(schedule.as_ref(), slot, horizon);
        if occupied(slot) {
            assert_eq!(chosen, Some((0, NodeId::dummy(0))), "{slot:?}");
        } else {
            assert_eq!(chosen, None, "{slot:?}");
            vacant.push(slot);
        }
    }
    // its own handovers leave slots with no proposer at all
    assert!(!vacant.is_empty());
    let empty = Constant(schedule.proposers_at(vacant[0]).unwrap());
    assert_eq!(choose_leader(&empty, Slot(0), horizon), None);

    // a route skips those to the next slot the validator proposes in
    let planner = Planner::new(&lone(), None).unwrap();
    for k in 0..400 {
        let now = at(GENESIS_MS + k * 25);
        let route = planner.route(now).expect("a route");
        let target = planner.target_slot(now);
        let first = (target.0..).map(Slot).find(|slot| occupied(*slot)).unwrap();
        assert_eq!(
            route,
            Route {
                slot: first,
                lane: 0,
                leader: NodeId::dummy(0)
            },
            "at +{} ms",
            k * 25
        );
    }
}

// no memory between sends: each route is a function of its own time
#[test]
fn each_route_is_recomputed_from_its_own_time() {
    let config = swarm(SWARM);
    let planner = Planner::new(&config, None).unwrap();
    let schedule = schedule_of(&config);
    let horizon = horizon_of(&schedule);
    let mut leaders = BTreeSet::new();
    let mut previous: Option<Route> = None;
    for k in 0..SLOTS {
        let now = at(GENESIS_MS + k * 100 + 37);
        let route = planner.route(now).unwrap();
        assert_eq!(
            route.slot,
            planner.target_slot(now),
            "every slot has a proposer"
        );
        assert_eq!(
            Some((route.lane, route.leader)),
            most_tenured(schedule.as_ref(), route.slot, horizon),
            "at slot {k}"
        );
        assert_eq!(planner.route(now), Some(route), "deterministic");
        if let Some(previous) = previous {
            assert_eq!(route.slot, Slot(previous.slot.0 + 1));
        }
        leaders.insert(u64::from(route.leader));
        previous = Some(route);
    }
    // over a full cycle each validator's fresh tenure makes it the leader
    assert_eq!(leaders, (0..SWARM).collect::<BTreeSet<_>>());
}
