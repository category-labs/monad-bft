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

use std::{
    cmp::Reverse,
    collections::{BTreeSet, HashMap},
    net::SocketAddr,
    time::Duration,
};

use common::{horizon_of, most_tenured, node_config, run_of, schedule_of};
use monad_mcp_node::{
    NodeProposerSchedule,
    chorus::types::{
        FixedProposerSchedule, NodeId, ProposerSchedule, ProposerSet, ScheduleError, Slot,
        Timestamp, TimestampDelta,
    },
    config::NodeConfig,
};
use monad_mcp_rpc::schedule::{
    Latency, Planner, Route, SlotClock, choose_leader, choose_nearest, lead, tenure_horizon,
};

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
    NodeConfig::single_node(9000, at(GENESIS_MS), "unused-ledger")
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
        let route = planner.route(now, None, None).expect("a route");
        assert_eq!(
            planner.route(now, Some(0), None),
            Some(route),
            "its only lane"
        );
        assert_eq!(
            planner.route(now, Some(3), None),
            None,
            "a lane it never holds"
        );
        let target = planner.target_slot(now);
        let first = (target.0..).map(Slot).find(|slot| occupied(*slot)).unwrap();
        assert_eq!(
            route,
            Route {
                slot: first,
                lane: 0,
                leader: NodeId::dummy(0),
                latency: None,
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
        let route = planner.route(now, None, None).unwrap();
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
        assert_eq!(planner.route(now, None, None), Some(route), "deterministic");
        if let Some(previous) = previous {
            assert_eq!(route.slot, Slot(previous.slot.0 + 1));
        }
        leaders.insert(u64::from(route.leader));
        previous = Some(route);
    }
    // over a full cycle each validator's fresh tenure makes it the leader
    assert_eq!(leaders, (0..SWARM).collect::<BTreeSet<_>>());
}

// a pinned lane goes to that lane's proposer at the first slot from the target
// where it has one, whatever its tenure
#[test]
fn a_pinned_lane_routes_to_its_own_proposer() {
    let config = swarm(SWARM);
    let planner = Planner::new(&config, None).unwrap();
    let schedule = schedule_of(&config);
    let proposer = |slot: Slot, lane| schedule.proposers_at(slot).unwrap().proposer(lane);
    let mut skipped = 0;
    for k in 0..SLOTS {
        let now = at(GENESIS_MS + k * 100 + 37);
        let target = planner.target_slot(now);
        for lane in 0..schedule.num_indices() {
            let route = planner
                .route(now, Some(lane), None)
                .expect("every lane is held");
            let first = (target.0..)
                .map(Slot)
                .find(|slot| proposer(*slot, lane).is_some())
                .unwrap();
            skipped += u64::from(first != target);
            assert_eq!(
                route,
                Route {
                    slot: first,
                    lane,
                    leader: proposer(first, lane).unwrap(),
                    latency: None,
                },
                "slot {k} lane {lane}"
            );
        }
    }
    assert!(skipped > 0, "handovers leave a lane vacant for a few slots");
}

// lane 1 opens a 100-slot rotation after genesis: refused until it is within
// 5 s of the target, then routed to its first slot
#[test]
fn a_pinned_lane_is_only_routed_within_the_lookahead() {
    let mut config = swarm(SWARM);
    config.leader_election.rotation_slack = 95;
    let planner = Planner::new(&config, None).unwrap();
    let schedule = schedule_of(&config);
    let proposer = |slot: Slot| schedule.proposers_at(slot).unwrap().proposer(1);
    let (mut refused, mut routed) = (0, 0);
    for k in 0..200 {
        let now = at(GENESIS_MS + k * 100);
        let target = planner.target_slot(now);
        let last = planner.clock().slot_at(now + planner.lead() + ms(5_000));
        let first = (target.0..)
            .map(Slot)
            .find(|slot| proposer(*slot).is_some())
            .unwrap();
        let expected = (first <= last).then(|| Route {
            slot: first,
            lane: 1,
            leader: proposer(first).unwrap(),
            latency: None,
        });
        assert_eq!(
            planner.route(now, Some(1), None),
            expected,
            "at +{} ms",
            k * 100
        );
        if expected.is_some() {
            routed += 1;
        } else {
            refused += 1;
        }
    }
    assert!(
        refused > 0 && routed > 0,
        "refused {refused}, routed {routed}"
    );
}

// four validators on three lanes
fn small() -> NodeConfig {
    let mut config = swarm(4);
    config.proposal.num_proposals = 3;
    config
}

// node i is 10·i ms from the rpc's own node 0
fn latency_row(n: u64) -> Latency {
    (0..n)
        .map(|id| (NodeId::dummy(id), Duration::from_millis(10 * id)))
        .collect()
}

// each proposer at `slot` other than `avoid`, with its run from `slot`
fn runs(
    schedule: &NodeProposerSchedule,
    slot: Slot,
    horizon: u64,
    avoid: Option<NodeId>,
) -> Vec<(usize, NodeId, u64)> {
    let set = schedule.proposers_at(slot).unwrap();
    set.iter()
        .filter_map(|(lane, proposer)| Some((lane, proposer?)))
        .filter(|(_, node)| Some(*node) != avoid)
        .map(|(lane, node)| (lane, node, run_of(schedule, slot, lane, node, horizon)))
        .collect()
}

// the reference rule: the nearest of those running `min` slots, then the
// longest run, then the lower lane; with none, the longest run
fn nearest(
    schedule: &NodeProposerSchedule,
    slot: Slot,
    latency: &Latency,
    min: u64,
    avoid: Option<NodeId>,
) -> Option<(usize, NodeId)> {
    let runs = runs(schedule, slot, horizon_of(schedule), avoid);
    runs.iter()
        .filter(|(_, _, run)| *run >= min)
        .min_by_key(|(lane, node, run)| (latency[node], Reverse(*run), *lane))
        .or_else(|| runs.iter().min_by_key(|(_, _, run)| Reverse(*run)))
        .map(|&(lane, node, _)| (lane, node))
}

#[test]
fn the_nearest_proposer_with_enough_tenure_wins() {
    let schedule = schedule_of(&small());
    let horizon = horizon_of(&schedule);
    let latency = latency_row(4);
    let mut changed = 0;
    for slot in (0..SLOTS).map(Slot) {
        let chosen = choose_nearest(schedule.as_ref(), slot, horizon, &latency, 2, None);
        assert_eq!(
            chosen,
            nearest(&schedule, slot, &latency, 2, None),
            "{slot:?}"
        );
        changed += u64::from(chosen != choose_leader(schedule.as_ref(), slot, horizon));
    }
    assert!(changed > 0, "latency never changed the leader");
}

#[test]
fn a_near_proposer_about_to_hand_over_loses_to_a_farther_one() {
    let schedule = schedule_of(&small());
    let horizon = horizon_of(&schedule);
    let latency = latency_row(4);
    let mut handovers = 0;
    for slot in (0..SLOTS).map(Slot) {
        let runs = runs(&schedule, slot, horizon, None);
        let &(_, near, run) = runs
            .iter()
            .min_by_key(|(_, node, _)| latency[node])
            .unwrap();
        if run >= 2 || runs.iter().all(|(_, _, run)| *run < 2) {
            continue;
        }
        handovers += 1;
        let (lane, leader) =
            choose_nearest(schedule.as_ref(), slot, horizon, &latency, 2, None).unwrap();
        assert_ne!(leader, near, "{slot:?}");
        assert!(latency[&leader] > latency[&near], "{slot:?}");
        assert!(run_of(schedule.as_ref(), slot, lane, leader, horizon) >= 2);
    }
    assert!(
        handovers > 0,
        "the nearest never handed over in {SLOTS} slots"
    );
}

#[test]
fn the_own_node_wins_whenever_it_leads_long_enough() {
    let schedule = schedule_of(&small());
    let horizon = horizon_of(&schedule);
    let own = NodeId::dummy(0);
    let mut led = 0;
    for slot in (0..SLOTS).map(Slot) {
        let holds = runs(&schedule, slot, horizon, None)
            .iter()
            .any(|&(_, node, run)| node == own && run >= 2);
        let (_, leader) =
            choose_nearest(schedule.as_ref(), slot, horizon, &latency_row(4), 2, None).unwrap();
        assert_eq!(leader == own, holds, "{slot:?}");
        led += u64::from(holds);
    }
    assert!(led > 0);
}

#[test]
fn with_no_proposer_long_enough_the_most_tenured_wins() {
    let schedule = schedule_of(&small());
    let horizon = horizon_of(&schedule);
    for slot in (0..SLOTS).map(Slot) {
        for min in [horizon + 1, u64::MAX] {
            assert_eq!(
                choose_nearest(schedule.as_ref(), slot, horizon, &latency_row(4), min, None),
                choose_leader(schedule.as_ref(), slot, horizon),
                "{slot:?}"
            );
        }
    }
}

// no latency matrix: today's rule, and a resend's last leader is not avoided
#[test]
fn without_latency_routes_follow_the_most_tenured() {
    let config = small();
    let planner = Planner::new(&config, None).unwrap();
    assert!(planner.latency().is_none());
    let schedule = schedule_of(&config);
    for k in 0..SLOTS {
        let now = at(GENESIS_MS + k * 100 + 37);
        let route = planner.route(now, None, None).unwrap();
        assert_eq!(route.latency, None);
        assert_eq!(
            Some((route.lane, route.leader)),
            choose_leader(schedule.as_ref(), route.slot, horizon_of(&schedule))
        );
        assert_eq!(planner.route(now, None, Some(route.leader)), Some(route));
    }
}

#[test]
fn a_resend_skips_the_last_leader() {
    let config = small();
    let planner = Planner::new(&config, None)
        .unwrap()
        .with_latency(latency_row(4), 2);
    let schedule = schedule_of(&config);
    let latency = latency_row(4);
    for k in 0..SLOTS {
        let now = at(GENESIS_MS + k * 100 + 37);
        let first = planner.route(now, None, None).unwrap();
        assert_eq!(first.latency, Some(latency[&first.leader]));
        let resend = planner.route(now, None, Some(first.leader)).unwrap();
        assert_ne!(resend.leader, first.leader, "at slot {k}");
        // before the lanes ramp up, the first leader may lead alone
        let other = (first.slot.0..)
            .map(Slot)
            .find(|slot| !runs(&schedule, *slot, 1, Some(first.leader)).is_empty());
        assert_eq!(Some(resend.slot), other, "at slot {k}");
        assert_eq!(
            Some((resend.lane, resend.leader)),
            nearest(&schedule, resend.slot, &latency, 2, Some(first.leader))
        );
        // a pinned lane keeps its one proposer
        assert_eq!(
            planner.route(now, Some(first.lane), Some(first.leader)),
            planner.route(now, Some(first.lane), None)
        );
    }
}

#[test]
fn a_sole_proposer_is_reused_on_resend() {
    let own = NodeId::dummy(0);
    let latency = HashMap::from([(own, Duration::ZERO)]);
    let planner = Planner::new(&lone(), None)
        .unwrap()
        .with_latency(latency, 2);
    for k in 0..400 {
        let now = at(GENESIS_MS + k * 25);
        let route = planner.route(now, None, None).expect("a route");
        assert_eq!(route.leader, own);
        assert_eq!(route.latency, Some(Duration::ZERO));
        assert_eq!(planner.route(now, None, Some(own)), Some(route));
    }
}
