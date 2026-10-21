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

//! Fallback-path swarm scenarios (§14.3): split proposer, invalid
//! encoding, diverging adoption with a silent Coda leader.

mod common;

use std::sync::Arc;

use common::{LanePlan, Params, Scenario, lane_header, lane_pid};
use monad_mcp_chorus::stub::types::Slot;
use monad_pq_chorus::{PqPath, types::Entry};

const SPLIT_SLOT: u64 = 3;
const SPLIT_LANE: usize = 3;

// the split lane reaches validators 0 and 1 (f+1 of 4) by the deadline;
// the others decode it one hop after the deadline. Pulls are slower than
// the time left to the deadline: a pull that decodes before D turns the
// lane positive (D3) and the slot fast.
#[test]
fn split_proposer_finalizes_in_coda_view_one() {
    let mut params = Params::new(4, 4);
    params.config.early_vote = true;
    params.pull_delay = 100;
    let (lead, h) = (params.lead as i64, params.latency as i64);
    let delta = params.config.delta.as_millis() as i64;
    let timely = params.timely();
    let mut scenario = Scenario::new(params.clone());
    scenario.plan = Arc::new(move |node, slot, j| {
        if slot != Slot(SPLIT_SLOT) || j != SPLIT_LANE {
            return timely(node, slot, j);
        }
        let holder = u64::from(node) < 2;
        vec![LanePlan {
            header: lane_header(slot, j, 0),
            fulfilled_at: holder.then_some(-lead + h),
            decoded_at: Some(if holder { -lead + 2 * h } else { h }),
            valid: true,
        }]
    });
    let outcome = scenario.run_until_ms(params.deadline(20).as_nanos() as u64 / 1_000_000);
    outcome.assert_safety();

    for node in 0..4 {
        let (at, metablock, path) = outcome.final_of(node, SPLIT_SLOT).unwrap();
        assert_eq!(path, PqPath::Coda);
        // adoption at D + 2Δ, then Vote1 and Vote2 hops of view 1
        assert_eq!(outcome.rel(SPLIT_SLOT, at), 2 * delta + 2 * h);
        assert_eq!(
            metablock[SPLIT_LANE],
            Entry::Root(lane_pid(Slot(SPLIT_SLOT), SPLIT_LANE, 0)),
            "the late proposal is included through fallback entries"
        );
        let log = outcome.log(node);
        assert!(!log.speculative.contains_key(&Slot(SPLIT_SLOT)));
        assert_eq!(log.coda_views[&Slot(SPLIT_SLOT)], 1);
        drop(log);

        // no cascade: every other slot stays on the fast path
        for slot in (0..20).filter(|slot| *slot != SPLIT_SLOT) {
            let (at, _, path) = outcome.final_of(node, slot).unwrap();
            assert_eq!(path, PqPath::Fast, "slot {slot}");
            assert_eq!(outcome.rel(slot, at), -lead + 3 * h);
        }
    }
    outcome.assert_all_included(SPLIT_SLOT);
}

// with fast pulls the f+1 early positive votes let the others recover the
// lane before the deadline, so the slot stays fast
#[test]
fn a_split_lane_recovered_before_the_deadline_stays_fast() {
    let mut params = Params::new(4, 4);
    params.config.early_vote = true;
    let (lead, h) = (params.lead as i64, params.latency as i64);
    let timely = params.timely();
    let mut scenario = Scenario::new(params.clone());
    scenario.plan = Arc::new(move |node, slot, j| {
        if slot != Slot(SPLIT_SLOT) || j != SPLIT_LANE {
            return timely(node, slot, j);
        }
        let holder = u64::from(node) < 2;
        vec![LanePlan {
            header: lane_header(slot, j, 0),
            fulfilled_at: holder.then_some(-lead + h),
            decoded_at: holder.then_some(-lead + 2 * h),
            valid: true,
        }]
    });
    let outcome = scenario.run_until_ms(params.deadline(8).as_nanos() as u64 / 1_000_000);
    outcome.assert_safety();
    for node in 0..4 {
        let (_, _, path) = outcome.final_of(node, SPLIT_SLOT).unwrap();
        assert_eq!(path, PqPath::Fast);
    }
    outcome.assert_all_included(SPLIT_SLOT);
}

#[test]
fn an_invalid_encoding_is_excluded_on_the_fast_path() {
    let params = Params::new(4, 4);
    let timely = params.timely();
    let mut scenario = Scenario::new(params.clone());
    scenario.plan = Arc::new(move |node, slot, j| {
        let mut lanes = timely(node, slot, j);
        lanes[0].valid = j != 1;
        lanes
    });
    let outcome = scenario.run_until_ms(params.deadline(10).as_nanos() as u64 / 1_000_000);
    outcome.assert_safety();
    for node in 0..4 {
        for slot in 0..10 {
            let (_, metablock, path) = outcome.final_of(node, slot).unwrap();
            assert_eq!(path, PqPath::Fast);
            assert_eq!(metablock[1], Entry::Bot);
            assert_ne!(metablock[0], Entry::Bot);
            assert_eq!(outcome.log(node).speculative[&Slot(slot)][0][1], Entry::Bot);
        }
    }
}

// n = 7 (f = 2, weak 3, strong 5) with validator 6 silent. The split lane
// reaches group A = {0, 1, 2} only; group B = {3, 4, 5} never decodes it.
// Links inside a group take 5 ms, across groups 40 ms, so A's clean sets
// hold all three ⟨fallback, r⟩ entries and B's only two: A adopts r, B ⊥.
// View 1 cannot lock, view 2 is led by the silent validator 6
// (leader(V) = validators[(slot + V) mod 7]), and view 3 by validator 0.
#[test]
fn diverging_adoption_with_a_silent_leader_decides_in_view_three() {
    const SLOT: u64 = 4;
    let mut params = Params::new(7, 3);
    params.slots = 16;
    // one window for the whole run: slot 4 holds the cap for about 1 s
    params.slots_per_window = 20;
    // B's pulls of the lane miss the decode grace
    params.pull_delay = 1_000;
    let (lead, h) = (params.lead as i64, params.latency as i64);
    let timely = params.timely();
    let mut scenario = Scenario::new(params.clone());
    scenario.silent.insert(6);
    for from in 0..7u64 {
        for to in 0..7u64 {
            let same_group = (from < 3) == (to < 3);
            scenario
                .link_latency
                .insert((from, to), if same_group { 5 } else { 40 });
        }
    }
    scenario.plan = Arc::new(move |node, slot, j| {
        if slot != Slot(SLOT) || j != 0 {
            return timely(node, slot, j);
        }
        let in_a = u64::from(node) < 3;
        vec![LanePlan {
            header: lane_header(slot, j, 0),
            fulfilled_at: in_a.then_some(-lead + h),
            decoded_at: in_a.then_some(-lead + 2 * h),
            valid: true,
        }]
    });
    let outcome = scenario.run_until_ms(params.deadline(16).as_nanos() as u64 / 1_000_000);
    outcome.assert_safety();

    for node in 0..6 {
        let log = outcome.log(node);
        assert_eq!(
            log.coda_views.get(&Slot(SLOT)),
            Some(&3),
            "node {node}: {:?} {:?}",
            log.finals.get(&Slot(SLOT)),
            log.halts.get(&Slot(SLOT))
        );
    }
    let finals: Vec<_> = (0..6)
        .map(|node| outcome.final_of(node, SLOT).unwrap())
        .collect();
    for (at, metablock, path) in &finals {
        assert_eq!(*path, PqPath::Coda);
        // view change ties between r and ⊥ break towards the root
        assert_eq!(metablock[0], Entry::Root(lane_pid(Slot(SLOT), 0, 0)));
        // adoption by D + 2Δ + 2 cross hops, two timed-out views, then
        // view 3's three hops
        let delta = params.config.delta.as_millis() as i64;
        assert!(outcome.rel(SLOT, *at) <= 2 * delta + 80 + 2 * (5 * delta + 3 * 40) + 3 * 40);
    }
    for slot in (0..16).filter(|slot| *slot != SLOT) {
        for node in 0..6 {
            assert_eq!(outcome.final_of(node, slot).unwrap().2, PqPath::Fast);
        }
    }
}
