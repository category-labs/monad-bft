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

//! Fast-path swarm scenarios (§14.3): timely proposers, early vote, vacant
//! lanes, across several conductor windows.

mod common;

use std::sync::Arc;

use common::{LanePlan, Params, Scenario};
use monad_mcp_chorus::stub::types::{
    NodeId, ProposerConfig, ProposerSchedule, RotatingProposerSchedule, RoundRobinLeaderSchedule,
    Slot,
};
use monad_pq_chorus::{PqPath, types::Entry};

const SLOTS: u64 = 35;

// seal at D − L; chunks at +h; votes at +2h; path votes at +3h; Finalized
// at +4h
fn fast_path(n: u64) {
    let params = Params::new(n, 4.min(n as usize));
    let (lead, h) = (params.lead as i64, params.latency as i64);
    let run_until = params.deadline(SLOTS).as_nanos() as u64 / 1_000_000;
    let outcome = Scenario::new(params).run_until_ms(run_until);
    outcome.assert_safety();

    for node in 0..n {
        let finalized = outcome.finalized_slots(node);
        assert_eq!(finalized[..SLOTS as usize], (0..SLOTS).collect::<Vec<_>>());
        for slot in 0..SLOTS {
            let (at, _, path) = outcome.final_of(node, slot).unwrap();
            assert_eq!(path, PqPath::Fast);
            assert_eq!(
                outcome.rel(slot, at),
                -lead + 3 * h,
                "slot {slot} node {node}"
            );
            let log = outcome.log(node);
            let (halted, _) = log.halts[&Slot(slot)][0].clone();
            assert_eq!(outcome.rel(slot, halted), -lead + 4 * h);
            assert_eq!(log.speculative[&Slot(slot)].len(), 1);
        }
    }
    for slot in 0..SLOTS {
        outcome.assert_all_included(slot);
    }
}

#[test]
fn fast_path_four_nodes() {
    fast_path(4);
}

#[test]
fn fast_path_eight_nodes() {
    fast_path(8);
}

#[test]
fn without_early_vote_finalizes_two_hops_after_the_deadline() {
    let mut params = Params::new(4, 4);
    params.config.early_vote = false;
    let h = params.latency as i64;
    let run_until = params.deadline(12).as_nanos() as u64 / 1_000_000;
    let outcome = Scenario::new(params).run_until_ms(run_until);
    outcome.assert_safety();
    for node in 0..4 {
        for slot in 0..12 {
            let (at, _, path) = outcome.final_of(node, slot).unwrap();
            assert_eq!(path, PqPath::Fast);
            assert_eq!(outcome.rel(slot, at), 2 * h);
        }
    }
}

#[test]
fn a_proposer_that_never_sends_is_excluded_fast() {
    let params = Params::new(4, 4);
    let timely = params.timely();
    let mut scenario = Scenario::new(params.clone());
    // lane 2's proposer is silent towards everyone: 2f+1 negative votes
    scenario.plan = Arc::new(move |node, slot, j| match j {
        2 => Vec::new(),
        _ => timely(node, slot, j),
    });
    let run_until = params.deadline(12).as_nanos() as u64 / 1_000_000;
    let outcome = scenario.run_until_ms(run_until);
    outcome.assert_safety();
    for node in 0..4 {
        for slot in 0..12 {
            let (at, metablock, path) = outcome.final_of(node, slot).unwrap();
            assert_eq!(path, PqPath::Fast);
            assert_eq!(metablock[2], Entry::Bot);
            assert!(metablock[0] != Entry::Bot);
            // no early vote: the deadline vote, then two hops
            assert_eq!(outcome.rel(slot, at), 2 * params.latency as i64);
        }
    }
}

#[test]
fn single_node_with_vacant_lanes_finalizes_every_slot() {
    let mut params = Params::new(1, 5);
    params.slots = 30;
    let config = ProposerConfig {
        concurrent_proposers: 5,
        observation_cutoff: 5,
        rotation_slack: 5,
        slots_per_epoch: 100,
    };
    let algorithm = RoundRobinLeaderSchedule::new(&config);
    let schedule =
        RotatingProposerSchedule::new(config, algorithm, common::validator_data(1)).unwrap();
    let schedule: Arc<dyn ProposerSchedule + Send + Sync> = Arc::new(schedule);

    let seal = -(params.lead as i64);
    let mut scenario = Scenario::new(params.clone());
    scenario.schedule = schedule.clone();
    scenario.plan = Arc::new(move |_node, slot, j| {
        vec![LanePlan {
            header: common::lane_header(slot, j, 0),
            fulfilled_at: Some(seal),
            decoded_at: Some(seal),
            valid: true,
        }]
    });
    let outcome = scenario.run_until_ms(params.deadline(30).as_nanos() as u64 / 1_000_000);
    outcome.assert_safety();
    // slot 30 is sealed, and so finalized, before its deadline
    assert_eq!(outcome.finalized_slots(0), (0..=30).collect::<Vec<_>>());
    for slot in 0..30 {
        let (at, metablock, _) = outcome.final_of(0, slot).unwrap();
        let proposers = schedule.proposers_at(Slot(slot)).unwrap();
        // everything cascades within the seal's handler chain; a slot with
        // every lane vacant has no input before its deadline
        let all_vacant = proposers.iter().all(|(_, proposer)| proposer.is_none());
        assert_eq!(outcome.rel(slot, at), if all_vacant { 0 } else { seal });
        for (j, proposer) in proposers.iter() {
            match proposer {
                Some(proposer) => {
                    assert_eq!(proposer, NodeId::dummy(0));
                    assert_ne!(metablock[j], Entry::Bot);
                }
                None => assert_eq!(metablock[j], Entry::Bot),
            }
        }
        assert_eq!(outcome.log(0).halts[&Slot(slot)].len(), 1);
    }
}
