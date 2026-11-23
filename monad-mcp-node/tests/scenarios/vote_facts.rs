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

//! Vote facts for proposing: a proposing component hears the vote of each
//! validator for each lane once, with the arrival time. A component that
//! records them changes nothing in the run.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use monad_mcp_common::{
    FinalizationPath, Lane, Slot, Timestamp, ValidatorIndex, spec::ValidatorData as _,
};
use monad_mcp_node::{
    Component, Node,
    common::NodeId,
    component::{Proposing, ProposingInput, ProposingOutput},
    election::ProposerSchedule as _,
};
use monad_sim_swarm::Network;

use crate::harness::{
    SLOT_INTERVAL, SimNode, assert_progress, deadline_of_slot, node_config, run_digest,
    start_swarm, time_of,
};

const VALIDATORS: u64 = 4;
const SLOTS: u64 = 30;
const SEED: u64 = 0xFAC7;

#[derive(Default)]
struct Seen {
    // per slot, lane and voter, the arrival time of each vote
    votes: BTreeMap<Slot, BTreeMap<Lane, BTreeMap<ValidatorIndex, Vec<Timestamp>>>>,
    paths: BTreeMap<Slot, FinalizationPath>,
}

// a proposing component that records the facts it is given
struct Recording {
    inner: Proposing,
    seen: Arc<Mutex<Seen>>,
}

impl Component for Recording {
    type Input = ProposingInput;
    type Output = ProposingOutput;

    fn handle(&mut self, now: Timestamp, input: ProposingInput) {
        {
            let mut seen = self.seen.lock().expect("not poisoned");
            match &input {
                ProposingInput::VoteReceived(vote) => {
                    let lanes = seen.votes.entry(vote.slot).or_default();
                    let voters = lanes.entry(vote.lane).or_default();
                    voters.entry(vote.voter).or_default().push(vote.at);
                }
                ProposingInput::Finalized(slot, _, path) => {
                    seen.paths.insert(*slot, *path);
                }
                _ => {}
            }
        }
        self.inner.handle(now, input);
    }

    fn poll(&mut self) -> Option<ProposingOutput> {
        self.inner.poll()
    }

    fn next_due(&self) -> Option<Timestamp> {
        self.inner.next_due()
    }

    fn handle_due(&mut self, now: Timestamp) {
        self.inner.handle_due(now);
    }
}

// `record`: node 0 records the facts that its proposing component gets
fn run(record: bool) -> (u64, Arc<Mutex<Seen>>) {
    let seen = Arc::new(Mutex::new(Seen::default()));
    let network = Network::reliable(Duration::from_millis(10));
    let nodes = (0..VALIDATORS)
        .map(|id| {
            let node_id = NodeId::dummy(id);
            let mut node = Node::new(&node_config(SEED, id, VALIDATORS)).expect("config builds");
            if record && id == 0 {
                let seen = seen.clone();
                node = node.map_proposing(|inner| Box::new(Recording { inner, seen }));
            }
            (node_id, SimNode::new(node_id, node))
        })
        .collect();
    let mut swarm = start_swarm(SEED, network, nodes, None);
    let margin = SLOT_INTERVAL.checked_mul(5).expect("in range");
    swarm.run_until_time(time_of(deadline_of_slot(SLOTS) + margin));
    assert_progress(&swarm, Slot(SLOTS / 2));
    (run_digest(&swarm), seen)
}

#[test]
fn proposing_sees_each_vote_for_each_lane_once() {
    let (digest, seen) = run(true);
    let seen = seen.lock().expect("not poisoned");
    let epoch = node_config(SEED, 0, VALIDATORS).epoch_handle();
    let epoch = epoch.expect("config builds");
    let validators: Vec<ValidatorIndex> = epoch.validator_data.indices().collect();

    let finalized: Vec<Slot> = seen.paths.keys().copied().collect();
    assert!(finalized.len() as u64 >= SLOTS / 2, "{finalized:?}");
    for slot in &finalized {
        let lanes = seen.votes.get(slot).expect("votes for a finalized slot");
        let set = epoch.proposers.proposers_at(*slot).expect("a schedule");
        assert_eq!(lanes.len(), set.num_lanes(), "slot {slot:?}");

        // every validator once, our own vote included, after the deadline
        for voters in lanes.values() {
            let heard: Vec<ValidatorIndex> = voters.keys().copied().collect();
            assert_eq!(heard, validators, "slot {slot:?}");
            for arrivals in voters.values() {
                let [at] = arrivals[..] else {
                    panic!("a vote reported twice at slot {slot:?}");
                };
                assert!(at >= deadline_of_slot(slot.0), "slot {slot:?} at {at:?}");
            }
        }
    }

    // recording changes nothing
    assert_eq!(run(false).0, digest);
}
