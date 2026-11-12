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

//! Vote facts for proposing: a proposing component that wants them sees
//! every peer's batch vote with its arrival time, one that does not sees
//! none, and either way the run is the same as without them.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
    time::Duration,
};

use monad_mcp_common::{Slot, Timestamp};
use monad_mcp_node::{
    Component, Node,
    cadence::slot::chorus::FinalizationPath,
    common::NodeId,
    component::{Proposing, ProposingComponent, ProposingInput, ProposingOutput},
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
    // per slot, the voters heard from, with the first arrival
    votes: BTreeMap<Slot, BTreeMap<NodeId, Timestamp>>,
    paths: BTreeMap<Slot, FinalizationPath>,
}

// a proposing component that records the facts it is given
struct Recording {
    inner: Proposing,
    wants: bool,
    seen: Arc<Mutex<Seen>>,
}

impl ProposingComponent for Recording {
    fn wants_vote_facts(&self) -> bool {
        self.wants
    }
}

impl Component for Recording {
    type Input = ProposingInput;
    type Output = ProposingOutput;

    fn handle(&mut self, now: Timestamp, input: ProposingInput) {
        {
            let mut seen = self.seen.lock().expect("not poisoned");
            match &input {
                ProposingInput::BatchVote { slot, from, at, .. } => {
                    let voters = seen.votes.entry(*slot).or_default();
                    voters.entry(*from).or_insert(*at);
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

// node 0 records with vote facts, node 1 records without; `wrap` false:
// no node is wrapped
fn run(wrap: bool) -> (u64, [Arc<Mutex<Seen>>; 2]) {
    let seen = [(); 2].map(|()| Arc::new(Mutex::new(Seen::default())));
    let network = Network::reliable(Duration::from_millis(10));
    let nodes = (0..VALIDATORS)
        .map(|id| {
            let node_id = NodeId::dummy(id);
            let mut node = Node::new(&node_config(SEED, id, VALIDATORS)).expect("config builds");
            if wrap && id < 2 {
                let seen = seen[id as usize].clone();
                node = node.map_proposing(|inner| {
                    let wants = id == 0;
                    Box::new(Recording { inner, wants, seen })
                });
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
fn proposing_sees_vote_facts_only_when_it_wants_them() {
    let (digest, [wanting, not_wanting]) = run(true);
    let wanting = wanting.lock().expect("not poisoned");

    // every peer's batch vote of each finalized slot, after its deadline;
    // never our own (the router sees only inbound messages)
    let peers: BTreeSet<NodeId> = (1..VALIDATORS).map(NodeId::dummy).collect();
    let finalized: Vec<Slot> = wanting.paths.keys().copied().collect();
    assert!(finalized.len() as u64 >= SLOTS / 2, "{finalized:?}");
    for slot in &finalized {
        let voters = wanting.votes.get(slot).expect("votes for a finalized slot");
        let heard: BTreeSet<NodeId> = voters.keys().copied().collect();
        assert_eq!(heard, peers, "slot {slot:?}");
        for at in voters.values() {
            assert!(*at >= deadline_of_slot(slot.0), "slot {slot:?} at {at:?}");
        }
    }

    // the finalization path comes with each finalized slot; without vote
    // facts, only that
    let not_wanting = not_wanting.lock().expect("not poisoned");
    assert!(not_wanting.votes.is_empty());
    assert_eq!(not_wanting.paths, wanting.paths);

    // observing changes nothing
    assert_eq!(run(false).0, digest);
}
