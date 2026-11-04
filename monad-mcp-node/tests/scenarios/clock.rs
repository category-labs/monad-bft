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

//! Per-node clock offsets: a node's clock reads sim time plus its offset,
//! so its deadlines fire earlier (ahead) or later (behind) in sim time.

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use monad_mcp_common::{Slot, Timestamp};
use monad_mcp_node::{Component, Node, NodeOutput, common::NodeId, network::NodeInbound};
use monad_sim::{ClockDiff, Time};
use monad_sim_swarm::Network;

use crate::harness::{
    SLOT_INTERVAL, SimNode, assert_consistent_progress, deadline_of_slot, node_config,
    parallel_from_env, start_swarm, time_of,
};

const DUE: Timestamp = Timestamp::from_millis(100);

fn millis(ms: i128) -> ClockDiff {
    ClockDiff::from_nanos(ms * 1_000_000)
}

fn at_millis(ms: i128) -> Time {
    Time(ms * 1_000_000)
}

// a component with one timer at local time DUE; records the local times
// its timer fires at
struct Probe {
    due: Option<Timestamp>,
    fired: Arc<Mutex<Vec<Timestamp>>>,
}

impl Component for Probe {
    type Input = NodeInbound;
    type Output = NodeOutput;

    fn handle(&mut self, _now: Timestamp, _input: NodeInbound) {}

    fn poll(&mut self) -> Option<NodeOutput> {
        None
    }

    fn next_due(&self) -> Option<Timestamp> {
        self.due
    }

    fn handle_due(&mut self, now: Timestamp) {
        self.fired.lock().expect("not poisoned").push(now);
        self.due = None;
    }
}

// one probe node with `offset`, optionally stepped to `step.1` at sim time
// `step.0`; returns the sim time its timer fired at and the local times
fn probe(offset: ClockDiff, step: Option<(Time, ClockDiff)>) -> (Time, Vec<Timestamp>) {
    let fired = Arc::new(Mutex::new(Vec::new()));
    let node = Probe {
        due: Some(DUE),
        fired: fired.clone(),
    };
    let id = NodeId::dummy(0);
    let sim_node = SimNode::new(id, node).with_clock_offset(offset);
    let network = Network::reliable(Duration::from_millis(1));
    let mut swarm = start_swarm(0, network, vec![(id, sim_node)], None);
    let handle = swarm.handle(&id).expect("node just built");
    if let Some((at, offset)) = step {
        swarm.sim().schedule(handle, at, "step", move |node, ctx| {
            node.set_clock_offset(offset, ctx)
        });
    }
    swarm.run_to_completion();
    let now = swarm.simulation().now();
    let fired = fired.lock().expect("not poisoned").clone();
    (now, fired)
}

#[test]
fn a_node_ahead_acts_early() {
    let (at, fired) = probe(millis(30), None);
    assert_eq!(at, at_millis(70));
    assert_eq!(fired, vec![DUE]);
}

#[test]
fn a_node_behind_acts_late() {
    let (at, fired) = probe(millis(-30), None);
    assert_eq!(at, at_millis(130));
    assert_eq!(fired, vec![DUE]);
}

// stepping the clock forward by 50 ms at sim 20 ms moves the pending
// due time from sim 100 ms to sim 50 ms
#[test]
fn a_clock_step_moves_the_pending_due() {
    let (at, fired) = probe(ClockDiff(0), Some((at_millis(20), millis(50))));
    assert_eq!(at, at_millis(50));
    assert_eq!(fired, vec![DUE]);
}

// 8 validators whose clocks spread over 13 ms keep finalizing, all agreeing
#[test]
fn offset_clocks_keep_finalizing() {
    const VALIDATORS: u64 = 8;
    const SLOTS: u64 = 60;
    const OFFSETS_MS: [i128; VALIDATORS as usize] = [-6, -3, -1, 0, 1, 2, 4, 7];

    const SEED: u64 = 0xC0FFEE;

    let network = Network::reliable(Duration::from_millis(10));
    let mut nodes = Vec::new();
    for id in 0..VALIDATORS {
        let node_id = NodeId::dummy(id);
        let node = Node::new(&node_config(SEED, id, VALIDATORS)).expect("config builds");
        let offset = millis(OFFSETS_MS[usize::try_from(id).expect("small")]);
        nodes.push((
            node_id,
            SimNode::new(node_id, node).with_clock_offset(offset),
        ));
    }

    let mut swarm = start_swarm(SEED, network, nodes, parallel_from_env());

    let margin = SLOT_INTERVAL.checked_mul(5).expect("in range");
    swarm.run_until_time(time_of(deadline_of_slot(SLOTS) + margin));

    assert_consistent_progress(&swarm, Slot(SLOTS - 20));
}
