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

//! A node that takes hollow proposals up long after every other node let
//! go of them. Hollow messages live in a store the nodes of the process
//! share; a node within the horizon (`encoding_scheme::hollow::HORIZON`
//! in monad-mcp-da, 10 s) of a proposal's authoring takes its message
//! however late it comes, and beyond it resolves the proposal invalid, on
//! any number of threads.

use std::{collections::VecDeque, time::Duration};

use monad_mcp_common::{Slot, Timestamp, TimestampDelta};
use monad_mcp_node::{
    Node, NodeOutput, common::NodeId, component::Component, da::ProposalScheme,
    network::NodeInbound,
};
use monad_sim_swarm::{Network, Parallel, Swarm};

use crate::harness::{
    SLOT_INTERVAL, SimNode, deadline_of_slot, node_config, run_digest, start_swarm, time_of,
};

const VALIDATORS: u64 = 8;
const LAGGING: u64 = VALIDATORS - 1;
const SLOTS: u64 = 250;
const SEED: u64 = 0x1A6;
const MAX_PROPOSAL_LEN: usize = 2000;

// a node that handles every message it receives `lag` after it arrives,
// in arrival order: it sees the run as it was `lag` ago
struct Lagging {
    node: Node,
    lag: TimestampDelta,
    held: VecDeque<(Timestamp, NodeInbound)>,
}

impl Component for Lagging {
    type Input = NodeInbound;
    type Output = NodeOutput;

    fn handle(&mut self, now: Timestamp, input: NodeInbound) {
        self.held.push_back((now + self.lag, input));
    }

    fn poll(&mut self) -> Option<NodeOutput> {
        self.node.poll()
    }

    fn next_due(&self) -> Option<Timestamp> {
        let held = self.held.front().map(|(at, _)| *at);
        match (self.node.next_due(), held) {
            (Some(due), Some(held)) => Some(due.min(held)),
            (due, held) => due.or(held),
        }
    }

    fn handle_due(&mut self, now: Timestamp) {
        while let Some((at, _)) = self.held.front() {
            if *at > now {
                break;
            }
            let (_, input) = self.held.pop_front().expect("a front");
            self.node.handle(now, input);
        }
        if self.node.next_due().is_some_and(|due| due <= now) {
            self.node.handle_due(now);
        }
    }
}

// hollow validators, the last of which handles every message `lag` late
fn run(lag: Duration, parallel: Option<Parallel<NodeId>>) -> Swarm<SimNode> {
    let network = Network::reliable(Duration::from_millis(10));
    let lag = TimestampDelta::from_millis(lag.as_millis() as u64);
    let mut nodes = Vec::new();
    for id in 0..VALIDATORS {
        let node_id = NodeId::dummy(id);
        let mut config = node_config(SEED, id, VALIDATORS);
        config.proposal.max_payload_bytes = Some(MAX_PROPOSAL_LEN);
        config.proposal.encoding_scheme = ProposalScheme::D33Hollow;
        let sim_node = if id == LAGGING {
            // it keeps its slots open until it catches up with them
            config.da.completed_slot_retention = 500;
            let node = Node::new(&config).expect("config builds");
            let lagging = Lagging {
                node,
                lag,
                held: VecDeque::new(),
            };
            SimNode::new(node_id, lagging)
        } else {
            SimNode::new(node_id, Node::new(&config).expect("config builds"))
        };
        nodes.push((node_id, sim_node));
    }

    let mut swarm = start_swarm(SEED, network, nodes, parallel);
    let margin = SLOT_INTERVAL.checked_mul(5).expect("in range");
    swarm.run_until_time(time_of(deadline_of_slot(SLOTS) + margin));
    swarm
}

// the slots, from the 20th, that the lagging node handed to its ledger,
// with the lanes it found invalid; and what the others found invalid
fn outcomes(swarm: &Swarm<SimNode>) -> (Vec<Slot>, Vec<(Slot, Vec<usize>)>, usize) {
    let lagging = NodeId::dummy(LAGGING);
    let (slots, invalid) = swarm.with_node(&lagging, |node| {
        let slots: Vec<Slot> = node
            .finalized()
            .range(Slot(20)..)
            .map(|(s, _)| *s)
            .collect();
        let invalid: Vec<(Slot, Vec<usize>)> = node
            .invalid()
            .range(Slot(20)..)
            .map(|(slot, lanes)| (*slot, lanes.clone()))
            .collect();
        (slots, invalid)
    });
    let mut others_invalid = 0;
    for id in 0..LAGGING {
        others_invalid += swarm.with_node(&NodeId::dummy(id), |node| node.invalid().len());
    }
    (slots, invalid, others_invalid)
}

// The lagging node takes every proposal up about 7 s after it was
// authored: past the 50 slots (5 s) every other node keeps a slot after
// the cap, so every node but the author has dropped it. Within the
// horizon, it decodes them all.
#[test]
fn a_node_lagging_within_the_horizon_decodes_every_proposal() {
    let swarm = run(Duration::from_secs(7), None);
    let (slots, invalid, others_invalid) = outcomes(&swarm);
    assert!(
        slots.len() > 100,
        "the lagging node finalized {} slots",
        slots.len()
    );
    assert_eq!(invalid, vec![], "the lagging node found lanes invalid");
    assert_eq!(others_invalid, 0);
    // and finalized what the others did
    let lagging = NodeId::dummy(LAGGING);
    let lagging_view = swarm.with_node(&lagging, |node| node.finalized().clone());
    let reference = swarm.with_node(&NodeId::dummy(0), |node| node.finalized().clone());
    for (slot, pattern) in &lagging_view {
        if let Some(other) = reference.get(slot) {
            assert_eq!(pattern, other, "slot {slot:?}");
        }
    }
}

// The lagging node takes proposals up about 13 s after they were
// authored, beyond the horizon: it resolves every other node's proposal
// invalid at k chunks, and still hands every slot to its ledger.
#[test]
fn a_node_lagging_beyond_the_horizon_resolves_proposals_invalid() {
    let swarm = run(Duration::from_secs(13), None);
    let (slots, invalid, others_invalid) = outcomes(&swarm);
    assert!(
        slots.len() > 50,
        "the lagging node finalized {} slots",
        slots.len()
    );
    assert_eq!(others_invalid, 0);
    // every slot it finalized has invalid lanes: all but its own
    let invalid_slots: Vec<Slot> = invalid.iter().map(|(slot, _)| *slot).collect();
    assert_eq!(invalid_slots, slots);
}

// Within and beyond the horizon, the run is the same sequentially and on
// any number of threads: no outcome depends on when, by the wall clock,
// another node let go of a message.
#[test]
fn a_lagging_node_does_not_depend_on_the_thread_count() {
    for lag in [Duration::from_secs(7), Duration::from_secs(13)] {
        let sequential = run_digest(&run(lag, None));
        for threads in [2, 4, 10] {
            let parallel = Parallel::default().threads(threads);
            let digest = run_digest(&run(lag, Some(parallel)));
            assert_eq!(digest, sequential, "{lag:?} lag, {threads} threads");
        }
    }
}
