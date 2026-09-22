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

use std::time::Duration;

use monad_mcp_node::{
    Effect, Node,
    chorus::types::{NodeId, Slot},
};
use monad_sim::Time;
use monad_sim_swarm::{Network, Swarm};

use crate::{
    harness::{
        SLOT_INTERVAL, SimNode, assert_consistent_progress, deadline_of_slot, node_config, time_of,
    },
    tamper::Tampered,
};

// 8 validators, 2 of them run but never send: the other 6 keep
// finalizing, and the mute ones follow along on what they receive
#[test]
fn six_of_eight_finalize_beside_two_mute_nodes() {
    const VALIDATORS: u64 = 8;
    const MUTE: u64 = 2;
    const SLOTS: u64 = 60;

    let network = Network::reliable(Duration::from_millis(10));
    let mut nodes = Vec::new();
    for id in 0..VALIDATORS {
        let node_id = NodeId::dummy(id);
        let node = Node::new(&node_config(id, VALIDATORS)).expect("config builds");
        let sim_node = if id < MUTE {
            let node = node.map_runtime(|runtime| Tampered::from(runtime).filtered(drop_network));
            SimNode::new(node_id, node)
        } else {
            SimNode::new(node_id, node)
        };
        nodes.push((node_id, sim_node));
    }

    let mut swarm = Swarm::build(0xC0FFEE, network, nodes);
    for id in swarm.node_ids() {
        let handle = swarm.handle(&id).expect("node just built");
        let sim = swarm.sim();
        sim.schedule(handle, Time(0), "init", |node, ctx| node.init(ctx));
    }

    let margin = SLOT_INTERVAL.checked_mul(5).expect("in range");
    swarm.run_until_time(time_of(deadline_of_slot(SLOTS) + margin));

    assert_consistent_progress(&swarm, Slot(SLOTS - 20));
}

fn drop_network(effect: Effect) -> Vec<Effect> {
    match effect {
        Effect::Network(_) => vec![],
        effect => vec![effect],
    }
}
