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

use monad_pq_chorus::PqPath;
use monad_pq_node::{
    PqEffect, PqNode,
    chorus::types::{NodeId, Slot},
};
use monad_sim_swarm::Network;

use crate::{
    harness::{SimNode, assert_consistent_progress, build_swarm, node_config, run_through},
    tamper::Tampered,
};

// 8 validators, 2 of them run but never send: the other 6 keep finalizing
// on the fast path, and the mute ones follow through the gadget
#[test]
fn six_of_eight_finalize_beside_two_mute_nodes() {
    const VALIDATORS: u64 = 8;
    const MUTE: u64 = 2;
    const SLOTS: u64 = 80;

    let network = Network::reliable(Duration::from_millis(10));
    let nodes = (0..VALIDATORS)
        .map(|id| {
            let node_id = NodeId::dummy(id);
            let node = PqNode::new(&node_config(id, VALIDATORS)).expect("config builds");
            let sim_node = if id < MUTE {
                let node =
                    node.map_runtime(|runtime| Tampered::from(runtime).filtered(drop_network));
                SimNode::new(node_id, node)
            } else {
                SimNode::new(node_id, node)
            };
            (node_id, sim_node)
        })
        .collect();
    let mut swarm = build_swarm(0xC0FFEE, network, nodes);
    run_through(&mut swarm, SLOTS);

    let all: Vec<NodeId> = (0..VALIDATORS).map(NodeId::dummy).collect();
    assert_consistent_progress(&swarm, &all, Slot(SLOTS - 20));
    // the 6 that send are exactly the strong quorum: no slot needs Coda
    for id in &all {
        swarm.with_node(id, |node| {
            assert!(
                node.finalized()
                    .values()
                    .all(|record| record.path != PqPath::Coda),
                "{id:?} fell back to Coda"
            );
        });
    }
}

fn drop_network(effect: PqEffect) -> Vec<PqEffect> {
    match effect {
        PqEffect::Network(_) => vec![],
        effect => vec![effect],
    }
}
