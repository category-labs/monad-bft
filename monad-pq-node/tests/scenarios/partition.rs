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

use monad_pq_node::{
    PqNode,
    chorus::types::{NodeId, Slot, Timestamp},
};
use monad_sim_swarm::Network;

use crate::harness::{SimNode, assert_progress, build_swarm, node_config, run_through, time_of};

// §14.4: f of 8 validators cut off for a second, then healed. The other
// 6 keep finalizing; the two catch up through the Finalized repeater, the
// gadget or a cap jump, and nobody diverges
#[test]
fn a_healed_partition_of_f_nodes_catches_up() {
    const VALIDATORS: u64 = 8;
    const SLOTS: u64 = 80;

    let cut = time_of(Timestamp::from_millis(2_000))..time_of(Timestamp::from_millis(3_000));
    let all: Vec<NodeId> = (0..VALIDATORS).map(NodeId::dummy).collect();
    let (rest, isolated) = all.split_at(6);
    let network = Network::reliable(Duration::from_millis(10))
        .partition(cut, [rest.to_vec(), isolated.to_vec()]);
    let nodes = all
        .iter()
        .map(|&id| {
            let node = PqNode::new(&node_config(u64::from(id), VALIDATORS)).expect("config builds");
            (id, SimNode::new(id, node))
        })
        .collect();
    let mut swarm = build_swarm(0xC0FFEE, network, nodes);
    run_through(&mut swarm, SLOTS);

    assert_progress(&swarm, &all, Slot(SLOTS - 10));
    // the majority side never stalled
    for id in rest {
        swarm.with_node(id, |node| {
            for slot in 0..SLOTS - 10 {
                assert!(
                    node.finalized().contains_key(&Slot(slot)),
                    "{id:?} missed {slot}"
                );
            }
        });
    }
}
