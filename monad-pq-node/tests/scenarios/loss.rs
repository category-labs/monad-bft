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
    chorus::types::{NodeId, Slot},
};
use monad_sim_swarm::Network;

use crate::harness::{SimNode, assert_progress, build_swarm, node_config, run_through};

// 8 validators, 2 never start, every wire message dropped with 10%
// probability: the live 6 (exactly the strong quorum) keep finalizing
// through retransmission
#[test]
fn six_of_eight_finalize_through_ten_percent_loss() {
    const VALIDATORS: u64 = 8;
    const LIVE: u64 = 6;
    const SLOTS: u64 = 300;

    let network = Network::reliable(Duration::from_millis(10)).loss(0.1);
    let nodes = (0..LIVE)
        .map(|id| {
            let node = PqNode::new(&node_config(id, VALIDATORS)).expect("config builds");
            (NodeId::dummy(id), SimNode::new(NodeId::dummy(id), node))
        })
        .collect();
    let mut swarm = build_swarm(0xC0FFEE, network, nodes);
    run_through(&mut swarm, SLOTS);

    let live: Vec<NodeId> = (0..LIVE).map(NodeId::dummy).collect();
    assert_progress(&swarm, &live, Slot(SLOTS * 8 / 10));
}
