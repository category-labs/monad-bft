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
    Node, Runtime,
    chorus::types::{NodeId, Slot},
    component::ProposingOutput,
};
use monad_sim::Time;
use monad_sim_swarm::{Network, Swarm};

use crate::{
    harness::{SLOT_INTERVAL, SimNode, assert_progress, deadline_of_slot, node_config, time_of},
    tamper::Tampered,
};

fn proposal_capped<R: Runtime>(runtime: R, max_len: usize) -> Tampered<R> {
    let mut tampered = Tampered::from(runtime);

    tampered.proposal_handler = Some(Box::new(
        move |runtime: &mut R, now, proposal: ProposingOutput| {
            let mut effects = vec![];
            let (slot, index, mut message) = proposal;
            message.truncate(max_len);
            let proposal = (slot, index, message);
            runtime.handle_proposal(now, proposal, &mut effects);
            effects
        },
    ));

    tampered
}

// 8 validators, 2 never start, every wire message dropped with 10%
// probability: the live 6 keep finalizing
#[test]
fn six_of_eight_finalize_through_ten_percent_loss() {
    const VALIDATORS: u64 = 8;
    const DOWN: u64 = 2;
    const SLOTS: u64 = 500;
    const MAX_PROPOSAL_LEN: usize = 2000;

    let network = Network::reliable(Duration::from_millis(10)).loss(0.1);

    let mut nodes = Vec::new();
    for id in 0..VALIDATORS {
        let node_id = NodeId::dummy(id);
        let node = Node::new(&node_config(id, VALIDATORS)).expect("config builds");
        // cap proposal size to reduce # of chunks
        let node = node.map_runtime(|runtime| proposal_capped(runtime, MAX_PROPOSAL_LEN));
        let sim_node = SimNode::new(node_id, node);
        nodes.push((node_id, sim_node));
    }

    for _ in 0..DOWN {
        // drop the last `DOWN` nodes, so they never start
        nodes.pop().expect("enough nodes");
    }

    let mut swarm = Swarm::build(0xC0FFEE, network, nodes);
    for id in swarm.node_ids() {
        let handle = swarm.handle(&id).expect("node just built");
        let sim = swarm.sim();
        sim.schedule(handle, Time(0), "init", |node, ctx| node.init(ctx));
    }

    let margin = SLOT_INTERVAL.checked_mul(5).expect("in range");
    swarm.run_until_time(time_of(deadline_of_slot(SLOTS) + margin));

    // should progress to >= 80% of the slots. todo: strengthen this
    // to assert_consistent_progress.
    assert_progress(&swarm, Slot(SLOTS * 8 / 10));
}
