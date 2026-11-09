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

use monad_mcp_common::Slot;
use monad_mcp_node::{Node, common::NodeId, da::ProposalScheme};
use monad_sim_swarm::Network;

use crate::harness::{
    SLOT_INTERVAL, SimNode, assert_progress, deadline_of_slot, node_config, parallel_from_env,
    start_swarm, time_of,
};

// 8 validators, 2 never start, every wire message dropped with 10%
// probability: the live 6 keep finalizing
#[test]
fn six_of_eight_finalize_through_ten_percent_loss() {
    six_of_eight_finalize_under(ProposalScheme::D33);
}

// recovery from loss works the same without chunk data on the wire
#[test]
fn six_of_eight_finalize_hollow_chunks_through_ten_percent_loss() {
    six_of_eight_finalize_under(ProposalScheme::D33Hollow);
}

fn six_of_eight_finalize_under(scheme: ProposalScheme) {
    const VALIDATORS: u64 = 8;
    const DOWN: u64 = 2;
    const SLOTS: u64 = 500;
    const SEED: u64 = 0xC0FFEE;
    const MAX_PROPOSAL_LEN: usize = 2000;

    let network = Network::reliable(Duration::from_millis(10)).loss(0.1);

    let mut nodes = Vec::new();
    for id in 0..VALIDATORS {
        let node_id = NodeId::dummy(id);
        let mut config = node_config(SEED, id, VALIDATORS);
        // cap proposal size to reduce # of chunks
        config.proposal.max_payload_bytes = Some(MAX_PROPOSAL_LEN);
        config.proposal.encoding_scheme = scheme;
        let node = Node::new(&config).expect("config builds");
        let sim_node = SimNode::new(node_id, node);
        nodes.push((node_id, sim_node));
    }

    for _ in 0..DOWN {
        // drop the last `DOWN` nodes, so they never start
        nodes.pop().expect("enough nodes");
    }

    let mut swarm = start_swarm(SEED, network, nodes, parallel_from_env());

    let margin = SLOT_INTERVAL.checked_mul(5).expect("in range");
    swarm.run_until_time(time_of(deadline_of_slot(SLOTS) + margin));

    // should progress to >= 80% of the slots. todo: strengthen this
    // to assert_consistent_progress.
    assert_progress(&swarm, Slot(SLOTS * 8 / 10));
}
