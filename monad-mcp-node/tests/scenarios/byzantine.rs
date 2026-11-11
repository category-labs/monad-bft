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

//! Byzantine DA behavior, simulated with hollow chunks.

use std::time::Duration;

use monad_mcp_common::Slot;
use monad_mcp_node::{Node, common::NodeId, da::ProposalScheme};
use monad_sim_swarm::Network;

use crate::harness::{
    SLOT_INTERVAL, SimNode, assert_consistent_progress, deadline_of_slot, node_config,
    parallel_from_env, start_swarm, time_of,
};

// A proposer whose chunks form no codeword: every chunk is valid under
// its root, so validators receive their chunks and vote, and its lanes
// commit; then every node finds at decoding that the message does not
// encode to the root. With hollow chunks, the proposer declares a root its
// message does not encode to (ProposalScheme::D33HollowInconsistent).
// Every node must resolve those lanes invalid alike and keep finalizing.
#[test]
fn the_lanes_of_an_inconsistent_proposer_resolve_invalid_on_every_node() {
    const VALIDATORS: u64 = 8;
    const BYZANTINE: u64 = 0;
    const SLOTS: u64 = 60;
    const SEED: u64 = 0xB12;
    const MAX_PROPOSAL_LEN: usize = 2000;

    let network = Network::reliable(Duration::from_millis(10));
    let mut nodes = Vec::new();
    for id in 0..VALIDATORS {
        let node_id = NodeId::dummy(id);
        let mut config = node_config(SEED, id, VALIDATORS);
        config.proposal.max_payload_bytes = Some(MAX_PROPOSAL_LEN);
        config.proposal.encoding_scheme = if id == BYZANTINE {
            ProposalScheme::D33HollowInconsistent
        } else {
            ProposalScheme::D33Hollow
        };
        let node = Node::new(&config).expect("config builds");
        nodes.push((node_id, SimNode::new(node_id, node)));
    }

    let mut swarm = start_swarm(SEED, network, nodes, parallel_from_env());
    let margin = SLOT_INTERVAL.checked_mul(5).expect("in range");
    swarm.run_until_time(time_of(deadline_of_slot(SLOTS) + margin));

    // every node hands every slot to its ledger: no lane waits forever for
    // a message
    let horizon = Slot(SLOTS - 20);
    assert_consistent_progress(&swarm, horizon);

    // the byzantine lanes committed and resolved invalid, alike everywhere
    let below = |node: &SimNode| {
        let invalid = node.invalid().range(..horizon);
        invalid
            .map(|(slot, lanes)| (*slot, lanes.clone()))
            .collect::<Vec<_>>()
    };
    let reference = swarm.with_node(&NodeId::dummy(1), below);
    assert!(
        !reference.is_empty(),
        "the byzantine proposer committed no lane"
    );
    for id in swarm.node_ids() {
        assert_eq!(swarm.with_node(&id, below), reference, "node {id:?}");
    }
}
