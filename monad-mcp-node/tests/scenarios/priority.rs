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

//! Strict first-hop priority on each node's uplink (docs/0005 §5.6):
//! validators over the public-internet table with 1 Gbit/s uplinks and
//! large proposals still finalize, the same sequentially and in parallel,
//! and the first hop leaves no later than without priority.

use monad_mcp_common::Slot;
use monad_mcp_node::{Node, common::NodeId};
use monad_sim_swarm::{Parallel, RegionTable, Swarm};

use crate::harness::{
    GBIT, SLOT_INTERVAL, SimNode, assert_progress, by_region, deadline_of_slot, geo, node_config,
    run_digest, start_swarm, time_of,
};

const VALIDATORS: u64 = 16;
const SLOTS: u64 = 30;
const SEED: u64 = 0x9E1;

fn run(
    priority: bool,
    parallel: impl FnOnce(&crate::harness::Geo) -> Option<Parallel<NodeId>>,
) -> Swarm<SimNode> {
    let geo = geo(&RegionTable::public_internet(), VALIDATORS, SEED);
    let parallel = parallel(&geo);
    let nodes = (0..VALIDATORS)
        .map(|id| {
            let node_id = NodeId::dummy(id);
            let mut config = node_config(SEED, id, VALIDATORS);
            config.proposal.max_payload_bytes = Some(256 << 10);
            let node = Node::new(&config).expect("config builds");
            let sim_node = SimNode::new(node_id, node);
            let sim_node = if priority {
                sim_node.first_hop_priority(GBIT, VALIDATORS)
            } else {
                sim_node
            };
            (node_id, sim_node)
        })
        .collect();
    let mut swarm = start_swarm(SEED, geo.network, nodes, parallel);
    let margin = SLOT_INTERVAL.checked_mul(5).expect("in range");
    swarm.run_until_time(time_of(deadline_of_slot(SLOTS) + margin));
    swarm
}

#[test]
fn first_hop_priority_finalizes_the_same_in_parallel() {
    let sequential = run(true, |_| None);
    assert_progress(&sequential, Slot(SLOTS / 2));
    let digest = run_digest(&sequential);

    let per_region = run(true, |geo| Some(by_region(geo).threads(4)));
    assert_eq!(run_digest(&per_region), digest, "one domain per region");

    // the queue changes when messages leave, so the run differs
    assert_ne!(run_digest(&run(false, |_| None)), digest);
}
