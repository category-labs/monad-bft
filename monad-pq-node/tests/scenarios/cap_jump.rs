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

use monad_mcp_node::config::SourceKind;
use monad_pq_node::{
    PqNode,
    chorus::types::{NodeId, Slot, Timestamp},
};
use monad_sim_swarm::Network;

use crate::harness::{
    SimNode, assert_consistent_progress, assert_progress, build_swarm, node_config, run_through,
    time_of,
};

// 2 of 8 validators cut off for 4 s, well past cadence.lag_threshold (20
// slots): they rejoin by a cap jump over the slots they missed (the chain
// gate, mempool expiry and collector all follow the conductor's cap), and
// nobody diverges
#[test]
fn a_partition_past_the_lag_threshold_heals_by_a_cap_jump() {
    const VALIDATORS: u64 = 8;
    const SLOTS: u64 = 120;
    const HORIZON: Slot = Slot(SLOTS - 10);

    let cut = time_of(Timestamp::from_millis(2_000))..time_of(Timestamp::from_millis(6_000));
    let all: Vec<NodeId> = (0..VALIDATORS).map(NodeId::dummy).collect();
    let (rest, isolated) = all.split_at(6);
    let network = Network::reliable(Duration::from_millis(10))
        .partition(cut, [rest.to_vec(), isolated.to_vec()]);
    let nodes = all
        .iter()
        .map(|&id| {
            let mut config = node_config(u64::from(id), VALIDATORS);
            config.base.proposal.source = SourceKind::Mempool;
            let node = PqNode::new(&config).expect("config builds");
            (id, SimNode::new(id, node))
        })
        .collect();
    let mut swarm = build_swarm(0xC0FFEE, network, nodes);
    run_through(&mut swarm, SLOTS);

    assert_progress(&swarm, &all, HORIZON);
    assert_consistent_progress(&swarm, rest, HORIZON);
    // the isolated pair skipped slots: it caught up by the jump, not slot by slot
    for id in isolated {
        let finalized = swarm.with_node(id, |node| node.finalized().clone());
        let skipped = (0..HORIZON.0)
            .filter(|&slot| !finalized.contains_key(&Slot(slot)))
            .count();
        assert!(skipped > 0, "{id:?} never jumped");
    }
}
