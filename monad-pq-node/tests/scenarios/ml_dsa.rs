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

//! ML-DSA-44 signatures (dev keys) with fragmented votes and path votes.

use std::time::Duration;

use monad_pq_chorus::PqPath;
use monad_pq_node::{
    PqNode,
    chorus::types::{NodeId, Slot},
    config::{PqNodeConfig, SignatureKind},
    network::Packet,
};
use monad_sim_swarm::{Network, Swarm};

use crate::harness::{SimNode, assert_consistent_progress, build_swarm, node_config, run_through};

const VALIDATORS: u64 = 8;

fn ml_dsa_config(id: u64) -> PqNodeConfig {
    let mut config = node_config(id, VALIDATORS);
    config.quantum.signature = SignatureKind::MlDsa44;
    config
}

fn run(network: Network<NodeId, Packet>, live: u64, slots: u64) -> (Swarm<SimNode>, Vec<NodeId>) {
    let ids: Vec<NodeId> = (0..live).map(NodeId::dummy).collect();
    let nodes = ids
        .iter()
        .map(|&id| {
            let node = PqNode::new(&ml_dsa_config(u64::from(id))).expect("config builds");
            (id, SimNode::new(id, node))
        })
        .collect();
    let mut swarm = build_swarm(0x5157, network, nodes);
    run_through(&mut swarm, slots);
    (swarm, ids)
}

// 4 s of virtual time: every slot finalizes on the fast path, in agreement
#[test]
fn eight_nodes_finalize_fast_with_ml_dsa_44() {
    const SLOTS: u64 = 40;
    let (swarm, ids) = run(
        Network::reliable(Duration::from_millis(10)),
        VALIDATORS,
        SLOTS,
    );
    assert_consistent_progress(&swarm, &ids, Slot(SLOTS - 5));
    for id in &ids {
        let (finalized, fragments) =
            swarm.with_node(id, |node| (node.finalized().clone(), node.fragments_sent()));
        assert!(fragments > 0, "{id:?} never fragmented");
        let slow: Vec<_> = finalized
            .iter()
            .filter(|(_, record)| record.path != PqPath::Fast)
            .map(|(slot, _)| *slot)
            .collect();
        assert!(slow.is_empty(), "{id:?} left the fast path at {slow:?}");
        assert!(swarm.with_node(id, |node| node.evidence().is_empty()));
    }
}

// a lost fragment loses its message: 6 of 8 live with 10% packet loss
// still finalize every slot through retransmission
#[test]
fn six_of_eight_finalize_through_fragment_loss_with_ml_dsa_44() {
    const SLOTS: u64 = 80;
    let network = Network::reliable(Duration::from_millis(10)).loss(0.1);
    let (swarm, live) = run(network, 6, SLOTS);
    assert_consistent_progress(&swarm, &live, Slot(SLOTS - 10));
}
