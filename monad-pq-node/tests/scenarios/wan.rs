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

//! The mcp_test timing (§8.3) over the 8 mcp_test sites' measured
//! latencies, in virtual time.

use std::time::Duration;

use monad_pq_chorus::PqPath;
use monad_pq_node::{
    PqNode,
    chorus::types::{NodeId, Slot, TimestampDelta},
    config::PqNodeConfig,
    network::Packet,
};
use monad_sim_swarm::{Link, Network, NetworkModel, Swarm};
use rand::Rng as _;
use rand_chacha::ChaChaRng;

use crate::harness::{SimNode, assert_consistent_progress, build_swarm, node_config, run_through};

const VALIDATORS: u64 = 8;
const SLOTS: u64 = 300;
const HORIZON: Slot = Slot(SLOTS - 15);
const MIN_FAST_FRACTION: f64 = 0.95;

// p50 RTT ms between the 8 mcp_test hosts, row = from, column = to
// (monad-mcp-node/deploy/config/latency.toml, measured 2026-10-03)
const RTT_MS: [[f64; 8]; 8] = [
    [0.0, 6.71, 69.7, 162.0, 34.4, 163.0, 19.6, 88.4],
    [6.7, 0.0, 75.2, 150.0, 27.8, 153.0, 18.8, 88.1],
    [69.7, 75.2, 0.0, 61.6, 110.0, 207.0, 89.3, 6.97],
    [162.0, 150.0, 61.7, 0.0, 172.0, 176.0, 162.0, 65.6],
    [34.4, 27.8, 110.0, 172.0, 0.0, 195.0, 13.0, 114.0],
    [163.0, 153.0, 207.0, 176.0, 195.0, 0.0, 182.0, 238.0],
    [19.6, 18.8, 89.2, 162.0, 13.0, 182.0, 0.0, 116.0],
    [88.4, 88.1, 6.97, 65.5, 114.0, 238.0, 116.0, 0.0],
];
const JITTER_MS: u64 = 3;

// one-way delay RTT / 2 plus up to JITTER_MS, each message lost with
// probability `loss`
struct Wan {
    loss: f64,
}

impl NetworkModel<NodeId, Packet> for Wan {
    fn deliveries(
        &mut self,
        link: &Link<NodeId>,
        _: &Packet,
        rng: &mut ChaChaRng,
    ) -> Vec<Duration> {
        if self.loss > 0.0 && rng.r#gen::<f64>() < self.loss {
            return Vec::new();
        }
        let from = usize::try_from(u64::from(link.from)).expect("small id");
        let to = usize::try_from(u64::from(link.to)).expect("small id");
        let one_way = Duration::from_secs_f64(RTT_MS[from][to] / 2.0 / 1_000.0);
        vec![one_way + Duration::from_millis(rng.gen_range(0..JITTER_MS))]
    }
}

// the mcp_test values: Δ 150, 100 ms slots, propose 150 ms ahead and
// withhold under 140, decode grace Δ/2 = 75, repeater every 500 ms
fn mcp_test_config(id: u64) -> PqNodeConfig {
    let mut config = node_config(id, VALIDATORS);
    config.base.cadence.delta = TimestampDelta::from_millis(150);
    config.base.cadence.slot_interval = TimestampDelta::from_millis(100);
    config.base.proposal.propose_before_deadline = TimestampDelta::from_millis(150);
    config.base.proposal.withhold_before_deadline = TimestampDelta::from_millis(140);
    config.base.repeater.as_mut().expect("set").interval = TimestampDelta::from_millis(500);
    config
}

fn run_wan(loss: f64, live: u64) -> (Swarm<SimNode>, Vec<NodeId>) {
    let ids: Vec<NodeId> = (0..live).map(NodeId::dummy).collect();
    let nodes = ids
        .iter()
        .map(|&id| {
            let config = mcp_test_config(u64::from(id));
            assert!(config.quantum_config().decode_grace <= TimestampDelta::from_millis(75));
            let node = PqNode::new(&config).expect("config builds");
            (id, SimNode::new(id, node))
        })
        .collect();
    let mut swarm = build_swarm(7, Network::custom(Wan { loss }), nodes);
    run_through(&mut swarm, SLOTS);
    (swarm, ids)
}

#[test]
fn eight_nodes_stay_on_the_fast_path_over_mcp_test_latencies() {
    let (swarm, all) = run_wan(0.0, VALIDATORS);
    assert_consistent_progress(&swarm, &all, HORIZON);
    for id in &all {
        let finalized = swarm.with_node(id, |node| node.finalized().clone());
        let fast = finalized
            .values()
            .filter(|record| record.path == PqPath::Fast)
            .count();
        let fraction = fast as f64 / finalized.len() as f64;
        assert!(
            fraction >= MIN_FAST_FRACTION,
            "{id:?} finalized only {fast} of {} slots fast",
            finalized.len()
        );
    }
}

// one site down and 2% loss: the other 7 finalize every slot, in agreement
#[test]
fn seven_of_eight_finalize_every_slot_over_lossy_mcp_test_latencies() {
    let (swarm, live) = run_wan(0.02, VALIDATORS - 1);
    assert_consistent_progress(&swarm, &live, HORIZON);
}
