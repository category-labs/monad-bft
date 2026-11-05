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

//! The adaptive proposal lead (docs/0005) on the public-internet table:
//! 1 Gbit/s uplinks with first-hop priority, fixed-size proposals, the
//! deployed static lead of 150 ms as the cold start. Once warm, every
//! proposer seals at its own lead, most well after the static one, and the
//! slots keep the fast path.
//!
//! The run at n = 64 is slow in a debug build; run it with
//! `cargo test --release -p monad-mcp-node --test scenarios
//!     adaptive::converges -- --ignored --nocapture`
//! (`MCP_SIM_THREADS` runs it in parallel, with the same result).

use std::collections::BTreeMap;

use monad_mcp_common::{Slot, TimestampDelta};
use monad_mcp_node::{Node, cadence::slot::chorus::FinalizationPath, common::NodeId};
use monad_sim_swarm::{Parallel, RegionTable, Swarm};

use crate::harness::{
    DELTA, GBIT, Geo, SLOT_INTERVAL, SimNode, assert_progress, by_region, deadline_of_slot, geo,
    node_config, parallel_from_env, run_digest, start_swarm, time_of, timestamp_of,
};

const SEED: u64 = 0xADA;
// the deployed static lead, the cold start
const STATIC_LEAD_MS: u64 = 150;
// warm-up (20 observed slots) plus margin: steady state from here
const STEADY: u64 = 30;

fn run(
    validators: u64,
    slots: u64,
    payload: usize,
    parallel: impl FnOnce(&Geo) -> Option<Parallel<NodeId>>,
) -> Swarm<SimNode> {
    use monad_mcp_node::config::SourceKind;

    let geo = geo(&RegionTable::public_internet(), validators, SEED);
    let parallel = parallel(&geo);
    let nodes = (0..validators)
        .map(|id| {
            let node_id = NodeId::dummy(id);
            let mut config = node_config(SEED, id, validators);
            config.proposal.source = SourceKind::Fixed;
            config.proposal.max_payload_bytes = Some(payload);
            config.proposal.propose_before_deadline = TimestampDelta::from_millis(STATIC_LEAD_MS);
            config.proposal.adaptive.enabled = true;
            config.proposal.adaptive.max_lead_ms = DELTA.as_millis() as f64;
            let node = Node::new(&config).expect("config builds");
            let sim_node = SimNode::new(node_id, node).first_hop_priority(GBIT, validators);
            (node_id, sim_node)
        })
        .collect();
    let mut swarm = start_swarm(SEED, geo.network, nodes, parallel);
    let margin = SLOT_INTERVAL.checked_mul(5).expect("in range");
    swarm.run_until_time(time_of(deadline_of_slot(slots) + margin));
    swarm
}

// per proposer, the leads of its steady-state seals, ms
fn steady_leads(swarm: &Swarm<SimNode>, horizon: u64) -> BTreeMap<NodeId, Vec<f64>> {
    let mut leads = BTreeMap::new();
    for id in swarm.node_ids() {
        let first_hops = swarm.with_node(&id, |node| node.first_hops().clone());
        let own: Vec<f64> = first_hops
            .range(Slot(STEADY)..Slot(horizon))
            .map(|(slot, at)| {
                let sealed = timestamp_of(*at);
                let lead = deadline_of_slot(slot.0).duration_since(sealed);
                lead.expect("sealed before the deadline").as_nanos() as f64 / 1e6
            })
            .collect();
        if !own.is_empty() {
            leads.insert(id, own);
        }
    }
    leads
}

fn fallbacks(swarm: &Swarm<SimNode>, from: u64, horizon: u64) -> Vec<Slot> {
    let id = swarm.node_ids()[0];
    let paths = swarm.with_node(&id, |node| node.paths().clone());
    paths
        .range(Slot(from)..Slot(horizon))
        .filter(|(_, path)| **path == FinalizationPath::Fallback)
        .map(|(slot, _)| *slot)
        .collect()
}

fn mean(values: &[f64]) -> f64 {
    values.iter().sum::<f64>() / values.len() as f64
}

// small and cheap: the adaptive run is the same sequentially and with one
// domain per region
#[test]
fn an_adaptive_run_is_the_same_in_parallel() {
    const VALIDATORS: u64 = 16;
    const SLOTS: u64 = 45;
    let sequential = run(VALIDATORS, SLOTS, 64 << 10, |_| None);
    assert_progress(&sequential, Slot(SLOTS / 2));
    let digest = run_digest(&sequential);
    let per_region = run(VALIDATORS, SLOTS, 64 << 10, |geo| {
        Some(by_region(geo).threads(4))
    });
    assert_eq!(run_digest(&per_region), digest, "one domain per region");
    assert!(!steady_leads(&sequential, SLOTS).is_empty(), "warm seals");
}

// n = 64, 256 KiB proposals, the default θ = 1/4: the adaptive lead
// converges without splits. Here a region's validators share one latency
// (no clock offsets, at most 1 ms of jitter) and cross their threshold as
// one block; with independent votes in the risk rule, the vote calibration
// walked the lead down until such a block flipped, past 1/3 in 2 of ~85
// steady slots. The block variance (5 ms blocks) keeps the margin
#[test]
#[ignore = "slow: run with --release"]
fn converges() {
    const VALIDATORS: u64 = 64;
    const SLOTS: u64 = 120;
    let horizon = SLOTS - 5;
    let swarm = run(VALIDATORS, SLOTS, 256 << 10, |_| parallel_from_env());
    assert_progress(&swarm, Slot(horizon));
    println!("digest: {:016x}", run_digest(&swarm));

    let leads = steady_leads(&swarm, horizon);
    for (id, own) in &leads {
        let max = own.iter().copied().fold(f64::MIN, f64::max);
        let min = own.iter().copied().fold(f64::MAX, f64::min);
        println!(
            "proposer {:>2}: {:>3} seals, lead mean {:6.1} ms, min {:6.1}, max {:6.1}",
            u64::from(*id),
            own.len(),
            mean(own),
            min,
            max
        );
    }
    let all: Vec<f64> = leads.values().flatten().copied().collect();
    println!(
        "{} steady seals from {} proposers, mean lead {:.1} ms",
        all.len(),
        leads.len(),
        mean(&all)
    );
    let fallbacks = fallbacks(&swarm, STEADY, horizon);
    println!("steady-state fallback slots: {fallbacks:?}");

    assert!(leads.len() >= 4, "steady seals from several proposers");
    assert_eq!(fallbacks, [], "no splits in steady state");
    // every seal adapted: within the clamps
    assert!(
        all.iter()
            .all(|lead| (20.0..=DELTA.as_millis() as f64 + 1.0).contains(lead))
    );
    // the nearest proposer seals well after the static lead
    let nearest = leads.values().map(|own| mean(own)).fold(f64::MAX, f64::min);
    assert!(
        nearest < STATIC_LEAD_MS as f64 - 30.0,
        "nearest proposer's mean lead {nearest:.1} ms"
    );
}
