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

//! Measurements, ignored by default. Run with `--release` and `--ignored
//! --nocapture`, e.g.
//!
//! ```text
//! BENCH_N=64,190 cargo test --release -p monad-mcp-node --test scenarios \
//!     bench::slot_cost -- --ignored --nocapture
//! ```
//!
//! - `scaling`: the sequential run's available parallelism `W / D` and wall
//!   time, then the parallel engine's speedup per thread count (one domain
//!   per node), with the run digest to show the runs are the same.
//! - `slot_cost`: wall-clock per simulated slot with every proposal exactly
//!   400 KB, on the testnet table and on the public-internet table, run
//!   sequentially and in parallel (one domain per node, or per region).
//!
//! Knobs: `BENCH_N` (validator counts, comma separated), `BENCH_SLOTS`,
//! `BENCH_THREADS` (comma separated), `BENCH_PAYLOAD` (bytes; `scaling` caps
//! random payloads at it, `slot_cost` fixes them to it); for `slot_cost`,
//! `BENCH_DOMAINS` (`node`, the default, or `region`), `BENCH_GROUPS` (the
//! target group count with one domain per node) and `BENCH_SEQUENTIAL=0` to
//! skip the sequential run. `BENCH_SCHEME=d33_hollow` runs either bench
//! without chunk data on the wire; `BENCH_SHARED_ENCODINGS=1` runs it with
//! the nodes sharing their encodings (same runs, less work).

use std::time::{Duration, Instant};

use monad_mcp_node::{Node, common::NodeId, config::SourceKind, da::ProposalScheme};
use monad_sim::{DEFAULT_GROUPS, Grouping};
use monad_sim_swarm::{Network, Parallel, RegionTable, Swarm};

use crate::harness::{
    ChunkBytes, SLOT_INTERVAL, SimNode, by_region, deadline_of_slot, geo, node_config, run_digest,
    start_swarm, testnet_table, time_of,
};

fn list(name: &str, default: &[u64]) -> Vec<u64> {
    match std::env::var(name) {
        Ok(value) => value
            .split(',')
            .map(|v| v.trim().parse().expect("a number"))
            .collect(),
        Err(_) => default.to_vec(),
    }
}

fn one(name: &str, default: u64) -> u64 {
    list(name, &[default])[0]
}

fn scheme() -> ProposalScheme {
    match std::env::var("BENCH_SCHEME").as_deref() {
        Err(_) | Ok("d33") => ProposalScheme::D33,
        Ok("d33_hollow") => ProposalScheme::D33Hollow,
        Ok(other) => panic!("BENCH_SCHEME is d33 or d33_hollow, not {other}"),
    }
}

fn nodes(seed: u64, validators: u64, payload: usize, source: SourceKind) -> Vec<(NodeId, SimNode)> {
    (0..validators)
        .map(|id| {
            let node_id = NodeId::dummy(id);
            let mut config = node_config(seed, id, validators);
            config.proposal.source = source;
            config.proposal.max_payload_bytes = Some(payload);
            config.proposal.encoding_scheme = scheme();
            config.da.shared_encodings =
                std::env::var("BENCH_SHARED_ENCODINGS").is_ok_and(|v| v == "1");
            let node = Node::new(&config).expect("config builds");
            (node_id, SimNode::new(node_id, node))
        })
        .collect()
}

// the slot every node finalized through, and the lanes committed in all
// the slots node 0 finalized: equal runs did the same protocol work
fn progress(swarm: &Swarm<SimNode>) -> (u64, usize) {
    let mut through = u64::MAX;
    for id in swarm.node_ids() {
        let last = swarm.with_node(&id, |node| node.finalized().keys().next_back().copied());
        through = through.min(last.map_or(0, |slot| slot.0));
    }
    let committed = swarm.with_node(&NodeId::dummy(0), |node| {
        let mut committed = 0;
        for pattern in node.finalized().values() {
            for lane in pattern.lanes() {
                let entry = pattern.get(usize::from(lane)).expect("in range");
                committed += usize::from(entry.is_some());
            }
        }
        committed
    });
    (through, committed)
}

// the chunk bytes delivered to every node, summed over the nodes
fn chunk_bytes(swarm: &Swarm<SimNode>) -> ChunkBytes {
    let mut total = ChunkBytes::default();
    for id in swarm.node_ids() {
        let node = swarm.with_node(&id, SimNode::chunk_bytes);
        total.wire += node.wire;
        total.charged += node.charged;
    }
    total
}

// run `slots` slots; the wall time covers the run only, not the build
fn timed(mut swarm: Swarm<SimNode>, slots: u64) -> (Swarm<SimNode>, Duration) {
    let margin = SLOT_INTERVAL.checked_mul(5).expect("in range");
    let started = Instant::now();
    swarm.run_until_time(time_of(deadline_of_slot(slots) + margin));
    (swarm, started.elapsed())
}

#[test]
#[ignore = "measurement"]
fn scaling() {
    const SEED: u64 = 0x5CA1E;
    let slots = one("BENCH_SLOTS", 20);
    let payload = one("BENCH_PAYLOAD", 2000) as usize;
    let network = || Network::reliable(Duration::from_millis(10)).loss(0.05);
    for validators in list("BENCH_N", &[16, 64]) {
        let build = |parallel| {
            let nodes = nodes(SEED, validators, payload, SourceKind::Random);
            start_swarm(SEED, network(), nodes, parallel)
        };
        let (swarm, wall) = timed(build(None), slots);
        let metrics = swarm.simulation().metrics();
        println!(
            "n {validators}, {slots} slots, payload <= {payload} B: sequential {:.2} s, \
             {} steps, W/D {:.1}, digest {:016x}",
            wall.as_secs_f64(),
            metrics.steps_executed,
            metrics.parallelism(),
            run_digest(&swarm),
        );
        let mut one_thread = None;
        for threads in list("BENCH_THREADS", &[1, 2, 4, 8, 10]) {
            let parallel = Parallel::default().threads(threads as usize);
            let (swarm, wall) = timed(build(Some(parallel)), slots);
            let base = *one_thread.get_or_insert(wall.as_secs_f64());
            println!(
                "  {threads:2} threads ({} groups, {:?} windows): {:.2} s, speedup {:.2}, \
                 digest {:016x}",
                swarm.simulation().groups(),
                swarm.simulation().window(),
                wall.as_secs_f64(),
                base / wall.as_secs_f64(),
                run_digest(&swarm),
            );
        }
    }
}

#[test]
#[ignore = "measurement"]
fn slot_cost() {
    const SEED: u64 = 0x510;
    let slots = one("BENCH_SLOTS", 5);
    let payload = one("BENCH_PAYLOAD", 400_000) as usize;
    let threads = one("BENCH_THREADS", 10) as usize;
    let groups = one("BENCH_GROUPS", DEFAULT_GROUPS as u64) as usize;
    let per_node = std::env::var("BENCH_DOMAINS").map_or(true, |d| d != "region");
    let sequential = std::env::var("BENCH_SEQUENTIAL").map_or(true, |s| s != "0");
    let tables = [
        ("testnet", testnet_table()),
        ("public internet", RegionTable::public_internet()),
    ];
    for validators in list("BENCH_N", &[64, 190]) {
        for (name, table) in &tables {
            let run = |parallel: bool| {
                let geo = geo(table, validators, SEED);
                let parallel = parallel.then(|| {
                    if per_node {
                        Parallel::default()
                            .threads(threads)
                            .grouping(Grouping::Auto { groups })
                    } else {
                        by_region(&geo).threads(threads)
                    }
                });
                let nodes = nodes(SEED, validators, payload, SourceKind::Fixed);
                let (swarm, wall) = timed(start_swarm(SEED, geo.network, nodes, parallel), slots);
                (wall.as_secs_f64() / slots as f64, swarm)
            };
            let sequential = sequential.then(|| run(false).0);
            let (parallel, swarm) = run(true);
            let steps = swarm.simulation().step_count();
            let (through, committed) = progress(&swarm);
            let chunks = chunk_bytes(&swarm);
            let mb_per_slot = |bytes: u64| bytes as f64 / 1e6 / slots as f64;
            println!(
                "n {validators}, {name}, {:?}, {payload} B proposals, {slots} slots: \
                 {} s per slot sequential, {:.2} s per slot on {threads} threads \
                 ({} groups, {:?} windows), {:.0} steps per slot, finalized through \
                 slot {through}, {committed} lanes committed; chunks delivered per slot: \
                 {:.1} MB on the wire, {:.1} MB charged; digest {:016x}",
                scheme(),
                sequential.map_or("-".to_string(), |s| format!("{s:.2}")),
                parallel,
                swarm.simulation().groups(),
                swarm.simulation().window(),
                steps as f64 / slots as f64,
                mb_per_slot(chunks.wire),
                mb_per_slot(chunks.charged),
                run_digest(&swarm),
            );
        }
    }
}
