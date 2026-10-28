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

//! A run is a pure function of its configuration and seed: the finalized
//! history and every delivered message with its time are the same in every
//! run, in one process and across processes. std's hashers are seeded per
//! process and per instance, so both are checked.

use std::{process::Command, time::Duration};

use monad_mcp_common::Slot;
use monad_mcp_node::{Node, common::NodeId};
use monad_sim::Time;
use monad_sim_swarm::{Network, Swarm};

use crate::harness::{
    SLOT_INTERVAL, SimNode, assert_progress, deadline_of_slot, node_config, run_digest, time_of,
};

const SEED: u64 = 0xD1CE;

// 8 validators, one never starts, 5% loss: exercises random payloads, the
// network's loss draws, rebroadcast fan-out and pull recovery
fn run(seed: u64) -> u64 {
    const VALIDATORS: u64 = 8;
    const SLOTS: u64 = 60;
    const MAX_PROPOSAL_LEN: usize = 16 << 10;

    let network = Network::reliable(Duration::from_millis(10)).loss(0.05);
    let mut nodes = Vec::new();
    for id in 0..VALIDATORS - 1 {
        let node_id = NodeId::dummy(id);
        let mut config = node_config(seed, id, VALIDATORS);
        config.proposal.max_payload_bytes = Some(MAX_PROPOSAL_LEN);
        let node = Node::new(&config).expect("config builds");
        nodes.push((node_id, SimNode::new(node_id, node)));
    }

    let mut swarm = Swarm::build(seed, network, nodes);
    for id in swarm.node_ids() {
        let handle = swarm.handle(&id).expect("node just built");
        let sim = swarm.sim();
        sim.schedule(handle, Time(0), "init", |node, ctx| node.init(ctx));
    }

    let margin = SLOT_INTERVAL.checked_mul(5).expect("in range");
    swarm.run_until_time(time_of(deadline_of_slot(SLOTS) + margin));
    assert_progress(&swarm, Slot(SLOTS / 2));
    run_digest(&swarm)
}

#[test]
fn a_run_repeats_within_a_process() {
    assert_eq!(run(SEED), run(SEED));
}

#[test]
fn a_run_repeats_across_processes() {
    let in_process = run(SEED);
    for _ in 0..2 {
        assert_eq!(digest_in_child_process(), in_process);
    }
}

#[test]
fn the_seed_changes_the_run() {
    assert_ne!(run(SEED), run(SEED + 1));
}

#[test]
#[ignore = "run in a child process by a_run_repeats_across_processes"]
fn print_digest() {
    println!("digest: {:016x}", run(SEED));
}

// runs print_digest in a fresh process of this test binary
fn digest_in_child_process() -> u64 {
    let exe = std::env::current_exe().expect("test binary path");
    let output = Command::new(exe)
        .args([
            "--exact",
            "determinism::print_digest",
            "--ignored",
            "--nocapture",
        ])
        .output()
        .expect("child process runs");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "child failed:\n{stdout}\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let digest = stdout
        .lines()
        .find_map(|line| line.strip_prefix("digest: "))
        .unwrap_or_else(|| panic!("no digest in child output:\n{stdout}"));
    u64::from_str_radix(digest, 16).expect("hex digest")
}
