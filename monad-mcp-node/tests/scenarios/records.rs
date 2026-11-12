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

//! Scenario setup and recording for comparing a run with a model: a
//! proposer's own slots show at its recording peers as first-hop arrivals,
//! recording changes nothing, d33 and hollow runs record alike, and the
//! network exports as JSON.

use monad_mcp_common::{Slot, TimestampDelta};
use monad_mcp_node::{Node, common::NodeId, config::SourceKind, da::ProposalScheme};
use monad_sim_swarm::{RegionTable, Swarm};

use crate::harness::{
    SLOT_INTERVAL, SimNode, assert_progress, deadline_of_slot, first_hop_priority, geo,
    node_config, normal_clock_offsets, run_digest, start_swarm, time_of, topology_json,
};

const VALIDATORS: u64 = 8;
const SLOTS: u64 = 30;
const SEED: u64 = 0x5EC;
const CLOCK_SIGMA_MS: f64 = 4.0;

fn run(scheme: ProposalScheme, recording: bool) -> Swarm<SimNode> {
    let mut geo = geo(&RegionTable::public_internet(), VALIDATORS, SEED);
    geo.network = first_hop_priority(geo.network);
    let offsets = normal_clock_offsets(SEED, VALIDATORS, CLOCK_SIGMA_MS);
    let nodes = (0..VALIDATORS)
        .map(|id| {
            let node_id = NodeId::dummy(id);
            let mut config = node_config(SEED, id, VALIDATORS);
            config.proposal.source = SourceKind::Fixed;
            config.proposal.max_payload_bytes = Some(64 << 10);
            config.proposal.propose_before_deadline = TimestampDelta::from_millis(150);
            config.proposal.encoding_scheme = scheme;
            let node = Node::new(&config).expect("config builds");
            let sim_node = SimNode::new(node_id, node).with_clock_offset(offsets[id as usize]);
            let sim_node = if recording {
                sim_node.recording()
            } else {
                sim_node
            };
            (node_id, sim_node)
        })
        .collect();
    let mut swarm = start_swarm(SEED, geo.network, nodes, None);
    let margin = SLOT_INTERVAL.checked_mul(5).expect("in range");
    swarm.run_until_time(time_of(deadline_of_slot(SLOTS) + margin));
    assert_progress(&swarm, Slot(SLOTS / 2));
    swarm
}

// every proposer's own slots show at its peers as first-hop chunks from
// it, arriving after its seal
fn check_records(swarm: &Swarm<SimNode>) {
    let ids = swarm.node_ids();
    let mut own_slots = 0;
    for proposer in &ids {
        let first_hops = swarm.with_node(proposer, |node| node.first_hops().clone());
        for (slot, (sealed, lane)) in first_hops.range(..Slot(SLOTS)) {
            own_slots += 1;
            for peer in ids.iter().filter(|id| *id != proposer) {
                let seen = swarm.with_node(peer, |node| {
                    let records = node.records().expect("recording");
                    records.first_hop_in.get(&(*slot, *lane)).cloned()
                });
                let (from, arrivals) = seen.expect("every peer gets a first hop");
                assert_eq!(from, *proposer);
                assert!(!arrivals.is_empty());
                assert!(arrivals.iter().all(|at| at > sealed));
            }
        }
    }
    assert!(own_slots as u64 >= SLOTS, "{own_slots} own slots");
}

#[test]
fn records_are_consistent_and_change_nothing() {
    let recorded = run(ProposalScheme::D33, true);
    check_records(&recorded);
    for id in recorded.node_ids() {
        let paths = recorded.with_node(&id, |node| node.paths().len());
        assert!(paths as u64 >= SLOTS / 2);
    }
    let plain = run(ProposalScheme::D33, false);
    assert_eq!(run_digest(&recorded), run_digest(&plain));
}

#[test]
fn hollow_runs_record_the_same_way() {
    check_records(&run(ProposalScheme::D33Hollow, true));
}

#[test]
fn the_network_exports_as_json() {
    let geo = geo(&RegionTable::public_internet(), VALIDATORS, SEED);
    let offsets = normal_clock_offsets(SEED, VALIDATORS, CLOCK_SIGMA_MS);
    assert_eq!(
        offsets,
        normal_clock_offsets(SEED, VALIDATORS, CLOCK_SIGMA_MS)
    );
    assert!(offsets.iter().any(|offset| offset.0 != 0));
    assert!(
        offsets.iter().all(|offset| offset.0 % 1000 == 0),
        "whole µs"
    );
    let zero = normal_clock_offsets(SEED, VALIDATORS, 0.0);
    assert!(zero.iter().all(|offset| offset.0 == 0));

    // the exported matrix is the one the network samples from
    let ids: Vec<NodeId> = (0..VALIDATORS).map(NodeId::dummy).collect();
    for (i, from) in ids.iter().enumerate() {
        for (j, to) in ids.iter().enumerate().filter(|(j, _)| *j != i) {
            assert_eq!(geo.one_way[i][j], geo.matrix.latency(from, to));
        }
    }

    let topology = topology_json(&geo, &offsets);
    let n = VALIDATORS as usize;
    assert_eq!(topology["n"], n);
    assert_eq!(topology["labels"].as_array().expect("labels").len(), n);
    let one_way = topology["one_way_ms"].as_array().expect("matrix");
    assert_eq!(one_way.len(), n);
    for (i, row) in one_way.iter().enumerate() {
        let row = row.as_array().expect("row");
        assert_eq!(row.len(), n);
        assert_eq!(row[i], 0.0);
        let others = row.iter().enumerate().filter(|(j, _)| *j != i);
        assert!(others.into_iter().all(|(_, ms)| ms.as_f64() > Some(0.0)));
    }
    let exported: Vec<f64> = topology["clock_offset_ms"]
        .as_array()
        .expect("offsets")
        .iter()
        .map(|ms| ms.as_f64().expect("a number"))
        .collect();
    let expected: Vec<f64> = offsets.iter().map(|o| o.0 as f64 / 1e6).collect();
    assert_eq!(exported, expected);
}
