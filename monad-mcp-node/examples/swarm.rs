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

//! A swarm of stub validators in one process, talking over loopback
//! UDP. `cargo run --example swarm -- [nodes] [seconds]`, defaults 10
//! nodes for 60 seconds.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use monad_mcp_chorus::spec::vote::KeyPair as _;
use monad_mcp_node::{
    RunError,
    chorus::types::{KeyPair, NodeId, Timestamp, TimestampDelta},
    config::{NetworkConfig, NodeConfig, ValidatorConfig},
    da::ProposalKeyPair,
    init_logging, run_node,
};

const BASE_PORT: u16 = 9100;
const GENESIS_DELAY: TimestampDelta = TimestampDelta::from_millis(1000);

fn validators(nodes: u64) -> Vec<ValidatorConfig> {
    let mut validators = Vec::new();
    for i in 0..nodes {
        validators.push(ValidatorConfig {
            node_id: NodeId::dummy(i),
            stake: 1,
            chorus_pubkey: KeyPair::dummy(i).pubkey(),
            address: ([127, 0, 0, 1], BASE_PORT + i as u16).into(),
        });
    }
    validators
}

fn config(i: u64, nodes: u64, genesis_deadline: Timestamp) -> NodeConfig {
    NodeConfig {
        node_id: NodeId::dummy(i),
        proposal_key_pair: ProposalKeyPair::dummy(NodeId::dummy(i)),
        cadence_key_pair: KeyPair::dummy(i),
        validators: validators(nodes),
        genesis_deadline,
        network: NetworkConfig {
            port: BASE_PORT + i as u16,
        },
        cadence: Default::default(),
        da: Default::default(),
        proposal: Default::default(),
    }
}

fn genesis_deadline() -> Timestamp {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("the clock is past 1970");
    Timestamp::from_nanos(now.as_nanos())
        .checked_add_delta(GENESIS_DELAY)
        .expect("no overflow")
}

async fn run_logged(i: u64, config: NodeConfig) {
    if let Err(error) = run_node(config).await {
        tracing::error!(node = i, %error, "node stopped");
    }
}

#[tokio::main]
async fn main() -> Result<(), RunError> {
    init_logging();

    let mut args = std::env::args().skip(1);
    let nodes: u64 = args.next().map(|n| n.parse()).transpose()?.unwrap_or(10);
    let seconds: u64 = args.next().map(|n| n.parse()).transpose()?.unwrap_or(60);

    let genesis_deadline = genesis_deadline();
    let mut runs = Vec::new();
    for i in 0..nodes {
        runs.push(tokio::spawn(run_logged(
            i,
            config(i, nodes, genesis_deadline),
        )));
    }

    tokio::select! {
        _ = tokio::time::sleep(Duration::from_secs(seconds)) => {}
        _ = tokio::signal::ctrl_c() => {}
    }
    for run in runs {
        run.abort();
    }
    Ok(())
}
