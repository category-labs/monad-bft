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

//! A swarm of pq stub validators in one process, talking over loopback
//! UDP. `cargo run --example swarm -- [nodes] [seconds]`, defaults 4 nodes
//! for 60 seconds, the local demo values of config.example.toml.

use std::{
    net::SocketAddr,
    path::Path,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use monad_mcp_chorus::spec::vote::KeyPair as _;
use monad_mcp_node::config::ValidatorConfig;
use monad_pq_node::{
    RunError,
    chorus::types::{KeyPair, NodeId, Timestamp, TimestampDelta},
    config::PqNodeConfig,
    init_logging, run_node,
};

// clear of swarm.sh (9300 up) and config.example.toml (9290)
const BASE_PORT: u16 = 9400;
const GENESIS_DELAY: TimestampDelta = TimestampDelta::from_millis(2000);

fn address(i: u64) -> SocketAddr {
    ([127, 0, 0, 1], BASE_PORT + i as u16).into()
}

fn config(i: u64, nodes: u64, genesis_deadline: Timestamp, ledgers: &Path) -> PqNodeConfig {
    let mut config = PqNodeConfig::single_node(
        BASE_PORT + i as u16,
        genesis_deadline,
        ledgers.join(format!("ledger-{i}")),
    );
    let base = &mut config.base;
    base.node_id = NodeId::dummy(i);
    base.proposal_key_pair = monad_mcp_node::da::ProposalKeyPair::dummy(NodeId::dummy(i));
    base.cadence_key_pair = KeyPair::dummy(i);
    base.validators = (0..nodes)
        .map(|v| ValidatorConfig {
            node_id: NodeId::dummy(v),
            stake: 1,
            chorus_pubkey: KeyPair::dummy(v).pubkey(),
            address: address(v),
        })
        .collect();
    config.proposal_key = i;
    config
}

fn genesis_deadline() -> Timestamp {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("the clock is past 1970");
    Timestamp::from_nanos(now.as_nanos())
        .checked_add_delta(GENESIS_DELAY)
        .expect("no overflow")
}

async fn run_logged(i: u64, config: PqNodeConfig) {
    if let Err(error) = run_node(config).await {
        tracing::error!(node = i, %error, "node stopped");
    }
}

#[tokio::main]
async fn main() -> Result<(), RunError> {
    init_logging();

    let mut args = std::env::args().skip(1);
    let nodes: u64 = args.next().map(|n| n.parse()).transpose()?.unwrap_or(4);
    let seconds: u64 = args.next().map(|n| n.parse()).transpose()?.unwrap_or(60);

    // dropped, and so removed, once the swarm stops
    let ledgers = tempfile::tempdir()?;
    let genesis_deadline = genesis_deadline();
    let mut runs = Vec::new();
    for i in 0..nodes {
        let config = config(i, nodes, genesis_deadline, ledgers.path());
        runs.push(tokio::spawn(run_logged(i, config)));
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
