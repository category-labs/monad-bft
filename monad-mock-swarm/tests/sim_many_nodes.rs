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

//! Larger swarms, ported onto the `monad-sim` harness (mirrors the legacy
//! `many_nodes.rs`): a 40-node happy path, and a 10-node swarm with one node
//! whose outbound messages are dropped (offline).

mod sim_common;

use std::time::Duration;

use monad_chain_config::revision::ChainParams;
use monad_mock_swarm::{sim::Network, sim_verify::SimVerifier};
use monad_sim::{dist::uniform_duration, time::secs, Time};
use monad_types::Round;

use crate::sim_common::NoSerConfig;

static CHAIN_PARAMS: ChainParams = ChainParams {
    tx_limit: 10_000,
    proposal_gas_limit: 300_000_000,
    proposal_byte_limit: 4_000_000,
    max_reserve_balance: 1_000_000_000_000_000_000,
    vote_pace: Duration::from_millis(5),
};

#[test]
fn many_nodes_noser() {
    let delta = Duration::from_millis(20);
    let mut swarm = NoSerConfig::new(40, delta, &CHAIN_PARAMS).swarm(|_| Network::reliable(delta));

    assert!(
        swarm.run_until_blocks(1024, Time(0) + secs(120)),
        "did not reach 1024 blocks"
    );

    let ids = swarm.node_ids();
    let mut verifier = SimVerifier::new();
    verifier
        // Deterministic exact tick (legacy ran to a fixed 47s window).
        .tick_exact(Time(0) + Duration::from_millis(41_040))
        .ledger_min_len(1024)
        .metrics_happy_path(&ids, &swarm);
    verifier.assert(&swarm);
    swarm.assert_agreement();
}

#[test]
fn many_nodes_noser_one_offline() {
    let delta = Duration::from_millis(20);
    let num_nodes = 10u16;
    let num_offline_nodes = 1u64;
    let num_rounds = 100u64;

    // base 1ms + uniform [1, 20)ms, matching Latency(1ms) + RandLatency(0, 19ms).
    let mut swarm = NoSerConfig::new(num_nodes, delta, &CHAIN_PARAMS).swarm(|ids| {
        let offline = ids[0];
        Network::with_latency(uniform_duration(
            Duration::from_millis(2),
            Duration::from_millis(20),
        ))
        .drop_if(move |link, _| link.from == offline)
    });

    assert!(
        swarm.run_until_round(Round(num_rounds), Time(0) + secs(120)),
        "did not reach round 100"
    );

    let max_observed_local_timeouts = swarm
        .node_ids()
        .iter()
        .map(|&id| swarm.with_node(id, |node| node.metrics().consensus_events.local_timeout.get()))
        .max()
        .unwrap()
        // subtract 1 for the initial timeout on startup
        - 1;

    let max_allowed_local_timeouts = (num_rounds * num_offline_nodes).div_ceil(num_nodes.into());

    assert!(
        max_observed_local_timeouts <= max_allowed_local_timeouts,
        "max observed timeouts {} > allowed {}",
        max_observed_local_timeouts,
        max_allowed_local_timeouts
    );
}
