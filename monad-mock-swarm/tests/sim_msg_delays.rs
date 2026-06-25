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

//! Per-link XOR latency, ported onto the `monad-sim` harness (mirrors the
//! legacy `msg_delays.rs`). The deterministic XOR latency lets the final tick
//! be asserted exactly (the legacy engine only bounded it below the max).

mod sim_common;

use std::time::Duration;

use monad_chain_config::revision::ChainParams;
use monad_mock_swarm::{sim::Network, sim_verify::SimVerifier};
use monad_sim::{time::secs, Time};

use crate::sim_common::{NoSerConfig, XorLatency};

static CHAIN_PARAMS: ChainParams = ChainParams {
    tx_limit: 10_000,
    proposal_gas_limit: 300_000_000,
    proposal_byte_limit: 4_000_000,
    max_reserve_balance: 1_000_000_000_000_000_000,
    vote_pace: Duration::from_millis(5),
};

#[test]
fn xor_latency_four_nodes() {
    let delta = Duration::from_millis(u8::MAX as u64);
    let mut swarm = NoSerConfig::new(4, delta, &CHAIN_PARAMS)
        .swarm(|_| Network::custom(XorLatency { max: delta }));

    assert!(
        swarm.run_until_blocks(100, Time(0) + secs(600)),
        "did not reach 100 blocks"
    );

    let ids = swarm.node_ids();
    let mut verifier = SimVerifier::new();
    verifier
        // Deterministic: exact tick (sub-ms tail comes from the float XOR
        // latency). The legacy engine only bounded this below the theoretical
        // max because its same-tick ordering was randomized.
        .tick_exact(Time(8_954_000_078))
        .ledger_min_len(98)
        .metrics_happy_path(&ids, &swarm);
    verifier.assert(&swarm);
    swarm.assert_agreement();
}
