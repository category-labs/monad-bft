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

//! Two-node happy-path coverage, ported onto the `monad-sim` harness (mirrors
//! the legacy `two_nodes.rs`, which it does not replace). The deterministic
//! scheduler lets the final tick be asserted exactly, with no tolerance window.

use std::time::Duration;

use monad_mock_swarm::{
    sim::{build_swarm_with, Network},
    sim_verify::SimVerifier,
};
use monad_sim::{time::secs, Time};

#[test]
fn two_nodes_noser() {
    let delta = Duration::from_millis(100);
    let mut swarm = build_swarm_with(2, 0, |_| Network::reliable(delta));

    assert!(
        swarm.run_until_blocks(1026, Time(0) + secs(600)),
        "did not reach 1026 blocks"
    );

    let ids = swarm.node_ids();
    let mut verifier = SimVerifier::new();
    verifier
        // The deterministic scheduler yields an exact tick (no tolerance window).
        .tick_exact(Time(0) + Duration::from_millis(205_500))
        .ledger_min_len(1024)
        .metrics_happy_path(&ids, &swarm);
    verifier.assert(&swarm);
    swarm.assert_agreement();
}
