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

//! Four nodes under random per-message latency, ported onto the `monad-sim`
//! harness (mirrors the legacy `rand_lat.rs`). The fuzzing seed parametrizes
//! the simulation seed (which seeds the network latency RNG), so each case
//! exercises a distinct latency schedule. Latency is non-deterministic across
//! seeds, so the final tick is not asserted exactly; the metric bounds and
//! ledger length carry the coverage.

mod sim_common;

use std::time::Duration;

use monad_chain_config::revision::ChainParams;
use monad_mock_swarm::{sim::Network, sim_metric, sim_verify::SimVerifier};
use monad_sim::{dist::uniform_duration, time::secs, Time};
use monad_types::SeqNum;
use test_case::test_case;

use crate::sim_common::NoSerConfig;

static CHAIN_PARAMS: ChainParams = ChainParams {
    tx_limit: 10_000,
    proposal_gas_limit: 300_000_000,
    proposal_byte_limit: 4_000_000,
    max_reserve_balance: 1_000_000_000_000_000_000,
    vote_pace: Duration::from_millis(10),
};

#[test_case(1; "seed1")]
#[test_case(2; "seed2")]
#[test_case(3; "seed3")]
#[test_case(4; "seed4")]
#[test_case(5; "seed5")]
#[test_case(6; "seed6")]
#[test_case(7; "seed7")]
#[test_case(8; "seed8")]
#[test_case(9; "seed9")]
#[test_case(10; "seed10")]
#[test_case(14710580201381303742; "seed11")]
#[test_case(11282773634027867923; "seed12")]
#[test_case(11868595526945931122; "seed13")]
#[test_case(4712443726697299681; "seed14")]
#[test_case(5153471631950140680; "seed15")]
#[test_case(4180491672667595808; "seed16")]
#[test_case(3250401801427586510; "seed17")]
#[test_case(13102732628471206412; "seed18")]
fn nodes_with_random_latency(latency_seed: u64) {
    let delta = Duration::from_millis(200);
    let last_block = 2000usize;
    let min_ledger_len = last_block - 5;
    let max_blocksync_requests = 55u64;

    let mut swarm = NoSerConfig::new(4, delta, &CHAIN_PARAMS)
        // avoid the state_root trigger in the random-latency setting
        .execution_delay(SeqNum::MAX)
        .genesis_seqnum(SeqNum::MAX)
        .epoch_length(SeqNum(3000))
        .seed(latency_seed)
        .swarm(|_| Network::with_latency(uniform_duration(Duration::from_millis(1), delta)));

    assert!(
        swarm.run_until_blocks(last_block, Time(0) + secs(2000)),
        "seed {}: did not reach {} blocks",
        latency_seed,
        last_block
    );

    let ids = swarm.node_ids();
    let mut verifier = SimVerifier::new();
    verifier
        .ledger_min_len(min_ledger_len)
        // nodes time out only on init
        .metric_exact(&ids, sim_metric!(consensus_events.local_timeout), 1)
        // last_block is committed by processing 2 QCs after it; no branching.
        // the node with the most blocksync requests may have the fewest blocks.
        .metric_range(
            &ids,
            sim_metric!(consensus_events.process_qc),
            min_ledger_len as u64 - max_blocksync_requests,
            last_block as u64 + 2,
        )
        .metric_max(
            &ids,
            sim_metric!(blocksync_events.self_headers_request),
            max_blocksync_requests,
        )
        .metric_max(
            &ids,
            sim_metric!(blocksync_events.self_payload_request),
            max_blocksync_requests,
        );
    verifier.assert(&swarm);
    swarm.assert_agreement();
}
