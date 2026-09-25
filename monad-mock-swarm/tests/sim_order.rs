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

//! Message-reordering recovery, ported onto the `monad-sim` harness (mirrors
//! the legacy `order.rs`). One node is isolated for the first 500ms; its
//! messages are then burst-delivered in Forward / Reverse / Random order, and
//! it must catch up (via blocksync) regardless of the order.

mod sim_common;

#[cfg(test)]
mod test {
    use std::time::Duration;

    use monad_chain_config::revision::ChainParams;
    use monad_mock_swarm::{sim_metric, sim_verify::SimVerifier};
    use monad_sim::{time::millis, Time};
    use monad_types::SeqNum;
    use test_case::test_case;

    use crate::sim_common::{replay_network, NoSerConfig, ReplayOrder};

    static CHAIN_PARAMS: ChainParams = ChainParams {
        tx_limit: 10_000,
        proposal_gas_limit: 300_000_000,
        proposal_byte_limit: 4_000_000,
        max_reserve_balance: 1_000_000_000_000_000_000,
        vote_pace: Duration::from_millis(0),
    };

    #[test_case(ReplayOrder::Forward; "in order")]
    #[test_case(ReplayOrder::Reverse; "reverse order")]
    #[test_case(ReplayOrder::Random(1); "random seed 1")]
    #[test_case(ReplayOrder::Random(2); "random seed 2")]
    #[test_case(ReplayOrder::Random(3); "random seed 3")]
    #[test_case(ReplayOrder::Random(4); "random seed 4")]
    #[test_case(ReplayOrder::Random(5); "random seed 5")]
    fn all_messages_delayed(order: ReplayOrder) {
        let delta = Duration::from_millis(20);
        let release = Time(0) + millis(500);

        // execution_delay 1 so the replay-transformer burst can deliver within a
        // single Duration without tripping the state-root check (as in legacy).
        let mut swarm = NoSerConfig::new(4, delta, &CHAIN_PARAMS)
            .execution_delay(SeqNum(1))
            .genesis_seqnum(SeqNum(1))
            .swarm(|ids| replay_network(ids[0], release, delta, order));
        let ids = swarm.node_ids();
        let running: Vec<_> = ids[1..].to_vec();

        // run to just before the replay: the running nodes have progressed and
        // never needed blocksync; the isolated node is behind.
        swarm.run_until(Time(0) + millis(499));
        let longest_before = swarm.finalized_blocks().into_iter().max().unwrap();
        SimVerifier::new()
            .metric_exact(
                &running,
                sim_metric!(blocksync_events.self_headers_request),
                0,
            )
            .metric_exact(
                &running,
                sim_metric!(blocksync_events.self_payload_request),
                0,
            )
            .metric_min(
                &running,
                sim_metric!(consensus_events.handle_proposal),
                longest_before as u64,
            )
            .metric_min(
                &running,
                sim_metric!(consensus_events.created_vote),
                longest_before as u64,
            )
            // enter rounds via QC, except round 2
            .metric_min(
                &running,
                sim_metric!(consensus_events.enter_new_round_qc),
                longest_before as u64 - 1,
            )
            .assert(&swarm);

        // run past the replay: the isolated node receives the reordered burst and
        // catches up, so every node reaches the same progress as a healthy swarm.
        swarm.run_until(Time(0) + millis(1000));
        // inverse of the happy-path block->tick formula over the 500ms / 20ms
        // window, minus 3 (2 uncommitted blocks in the tree + the blackout node
        // can't propose while it has unvalidated blocks): (500/20 - 1)/2 - 3 = 9.
        let expected_blocks: usize = (500 / 20 - 1) / 2 - 3;
        SimVerifier::new()
            .ledger_min_len(longest_before + expected_blocks)
            .assert(&swarm);
        swarm.assert_agreement();
    }
}
