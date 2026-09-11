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

//! Regression gate for an open production `todo!()` in shared consensus code:
//! a node computes upcoming leaders for an epoch whose validator set has not yet
//! been delivered.
//!
//! Panic site: `monad-consensus-state/src/lib.rs`,
//! `compute_upcoming_leader_round_pairs` — "handle non-existent validatorset for
//! next k round epoch".
//!
//! Epoch E's validator set is delivered only once epoch (E-1)'s boundary block is
//! *finalized*. But a node's leader look-ahead keys off `current_round`, which can
//! outrun finalized height — rounds advance via timeouts (TCs) under a validator
//! outage. When the look-ahead reaches E before E's set is delivered,
//! `val_epoch_map` has no entry and the `todo!()` fires. `epoch_start_delay` is
//! the buffer that normally hides this.
//!
//! The reproducing test is `#[should_panic]`: GREEN while the bug exists, RED
//! once the case is handled. The epoch parameters are tiny for tractability, so
//! this proves the path is *reachable*, not its production-scale frequency
//! (production `epoch_start_delay` is 1k-5k, so the gap there needs a prolonged
//! liveness disruption near a boundary, not the single-validator outage
//! compressed here). The `healthy_baseline_crosses_boundaries` control confirms
//! the trigger is the fault, not the small parameter alone.

mod sim_common;

#[cfg(test)]
mod test {
    use std::time::Duration;

    use monad_chain_config::revision::ChainParams;
    use monad_mock_swarm::sim::Network;
    use monad_sim::{time::secs, Time};
    use monad_types::{Round, SeqNum};

    use crate::sim_common::NoSerConfig;

    static CHAIN_PARAMS: ChainParams = ChainParams {
        tx_limit: 10_000,
        proposal_gas_limit: 300_000_000,
        proposal_byte_limit: 4_000_000,
        max_reserve_balance: 1_000_000_000_000_000_000,
        vote_pace: Duration::from_millis(0),
    };

    // Tiny epoch params: an epoch boundary is reached in a few simulated seconds.
    // (Production uses epoch_length 10k-50k, epoch_start_delay 1k-5k.)
    const EPOCH_LENGTH: SeqNum = SeqNum(20);
    const EPOCH_START_DELAY: Round = Round(5);

    /// REPRO: with one validator partitioned off, the surviving supermajority's
    /// rounds advance via timeouts faster than blocks finalize; approaching the
    /// first epoch boundary, the leader look-ahead reaches epoch 2 before epoch
    /// 2's validator set has been delivered -> production `todo!()` at
    /// `monad-consensus-state/src/lib.rs:2023`.
    ///
    /// `#[should_panic]` so this is GREEN while the bug exists and turns RED when
    /// the missing-validator-set case is handled (a built-in regression signal).
    #[test]
    #[should_panic(expected = "non-existent validatorset")]
    fn epoch_boundary_missing_valset_under_outage() {
        let delta = Duration::from_millis(20);
        let mut swarm = NoSerConfig::new(4, delta, &CHAIN_PARAMS)
            .execution_delay(SeqNum(4))
            .genesis_seqnum(SeqNum(4))
            .epoch_length(EPOCH_LENGTH)
            .epoch_start_delay(EPOCH_START_DELAY)
            // node 0 is a partitioned (down) validator for the whole run
            .swarm(|ids| {
                let down = ids[0];
                Network::reliable(delta).partition(Time(0)..Time(i128::MAX), [vec![down]])
            });

        // drive the surviving supermajority forward. Epochs 1-2 are genesis-
        // locked (their sets are present without any finalization), so the gap
        // first bites at a later boundary, whose set must be delivered by
        // finalizing the boundary block while rounds outrun finalization under the
        // outage. It panics well before reaching this target.
        swarm.run_until_any_blocks(EPOCH_LENGTH.0 as usize * 6, Time(0) + secs(300));
    }

    /// Baseline / control: with no fault, the same tiny `epoch_start_delay`
    /// crosses several epoch boundaries cleanly. Confirms the panic above is
    /// caused by the outage, not by the parameter value alone.
    #[test]
    fn healthy_baseline_crosses_boundaries() {
        let delta = Duration::from_millis(20);
        let mut swarm = NoSerConfig::new(4, delta, &CHAIN_PARAMS)
            .execution_delay(SeqNum(4))
            .genesis_seqnum(SeqNum(4))
            .epoch_length(EPOCH_LENGTH)
            .epoch_start_delay(EPOCH_START_DELAY)
            .swarm(|_| Network::reliable(delta));

        // crosses boundaries at blocks 19, 39, 59 with all four nodes healthy
        assert!(swarm.run_until_blocks(70, Time(0) + secs(120)));
    }
}
