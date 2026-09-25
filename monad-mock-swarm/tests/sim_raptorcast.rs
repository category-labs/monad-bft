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

#![cfg(feature = "raptorcast")]

//! RaptorCast-chunk transport over the `monad-sim` harness, ported from the
//! legacy `raptorcast.rs` (which it does not replace). Drives the
//! `RaptorcastSwarm` relation — whose `RouterScheduler` chunks outbound messages
//! and reassembles inbound chunks — across several epoch boundaries, exercising
//! the harness's forwarding of `AddEpochValidatorSet` / `UpdateCurrentRound`
//! (which the chunk scheduler needs to resolve broadcast recipients).

use std::time::Duration;

use monad_chain_config::{revision::ChainParams, MockChainConfig};
use monad_consensus_types::{block::PassthruBlockPolicy, block_validator::MockValidator};
use monad_crypto::certificate_signature::CertificateKeyPair;
use monad_execution_state_read::InMemoryStateInner;
use monad_mock_swarm::{
    raptorcast::{RaptorcastRouterConfig, RaptorcastSwarm},
    sim::{Network, SimNodeBuilder, SimSwarm, DEFAULT_TIMESTAMP_PERIOD},
    swarm::make_state_configs,
};
use monad_router_scheduler::RouterSchedulerBuilder;
use monad_sim::{time::secs, Time};
use monad_types::{Epoch, NodeId, Round, SeqNum};
use monad_updaters::{
    ledger::MockLedger, statesync::MockStateSyncExecutor, txpool::MockTxPoolExecutor,
    val_set::MockValSetUpdaterNop,
};
use monad_validator::{simple_round_robin::SimpleRoundRobin, validator_set::ValidatorSetFactory};

static CHAIN_PARAMS: ChainParams = ChainParams {
    tx_limit: 10_000,
    proposal_gas_limit: 300_000_000,
    proposal_byte_limit: 4_000_000,
    max_reserve_balance: 1_000_000_000_000_000_000,
    vote_pace: Duration::from_millis(5),
};

#[test]
fn raptorcast_smoke_four_nodes() {
    let delta = Duration::from_millis(100);
    let epoch_length = SeqNum(20);
    let epoch_start_delay = Round(5);

    let builders: Vec<SimNodeBuilder<RaptorcastSwarm>> = make_state_configs::<RaptorcastSwarm>(
        4,
        ValidatorSetFactory::default,
        SimpleRoundRobin::default,
        || MockValidator,
        || PassthruBlockPolicy,
        || InMemoryStateInner::genesis(SeqNum(4)),
        SeqNum(4),
        delta,
        MockChainConfig::new_with_epoch_params(&CHAIN_PARAMS, epoch_length, epoch_start_delay),
        SeqNum(100),
    )
    .into_iter()
    .map(|state_builder| {
        let state_read = state_builder.state_read.clone();
        let validators = state_builder.locked_epoch_validators[0].clone();
        let self_id = NodeId::new(state_builder.key.pubkey());
        SimNodeBuilder {
            id: self_id,
            state_builder,
            router_scheduler: RaptorcastRouterConfig::<_, _, _>::new(self_id).build(),
            val_set_updater: MockValSetUpdaterNop::new(validators.validators, epoch_length),
            txpool_executor: MockTxPoolExecutor::default().with_chain_params(&CHAIN_PARAMS),
            ledger: MockLedger::new(state_read.clone()),
            statesync_executor: MockStateSyncExecutor::new(state_read),
            timestamp_period: DEFAULT_TIMESTAMP_PERIOD,
        }
    })
    .collect();

    let mut swarm =
        SimSwarm::<RaptorcastSwarm>::from_builders(0, builders, |_| Network::reliable(delta));

    // Run past epoch 4, i.e. (4 - 1) * epoch_length committed blocks — enough to
    // cross several boundaries with the chunked transport.
    let min_blocks = ((Epoch(4).0 - 1) * epoch_length.0) as usize;
    assert!(
        swarm.run_until_blocks(min_blocks, Time(0) + secs(120)),
        "raptorcast swarm did not reach {min_blocks} blocks"
    );
    swarm.assert_agreement();
}
