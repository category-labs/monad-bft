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

use std::time::Duration;

use monad_chain_config::{revision::ChainParams, MockChainConfig};
use monad_consensus_types::{block::PassthruBlockPolicy, block_validator::MockValidator};
use monad_crypto::certificate_signature::CertificateKeyPair;
use monad_execution_state_read::InMemoryStateInner;
use monad_mock_swarm::{
    mock::TimestamperConfig,
    mock_swarm::SwarmBuilder,
    node::NodeBuilder,
    raptorcast::{RaptorcastRouterConfig, RaptorcastSwarm},
    swarm::{make_state_configs, swarm_ledger_verification},
    terminator::UntilTerminator,
};
use monad_router_scheduler::RouterSchedulerBuilder;
use monad_transformer::{GenericTransformer, LatencyTransformer, ID};
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
    let state_configs = make_state_configs::<RaptorcastSwarm>(
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
    );

    let swarm_config = SwarmBuilder::<RaptorcastSwarm>(
        state_configs
            .into_iter()
            .enumerate()
            .map(|(seed, state_builder)| {
                let state_backend = state_builder.state_read.clone();
                let validators = state_builder.locked_epoch_validators[0].clone();
                let self_id = NodeId::new(state_builder.key.pubkey());
                let router_config = RaptorcastRouterConfig::<_, _, _>::new(self_id);
                NodeBuilder::<RaptorcastSwarm>::new(
                    ID::new(self_id),
                    state_builder,
                    router_config.build(),
                    MockValSetUpdaterNop::new(validators.validators, epoch_length),
                    MockTxPoolExecutor::default().with_chain_params(&CHAIN_PARAMS),
                    MockLedger::new(state_backend.clone()),
                    MockStateSyncExecutor::new(state_backend),
                    vec![GenericTransformer::Latency(LatencyTransformer::new(delta))],
                    vec![],
                    TimestamperConfig::default(),
                    seed.try_into().unwrap(),
                )
            })
            .collect(),
    );

    let mut swarm = swarm_config.build();
    let target_epoch = Epoch(4);
    while swarm
        .step_until(&mut UntilTerminator::new().until_epoch(target_epoch))
        .is_some()
    {}

    let min_blocks = (target_epoch.0 - 1) * epoch_length.0;
    swarm_ledger_verification(&swarm, min_blocks as usize);
}
