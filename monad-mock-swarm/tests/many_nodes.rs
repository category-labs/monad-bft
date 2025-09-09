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

use std::{collections::BTreeSet, time::Duration};

use itertools::Itertools;
use monad_chain_config::{revision::ChainParams, MockChainConfig};
use monad_consensus_types::{block::PassthruBlockPolicy, block_validator::MockValidator};
use monad_crypto::certificate_signature::CertificateKeyPair;
use monad_mock_swarm::{
    mock::TimestamperConfig, mock_swarm::SwarmBuilder, node::NodeBuilder,
    swarm_relation::NoSerSwarm, terminator::UntilTerminator, verifier::MockSwarmVerifier,
};
use monad_router_scheduler::{NoSerRouterConfig, RouterSchedulerBuilder};
use monad_state_backend::InMemoryStateInner;
use monad_testutil::swarm::{make_state_configs, swarm_ledger_verification};
use monad_transformer::{
    DropTransformer, GenericTransformer, LatencyTransformer, RandLatencyTransformer, ID,
};
use monad_types::{Balance, NodeId, Round, SeqNum};
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

    validate_system_txs: true,
};

#[test]
fn many_nodes_noser() {
    // block commits every 2∆ on happy path; 2 * 20ms * 1024 + (vote_pace*1024) = 47s
    // and consensus starts with a timeout
    let runtime = Duration::from_secs(47);
    let delta = Duration::from_millis(20);
    let num_expected_blocks = 1024;
    let state_configs = make_state_configs::<NoSerSwarm>(
        40, // num_nodes
        ValidatorSetFactory::default,
        SimpleRoundRobin::default,
        || MockValidator,
        || PassthruBlockPolicy,
        || InMemoryStateInner::genesis(Balance::MAX, SeqNum(4)),
        SeqNum(4),                           // execution_delay
        delta,                               // delta
        MockChainConfig::new(&CHAIN_PARAMS), // chain config
        SeqNum(100),                         // state_sync_threshold
    );
    let all_peers: BTreeSet<_> = state_configs
        .iter()
        .map(|state_config| NodeId::new(state_config.key.pubkey()))
        .collect();
    let swarm_config = SwarmBuilder::<NoSerSwarm>(
        state_configs
            .into_iter()
            .enumerate()
            .map(|(seed, state_builder)| {
                let state_backend = state_builder.state_backend.clone();
                let validators = state_builder.locked_epoch_validators[0].clone();
                NodeBuilder::<NoSerSwarm>::new(
                    ID::new(NodeId::new(state_builder.key.pubkey())),
                    state_builder,
                    NoSerRouterConfig::new(all_peers.clone()).build(),
                    MockValSetUpdaterNop::new(validators.validators.clone(), SeqNum(2000)),
                    MockTxPoolExecutor::default().with_chain_params(&CHAIN_PARAMS),
                    MockLedger::new(state_backend.clone()),
                    MockStateSyncExecutor::new(
                        state_backend,
                        validators
                            .validators
                            .0
                            .into_iter()
                            .map(|v| v.node_id)
                            .collect(),
                    ),
                    vec![GenericTransformer::Latency(LatencyTransformer::new(delta))],
                    vec![],
                    TimestamperConfig::default(),
                    seed.try_into().unwrap(),
                )
            })
            .collect(),
    );

    let mut swarm = swarm_config.build();
    swarm.batch_step_until(&mut UntilTerminator::new().until_tick(runtime));
    swarm_ledger_verification(&swarm, num_expected_blocks);

    let mut verifier = MockSwarmVerifier::default().tick_range(runtime, delta);
    let node_ids = swarm.states().keys().copied().collect_vec();
    verifier.metrics_happy_path(&node_ids, &swarm);

    assert!(verifier.verify(&swarm));
}

#[test]
fn many_nodes_noser_one_offline() {
    let delta = Duration::from_millis(20);
    let num_nodes = 10;
    let num_offline_nodes = 1;

    let state_configs = make_state_configs::<NoSerSwarm>(
        num_nodes,
        ValidatorSetFactory::default,
        SimpleRoundRobin::default,
        || MockValidator,
        || PassthruBlockPolicy,
        || InMemoryStateInner::genesis(Balance::MAX, SeqNum(4)),
        SeqNum(4),                           // execution_delay
        delta,                               // delta
        MockChainConfig::new(&CHAIN_PARAMS), // chain config
        SeqNum(100),                         // state_sync_threshold
    );
    let all_peers: BTreeSet<_> = state_configs
        .iter()
        .map(|state_config| NodeId::new(state_config.key.pubkey()))
        .collect();
    let swarm_config = SwarmBuilder::<NoSerSwarm>(
        state_configs
            .into_iter()
            .enumerate()
            .map(|(seed, state_builder)| {
                let state_backend = state_builder.state_backend.clone();
                let validators = state_builder.locked_epoch_validators[0].clone();
                NodeBuilder::<NoSerSwarm>::new(
                    ID::new(NodeId::new(state_builder.key.pubkey())),
                    state_builder,
                    NoSerRouterConfig::new(all_peers.clone()).build(),
                    MockValSetUpdaterNop::new(validators.validators.clone(), SeqNum(2000)),
                    MockTxPoolExecutor::default().with_chain_params(&CHAIN_PARAMS),
                    MockLedger::new(state_backend.clone()),
                    MockStateSyncExecutor::new(
                        state_backend,
                        validators
                            .validators
                            .0
                            .into_iter()
                            .map(|v| v.node_id)
                            .collect(),
                    ),
                    vec![
                        GenericTransformer::Latency(LatencyTransformer::new(
                            Duration::from_millis(1),
                        )),
                        GenericTransformer::RandLatency(RandLatencyTransformer::new(
                            0,
                            delta - Duration::from_millis(1),
                        )),
                        GenericTransformer::Drop(
                            DropTransformer::new().drop_only_from(*all_peers.first().unwrap()),
                        ),
                    ],
                    vec![],
                    TimestamperConfig::default(),
                    seed.try_into().unwrap(),
                )
            })
            .collect(),
    );

    let mut swarm = swarm_config.build();

    let num_rounds = 100;
    swarm.batch_step_until(&mut UntilTerminator::new().until_round(Round(num_rounds)));
    let mut max_observed_local_timeouts = swarm
        .states()
        .values()
        .map(|node| node.state.metrics().consensus_events.local_timeout)
        .max()
        .unwrap();
    // subtract 1 for the initial timeout on startup
    max_observed_local_timeouts -= 1;

    let max_allowed_local_timeouts =
        (num_rounds * num_offline_nodes as u64).div_ceil(num_nodes.into());

    assert!(max_observed_local_timeouts <= max_allowed_local_timeouts);
}
