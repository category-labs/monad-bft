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

use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};

use monad_chain_config::{revision::ChainParams, MockChainConfig};
use monad_consensus_types::{block::PassthruBlockPolicy, block_validator::MockValidator};
use monad_crypto::certificate_signature::CertificateKeyPair;
use monad_execution_state_read::InMemoryStateInner;
use monad_mock_swarm::{
    mock::TimestamperConfig, mock_swarm::SwarmBuilder, node::NodeBuilder,
    swarm::make_state_configs, swarm_relation::NoSerSwarm,
};
use monad_router_scheduler::{NoSerRouterConfig, RouterSchedulerBuilder};
use monad_transformer::{GenericTransformer, LatencyTransformer, ID};
use monad_types::{NodeId, SeqNum, Stake};
use monad_updaters::{
    ledger::MockLedger, statesync::MockStateSyncExecutor, txpool::MockTxPoolExecutor,
    val_set::MockValSetUpdaterNop,
};
use monad_validator::{
    simple_round_robin::SimpleRoundRobin, validator_set::ValidatorSetFactory,
    weighted_round_robin::WeightedRoundRobin,
};
use wasm_bindgen::prelude::*;

mod graphql;
pub use graphql::GraphQLRoot;
mod network;
mod simulation;
use simulation::{ConfiguredSwarm, Simulation, ValidatorConfig};

#[wasm_bindgen(start)]
pub fn init() {
    std::panic::set_hook(Box::new(console_error_panic_hook::hook))
}

static CHAIN_PARAMS: ChainParams = ChainParams {
    tx_limit: 10_000,
    proposal_gas_limit: 300_000_000,
    proposal_byte_limit: 4_000_000,
    max_reserve_balance: 1_000_000_000_000_000_000,
    vote_pace: Duration::from_millis(5),
};

#[wasm_bindgen]
pub fn simulation_make() -> *mut Simulation {
    Box::into_raw(Box::new(Simulation::new(Box::new(default_swarm_config))))
}

pub(crate) fn default_swarm_config(config: &ValidatorConfig) -> ConfiguredSwarm {
    let mut state_configs = make_state_configs::<NoSerSwarm>(
        config.stakes().len().try_into().unwrap(),
        ValidatorSetFactory::default,
        SimpleRoundRobin::default,
        || MockValidator,
        || PassthruBlockPolicy,
        || InMemoryStateInner::genesis(SeqNum(4)),
        SeqNum(4),                           // execution_delay
        Duration::from_millis(50),           // delta
        MockChainConfig::new(&CHAIN_PARAMS), // chain config
        SeqNum(100),                         // state_sync_threshold
    );
    let validator_ids: Vec<_> = state_configs
        .iter()
        .map(|state_config| NodeId::new(state_config.key.pubkey()))
        .collect();
    let stakes_by_id: BTreeMap<_, _> = validator_ids
        .iter()
        .copied()
        .zip(config.stakes().iter().copied().map(Stake::from))
        .collect();
    for state_config in &mut state_configs {
        for locked_epoch in &mut state_config.locked_epoch_validators {
            for validator in &mut locked_epoch.validators.0 .0 {
                validator.stake = stakes_by_id[&validator.node_id];
            }
        }
    }
    let all_peers: BTreeSet<_> = state_configs
        .iter()
        .map(|state_config| NodeId::new(state_config.key.pubkey()))
        .collect();
    let swarm_config = SwarmBuilder::<NoSerSwarm>(
        state_configs
            .into_iter()
            .enumerate()
            .map(|(seed, state_builder)| {
                let state_read = state_builder.state_read.clone();
                let validators = state_builder.locked_epoch_validators[0].clone();
                NodeBuilder::<NoSerSwarm>::new(
                    ID::new(NodeId::new(state_builder.key.pubkey())),
                    state_builder,
                    NoSerRouterConfig::new(all_peers.clone()).build(),
                    MockValSetUpdaterNop::new(validators.validators, SeqNum(2000)),
                    MockTxPoolExecutor::default().with_chain_params(&CHAIN_PARAMS),
                    MockLedger::new(state_read.clone()),
                    MockStateSyncExecutor::new(state_read),
                    vec![GenericTransformer::Latency(LatencyTransformer::new(
                        Duration::from_millis(network::DEFAULT_LINK_LATENCY_MS),
                    ))],
                    vec![],
                    TimestamperConfig::default(),
                    seed.try_into().unwrap(),
                )
            })
            .collect(),
    );
    let mut builder = swarm_config.debug();
    for node in &mut builder.0 {
        node.state_builder.leader_election = Box::new(WeightedRoundRobin::default());
    }
    ConfiguredSwarm {
        builder,
        validator_ids,
    }
}

#[allow(clippy::not_unsafe_ptr_arg_deref)]
#[wasm_bindgen]
pub fn simulation_schema(ptr: *const Simulation) -> String {
    unsafe { &*ptr }.schema()
}

// TODO serialize result type appropriately
#[allow(clippy::not_unsafe_ptr_arg_deref)]
#[wasm_bindgen]
pub fn simulation_query(ptr: *const Simulation, query: &str) -> String {
    let result = unsafe { &*ptr }.execute_query(query).map_err(|errs| {
        errs.into_iter()
            .map(|server_err| format!("{:?}", server_err))
            .collect::<Vec<_>>()
            //.into()
            .join("\n")
    });
    serde_json::to_string(&result).unwrap()
}

#[allow(clippy::not_unsafe_ptr_arg_deref)]
#[wasm_bindgen]
pub fn simulation_set_tick(ptr: *mut Simulation, tick_ms: i32) {
    unsafe { &mut *ptr }.set_tick(Duration::from_millis(tick_ms.try_into().unwrap()))
}

#[allow(clippy::not_unsafe_ptr_arg_deref)]
#[wasm_bindgen]
pub fn simulation_step(ptr: *mut Simulation) {
    unsafe { &mut *ptr }.step()
}

#[allow(clippy::not_unsafe_ptr_arg_deref)]
#[wasm_bindgen]
pub fn simulation_reset(ptr: *mut Simulation) {
    unsafe { &mut *ptr }.reset()
}

#[allow(clippy::not_unsafe_ptr_arg_deref)]
#[wasm_bindgen]
pub fn simulation_restart(ptr: *mut Simulation) {
    unsafe { &mut *ptr }.restart()
}

#[allow(clippy::not_unsafe_ptr_arg_deref)]
#[wasm_bindgen]
pub fn simulation_apply_validator_config(ptr: *mut Simulation, stakes_json: &str) -> String {
    let result = serde_json::from_str(stakes_json)
        .map_err(|err| format!("invalid validator configuration: {err}"))
        .and_then(|stakes| unsafe { &mut *ptr }.apply_validator_config(stakes));
    serialize_result(result)
}

#[allow(clippy::not_unsafe_ptr_arg_deref)]
#[wasm_bindgen]
pub fn simulation_set_default_latency(ptr: *mut Simulation, latency_ms: i32) -> String {
    serialize_result(unsafe { &mut *ptr }.set_default_latency(latency_ms))
}

#[allow(clippy::not_unsafe_ptr_arg_deref)]
#[wasm_bindgen]
pub fn simulation_set_link_latency(
    ptr: *mut Simulation,
    from_id: &str,
    to_id: &str,
    latency_ms: i32,
) -> String {
    serialize_result(unsafe { &mut *ptr }.set_link_latency(from_id, to_id, latency_ms))
}

#[allow(clippy::not_unsafe_ptr_arg_deref)]
#[wasm_bindgen]
pub fn simulation_set_link_dropped(
    ptr: *mut Simulation,
    from_id: &str,
    to_id: &str,
    dropped: bool,
) -> String {
    serialize_result(unsafe { &mut *ptr }.set_link_dropped(from_id, to_id, dropped))
}

#[allow(clippy::not_unsafe_ptr_arg_deref)]
#[wasm_bindgen]
pub fn simulation_clear_link_rule(ptr: *mut Simulation, from_id: &str, to_id: &str) -> String {
    serialize_result(unsafe { &mut *ptr }.clear_link_rule(from_id, to_id))
}

#[allow(clippy::not_unsafe_ptr_arg_deref)]
#[wasm_bindgen]
pub fn simulation_set_node_position(ptr: *mut Simulation, node_id: &str, x: i32, y: i32) -> String {
    serialize_result(unsafe { &mut *ptr }.set_node_position(node_id, x, y))
}

#[allow(clippy::not_unsafe_ptr_arg_deref)]
#[wasm_bindgen]
pub fn simulation_set_node_online(ptr: *mut Simulation, node_id: &str, online: bool) -> String {
    serialize_result(unsafe { &mut *ptr }.set_node_online(node_id, online))
}

#[allow(clippy::not_unsafe_ptr_arg_deref)]
#[wasm_bindgen]
pub fn simulation_free(ptr: *mut Simulation) {
    unsafe {
        let _ = Box::from_raw(ptr);
    }
}

fn serialize_result(result: Result<(), String>) -> String {
    serde_json::to_string(&result).unwrap()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use monad_types::{Round, Stake};
    use monad_validator::{
        leader_election::LeaderElection,
        validator_set::{ValidatorSetType, ValidatorSetTypeFactory},
        weighted_round_robin::WeightedRoundRobin,
    };

    use super::{
        default_swarm_config, simulation_apply_validator_config, Simulation, ValidatorConfig,
    };

    #[test]
    fn malformed_validator_payload_does_not_change_simulation() {
        let mut simulation = Simulation::new(Box::new(default_swarm_config));
        let result = simulation_apply_validator_config(&mut simulation, "not json");

        assert!(result.contains("Err"));
        assert_eq!(simulation.validator_config.stakes(), &[1, 1, 1, 1]);
        assert_eq!(simulation.swarm.states().len(), 4);
    }

    #[test]
    fn configured_stakes_reach_every_node_and_weighted_election() {
        let config = ValidatorConfig::new(vec![1, 4, 2]).unwrap();
        let configured = default_swarm_config(&config);
        let expected_stakes: BTreeMap<_, _> = configured
            .validator_ids
            .iter()
            .copied()
            .zip(config.stakes().iter().copied().map(Stake::from))
            .collect();

        for node in &configured.builder.0 {
            let actual_stakes: BTreeMap<_, _> = node.state_builder.locked_epoch_validators[0]
                .validators
                .get_stakes()
                .into_iter()
                .collect();
            assert_eq!(actual_stakes, expected_stakes);
        }

        let round = Round(7);
        let actual_leader = configured.builder.0[0]
            .state_builder
            .leader_election
            .get_leader(round, &expected_stakes);
        let expected_leader = WeightedRoundRobin::default().get_leader(round, &expected_stakes);
        assert_eq!(actual_leader, expected_leader);

        let validator_set = configured.builder.0[0]
            .state_builder
            .validator_set_factory
            .create(
                expected_stakes
                    .iter()
                    .map(|(id, stake)| (*id, *stake))
                    .collect(),
            )
            .unwrap();
        assert!(!validator_set
            .has_super_majority_votes(&[configured.validator_ids[1]])
            .unwrap());
        assert!(validator_set
            .has_super_majority_votes(&[configured.validator_ids[0], configured.validator_ids[1]])
            .unwrap());
    }
}
