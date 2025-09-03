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

use monad_chain_config::{revision::ChainParams, MockChainConfig};
use monad_consensus_types::{block::PassthruBlockPolicy, block_validator::MockValidator};
use monad_crypto::certificate_signature::CertificateKeyPair;
use monad_mock_swarm::{
    mock::TimestamperConfig, mock_swarm::SwarmBuilder, node::NodeBuilder,
    swarm_relation::NoSerSwarm,
};
use monad_router_scheduler::{NoSerRouterConfig, RouterSchedulerBuilder};
use monad_state_backend::InMemoryStateInner;
use monad_testutil::swarm::make_state_configs;
use monad_transformer::{
    DropTransformer, GenericTransformer, LatencyTransformer, PeriodicTransformer,
    RandLatencyTransformer, ID,
};
use monad_types::{Balance, NodeId, SeqNum};
use monad_updaters::{
    ledger::MockLedger, statesync::MockStateSyncExecutor, txpool::MockTxPoolExecutor,
    val_set::MockValSetUpdaterNop,
};
use monad_validator::{simple_round_robin::SimpleRoundRobin, validator_set::ValidatorSetFactory};
use wasm_bindgen::prelude::*;

mod graphql;
pub use graphql::GraphQLRoot;
mod simulation;
use simulation::Simulation;

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

    validate_system_txs: true,
    eip_7702: true,
};

#[wasm_bindgen]
pub fn simulation_make() -> *mut Simulation {
    let config = || {
        let state_configs = make_state_configs::<NoSerSwarm>(
            4, // num_nodes
            ValidatorSetFactory::default,
            SimpleRoundRobin::default,
            || MockValidator,
            || PassthruBlockPolicy,
            || InMemoryStateInner::genesis(Balance::MAX, SeqNum(4)),
            SeqNum(4),                           // execution_delay
            Duration::from_millis(50),           // delta
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
                        MockTxPoolExecutor::default(),
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
                                Duration::from_millis(25),
                            )),
                            GenericTransformer::RandLatency(RandLatencyTransformer::new(
                                0,
                                Duration::from_millis(25),
                            )),
                            GenericTransformer::Periodic(PeriodicTransformer::new(
                                Duration::from_millis(1_000),
                                Duration::MAX,
                            )),
                            GenericTransformer::Drop(
                                DropTransformer::new().drop_only_from(*all_peers.last().unwrap()),
                            ),
                        ],
                        vec![],
                        TimestamperConfig::default(),
                        seed.try_into().unwrap(),
                    )
                })
                .collect(),
        );
        swarm_config.debug()
    };

    Box::into_raw(Box::new(Simulation::new(Box::new(config))))
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
pub fn simulation_free(ptr: *mut Simulation) {
    unsafe {
        let _ = Box::from_raw(ptr);
    }
}
