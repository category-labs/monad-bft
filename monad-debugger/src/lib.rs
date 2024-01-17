use std::{collections::BTreeSet, time::Duration};

use monad_consensus_types::{
    block_validator::MockValidator, payload::StateRoot, txpool::MockTxPool,
};
use monad_crypto::certificate_signature::CertificateKeyPair;
use monad_mock_swarm::{mock_swarm::SwarmBuilder, node::NodeBuilder, swarm_relation::BytesSwarm};
use monad_router_scheduler::{BytesRouterConfig, RouterSchedulerBuilder};
use monad_testutil::swarm::make_state_configs;
use monad_transformer::{GenericTransformer, LatencyTransformer, ID};
use monad_types::{NodeId, Round, SeqNum};
use monad_updaters::state_root_hash::MockStateRootHashNop;
use monad_validator::{simple_round_robin::SimpleRoundRobin, validator_set::ValidatorSetFactory};
use monad_wal::mock::MockWALoggerConfig;
use wasm_bindgen::prelude::*;

mod graphql;
pub use graphql::GraphQLRoot;
mod simulation;
use simulation::Simulation;

#[wasm_bindgen(start)]
pub fn init() {
    std::panic::set_hook(Box::new(console_error_panic_hook::hook))
}

#[wasm_bindgen]
pub fn simulation_make() -> *mut Simulation {
    let config = || {
        let state_configs = make_state_configs::<BytesSwarm>(
            4, // num_nodes
            ValidatorSetFactory::default,
            SimpleRoundRobin::default,
            MockTxPool::default,
            || MockValidator,
            || {
                StateRoot::new(
                    SeqNum(4), // state_root_delay
                )
            },
            Duration::from_millis(20), // delta
            100,                       // proposal_tx_limit
            SeqNum(2000),              // val_set_update_interval
            Round(50),                 // epoch_start_delay
        );
        let all_peers: BTreeSet<_> = state_configs
            .iter()
            .map(|state_config| NodeId::new(state_config.key.pubkey()))
            .collect();
        let swarm_config = SwarmBuilder::<BytesSwarm>(
            state_configs
                .into_iter()
                .enumerate()
                .map(|(seed, state_builder)| {
                    let validators = state_builder.validators.clone();
                    NodeBuilder::<BytesSwarm>::new(
                        ID::new(NodeId::new(state_builder.key.pubkey())),
                        state_builder,
                        MockWALoggerConfig::default(),
                        BytesRouterConfig::new(all_peers.clone()).build(),
                        MockStateRootHashNop::new(validators, SeqNum(2000)),
                        vec![GenericTransformer::Latency(LatencyTransformer::new(
                            Duration::from_millis(10),
                        ))],
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
