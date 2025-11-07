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
    collections::BTreeSet,
    env,
    time::{Duration, Instant},
};

use itertools::Itertools;
use monad_chain_config::{revision::ChainParams, MockChainConfig};
use monad_consensus_types::{
    block::PassthruBlockPolicy, block_validator::MockValidator, metrics::Metrics,
};
use monad_crypto::certificate_signature::CertificateKeyPair;
use monad_mock_swarm::{
    fetch_metric,
    mock::TimestamperConfig,
    mock_swarm::SwarmBuilder,
    node::NodeBuilder,
    swarm_relation::NoSerSwarm,
    terminator::UntilTerminator,
    verifier::{happy_path_tick_by_block, MockSwarmVerifier},
};
use monad_router_scheduler::{NoSerRouterConfig, RouterSchedulerBuilder};
use monad_state_backend::InMemoryStateInner;
use monad_testutil::swarm::{make_state_configs, swarm_ledger_verification};
use monad_transformer::{GenericTransformer, ID};
use monad_types::{Balance, NodeId, SeqNum};
use monad_updaters::{
    ledger::MockLedger, statesync::MockStateSyncExecutor, txpool::MockTxPoolExecutor,
    val_set::MockValSetUpdaterNop,
};
use monad_validator::{simple_round_robin::SimpleRoundRobin, validator_set::ValidatorSetFactory};
use rand::{rngs::StdRng, Rng, SeedableRng};
use test_case::test_case;

static CHAIN_PARAMS: ChainParams = ChainParams {
    tx_limit: 10_000,
    proposal_gas_limit: 300_000_000,
    proposal_byte_limit: 4_000_000,
    max_reserve_balance: 1_000_000_000_000_000_000,
    vote_pace: Duration::from_millis(10),
};

#[test]
#[ignore = "cron_test"]
fn nodes_with_random_latency_cron() {
    let time_seconds = match env::var("NODES_WITH_RANDOM_LATENCY_TIME_SECONDS") {
        Ok(v) => v.parse().unwrap(),
        Err(_e) => {
            println!("NODES_WITH_RANDOM_LATENCY_TIME_SECONDS is not set, using default of 60");
            60
        }
    };

    let mut seed = match env::var("RANDOM_TEST_SEED") {
        Ok(v) => v.parse().unwrap(),
        Err(_e) => {
            println!("RANDOM_TEST_SEED is not set, using default seed 0");
            0
        }
    };

    let start_time = Instant::now();

    let mut generator = StdRng::seed_from_u64(seed);
    while start_time.elapsed() < Duration::from_secs(time_seconds) {
        seed = generator.gen();

        match nodes_with_random_latency(seed) {
            Ok(_) => {}
            Err(_) => {
                panic!("failing seed is {}", seed);
            }
        }
    }
}

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
fn nodes_with_random_latency(latency_seed: u64) -> Result<(), String> {
    use std::time::Duration;

    use monad_transformer::RandLatencyTransformer;

    let delta = Duration::from_millis(200);
    let state_configs = make_state_configs::<NoSerSwarm>(
        4, // num_nodes
        ValidatorSetFactory::default,
        SimpleRoundRobin::default,
        || MockValidator,
        || PassthruBlockPolicy,
        || InMemoryStateInner::genesis(Balance::MAX, SeqNum::MAX),
        // avoid state_root trigger in rand latency setting
        // TODO-1, cover cases with low state_root_delay once state_sync is done
        SeqNum::MAX,                         // execution_delay
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
                    MockValSetUpdaterNop::new(validators.validators, SeqNum(3000)),
                    MockTxPoolExecutor::default().with_chain_params(&CHAIN_PARAMS),
                    MockLedger::new(state_backend.clone()),
                    MockStateSyncExecutor::new(state_backend),
                    vec![GenericTransformer::RandLatency(
                        RandLatencyTransformer::new(latency_seed, delta),
                    )],
                    vec![],
                    TimestamperConfig::default(),
                    seed.try_into().unwrap(),
                )
            })
            .collect(),
    );

    let mut swarm = swarm_config.build();
    let last_block = 2000;
    while swarm
        .step_until(&mut UntilTerminator::new().until_block(last_block))
        .is_some()
    {}

    // -5 is arbitrary. this is to ensure that nodes aren't lagging too
    // far behind because of the latency
    let min_ledger_len = last_block - 5;
    let max_blocksync_requests = 50;
    let max_tick = happy_path_tick_by_block(min_ledger_len, delta);

    let mut verifier = MockSwarmVerifier::default().tick_range(max_tick / 2, max_tick / 2);

    let node_ids = swarm.states().keys().copied().collect_vec();
    verifier
        // the node with the max blocksync requests could have the least
        // blocks in its ledger.
        // the last_block is committed by processing 2 QCs after it. there
        // should be no branching
        .metric_exact(
            &node_ids,
            fetch_metric!(consensus_events.local_timeout),
            1, // nodes time out on init
        )
        .metric_range(
            &node_ids,
            fetch_metric!(consensus_events.process_qc),
            min_ledger_len as u64 - max_blocksync_requests,
            last_block as u64 + 2,
        )
        .metric_maximum(
            &node_ids,
            fetch_metric!(blocksync_events.self_headers_request),
            max_blocksync_requests,
        )
        .metric_maximum(
            &node_ids,
            fetch_metric!(blocksync_events.self_payload_request),
            max_blocksync_requests,
        );

    if !verifier.verify(&swarm) {
        return Err("verification failed".to_string());
    }

    swarm_ledger_verification(&swarm, min_ledger_len);
    Ok(())
}
