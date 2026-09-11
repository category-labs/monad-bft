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

//! Vote-delay metrics, ported onto the `monad-sim` harness. This mirrors the
//! coverage of the legacy `vote_delay_metrics.rs` (which it does not replace)
//! using the new framework's self-contained surface.

use std::{collections::BTreeSet, time::Duration};

use monad_chain_config::{revision::ChainParams, MockChainConfig};
use monad_consensus_types::{block::PassthruBlockPolicy, block_validator::MockValidator};
use monad_crypto::certificate_signature::CertificateKeyPair;
use monad_execution_state_read::InMemoryStateInner;
use monad_mock_swarm::{
    sim::{Network, SimNodeBuilder, SimSwarm, DEFAULT_TIMESTAMP_PERIOD},
    swarm::make_state_configs,
    swarm_relation::NoSerSwarm,
};
use monad_router_scheduler::{NoSerRouterConfig, RouterSchedulerBuilder};
use monad_sim::{time::secs, Time};
use monad_types::{NodeId, SeqNum};
use monad_updaters::{
    ledger::MockLedger, statesync::MockStateSyncExecutor, txpool::MockTxPoolExecutor,
    val_set::MockValSetUpdaterNop,
};
use monad_validator::{simple_round_robin::SimpleRoundRobin, validator_set::ValidatorSetFactory};

static ON_TIME_CHAIN_PARAMS: ChainParams = ChainParams {
    tx_limit: 10_000,
    proposal_gas_limit: 300_000_000,
    proposal_byte_limit: 4_000_000,
    max_reserve_balance: 1_000_000_000_000_000_000,
    vote_pace: Duration::from_millis(500),
};

static LATE_CHAIN_PARAMS: ChainParams = ChainParams {
    tx_limit: 10_000,
    proposal_gas_limit: 300_000_000,
    proposal_byte_limit: 4_000_000,
    max_reserve_balance: 1_000_000_000_000_000_000,
    vote_pace: Duration::from_millis(5),
};

/// A two-node `NoSerSwarm` driven through the `monad-sim` harness, with link
/// latency `delta`.
fn two_node_sim(chain_params: &'static ChainParams, delta: Duration) -> SimSwarm<NoSerSwarm> {
    let state_configs = make_state_configs::<NoSerSwarm>(
        2,
        ValidatorSetFactory::default,
        SimpleRoundRobin::default,
        || MockValidator,
        || PassthruBlockPolicy,
        || InMemoryStateInner::genesis(SeqNum(4)),
        SeqNum(4),
        delta,
        MockChainConfig::new(chain_params),
        SeqNum(100),
    );
    let all_peers: BTreeSet<_> = state_configs
        .iter()
        .map(|config| NodeId::new(config.key.pubkey()))
        .collect();

    let builders: Vec<SimNodeBuilder<NoSerSwarm>> = state_configs
        .into_iter()
        .map(|config| {
            let state_read = config.state_read.clone();
            let validators = config.locked_epoch_validators[0].clone();
            SimNodeBuilder {
                id: NodeId::new(config.key.pubkey()),
                state_builder: config,
                router_scheduler: NoSerRouterConfig::new(all_peers.clone()).build(),
                val_set_updater: MockValSetUpdaterNop::new(validators.validators, SeqNum(2000)),
                txpool_executor: MockTxPoolExecutor::default().with_chain_params(chain_params),
                ledger: MockLedger::new(state_read.clone()),
                statesync_executor: MockStateSyncExecutor::new(state_read),
                timestamp_period: DEFAULT_TIMESTAMP_PERIOD,
            }
        })
        .collect();

    SimSwarm::from_builders(0, builders, |_| Network::reliable(delta))
}

#[test]
fn vote_delay_metrics_record_ready_after_timer_start_on_happy_path() {
    let delta = Duration::from_millis(100);
    let mut swarm = two_node_sim(&ON_TIME_CHAIN_PARAMS, delta);

    assert!(
        swarm.run_until_blocks(64, Time(0) + secs(120)),
        "did not reach 64 blocks"
    );
    assert!(swarm.finalized_blocks().iter().all(|&n| n >= 62));
    swarm.assert_agreement();

    for id in swarm.node_ids() {
        swarm.with_node(id, |node| {
            let metrics = node.metrics();
            let p50 = metrics.vote_delay.ready_after_timer_start_p50_ms.get();
            let p90 = metrics.vote_delay.ready_after_timer_start_p90_ms.get();
            let p99 = metrics.vote_delay.ready_after_timer_start_p99_ms.get();

            assert!(p50 > 0);
            assert!(p50 <= p90);
            assert!(p90 <= p99);
            assert!(p99 < ON_TIME_CHAIN_PARAMS.vote_pace.as_millis() as u64);
            assert!(metrics.consensus_events.local_timeout.get() <= 1);
        });
    }
}

#[test]
fn vote_delay_metrics_record_large_ready_after_timer_start_when_vote_pace_is_too_small() {
    let delta = Duration::from_millis(100);
    let mut swarm = two_node_sim(&LATE_CHAIN_PARAMS, delta);

    assert!(
        swarm.run_until_blocks(64, Time(0) + secs(120)),
        "did not reach 64 blocks"
    );
    assert!(swarm.finalized_blocks().iter().all(|&n| n >= 62));
    swarm.assert_agreement();

    for id in swarm.node_ids() {
        swarm.with_node(id, |node| {
            let metrics = node.metrics();
            let p50 = metrics.vote_delay.ready_after_timer_start_p50_ms.get();
            let p90 = metrics.vote_delay.ready_after_timer_start_p90_ms.get();
            let p99 = metrics.vote_delay.ready_after_timer_start_p99_ms.get();

            assert!(p50 > LATE_CHAIN_PARAMS.vote_pace.as_millis() as u64);
            assert!(p50 <= p90);
            assert!(p90 <= p99);
        });
    }
}
