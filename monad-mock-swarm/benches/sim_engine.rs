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

//! Wall-clock comparison of the legacy `Nodes` engine and the `monad-sim`
//! harness driving the same `NoSerSwarm` consensus to the same committed-block
//! target. Build time is excluded (measured per iteration via `iter_batched`).

use std::{collections::BTreeSet, time::Duration};

use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use monad_chain_config::{revision::ChainParams, MockChainConfig};
use monad_consensus_types::{block::PassthruBlockPolicy, block_validator::MockValidator};
use monad_crypto::certificate_signature::CertificateKeyPair;
use monad_execution_state_read::InMemoryStateInner;
use monad_mock_swarm::{
    mock::TimestamperConfig,
    mock_swarm::{Nodes, SwarmBuilder},
    node::NodeBuilder,
    sim::{build_swarm_with, Network, SimSwarm},
    swarm::make_state_configs,
    swarm_relation::NoSerSwarm,
    terminator::UntilTerminator,
};
use monad_router_scheduler::{NoSerRouterConfig, RouterSchedulerBuilder};
use monad_sim::Time;
use monad_transformer::{GenericTransformer, LatencyTransformer, ID};
use monad_types::{NodeId, SeqNum};
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

const LATENCY: Duration = Duration::from_millis(1);
const DELTA: Duration = Duration::from_millis(100);
const TARGET: usize = 100;
const NODE_COUNTS: [u16; 4] = [4, 16, 40, 64];

fn build_existing(num_nodes: u16) -> Nodes<NoSerSwarm> {
    let state_configs = make_state_configs::<NoSerSwarm>(
        num_nodes,
        ValidatorSetFactory::default,
        SimpleRoundRobin::default,
        || MockValidator,
        || PassthruBlockPolicy,
        || InMemoryStateInner::genesis(SeqNum(4)),
        SeqNum(4),
        DELTA,
        MockChainConfig::new(&CHAIN_PARAMS),
        SeqNum(100),
    );
    let all_peers: BTreeSet<_> = state_configs
        .iter()
        .map(|c| NodeId::new(c.key.pubkey()))
        .collect();
    SwarmBuilder::<NoSerSwarm>(
        state_configs
            .into_iter()
            .enumerate()
            .map(|(seed, sb)| {
                let state_read = sb.state_read.clone();
                let validators = sb.locked_epoch_validators[0].clone();
                NodeBuilder::<NoSerSwarm>::new(
                    ID::new(NodeId::new(sb.key.pubkey())),
                    sb,
                    NoSerRouterConfig::new(all_peers.clone()).build(),
                    MockValSetUpdaterNop::new(validators.validators, SeqNum(2000)),
                    MockTxPoolExecutor::default().with_chain_params(&CHAIN_PARAMS),
                    MockLedger::new(state_read.clone()),
                    MockStateSyncExecutor::new(state_read),
                    vec![GenericTransformer::Latency(LatencyTransformer::new(
                        LATENCY,
                    ))],
                    vec![],
                    TimestamperConfig::default(),
                    seed.try_into().unwrap(),
                )
            })
            .collect(),
    )
    .build()
}

fn run_existing(swarm: &mut Nodes<NoSerSwarm>) {
    while swarm
        .step_until(&mut UntilTerminator::new().until_block(TARGET))
        .is_some()
    {}
}

/// The legacy *parallel* path (rayon `batch_step_until`) — the one the new
/// serial heap replaces. Benchmarked so the "dropping parallelism" trade is
/// measured, not assumed.
fn run_existing_batch(swarm: &mut Nodes<NoSerSwarm>) {
    swarm.batch_step_until(&mut UntilTerminator::new().until_block(TARGET));
}

fn build_sim(num_nodes: u16) -> SimSwarm<NoSerSwarm> {
    build_swarm_with(num_nodes, 0, |_| Network::reliable(LATENCY))
}

fn run_sim(swarm: &mut SimSwarm<NoSerSwarm>) {
    swarm.run_until_blocks(TARGET, Time(i128::MAX));
}

fn benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("commit_blocks");
    for n in NODE_COUNTS {
        group.bench_function(format!("existing/{n}"), |b| {
            b.iter_batched(
                || build_existing(n),
                |mut swarm| run_existing(&mut swarm),
                BatchSize::SmallInput,
            )
        });
        group.bench_function(format!("sim/{n}"), |b| {
            b.iter_batched(
                || build_sim(n),
                |mut swarm| run_sim(&mut swarm),
                BatchSize::SmallInput,
            )
        });
        group.bench_function(format!("existing_batch/{n}"), |b| {
            b.iter_batched(
                || build_existing(n),
                |mut swarm| run_existing_batch(&mut swarm),
                BatchSize::SmallInput,
            )
        });
    }
    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(20);
    targets = benchmark
}
criterion_main!(benches);
