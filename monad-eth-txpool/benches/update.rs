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

use std::collections::BTreeMap;

use criterion::{criterion_group, criterion_main, Criterion};
use monad_chain_config::MockChainConfig;
use monad_eth_block_policy::EthBlockPolicy;
use monad_eth_testutil::generate_block_with_txs;
use monad_eth_txpool::{EthTxPoolEventTracker, EthTxPoolMetrics};
use monad_types::{Round, SeqNum, GENESIS_SEQ_NUM};

use self::common::{run_txpool_benches, BenchController, EXECUTION_DELAY};

mod common;

const BASE_FEE_PER_GAS: u64 = 100_000_000_000;

fn criterion_benchmark(c: &mut Criterion) {
    // TODO: change this to something more meaningful, i.e. what's is the block
    // policy state we want to benchmark
    let block_policy = EthBlockPolicy::new(GENESIS_SEQ_NUM, EXECUTION_DELAY);

    run_txpool_benches(
        c,
        "clear",
        |_, _, txs| {
            let metrics = EthTxPoolMetrics::default();

            let pool = BenchController::create_pool(
                &block_policy,
                &MockChainConfig::DEFAULT,
                Vec::default(),
                &metrics,
            );

            (
                pool,
                metrics,
                generate_block_with_txs(
                    Round(1),
                    SeqNum(1),
                    BASE_FEE_PER_GAS,
                    &MockChainConfig::DEFAULT,
                    txs,
                ),
            )
        },
        |(pool, metrics, block)| {
            pool.update_committed_block(
                &mut EthTxPoolEventTracker::new(metrics, &mut BTreeMap::default()),
                &MockChainConfig::DEFAULT,
                block.to_owned(),
            );
        },
    );
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
