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

use common::SignatureType;
use criterion::{criterion_group, criterion_main, Criterion};
use monad_chain_config::{revision::MockChainRevision, MockChainConfig};
use monad_eth_block_policy::EthBlockPolicy;
use monad_eth_txpool::{EthTxPool, EthTxPoolEventTracker, EthTxPoolMetrics, PoolTransactionKind};
use monad_types::GENESIS_SEQ_NUM;

use self::common::{run_txpool_benches, BenchController, SignatureCollectionType, EXECUTION_DELAY};

mod common;

fn criterion_benchmark(c: &mut Criterion) {
    // TODO: change this to something more meaningful, i.e. what's is the block
    // policy state we want to benchmark
    let block_policy: EthBlockPolicy<
        SignatureType,
        SignatureCollectionType,
        MockChainConfig,
        MockChainRevision,
    > = EthBlockPolicy::new(GENESIS_SEQ_NUM, EXECUTION_DELAY);

    run_txpool_benches(
        c,
        "insert",
        |_, _, txs| {
            let pool = EthTxPool::default_testing();

            let state_backend = BenchController::generate_state_backend_for_txs(&txs);

            (pool, txs, state_backend)
        },
        |(pool, txs, state_backend)| {
            pool.insert_txs(
                &mut EthTxPoolEventTracker::new(
                    &EthTxPoolMetrics::default(),
                    &mut BTreeMap::default(),
                ),
                &block_policy,
                state_backend,
                &MockChainConfig::DEFAULT,
                txs.iter()
                    .map(|tx| (tx.clone(), PoolTransactionKind::owned_default()))
                    .collect(),
                |_| {},
            );
        },
    );
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
