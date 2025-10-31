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

use alloy_consensus::{transaction::Recovered, TxEnvelope};
use criterion::{black_box, BatchSize, Criterion};
use monad_chain_config::MockChainConfig;
use monad_crypto::NopSignature;
use monad_perf_util::PerfController;
use monad_testutil::signing::MockSignatures;

pub use self::controller::BenchController;
use self::controller::BenchControllerConfig;
use crate::common::txs::{generate_txs, BenchTxSetup};

mod controller;
pub mod txs;

pub const EXECUTION_DELAY: u64 = 4;
pub const RESERVE_BALANCE: u128 = 10_000_000_000_000_000_000;

pub type SignatureType = NopSignature;
pub type SignatureCollectionType = MockSignatures<NopSignature>;

const BENCH_CONFIGS: [(&str, BenchTxSetup); 5] = [
    (
        "simple",
        BenchTxSetup {
            accounts: 1_000,
            txs: 1_000,
            nonce_var: 0,
            pending_blocks: 2,
        },
    ),
    (
        "random",
        BenchTxSetup {
            accounts: 3_000,
            txs: 1_000,
            nonce_var: 0,
            pending_blocks: 2,
        },
    ),
    (
        "duplicate_nonce",
        BenchTxSetup {
            accounts: 100,
            txs: 1_000,
            nonce_var: 0,
            pending_blocks: 2,
        },
    ),
    (
        "increasing_nonce",
        BenchTxSetup {
            accounts: 10,
            txs: 1_000,
            nonce_var: 5,
            pending_blocks: 2,
        },
    ),
    (
        "nonce_gaps",
        BenchTxSetup {
            accounts: 5,
            txs: 1_000,
            nonce_var: 10,
            pending_blocks: 2,
        },
    ),
];

pub fn run_txpool_benches<T>(
    c: &mut Criterion,
    func_name: &'static str,
    mut setup: impl FnMut(
        &BenchControllerConfig,
        Vec<Vec<Recovered<TxEnvelope>>>,
        Vec<Recovered<TxEnvelope>>,
    ) -> T,
    mut routine: impl FnMut(&mut T),
) {
    let mut group = c.benchmark_group("txpool");

    let mut perf_controller = match PerfController::from_env() {
        Ok(perf) => Some(perf),
        Err(e) => {
            println!(
                "Failed to initialize perf controller, continuing without sampling. Did you define the `PERF_CTL_FD` and `PERF_CTL_FD_ACK` environment variables? Error: {e:?}",
            );

            None
        }
    };

    for (
        bench_name,
        BenchTxSetup {
            accounts,
            txs,
            nonce_var,
            pending_blocks,
        },
    ) in &BENCH_CONFIGS
    {
        let name = format!("{func_name}-{bench_name}");

        let (pending_block_txs, pool_txs) =
            generate_txs(*accounts, *txs, *nonce_var, *pending_blocks);

        group.bench_function(name, |b| {
            b.iter_batched_ref(
                || {
                    setup(
                        &BenchControllerConfig {
                            chain_config: MockChainConfig::DEFAULT,
                            proposal_tx_limit: 1_000,
                        },
                        pending_block_txs.clone(),
                        pool_txs.clone(),
                    )
                },
                |t| {
                    if let Some(perf) = perf_controller.as_mut() {
                        perf.enable();
                    }

                    routine(black_box(t));

                    if let Some(perf) = perf_controller.as_mut() {
                        perf.disable();
                    }
                },
                BatchSize::SmallInput,
            );
        });
    }
}
