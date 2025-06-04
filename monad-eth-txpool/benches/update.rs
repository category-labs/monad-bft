use criterion::{criterion_group, criterion_main, Criterion};
use monad_eth_block_policy::EthBlockPolicy;
use monad_eth_testutil::generate_block_with_txs;
use monad_eth_txpool::{EthTxPoolEventTracker, EthTxPoolMetrics};
use monad_types::{Round, SeqNum, GENESIS_SEQ_NUM};

use self::common::{run_txpool_benches, BenchController, EXECUTION_DELAY};

mod common;

fn criterion_benchmark(c: &mut Criterion) {
    // TODO: change this to something more meaningful, i.e. what's is the block
    // policy state we want to benchmark
    let block_policy = EthBlockPolicy::new(GENESIS_SEQ_NUM, EXECUTION_DELAY, 1337);

    run_txpool_benches(
        c,
        "clear",
        |controller_config| {
            let (pending_txs, txs) = BenchController::generate_txs(
                controller_config.accounts,
                controller_config.txs,
                controller_config.nonce_var,
                0,
            );
            assert!(pending_txs.is_empty());

            let mut metrics = EthTxPoolMetrics::default();

            let pool = BenchController::create_pool(&block_policy, Vec::default(), &metrics);

            (
                pool,
                metrics,
                generate_block_with_txs(Round(1), SeqNum(1), txs),
            )
        },
        |(pool, metrics, block)| {
            pool.update_committed_block(
                &mut EthTxPoolEventTracker::new(metrics, &mut Vec::default()),
                block.to_owned(),
            );
        },
    );
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
