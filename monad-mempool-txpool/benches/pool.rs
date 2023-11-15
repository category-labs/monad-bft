use std::time::SystemTime;

use criterion::{criterion_group, criterion_main, Criterion};
use monad_mempool_testutil::create_signed_eth_txs;
use monad_mempool_txpool::{Pool, PoolConfig};

const WARMUP_SEED: u64 = 31415926;
const EXTRA_SEED: u64 = WARMUP_SEED + 1;
const WARMUP_TXS: u16 = 10000;
const EXTRA_TXS: u16 = 10000;
const LARGE_MULTIPLIER: u16 = 5;

pub fn benchmark_pool(c: &mut Criterion) {
    c.bench_function("mempool_small_txs_quantity_test", |b| {
        let new_txs = create_signed_eth_txs(WARMUP_SEED, EXTRA_TXS);
        b.iter_batched(
            || {
                let mut pool = Pool::new(PoolConfig::default());
                for tx in create_signed_eth_txs(EXTRA_SEED, WARMUP_TXS) {
                    pool.insert(tx, SystemTime::now()).unwrap();
                }
                (pool, new_txs.clone())
            },
            |(mut pool, new_txs)| {
                for tx in new_txs {
                    pool.insert(tx, SystemTime::now()).unwrap();
                }

                let proposal = pool.create_proposal(EXTRA_TXS as usize, vec![]);
                pool.remove_tx_hashes(proposal);
            },
            criterion::BatchSize::SmallInput,
        )
    });

    // large test case has txs cnt =  LARGE_MULTIPLIER * (small test case txs cnt)
    c.bench_function("mempool_large_txs_quantity_test", |b| {
        let new_txs = create_signed_eth_txs(EXTRA_SEED, EXTRA_TXS * LARGE_MULTIPLIER);
        b.iter_batched(
            || {
                let mut pool = Pool::new(PoolConfig::default());
                for tx in create_signed_eth_txs(WARMUP_SEED, WARMUP_TXS) {
                    pool.insert(tx, SystemTime::now()).unwrap();
                }
                (pool, new_txs.clone())
            },
            |(mut pool, new_txs)| {
                for tx in new_txs {
                    pool.insert(tx, SystemTime::now()).unwrap();
                }
                for _ in 0..LARGE_MULTIPLIER {
                    let proposal = pool.create_proposal(EXTRA_TXS as usize, vec![]);
                    pool.remove_tx_hashes(proposal);
                }
            },
            criterion::BatchSize::SmallInput,
        )
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(50);
    targets = benchmark_pool
}
criterion_main!(benches);
