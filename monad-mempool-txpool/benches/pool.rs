use std::{
    sync::{Arc, Mutex},
    thread,
    time::{Duration, SystemTime},
};

use criterion::{criterion_group, criterion_main, Criterion};
use monad_mempool_testutil::create_signed_eth_txs;
use monad_mempool_txpool::{Pool, PoolConfig};
use reth_primitives::TransactionSignedEcRecovered;

const THREAD_COUNT: u16 = 2;
const WARMUP_TXS: u16 = 10000;
const TX_PER_THREAD: u16 = 10000;

pub fn benchmark_pool(c: &mut Criterion) {
    c.bench_function("create single proposal with concurrent write/read", |b| {
        let txs_for_threads: Vec<Vec<TransactionSignedEcRecovered>> = (0..THREAD_COUNT)
            .map(|i| create_signed_eth_txs(i.into(), TX_PER_THREAD))
            .collect();

        b.iter_batched(
            || {
                let pool = Arc::new(Mutex::new(Pool::new(PoolConfig::default())));
                for tx in create_signed_eth_txs(128, WARMUP_TXS) {
                    pool.lock().unwrap().insert(tx, SystemTime::now()).unwrap();
                }
                (pool, txs_for_threads.clone())
            },
            |(pool, mut tx_for_threads)| {
                for _ in 0..THREAD_COUNT {
                    let txs = tx_for_threads.pop().unwrap();
                    let pool = pool.clone();
                    thread::spawn(move || {
                        let mut pool = pool.lock().unwrap();
                        for tx in txs {
                            pool.insert(tx.clone(), SystemTime::now()).unwrap();
                        }
                    });
                }

                let mut pool = pool.lock().unwrap();
                let proposal = pool.create_proposal(TX_PER_THREAD.into(), vec![]);
                pool.remove_tx_hashes(proposal);
            },
            criterion::BatchSize::SmallInput,
        )
    });

    c.bench_function("create multi proposal with concurrent write/read", |b| {
        let txs_for_threads: Vec<Vec<TransactionSignedEcRecovered>> = (0..THREAD_COUNT)
            .map(|i| create_signed_eth_txs(i.into(), TX_PER_THREAD))
            .collect();

        b.iter_batched(
            || {
                let pool = Arc::new(Mutex::new(Pool::new(PoolConfig::default())));
                for tx in create_signed_eth_txs(128, WARMUP_TXS) {
                    pool.lock().unwrap().insert(tx, SystemTime::now()).unwrap();
                }
                (pool, txs_for_threads.clone())
            },
            |(pool, mut tx_for_threads)| {
                for _ in 0..THREAD_COUNT {
                    let pool = pool.clone();
                    let txs = tx_for_threads.pop().unwrap();
                    thread::spawn(move || {
                        let mut pool = pool.lock().unwrap();
                        for tx in txs {
                            pool.insert(tx.clone(), SystemTime::now()).unwrap();
                        }
                    });
                }

                {
                    let mut pool: std::sync::MutexGuard<Pool> = pool.lock().unwrap();
                    let proposal = pool.create_proposal(TX_PER_THREAD.into(), vec![]);
                    pool.remove_tx_hashes(proposal);
                }

                thread::sleep(Duration::from_millis(1));

                {
                    let mut pool: std::sync::MutexGuard<Pool> = pool.lock().unwrap();
                    let proposal2 = pool.create_proposal(TX_PER_THREAD.into(), vec![]);
                    pool.remove_tx_hashes(proposal2);
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
