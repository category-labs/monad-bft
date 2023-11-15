use std::{
    collections::VecDeque,
    time::{Duration, SystemTime},
};

use criterion::{criterion_group, criterion_main, Criterion};
use monad_eth_types::EthTransactionList;
use monad_mempool_testutil::create_signed_eth_tx;
use monad_mempool_txpool::{Pool, PoolConfig};

const WARMUP_TXS: usize = 20000;
const EXTRA_TXS: usize = 10000;
const LARGE_TEST_MULTIPLIER: usize = 10;
// very large TTL_DURATION so txs don't get timed out
const TTL_DURATION: Duration = Duration::from_secs(86400);

pub fn benchmark_pool(c: &mut Criterion) {
    let pool_config = PoolConfig::default()
        .with_pending_duration(Duration::ZERO)
        .with_ttl_duration(TTL_DURATION);

    let timestamp = SystemTime::now();

    let setup = || {
        let mut pool = Pool::new(pool_config);
        // (tx, timestamp, priority)
        let mut gen = (1..).map(|i| (create_signed_eth_tx(i), timestamp, i as i64));

        // warm up the txpool
        for (tx, timestamp, priority) in gen.by_ref().take(WARMUP_TXS) {
            pool.insert_with_priority(tx, timestamp, priority).unwrap();
        }

        (pool, gen)
    };

    c.bench_function("mempool_small_txs_cnt_test", |b| {
        b.iter_batched(
            || {
                let (pool, gen) = setup();
                (pool, gen.take(EXTRA_TXS))
            },
            |(mut pool, txs)| {
                for (tx, timestamp, priority) in txs {
                    pool.insert_with_priority(tx, timestamp, priority).unwrap();
                }
                let proposal = pool.create_proposal(EXTRA_TXS, vec![]);
                assert_eq!(proposal.0.len(), EXTRA_TXS);
                pool.remove_tx_hashes(proposal);
            },
            criterion::BatchSize::SmallInput,
        )
    });

    c.bench_function("mempool_large_txs_cnt_test", |b| {
        b.iter_batched(
            || {
                let (pool, gen) = setup();
                (pool, gen.take(EXTRA_TXS * LARGE_TEST_MULTIPLIER))
            },
            |(mut pool, txs)| {
                for (tx, timestamp, priority) in txs {
                    pool.insert_with_priority(tx, timestamp, priority).unwrap();
                }
                for _ in 0..LARGE_TEST_MULTIPLIER {
                    let proposal = pool.create_proposal(EXTRA_TXS, vec![]);
                    assert_eq!(proposal.0.len(), EXTRA_TXS);
                    pool.remove_tx_hashes(proposal);
                }
            },
            criterion::BatchSize::SmallInput,
        )
    });

    c.bench_function("mempool_large_txs_with_pending_block_tree_test", |b| {
        b.iter_batched(
            || {
                let (pool, gen) = setup();
                (pool, gen.take(EXTRA_TXS * LARGE_TEST_MULTIPLIER))
            },
            |(mut pool, txs)| {
                for (tx, timestamp, priority) in txs {
                    pool.insert_with_priority(tx, timestamp, priority).unwrap();
                }
                let mut pending_blocks: VecDeque<EthTransactionList> = VecDeque::new();
                for _ in 0..LARGE_TEST_MULTIPLIER {
                    // simulate happy path, which is 2 blocks worth of pending txs.
                    let pending_blocktree_txs = pending_blocks.iter().cloned().collect::<Vec<_>>();
                    let proposal = pool.create_proposal(EXTRA_TXS, pending_blocktree_txs);
                    assert_eq!(proposal.0.len(), EXTRA_TXS);
                    pending_blocks.push_back(proposal);
                    while pending_blocks.len() > 2 {
                        // simulate block commits
                        pool.remove_tx_hashes(pending_blocks.pop_front().unwrap());
                    }
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
