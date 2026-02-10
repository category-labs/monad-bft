use std::collections::HashMap;

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use monad_fair_queue::{FairQueue, FairQueueBuilder, IdentityScore, PeerStatus, Score};

const NUM_IDENTITIES: u32 = 100_000;

#[derive(Clone)]
struct TestScorer {
    scores: HashMap<u32, f64>,
}

impl IdentityScore for TestScorer {
    type Identity = u32;

    fn score(&self, identity: &Self::Identity) -> PeerStatus {
        let s = self.scores.get(identity).copied().unwrap_or(1.0);
        if s >= 0.5 {
            PeerStatus::Promoted(Score::try_from(s).unwrap())
        } else {
            PeerStatus::Newcomer(Score::try_from(s).unwrap())
        }
    }
}

type TestQueue = FairQueue<TestScorer, u32>;

fn make_queue(scorer: TestScorer) -> TestQueue {
    FairQueueBuilder::new()
        .per_id_limit(1000)
        .max_size(10_000_000)
        .regular_bandwidth_pct(0)
        .build(scorer)
}

fn bench_push(c: &mut Criterion) {
    let mut group = c.benchmark_group("fair_queue_push");

    for num_ids in [1_000, 10_000, 100_000] {
        group.throughput(Throughput::Elements(num_ids as u64));
        group.bench_with_input(
            BenchmarkId::new("identities", num_ids),
            &num_ids,
            |b, &num_ids| {
                let scores: HashMap<u32, f64> =
                    (0..num_ids).map(|id| (id, (id % 10 + 1) as f64)).collect();
                let scorer = TestScorer { scores };

                b.iter(|| {
                    let mut queue = make_queue(scorer.clone());
                    for identity in 0..num_ids {
                        let _ = queue.push(black_box(identity), black_box(identity));
                    }
                    assert_eq!(queue.len(), num_ids as usize);
                })
            },
        );
    }

    group.finish();
}

fn bench_push_pop_interleaved(c: &mut Criterion) {
    let mut group = c.benchmark_group("fair_queue_push_pop");

    for num_ids in [1_000, 10_000, 100_000] {
        let ops = num_ids * 2;
        group.throughput(Throughput::Elements(ops as u64));
        group.bench_with_input(
            BenchmarkId::new("identities", num_ids),
            &num_ids,
            |b, &num_ids| {
                let scores: HashMap<u32, f64> =
                    (0..num_ids).map(|id| (id, (id % 10 + 1) as f64)).collect();
                let scorer = TestScorer { scores };

                b.iter_batched(
                    || {
                        let mut queue = make_queue(scorer.clone());
                        for identity in 0..num_ids {
                            let _ = queue.push(identity, identity);
                        }
                        queue
                    },
                    |mut queue| {
                        for identity in 0..num_ids {
                            let _ = queue.push(black_box(identity), black_box(identity));
                            black_box(queue.pop());
                        }
                    },
                    criterion::BatchSize::SmallInput,
                )
            },
        );
    }

    group.finish();
}

fn bench_pop_heavy(c: &mut Criterion) {
    let mut group = c.benchmark_group("fair_queue_pop");

    for num_ids in [1_000, 10_000, 100_000] {
        group.throughput(Throughput::Elements(num_ids as u64));
        group.bench_with_input(
            BenchmarkId::new("identities", num_ids),
            &num_ids,
            |b, &num_ids| {
                let scores: HashMap<u32, f64> =
                    (0..num_ids).map(|id| (id, (id % 10 + 1) as f64)).collect();
                let scorer = TestScorer { scores };

                b.iter_batched(
                    || {
                        let mut queue = make_queue(scorer.clone());
                        for identity in 0..num_ids {
                            let _ = queue.push(identity, identity);
                        }
                        queue
                    },
                    |mut queue| {
                        for _ in 0..num_ids {
                            black_box(queue.pop());
                        }
                        assert!(queue.is_empty());
                    },
                    criterion::BatchSize::SmallInput,
                )
            },
        );
    }

    group.finish();
}

fn bench_mixed_workload(c: &mut Criterion) {
    let mut group = c.benchmark_group("fair_queue_mixed");

    let num_ids = NUM_IDENTITIES;
    let items_per_id = 10u32;
    let total_items = num_ids * items_per_id;

    group.throughput(Throughput::Elements(total_items as u64));
    group.bench_function("100k_identities_10_items_each", |b| {
        let scores: HashMap<u32, f64> = (0..num_ids).map(|id| (id, (id % 10 + 1) as f64)).collect();
        let scorer = TestScorer { scores };

        b.iter_batched(
            || make_queue(scorer.clone()),
            |mut queue| {
                for identity in 0..num_ids {
                    for item in 0..items_per_id {
                        let _ = queue.push(black_box(identity), black_box(identity * 1000 + item));
                    }
                }
                assert_eq!(queue.len(), total_items as usize);

                for _ in 0..total_items {
                    black_box(queue.pop());
                }
                assert!(queue.is_empty());
            },
            criterion::BatchSize::SmallInput,
        )
    });

    group.finish();
}

fn bench_skewed_scores(c: &mut Criterion) {
    let mut group = c.benchmark_group("fair_queue_skewed");

    let num_ids = NUM_IDENTITIES;
    group.throughput(Throughput::Elements(num_ids as u64));

    group.bench_function("100k_identities_zipf_scores", |b| {
        let scores: HashMap<u32, f64> = (0..num_ids)
            .map(|id| (id, 1.0 / (id as f64 + 1.0).sqrt()))
            .collect();
        let scorer = TestScorer { scores };

        b.iter_batched(
            || {
                let mut queue = make_queue(scorer.clone());
                for identity in 0..num_ids {
                    let _ = queue.push(identity, identity);
                }
                queue
            },
            |mut queue| {
                for _ in 0..num_ids {
                    black_box(queue.pop());
                }
            },
            criterion::BatchSize::SmallInput,
        )
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_push,
    bench_push_pop_interleaved,
    bench_pop_heavy,
    bench_mixed_workload,
    bench_skewed_scores,
);
criterion_main!(benches);
