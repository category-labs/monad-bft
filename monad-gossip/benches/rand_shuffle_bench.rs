use criterion::{criterion_group, criterion_main, Criterion};
use rand::{seq::SliceRandom, SeedableRng};
use rand_chacha::{ChaCha20Rng, ChaCha8Rng};

#[allow(clippy::useless_vec)]
pub fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("chacha20", |b| {
        b.iter(|| {
            let mut rng = ChaCha20Rng::seed_from_u64(12);
            let mut vec = vec![100, 100];
            vec.shuffle(&mut rng);
        });
    });

    c.bench_function("fastrand (non-cryptographic)", |b| {
        b.iter(|| {
            fastrand::seed(12);
            let mut vec = vec![100, 100];
            fastrand::shuffle(&mut vec);
        });
    });

    c.bench_function("chacha8", |b| {
        b.iter(|| {
            let mut rng = ChaCha8Rng::seed_from_u64(12);
            let mut vec = vec![100, 100];
            vec.shuffle(&mut rng);
        });
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
