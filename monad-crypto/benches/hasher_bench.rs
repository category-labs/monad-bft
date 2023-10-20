use criterion::{criterion_group, criterion_main, Criterion};
use monad_crypto::hasher::{Blake3Hash, Hasher, Sha256Hash};

pub fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("sha256 (5_000 * 32 bytes)", |b| {
        let bytes = vec![0xff; 5_000 * 32];
        b.iter(|| hash::<Sha256Hash>(&bytes));
    });

    c.bench_function("blake3 (5_000 * 32 bytes)", |b| {
        let bytes = vec![0xff; 5_000 * 32];
        b.iter(|| hash::<Blake3Hash>(&bytes));
    });
}

fn hash<H: Hasher>(bytes: &[u8]) {
    let mut hasher = H::new();
    hasher.update(bytes);
    hasher.hash();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
