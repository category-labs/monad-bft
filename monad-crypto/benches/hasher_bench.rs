use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use monad_crypto::hasher::{Blake3Hash, Hasher, Sha256Hash};

pub fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("hasher");
    let total_bytes = 10_000 * 400;
    group.throughput(Throughput::Bytes(total_bytes as u64));
    group.bench_function("sha256 10KB batch", |b| {
        let batch = vec![vec![0xff; 10_000]; 400];
        b.iter(|| {
            for bytes in &batch {
                hash::<Sha256Hash>(bytes)
            }
        });
    });
    group.bench_function("sha256 1KB batch", |b| {
        let batch = vec![vec![0xff; 1_000]; 4_000];
        b.iter(|| {
            for bytes in &batch {
                hash::<Sha256Hash>(bytes)
            }
        });
    });

    group.bench_function("blake3 10KB batch", |b| {
        let batch = vec![vec![0xff; 10_000]; 400];
        b.iter(|| {
            for bytes in &batch {
                hash::<Blake3Hash>(bytes)
            }
        });
    });
    group.bench_function("blake3 1KB batch", |b| {
        let batch = vec![vec![0xff; 1_000]; 4_000];
        b.iter(|| {
            for bytes in &batch {
                hash::<Blake3Hash>(bytes)
            }
        });
    });
}

fn hash<H: Hasher>(bytes: &[u8]) {
    let mut hasher = H::new();
    hasher.update(bytes);
    hasher.hash();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
