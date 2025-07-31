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
