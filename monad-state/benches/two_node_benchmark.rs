use criterion::{criterion_group, criterion_main, Criterion};

#[path = "../tests/base.rs"]
mod base;
use base::run_nodes;

pub fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("two nodes", |b| b.iter(|| two_nodes()));
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);

fn two_nodes() {
    run_nodes(2, 1024);
}
