use std::time::Duration;

use criterion::{criterion_group, criterion_main, Criterion};

use monad_executor::mock_swarm::LatencyTransformer;
use monad_testutil::swarm::run_nodes;

pub fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("two nodes", |b| b.iter(|| two_nodes()));
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);

fn two_nodes() {
    run_nodes(
        2,
        1024,
        Duration::from_millis(2),
        LatencyTransformer(Duration::from_millis(1)),
    );
}
