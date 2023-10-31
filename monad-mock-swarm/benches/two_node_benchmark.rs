use std::time::Duration;

use criterion::{criterion_group, criterion_main, Criterion};
use monad_consensus_types::transaction_validator::MockValidator;
use monad_mock_swarm::{
    mock::{MockMempoolConfig, NoSerRouterConfig},
    mock_swarm::UntilTerminator,
    swarm_relation::NoSerSwarm,
    transformer::{GenericTransformer, LatencyTransformer},
};
use monad_testutil::swarm::{create_and_run_nodes, SwarmTestConfig};
use monad_wal::mock::MockWALoggerConfig;

pub fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("two_nodes", |b| b.iter(two_nodes));
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = criterion_benchmark
}
criterion_main!(benches);

fn two_nodes() {
    create_and_run_nodes::<NoSerSwarm, _, _>(
        MockValidator,
        |all_peers, _| NoSerRouterConfig {
            all_peers: all_peers.into_iter().collect(),
        },
        MockWALoggerConfig,
        MockMempoolConfig::default(),
        vec![GenericTransformer::Latency(LatencyTransformer(
            Duration::from_millis(1),
        ))],
        UntilTerminator::new().until_tick(Duration::from_secs(10)),
        SwarmTestConfig {
            num_nodes: 2,
            consensus_delta: Duration::from_millis(2),
            parallelize: false,
            expected_block: 1024,
            state_root_delay: 4,
            seed: 1,
            proposal_size: 5_000,
        },
    );
}
