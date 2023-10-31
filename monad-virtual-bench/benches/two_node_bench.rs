use std::time::Duration;

use monad_consensus_types::transaction_validator::MockValidator;
use monad_mock_swarm::{
    mock::{MockMempoolConfig, NoSerRouterConfig},
    mock_swarm::UntilTerminator,
    swarm_relation::NoSerSwarm,
    transformer::{GenericTransformer, LatencyTransformer},
};
use monad_testutil::swarm::{create_and_run_nodes, SwarmTestConfig};
use monad_wal::mock::MockWALoggerConfig;

fn two_nodes_virtual() -> u128 {
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
            proposal_size: 0,
        },
    )
    .as_millis()
}

monad_virtual_bench::virtual_bench_main! {two_nodes_virtual}
