use std::time::Duration;

use monad_consensus_types::block_validator::MockValidator;
use monad_mock_swarm::{
    mock::MockMempoolConfig, mock_swarm::UntilTerminator, swarm_relation::NoSerSwarm,
};
use monad_router_scheduler::NoSerRouterConfig;
use monad_testutil::swarm::{create_and_run_nodes, SwarmTestConfig};
use monad_transformer::{GenericTransformer, LatencyTransformer};
use monad_types::{Round, SeqNum};
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
            val_set_update_interval: SeqNum(2000),
            epoch_start_delay: Round(50),
        },
    )
    .as_millis()
}

monad_virtual_bench::virtual_bench_main! {two_nodes_virtual}
