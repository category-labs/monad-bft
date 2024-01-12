mod common;
use std::time::Duration;

use monad_consensus_types::block_validator::MockValidator;
use monad_mock_swarm::{mock_swarm::UntilTerminator, swarm_relation::NoSerSwarm};
use monad_router_scheduler::NoSerRouterConfig;
use monad_testutil::swarm::{create_and_run_nodes, SwarmTestConfig};
use monad_transformer::{GenericTransformer, XorLatencyTransformer};
use monad_types::{Round, SeqNum};
use monad_wal::mock::MockWALoggerConfig;

#[test]
fn two_nodes() {
    tracing_subscriber::fmt::init();

    create_and_run_nodes::<NoSerSwarm, _, _>(
        MockValidator,
        |all_peers, _| NoSerRouterConfig {
            all_peers: all_peers.into_iter().collect(),
        },
        MockWALoggerConfig,
        vec![GenericTransformer::XorLatency(XorLatencyTransformer::new(
            Duration::from_millis(u8::MAX as u64),
        ))],
        UntilTerminator::new().until_tick(Duration::from_secs(60)),
        SwarmTestConfig {
            num_nodes: 4,
            consensus_delta: Duration::from_millis(101),
            parallelize: false,
            expected_block: 40,
            state_root_delay: 4,
            seed: 1,
            proposal_size: 0,
            val_set_update_interval: SeqNum(2000),
            epoch_start_delay: Round(50),
        },
    );
}
