mod common;

use std::time::{Duration, Instant};

use common::{NoSerSwarm, QuicSwarm};
use monad_consensus_types::{multi_sig::MultiSig, transaction_validator::MockValidator};
use monad_crypto::NopSignature;
use monad_gossip::mock::MockGossipConfig;
use monad_mock_swarm::{
    mock::{MockMempoolConfig, NoSerRouterConfig},
    mock_swarm::UntilTerminator,
    transformer::{BytesTransformer, GenericTransformer, LatencyTransformer},
};
use monad_quic::QuicRouterSchedulerConfig;
use monad_state::MonadMessage;
use monad_testutil::swarm::{create_and_run_nodes, SwarmTestConfig};
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
        MockMempoolConfig::default(),
        vec![GenericTransformer::Latency::<
            MonadMessage<NopSignature, MultiSig<NopSignature>>,
        >(LatencyTransformer(Duration::from_millis(1)))],
        UntilTerminator::new().until_tick(Duration::from_secs(10)),
        SwarmTestConfig {
            num_nodes: 2,
            consensus_delta: Duration::from_millis(2),
            parallelize: false,
            expected_block: 1024,
            state_root_delay: 4,
            seed: 1,
        },
    );
}

#[test]
fn two_nodes_quic() {
    let zero_instant = Instant::now();

    create_and_run_nodes::<QuicSwarm, _, _>(
        MockValidator,
        |all_peers, me| QuicRouterSchedulerConfig {
            zero_instant,
            all_peers: all_peers.iter().cloned().collect(),
            me,

            tls_key_der: Vec::new(),
            master_seed: 7,

            gossip_config: MockGossipConfig { all_peers },
        },
        MockWALoggerConfig,
        MockMempoolConfig::default(),
        vec![BytesTransformer::Latency(LatencyTransformer(
            Duration::from_millis(1),
        ))],
        UntilTerminator::new().until_tick(Duration::from_secs(10)),
        SwarmTestConfig {
            num_nodes: 2,
            consensus_delta: Duration::from_millis(10),
            parallelize: false,
            expected_block: 256,
            state_root_delay: 4,
            seed: 1,
        },
    );
}
