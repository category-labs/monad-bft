mod common;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use common::{NoSerSwarm, QuicSwarm};
use monad_consensus_types::transaction_validator::MockValidator;
use monad_executor_glue::PeerId;
use monad_gossip::mock::MockGossipConfig;
use monad_mock_swarm::{
    mock::{MockMempoolConfig, NoSerRouterConfig},
    mock_swarm::UntilTerminator,
    transformer::{BwTransformer, BytesTransformer, GenericTransformer, LatencyTransformer},
};
use monad_quic::QuicRouterSchedulerConfig;
use monad_testutil::swarm::{create_and_run_nodes, SwarmTestConfig};
use monad_wal::mock::MockWALoggerConfig;
use tracing_test::traced_test;

#[test]
fn many_nodes_noser() {
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
        UntilTerminator::new().until_tick(Duration::from_secs(4)),
        SwarmTestConfig {
            num_nodes: 100,
            consensus_delta: Duration::from_millis(2),
            parallelize: true,
            expected_block: 1024,
            state_root_delay: 4,
            seed: 1,
        },
    );
}

#[test]
fn many_nodes_quic() {
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
        UntilTerminator::new().until_tick(Duration::from_secs(4)),
        SwarmTestConfig {
            num_nodes: 40,
            consensus_delta: Duration::from_millis(10),
            parallelize: true,
            expected_block: 10,
            state_root_delay: 4,
            seed: 1,
        },
    );
}

#[traced_test]
#[test]
fn many_nodes_quic_bw() {
    let zero_instant = Instant::now();

    let swarm_config = SwarmTestConfig {
        num_nodes: 40,
        consensus_delta: Duration::from_millis(1000),
        parallelize: false,
        expected_block: 10,
        state_root_delay: u64::MAX,
        seed: 1,
    };

    let xfmrs = vec![
        BytesTransformer::Latency(LatencyTransformer(Duration::from_millis(1))),
        BytesTransformer::Bw(BwTransformer::new(5)),
    ];

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
        xfmrs,
        UntilTerminator::new().until_tick(Duration::from_secs(100)),
        swarm_config,
    );

    logs_assert(|lines| {
        if lines
            .iter()
            .filter(|line| line.contains("monotonic_counter.bwtransfomer_dropped_msg"))
            .count()
            > 0
        {
            Ok(())
        } else {
            Err("Expected msg to be dropped".to_owned())
        }
    });
}

#[traced_test]
#[test]
fn many_nodes_quic_deterministic() {
    let zero_instant = Instant::now();

    let swarm_config = SwarmTestConfig {
        num_nodes: 40,
        consensus_delta: Duration::from_millis(1000),
        parallelize: false,
        expected_block: 35,
        state_root_delay: u64::MAX,
        seed: 1,
    };

    let terminator = UntilTerminator::new().until_block(40);

    let xfmrs = vec![
        BytesTransformer::Latency(LatencyTransformer(Duration::from_millis(1))),
        BytesTransformer::Bw(BwTransformer::new(3)),
    ];

    let tls_keys_der = Arc::new(Mutex::new(Vec::new()));
    let tls_keys_der_map = Arc::new(Mutex::new(HashMap::new()));

    for _ in 0..swarm_config.num_nodes {
        tls_keys_der.lock().unwrap().push(
            rcgen::KeyPair::generate(&rcgen::PKCS_ED25519).expect("generate keypair to succeed"),
        );
    }

    let router_scheduler_config = |all_peers: Vec<PeerId>, me: PeerId| {
        let key_der = tls_keys_der_map
            .lock()
            .unwrap()
            .entry(me.0.bytes())
            .or_insert_with(|| tls_keys_der.lock().unwrap().remove(0).serialize_der())
            .to_owned();

        QuicRouterSchedulerConfig {
            zero_instant,
            all_peers: all_peers.iter().cloned().collect(),
            me,
            tls_key_der: key_der,
            master_seed: 7,
            gossip_config: MockGossipConfig { all_peers },
        }
    };

    let duration1 = create_and_run_nodes::<QuicSwarm, _, _>(
        MockValidator,
        router_scheduler_config,
        MockWALoggerConfig,
        MockMempoolConfig::default(),
        xfmrs.clone(),
        terminator,
        swarm_config,
    );

    assert!(tls_keys_der.lock().unwrap().is_empty());
    assert_eq!(
        tls_keys_der_map.lock().unwrap().len(),
        swarm_config.num_nodes as usize
    );

    let duration2 = create_and_run_nodes::<QuicSwarm, _, _>(
        MockValidator,
        router_scheduler_config,
        MockWALoggerConfig,
        MockMempoolConfig::default(),
        xfmrs,
        terminator,
        swarm_config,
    );

    logs_assert(|lines| {
        if lines
            .iter()
            .filter(|line| line.contains("monotonic_counter.bwtransfomer_dropped_msg"))
            .count()
            > 0
        {
            Ok(())
        } else {
            Err("Expected msg to be dropped".to_owned())
        }
    });

    assert_eq!(duration1, duration2);
}
