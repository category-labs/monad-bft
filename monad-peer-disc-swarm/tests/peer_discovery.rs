use std::{
    collections::{BTreeMap, BTreeSet},
    net::{Ipv4Addr, SocketAddrV4},
    str::FromStr,
    time::Duration,
};

use alloy_rlp::Encodable;
use monad_crypto::{
    NopPubKey, NopSignature,
    certificate_signature::{
        CertificateKeyPair, CertificateSignature, CertificateSignaturePubKey, PubKey,
    },
    hasher::{Hasher, HasherType},
    signing_domain,
};
use monad_peer_disc_swarm::{
    NodeBuilder, PeerDiscSwarmRelation, SwarmPubKeyType, SwarmSignatureType,
    builder::PeerDiscSwarmBuilder,
};
use monad_peer_discovery::{
    MonadNameRecord, NameRecord, PeerDiscoveryAlgo, PeerDiscoveryEvent, PeerDiscoveryMessage,
    discovery::{
        GAUGE_PEER_DISC_DROP_LOOKUP_RESPONSE, GAUGE_PEER_DISC_DROP_PONG,
        GAUGE_PEER_DISC_LOOKUP_TIMEOUT, GAUGE_PEER_DISC_PING_TIMEOUT,
        GAUGE_PEER_DISC_RECV_LOOKUP_REQUEST, GAUGE_PEER_DISC_RECV_PING, GAUGE_PEER_DISC_RECV_PONG,
        GAUGE_PEER_DISC_RECV_TARGETED_LOOKUP_REQUEST, GAUGE_PEER_DISC_REFRESH,
        GAUGE_PEER_DISC_SEND_LOOKUP_REQUEST, GAUGE_PEER_DISC_SEND_PING, GAUGE_PEER_DISC_SEND_PONG,
        PeerDiscovery, PeerDiscoveryBuilder,
    },
};
use monad_router_scheduler::{NoSerRouterConfig, NoSerRouterScheduler, RouterSchedulerBuilder};
use monad_testutil::signing::create_keys;
use monad_transformer::{GenericTransformer, GenericTransformerPipeline, LatencyTransformer};
use monad_types::{Epoch, NodeId, Round};
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
use tracing_test::traced_test;
struct PeerDiscSwarm {}

impl PeerDiscSwarmRelation for PeerDiscSwarm {
    type SignatureType = NopSignature;

    type PeerDiscoveryAlgoType = PeerDiscovery<SwarmSignatureType<Self>>;

    type TransportMessage = PeerDiscoveryMessage<SwarmSignatureType<Self>>;

    type RouterSchedulerType = NoSerRouterScheduler<
        SwarmPubKeyType<Self>,
        PeerDiscoveryMessage<SwarmSignatureType<Self>>,
        PeerDiscoveryMessage<SwarmSignatureType<Self>>,
    >;

    type Pipeline = GenericTransformerPipeline<
        CertificateSignaturePubKey<Self::SignatureType>,
        Self::TransportMessage,
    >;
}

type PubKeyType = NopPubKey;
type SignatureType = NopSignature;
type KeyPairType = <SignatureType as CertificateSignature>::KeyPairType;

/// TestConfig can be used to configure different routing_info and pinned_full_nodes for each node
/// E.g. a routing_info of {0: {1,2}, 1: {0}, 2: {0,1}} means that
/// node0 has node1 and node2 in its routing_info
/// node1 has node0 in its routing_info
/// node2 has node0 and node1 in its routing_info
/// TODO: in the future, we can support different configs like different ping_period for different nodes too
#[derive(Clone)]
struct TestConfig {
    pub num_nodes: u32,
    pub current_round: Round,
    pub current_epoch: Epoch,
    pub epoch_validators: BTreeMap<Epoch, BTreeSet<usize>>,
    pub pinned_full_nodes: BTreeMap<usize, BTreeSet<usize>>,
    pub routing_info: BTreeMap<usize, BTreeSet<usize>>,
    pub ping_period: Duration,
    pub refresh_period: Duration,
    pub request_timeout: Duration,
    pub unresponsive_prune_threshold: u32,
    pub last_participation_prune_threshold: Round,
    pub min_num_peers: usize,
    pub max_num_peers: usize,
    pub outbound_pipeline: Vec<GenericTransformer<PubKeyType, PeerDiscoveryMessage<SignatureType>>>,
}

impl Default for TestConfig {
    // default setup is a 2 validators fully connected network
    fn default() -> Self {
        Self {
            num_nodes: 2,
            current_round: Round(1),
            current_epoch: Epoch(1),
            epoch_validators: BTreeMap::from([(Epoch(1), BTreeSet::from([0, 1]))]),
            pinned_full_nodes: BTreeMap::default(),
            routing_info: BTreeMap::from([(0, BTreeSet::from([1])), (1, BTreeSet::from([0]))]),
            ping_period: Duration::from_secs(5),
            refresh_period: Duration::from_secs(30),
            request_timeout: Duration::from_secs(1),
            unresponsive_prune_threshold: 3,
            last_participation_prune_threshold: Round(5000),
            min_num_peers: 5,
            max_num_peers: 50,
            outbound_pipeline: vec![],
        }
    }
}

fn generate_name_record(keypair: &KeyPairType) -> MonadNameRecord<SignatureType> {
    let mut hasher = HasherType::new();
    hasher.update(keypair.pubkey().bytes());
    let hash = hasher.hash();
    let ipaddr_v4 = Ipv4Addr::from_bits(u32::from_be_bytes(hash.0[28..32].try_into().unwrap()));
    assert_ne!(ipaddr_v4, Ipv4Addr::UNSPECIFIED);

    let name_record = NameRecord {
        address: SocketAddrV4::new(ipaddr_v4, 8000),
        seq: 0,
    };
    let mut encoded = Vec::new();
    name_record.encode(&mut encoded);
    let signature = SignatureType::sign::<signing_domain::NameRecord>(&encoded, keypair);
    MonadNameRecord {
        name_record,
        signature,
    }
}

fn setup_keys_and_swarm_builder(
    config: TestConfig,
) -> (
    Vec<KeyPairType>,
    PeerDiscSwarmBuilder<PeerDiscSwarm, PeerDiscoveryBuilder<SignatureType>>,
) {
    let keys = create_keys::<SignatureType>(config.num_nodes);
    let all_peers: BTreeMap<NodeId<PubKeyType>, MonadNameRecord<SignatureType>> = keys
        .iter()
        .map(|k| (NodeId::new(k.pubkey()), generate_name_record(k)))
        .collect();
    let epoch_validators = config
        .epoch_validators
        .iter()
        .map(|(epoch, validators)| {
            (
                *epoch,
                validators
                    .iter()
                    .map(|&i| NodeId::new(keys[i].pubkey()))
                    .collect(),
            )
        })
        .collect::<BTreeMap<_, _>>();

    (keys.clone(), PeerDiscSwarmBuilder {
        builders: keys
            .iter()
            .enumerate()
            .map(|(i, key)| {
                let self_id = NodeId::new(key.pubkey());
                let routing_info = config
                    .routing_info
                    .get(&i)
                    .unwrap_or(&BTreeSet::new())
                    .iter()
                    .map(|&id| {
                        let peer_key = &keys[id];
                        (
                            NodeId::new(peer_key.pubkey()),
                            generate_name_record(peer_key),
                        )
                    })
                    .collect::<BTreeMap<_, _>>();
                let pinned_full_nodes = config
                    .pinned_full_nodes
                    .get(&i)
                    .cloned()
                    .unwrap_or_default()
                    .into_iter()
                    .map(|id| NodeId::new(keys[id].pubkey()))
                    .collect::<BTreeSet<_>>();
                NodeBuilder {
                    id: NodeId::new(key.pubkey()),
                    addr: generate_name_record(key).address(),
                    algo_builder: PeerDiscoveryBuilder {
                        self_id,
                        self_record: generate_name_record(key),
                        current_round: config.current_round,
                        current_epoch: config.current_epoch,
                        epoch_validators: epoch_validators.clone(),
                        pinned_full_nodes,
                        routing_info,
                        ping_period: config.ping_period,
                        refresh_period: config.refresh_period,
                        request_timeout: config.request_timeout,
                        unresponsive_prune_threshold: config.unresponsive_prune_threshold,
                        last_participation_prune_threshold: config
                            .last_participation_prune_threshold,
                        min_num_peers: config.min_num_peers,
                        max_num_peers: config.max_num_peers,
                        rng: ChaCha8Rng::seed_from_u64(123456), // fixed seed for reproducibility
                    },
                    router_scheduler: NoSerRouterConfig::new(all_peers.keys().cloned().collect())
                        .build(),
                    seed: 123456,
                    outbound_pipeline: config.outbound_pipeline.clone(),
                }
            })
            .collect(),
        seed: 7,
    })
}

#[traced_test]
#[test]
fn test_ping_pong() {
    // 2 nodes: Node0, Node1
    let config = TestConfig {
        request_timeout: Duration::from_secs(3),
        outbound_pipeline: vec![GenericTransformer::Latency(LatencyTransformer::new(
            Duration::from_secs(1),
        ))],
        ..Default::default()
    };
    let (_, swarm_builder) = setup_keys_and_swarm_builder(config);
    let mut nodes = swarm_builder.build();

    while nodes.step_until(Duration::from_secs(20)) {}

    // first ping is sent out at t=0. we expect 5 send_ping at t=20
    // other metrics should be 4 due to message delay
    for state in nodes.states().values() {
        let metrics = state.peer_disc_driver.get_peer_disc_state().metrics();
        assert_eq!(metrics[GAUGE_PEER_DISC_SEND_PING], 5);
        assert_eq!(metrics[GAUGE_PEER_DISC_SEND_PONG], 4);
        assert_eq!(metrics[GAUGE_PEER_DISC_RECV_PING], 4);
        assert_eq!(metrics[GAUGE_PEER_DISC_RECV_PONG], 4);
    }
}

#[traced_test]
#[test]
fn test_new_node_joining() {
    // 3 nodes: Node0, Node1, Node2
    // two bootstrap nodes where addresses are known to each other
    // one new joining node where it knows the bootstrap nodes addresses but not vice versa
    // initialize routing info of the three nodes
    // Node0 name record: Node1
    // Node1 name record: Node0
    // Node2 name record: Node0, Node1
    // (we can assume that Node0 and Node1 are the bootstrap nodes in a network)
    let config = TestConfig {
        num_nodes: 3,
        epoch_validators: BTreeMap::from([(Epoch(1), BTreeSet::from([0, 1, 2]))]),
        routing_info: BTreeMap::from([
            (0, BTreeSet::from([1])),
            (1, BTreeSet::from([0])),
            (2, BTreeSet::from([0, 1])),
        ]),
        ..Default::default()
    };
    let (keys, swarm_builder) = setup_keys_and_swarm_builder(config);
    let node_ids = keys
        .iter()
        .map(|k| NodeId::new(k.pubkey()))
        .collect::<Vec<_>>();
    let mut nodes = swarm_builder.build();
    while nodes.step_until(Duration::from_secs(0)) {}

    // Node0, Node1 and Node2 should now have routing_info of each other
    for state in nodes.states().values() {
        let state = state.peer_disc_driver.get_peer_disc_state();
        for node_id in node_ids.iter() {
            if node_id == &state.self_id {
                continue;
            }
            assert!(state.routing_info.contains_key(node_id));
        }
    }
}

#[traced_test]
#[test]
fn test_update_name_record() {
    // 2 nodes: Node0, Node1
    let config = TestConfig::default();
    let (keys, swarm_builder) = setup_keys_and_swarm_builder(config.clone());
    let node_ids = keys
        .iter()
        .map(|k| NodeId::new(k.pubkey()))
        .collect::<Vec<_>>();
    let mut nodes = swarm_builder.build();

    while nodes.step_until(Duration::from_secs(0)) {}

    // Node0, Node1 should have routing info of each other
    for state in nodes.states().values() {
        let state = state.peer_disc_driver.get_peer_disc_state();
        for node_id in node_ids.iter() {
            if node_id == &state.self_id {
                continue;
            }
            assert!(state.routing_info.contains_key(node_id));
        }
    }

    // Node0 restarts with new name record
    // which will then initiate connections with other nodes to remain connected
    let node_0_key = &keys[0];
    let node_0 = node_ids[0];
    let node_1_key = &keys[1];
    let node_1 = node_ids[1];
    let _old_node_0_state = nodes
        .remove_state(&node_0)
        .expect("Node0 state should exist");

    // create new name record for Node0 with new IP and incremented seq number
    let new_name_record = NameRecord {
        address: SocketAddrV4::from_str("2.2.2.2:8000").unwrap(),
        seq: 1,
    };
    let mut encoded = Vec::new();
    new_name_record.encode(&mut encoded);
    let signature = SignatureType::sign::<signing_domain::NameRecord>(&encoded, node_0_key);
    let new_name_record = MonadNameRecord {
        name_record: new_name_record,
        signature,
    };

    let new_node_0_builder = NodeBuilder {
        id: node_0,
        addr: new_name_record.address(),
        algo_builder: PeerDiscoveryBuilder {
            self_id: node_0,
            self_record: new_name_record,
            current_round: config.current_round,
            current_epoch: config.current_epoch,
            epoch_validators: BTreeMap::new(),
            pinned_full_nodes: BTreeSet::new(),
            routing_info: BTreeMap::from([(node_1, generate_name_record(node_1_key))]),
            ping_period: config.ping_period,
            refresh_period: config.refresh_period,
            request_timeout: config.request_timeout,
            unresponsive_prune_threshold: config.unresponsive_prune_threshold,
            last_participation_prune_threshold: config.last_participation_prune_threshold,
            min_num_peers: config.min_num_peers,
            max_num_peers: config.max_num_peers,
            rng: ChaCha8Rng::seed_from_u64(123456),
        },
        router_scheduler: NoSerRouterConfig::new(node_ids.iter().cloned().collect()).build(),
        seed: 1,
        outbound_pipeline: vec![],
    };

    nodes.add_state(new_node_0_builder);

    while nodes.step_until(Duration::from_secs(0)) {}

    // Node1 should have the routing info of Node0 updated
    let node_1_state = nodes
        .states()
        .get(&node_1)
        .expect("Node1 state should exist");
    let routing_info = &node_1_state
        .peer_disc_driver
        .get_peer_disc_state()
        .routing_info;
    let node_0_record = routing_info
        .get(&node_0)
        .expect("Node1 should have node0 name record");

    assert_eq!(*node_0_record, new_name_record);
}

#[traced_test]
#[test]
fn test_prune_nodes() {
    let config = TestConfig {
        ping_period: Duration::from_secs(2),
        refresh_period: Duration::from_secs(10),
        epoch_validators: BTreeMap::from([(Epoch(1), BTreeSet::from([1]))]),
        ..Default::default()
    };
    let (keys, swarm_builder) = setup_keys_and_swarm_builder(config);
    let mut nodes = swarm_builder.build();

    // prune period is set to 10 seconds (i.e. prune every 10 seconds)
    // refresh is called at t=0 and t=10
    while nodes.step_until(Duration::from_secs(10)) {}
    for state in nodes.states().values() {
        let state = state.peer_disc_driver.get_peer_disc_state();
        let metrics = state.metrics();
        assert_eq!(metrics[GAUGE_PEER_DISC_REFRESH], 2);
        assert!(!state.connection_info.is_empty());
    }

    // a node goes offline
    let offline_key = &keys[0];
    let offline_node = NodeId::new(offline_key.pubkey());
    nodes.remove_state(&offline_node);

    // the offline node should be pruned (no longer sending pings to)
    while nodes.step_until(Duration::from_secs(20)) {}
    let mut num_send_pings = 0;
    assert_eq!(nodes.states().len(), 1);
    for state in nodes.states().values() {
        let state = state.peer_disc_driver.get_peer_disc_state();
        let metrics = state.metrics();
        assert_eq!(metrics[GAUGE_PEER_DISC_REFRESH], 3);
        assert!(state.connection_info.is_empty());

        num_send_pings = metrics[GAUGE_PEER_DISC_SEND_PING];
    }

    while nodes.step_until(Duration::from_secs(30)) {}
    for state in nodes.states().values() {
        let state = state.peer_disc_driver.get_peer_disc_state();
        let metrics = state.metrics();
        assert_eq!(metrics[GAUGE_PEER_DISC_SEND_PING], num_send_pings);
    }
}

#[traced_test]
#[test]
fn test_peer_lookup_open_discovery() {
    // 4 nodes: Node0, Node1, Node2, Node3, Node4
    // initialize routing info
    // Node0 name record: Node1, Node2, Node3
    // Node1 name record: Node0
    // Node2 name record: <empty>
    // Node3 name record: <empty>
    let config = TestConfig {
        num_nodes: 4,
        epoch_validators: BTreeMap::from([(Epoch(1), BTreeSet::from([0, 1, 2, 3]))]),
        routing_info: BTreeMap::from([
            (0, BTreeSet::from([1, 2, 3])),
            (1, BTreeSet::from([0])),
            (2, BTreeSet::new()),
            (3, BTreeSet::new()),
        ]),
        refresh_period: Duration::from_secs(5),
        ..Default::default()
    };
    let (keys, swarm_builder) = setup_keys_and_swarm_builder(config);
    let node_ids = keys
        .iter()
        .map(|k| NodeId::new(k.pubkey()))
        .collect::<Vec<_>>();
    let mut nodes = swarm_builder.build();
    while nodes.step_until(Duration::from_secs(0)) {}

    // Node1 should know about Node2 and Node3 through open discovery
    let node_1_state = nodes
        .states()
        .get(&node_ids[1])
        .expect("Node 1 state should exist")
        .peer_disc_driver
        .get_peer_disc_state();
    for node_id in node_ids.iter() {
        if node_id == &node_1_state.self_id {
            continue;
        }
        assert!(node_1_state.routing_info.contains_key(node_id));
    }

    // check that Node1 sends pings to new nodes
    let metrics = node_1_state.metrics();
    assert_eq!(metrics[GAUGE_PEER_DISC_SEND_PING], 3);

    // check that Node2 and Node3 now has name record of NodeB
    for (node_id, state) in nodes.states() {
        let state = state.peer_disc_driver.get_peer_disc_state();
        if node_id == &node_ids[1] {
            continue;
        }
        assert!(state.routing_info.contains_key(&node_ids[1]));
    }
}

#[traced_test]
#[test]
fn test_peer_lookup_targeted_nodes() {
    // 3 nodes: Node0, Node1, Node2
    // initialize routing info
    // Node0 name record: Node1, Node2
    // Node1 name record: Node0
    // All three nodes are validators, Node1 is missing Node2 name record
    let config = TestConfig {
        num_nodes: 3,
        epoch_validators: BTreeMap::from([(Epoch(1), BTreeSet::from([0, 1, 2]))]),
        routing_info: BTreeMap::from([
            (0, BTreeSet::from([1, 2])),
            (1, BTreeSet::from([0])),
            (2, BTreeSet::new()),
        ]),
        refresh_period: Duration::from_secs(10),
        min_num_peers: 1,
        ..Default::default()
    };
    let (keys, swarm_builder) = setup_keys_and_swarm_builder(config);
    let node_ids = keys
        .iter()
        .map(|k| NodeId::new(k.pubkey()))
        .collect::<Vec<_>>();
    let mut nodes = swarm_builder.build();
    nodes.remove_state(&node_ids[2]);

    while nodes.step_until(Duration::from_secs(10)) {}

    // Node1 has number of peers larger than min number of peers but still missing validator NodeC
    // Node1 should send targeted lookup request to Node0 asking for Node2
    for state in nodes.states().values() {
        let state = state.peer_disc_driver.get_peer_disc_state();
        let metrics = state.metrics();
        for node_id in node_ids.iter() {
            if node_id == &state.self_id {
                continue;
            }
            assert!(state.routing_info.contains_key(node_id));
        }

        if state.self_id == node_ids[0] {
            assert_eq!(metrics[GAUGE_PEER_DISC_RECV_LOOKUP_REQUEST], 1);
            assert_eq!(metrics[GAUGE_PEER_DISC_RECV_TARGETED_LOOKUP_REQUEST], 1);
        }
    }
}

#[traced_test]
#[test]
fn test_peer_lookup_retry() {
    // 3 nodes: Node0, Node1, Node2
    // Node0 name record: Node1, Node2
    // Node1 name record: Node0
    // Node1 send peer lookup request to Node0, requesting for Node2
    // Outbound message latency of Node1 is 2 seconds while timeout is 1 second
    let config = TestConfig {
        num_nodes: 3,
        ping_period: Duration::from_secs(15),
        routing_info: BTreeMap::from([
            (0, BTreeSet::from([1, 2])),
            (1, BTreeSet::from([0])),
            (2, BTreeSet::new()),
        ]),
        epoch_validators: BTreeMap::from([(Epoch(1), BTreeSet::from([0, 1, 2]))]),
        outbound_pipeline: vec![GenericTransformer::Latency(LatencyTransformer::new(
            Duration::from_secs(2),
        ))],
        ..Default::default()
    };
    let (keys, swarm_builder) = setup_keys_and_swarm_builder(config);
    let node_ids = keys
        .iter()
        .map(|k| NodeId::new(k.pubkey()))
        .collect::<Vec<_>>();
    let mut nodes = swarm_builder.build();
    nodes.remove_state(&node_ids[2]);

    while nodes.step_until(Duration::from_secs(10)) {}

    for (node, state) in nodes.states() {
        if node == &node_ids[1] {
            let state = state.peer_disc_driver.get_peer_disc_state();
            let metrics = state.metrics();
            assert_eq!(metrics[GAUGE_PEER_DISC_LOOKUP_TIMEOUT], 4);
            assert_eq!(metrics[GAUGE_PEER_DISC_DROP_LOOKUP_RESPONSE], 4);

            // Due to lookup timeout, Node1 still does not have name record of Node2
            assert!(!state.routing_info.contains_key(&node_ids[2]));
        }
    }
}

#[traced_test]
#[test]
fn test_ping_timeout() {
    // 2 nodes: Node0, Node1
    let config = TestConfig {
        ping_period: Duration::from_secs(5),
        refresh_period: Duration::from_secs(20),
        request_timeout: Duration::from_secs(1),
        epoch_validators: BTreeMap::default(),
        last_participation_prune_threshold: Round(3),
        outbound_pipeline: vec![GenericTransformer::Latency(LatencyTransformer::new(
            Duration::from_secs(2),
        ))],
        ..Default::default()
    };
    let (keys, swarm_builder) = setup_keys_and_swarm_builder(config);
    let node_ids = keys
        .iter()
        .map(|k| NodeId::new(k.pubkey()))
        .collect::<Vec<_>>();
    let mut nodes = swarm_builder.build();

    while nodes.step_until(Duration::from_secs(20)) {}

    // message latency of 2 seconds
    // ping timeout of 1 second
    // verify that ping timeout event is recorded correctly and subsequent pong is dropped
    // unresponsive_pings accumulate until being pruned when threshold is reached
    for state in nodes.states().values() {
        let state = state.peer_disc_driver.get_peer_disc_state();
        let metrics = state.metrics();
        // prune threshold is three, so it's pruned after 3 unresponsive pings
        assert_eq!(metrics[GAUGE_PEER_DISC_SEND_PING], 3);
        assert_eq!(metrics[GAUGE_PEER_DISC_PING_TIMEOUT], 3);
        assert_eq!(metrics[GAUGE_PEER_DISC_RECV_PONG], 3);
        assert_eq!(metrics[GAUGE_PEER_DISC_DROP_PONG], 3);
    }

    // no more pings after being pruned
    while nodes.step_until(Duration::from_secs(30)) {}
    for state in nodes.states().values() {
        let state = state.peer_disc_driver.get_peer_disc_state();
        let metrics = state.metrics();
        assert_eq!(metrics[GAUGE_PEER_DISC_SEND_PING], 3);
    }

    // eventually both name record and connection info should be pruned (during refresh)
    let round_change_event = PeerDiscoveryEvent::UpdateCurrentRound {
        round: Round(5),
        epoch: Epoch(1),
    };
    for node_id in &node_ids {
        nodes.insert_test_event(node_id, Duration::from_secs(30), round_change_event.clone());
    }
    while nodes.step_until(Duration::from_secs(40)) {}
    for state in nodes.states().values() {
        let state = state.peer_disc_driver.get_peer_disc_state();
        assert!(state.routing_info.is_empty());
        assert!(state.connection_info.is_empty());
    }
}

#[traced_test]
#[test]
fn test_min_watermark() {
    // 4 nodes: Node0, Node1, Node2, Node3
    // Node0 does not have any peer in the beginning
    // Node1, Node2, and Node3 has Node0 as their peer in the beginning
    let config = TestConfig {
        num_nodes: 4,
        epoch_validators: BTreeMap::from([(Epoch(1), BTreeSet::from([0, 1, 2, 3]))]),
        routing_info: BTreeMap::from([
            (0, BTreeSet::new()),
            (1, BTreeSet::from([0])),
            (2, BTreeSet::from([0])),
            (3, BTreeSet::from([0])),
        ]),
        min_num_peers: 2,
        max_num_peers: 10,
        ..Default::default()
    };
    let (keys, swarm_builder) = setup_keys_and_swarm_builder(config);
    let node_ids = keys
        .iter()
        .map(|k| NodeId::new(k.pubkey()))
        .collect::<Vec<_>>();
    let mut nodes = swarm_builder.build();

    while nodes.step_until(Duration::from_secs(0)) {}

    for (node_id, state) in nodes.states() {
        let state = state.peer_disc_driver.get_peer_disc_state();
        let metrics = state.metrics();

        // Node1, Node2, and Node3 should send lookup request to Node0 and discover each other
        for peer_id in node_ids.iter() {
            if peer_id == &state.self_id {
                continue;
            }
            assert!(state.routing_info.contains_key(peer_id));
        }

        if node_id == &node_ids[0] {
            assert_eq!(metrics[GAUGE_PEER_DISC_RECV_LOOKUP_REQUEST], 3);
        } else {
            assert_eq!(metrics[GAUGE_PEER_DISC_SEND_LOOKUP_REQUEST], 1);
        }
    }
}

#[traced_test]
#[test]
fn test_max_watermark() {
    // 5 nodes: Node0, Node1, Node2, Node3, Node4
    // Node0 is a validator, Node1, Node2, Node3 are full nodes
    // Node4 is a pinned full node for Node0
    let config = TestConfig {
        num_nodes: 5,
        epoch_validators: BTreeMap::from([(Epoch(1), BTreeSet::from([0]))]),
        pinned_full_nodes: BTreeMap::from([(0, BTreeSet::from([4]))]),
        routing_info: BTreeMap::from([
            (0, BTreeSet::from([1, 2, 3, 4])),
            (1, BTreeSet::from([0, 2, 3, 4])),
            (2, BTreeSet::from([0, 1, 3, 4])),
            (3, BTreeSet::from([0, 1, 2, 4])),
            (4, BTreeSet::from([0, 1, 2, 3])),
        ]),
        ping_period: Duration::from_secs(2),
        refresh_period: Duration::from_secs(5),
        unresponsive_prune_threshold: 1,
        min_num_peers: 1,
        max_num_peers: 2,
        ..Default::default()
    };
    let (keys, swarm_builder) = setup_keys_and_swarm_builder(config.clone());
    let node_ids = keys
        .iter()
        .map(|k| NodeId::new(k.pubkey()))
        .collect::<Vec<_>>();
    let mut nodes = swarm_builder.build();
    nodes.remove_state(&node_ids[4]); // remove Node4 to simulate inactive node

    while nodes.step_until(config.refresh_period) {}

    for (node_id, state) in nodes.states() {
        let state = state.peer_disc_driver.get_peer_disc_state();

        // Node4 is inactive, should be pruned by Node1, Node2 and Node3, but should not be pruned by Node0 (due to pinned full node)
        // Node4 is pinned full node for Node0, should not be pruned in routing info of Node0
        if node_id == &node_ids[0] {
            assert!(state.routing_info.contains_key(&node_ids[4]));
        }

        // Node4 being inactive should be pruned in connection info of all nodes
        assert!(!state.connection_info.contains_key(&node_ids[4]));

        // additional full nodes above max_num_peers are pruned
        assert!(state.routing_info.len() <= 2);
    }
}

#[traced_test]
#[test]
fn test_full_nodes_connection() {
    // 6 nodes: Node0, Node1, Node2, Node3, Node4, Node5
    // Node0, Node1, Node2, Node3 are validators, Node4 and Node 5 are full nodes
    // Node4 name record: Node0, Node1, Node2
    // Node5 name record: Node0, Node1, Node2
    let config = TestConfig {
        num_nodes: 6,
        epoch_validators: BTreeMap::from([(Epoch(1), BTreeSet::from([0, 1, 2, 3]))]),
        routing_info: BTreeMap::from([
            (0, BTreeSet::from([1, 2, 3])),
            (1, BTreeSet::from([0, 2, 3])),
            (2, BTreeSet::from([0, 1, 3])),
            (3, BTreeSet::from([0, 1, 2])),
            (4, BTreeSet::from([0, 1, 2])),
            (5, BTreeSet::from([0, 1, 2])),
        ]),
        ping_period: Duration::from_secs(2),
        refresh_period: Duration::from_secs(5),
        min_num_peers: 2,
        max_num_peers: 4,
        ..Default::default()
    };
    let (keys, swarm_builder) = setup_keys_and_swarm_builder(config.clone());
    let node_ids = keys
        .iter()
        .map(|k| NodeId::new(k.pubkey()))
        .collect::<Vec<_>>();
    let mut nodes = swarm_builder.build();
    // Node5 will join later, so we remove its state for now
    let _node_5_state = nodes
        .remove_state(&node_ids[5])
        .expect("Node5 state should exist");

    while nodes.step_until(Duration::from_secs(0)) {}

    for (node_id, state) in nodes.states() {
        let state = state.peer_disc_driver.get_peer_disc_state();

        // Node4 send pings to Node0, Node1, and Node2, so it should have connections to them
        // Node4 have the name record of Node3 but is not connected to it
        if node_id == &node_ids[4] {
            assert_eq!(state.connection_info.len(), 3);
            assert!(state.connection_info.contains_key(&node_ids[0]));
            assert!(state.connection_info.contains_key(&node_ids[1]));
            assert!(state.connection_info.contains_key(&node_ids[2]));
            assert_eq!(state.routing_info.len(), 4);
        }

        // Node0, Node1, and Node2 should have connections and name record of Node4
        // Node3 should not have connection to Node4
        if node_id == &node_ids[0] || node_id == &node_ids[1] || node_id == &node_ids[2] {
            assert!(state.connection_info.contains_key(&node_ids[4]));
            assert!(state.routing_info.contains_key(&node_ids[4]));
        } else if node_id == &node_ids[3] {
            assert!(!state.connection_info.contains_key(&node_ids[4]));
            assert!(!state.routing_info.contains_key(&node_ids[4]));
        }
    }

    // Node5 which is a full node now joins the network
    let node_5_builder = NodeBuilder {
        id: node_ids[5],
        addr: generate_name_record(&keys[5]).address(),
        algo_builder: PeerDiscoveryBuilder {
            self_id: node_ids[5],
            self_record: generate_name_record(&keys[5]),
            current_round: config.current_round,
            current_epoch: config.current_epoch,
            epoch_validators: BTreeMap::from([(
                Epoch(1),
                BTreeSet::from([node_ids[0], node_ids[1], node_ids[2], node_ids[3]]),
            )]),
            pinned_full_nodes: BTreeSet::new(),
            routing_info: BTreeMap::from([
                (node_ids[0], generate_name_record(&keys[0])),
                (node_ids[1], generate_name_record(&keys[1])),
                (node_ids[2], generate_name_record(&keys[2])),
                (node_ids[3], generate_name_record(&keys[3])),
            ]),
            ping_period: config.ping_period,
            refresh_period: config.refresh_period,
            request_timeout: config.request_timeout,
            unresponsive_prune_threshold: config.unresponsive_prune_threshold,
            last_participation_prune_threshold: config.last_participation_prune_threshold,
            min_num_peers: config.min_num_peers,
            max_num_peers: config.max_num_peers,
            rng: ChaCha8Rng::seed_from_u64(123456),
        },
        router_scheduler: NoSerRouterConfig::new(node_ids.iter().cloned().collect()).build(),
        seed: 1,
        outbound_pipeline: vec![],
    };
    nodes.add_state(node_5_builder);

    while nodes.step_until(Duration::from_secs(10)) {}

    // Node0, Node1, Node2 already have maximum number of peers, so they should not connect to Node5
    // and should not have its name record
    // Node5 will look for other validators and connect to Node3 instead
    for (node_id, state) in nodes.states() {
        let state = state.peer_disc_driver.get_peer_disc_state();
        if node_id == &node_ids[0] || node_id == &node_ids[1] || node_id == &node_ids[2] {
            assert!(!state.connection_info.contains_key(&node_ids[5]));
            assert!(!state.routing_info.contains_key(&node_ids[5]));
        } else if node_id == &node_ids[3] {
            // Node3 should now have connection to Node5
            assert!(state.connection_info.contains_key(&node_ids[5]));
            assert!(state.routing_info.contains_key(&node_ids[5]));
        } else if node_id == &node_ids[5] {
            // Node5 should have connection to Node3
            assert!(state.connection_info.contains_key(&node_ids[3]));
            assert!(state.routing_info.contains_key(&node_ids[3]));
        }
    }
}

#[traced_test]
#[test]
fn test_full_node_promoted_to_validator() {
    // 6 nodes: Node0, Node1, Node2, Node3, Node4, Node5
    // Node0, Node1, Node2, Node3, Node4 are validators, Node5 is a full node
    // Node5 name record: Node0, Node1, Node2
    let config = TestConfig {
        num_nodes: 6,
        epoch_validators: BTreeMap::from([
            (Epoch(1), BTreeSet::from([0, 1, 2, 3, 4])),
            (Epoch(2), BTreeSet::from([0, 1, 2, 3, 4])),
            (Epoch(3), BTreeSet::from([0, 1, 2, 3, 4, 5])),
        ]),
        routing_info: BTreeMap::from([
            (0, BTreeSet::from([1, 2, 3, 4])),
            (1, BTreeSet::from([0, 2, 3, 4])),
            (2, BTreeSet::from([0, 1, 3, 4])),
            (3, BTreeSet::from([0, 1, 2, 4])),
            (4, BTreeSet::from([0, 1, 2, 3])),
            (5, BTreeSet::from([0, 1, 2])),
        ]),
        ping_period: Duration::from_secs(2),
        refresh_period: Duration::from_secs(5),
        min_num_peers: 3,
        max_num_peers: 10,
        ..Default::default()
    };
    let (keys, swarm_builder) = setup_keys_and_swarm_builder(config);
    let node_ids = keys
        .iter()
        .map(|k| NodeId::new(k.pubkey()))
        .collect::<Vec<_>>();
    let mut nodes = swarm_builder.build();

    while nodes.step_until(Duration::from_secs(0)) {}

    for (node_id, state) in nodes.states() {
        let state = state.peer_disc_driver.get_peer_disc_state();

        // Node5 should have connections to Node0, Node1, and Node2 and not the other validators
        // Node5 should have the name records of all validators
        if node_id == &node_ids[5] {
            assert_eq!(state.connection_info.len(), 3);
            assert!(state.connection_info.contains_key(&node_ids[0]));
            assert!(state.connection_info.contains_key(&node_ids[1]));
            assert!(state.connection_info.contains_key(&node_ids[2]));
            assert_eq!(state.routing_info.len(), 5);
        }
    }

    // Node5 is promoted to validator
    let epoch_change_event = PeerDiscoveryEvent::UpdateCurrentRound {
        round: Round(2),
        epoch: Epoch(2),
    };
    nodes.insert_test_event(&node_ids[5], Duration::from_secs(0), epoch_change_event);

    while nodes.step_until(Duration::from_secs(0)) {}

    // Node5 should now have connections to all validators
    for (node_id, state) in nodes.states() {
        let state = state.peer_disc_driver.get_peer_disc_state();

        if node_id == &node_ids[5] {
            assert_eq!(state.connection_info.len(), 5);
            for peer_id in node_ids.iter() {
                if peer_id == &state.self_id {
                    continue;
                }
                assert!(state.connection_info.contains_key(peer_id));
                assert!(state.routing_info.contains_key(peer_id));
            }
        }
    }
}

#[traced_test]
#[test]
fn test_validator_demoted_to_full_node() {
    // 5 nodes: Node0, Node1, Node2, Node3, Node4
    // Node0, Node1, Node2, Node3, Node4 are validators
    // Fully connected network
    let config = TestConfig {
        num_nodes: 5,
        epoch_validators: BTreeMap::from([
            (Epoch(1), BTreeSet::from([0, 1, 2, 3, 4])),
            (Epoch(2), BTreeSet::from([0, 1, 2, 3])),
        ]),
        routing_info: BTreeMap::from([
            (0, BTreeSet::from([1, 2, 3, 4])),
            (1, BTreeSet::from([0, 2, 3, 4])),
            (2, BTreeSet::from([0, 1, 3, 4])),
            (3, BTreeSet::from([0, 1, 2, 4])),
            (4, BTreeSet::from([0, 1, 2, 3])),
        ]),
        ping_period: Duration::from_secs(2),
        refresh_period: Duration::from_secs(5),
        min_num_peers: 3,
        max_num_peers: 10,
        ..Default::default()
    };
    let (keys, swarm_builder) = setup_keys_and_swarm_builder(config);
    let node_ids = keys
        .iter()
        .map(|k| NodeId::new(k.pubkey()))
        .collect::<Vec<_>>();
    let mut nodes = swarm_builder.build();

    while nodes.step_until(Duration::from_secs(0)) {}

    for state in nodes.states().values() {
        let state = state.peer_disc_driver.get_peer_disc_state();

        // all nodes have connections to each other
        for peer_id in node_ids.iter() {
            if peer_id == &state.self_id {
                continue;
            }
            assert!(state.connection_info.contains_key(peer_id));
            assert!(state.routing_info.contains_key(peer_id));
        }
    }

    // Node4 is demoted to a full node
    let epoch_change_event = PeerDiscoveryEvent::UpdateCurrentRound {
        round: Round(2),
        epoch: Epoch(2),
    };
    nodes.insert_test_event(&node_ids[4], Duration::from_secs(0), epoch_change_event);

    while nodes.step_until(Duration::from_secs(10)) {}

    // Node4 should now be only connected to three upstream validators
    for (node_id, state) in nodes.states() {
        let state = state.peer_disc_driver.get_peer_disc_state();

        if node_id == &node_ids[4] {
            assert_eq!(state.connection_info.len(), 3);
            assert_eq!(state.routing_info.len(), 4);
            continue;
        }

        // other nodes should still have connections to each other
        for peer_id in node_ids.iter() {
            if peer_id == &state.self_id {
                continue;
            }
            assert!(state.connection_info.contains_key(peer_id));
            assert!(state.routing_info.contains_key(peer_id));
        }
    }
}

#[traced_test]
#[test]
fn test_prune_non_participating_full_node() {
    // 3 nodes: Node0, Node1, Node2
    // Node0 is a validator, Node1 and Node2 are full nodes
    // initialize routing info
    // Node0 name record: empty
    // Node1 name record: Node0
    // Node2 name record: Node0, Node1
    let config = TestConfig {
        num_nodes: 3,
        routing_info: BTreeMap::from([
            (0, BTreeSet::new()),
            (1, BTreeSet::from([0])),
            (2, BTreeSet::from([0, 1])),
        ]),
        epoch_validators: BTreeMap::from([(Epoch(1), BTreeSet::from([0]))]),
        last_participation_prune_threshold: Round(5),
        ping_period: Duration::from_secs(2),
        refresh_period: Duration::from_secs(5),
        min_num_peers: 1,
        ..Default::default()
    };
    let (keys, swarm_builder) = setup_keys_and_swarm_builder(config.clone());
    let node_ids = keys
        .iter()
        .map(|k| NodeId::new(k.pubkey()))
        .collect::<Vec<_>>();
    let mut nodes = swarm_builder.build();

    while nodes.step_until(Duration::from_secs(0)) {}

    // all nodes should have routing info of each other
    for state in nodes.states().values() {
        let state = state.peer_disc_driver.get_peer_disc_state();
        for node_id in node_ids.iter() {
            if node_id == &state.self_id {
                continue;
            }
            assert!(state.routing_info.contains_key(node_id));
        }
    }

    // node1 participated in secondary raptorcast
    let participation_event = PeerDiscoveryEvent::UpdateConfirmGroup {
        end_round: Round(20),
        peers: BTreeSet::from([node_ids[1]]),
    };
    nodes.insert_test_event(
        &node_ids[0],
        Duration::from_secs(0),
        participation_event.clone(),
    );
    nodes.insert_test_event(&node_ids[1], Duration::from_secs(0), participation_event);
    while nodes.step_until(Duration::from_secs(0)) {}

    // round update event
    let round_change_event = PeerDiscoveryEvent::UpdateCurrentRound {
        round: Round(10),
        epoch: Epoch(1),
    };
    for node_id in &node_ids {
        nodes.insert_test_event(node_id, Duration::from_secs(0), round_change_event.clone());
    }

    while nodes.step_until(config.refresh_period) {}
    for state in nodes.states().values() {
        let state = state.peer_disc_driver.get_peer_disc_state();

        // Node0 (validator) should have connection to Node1 (participating full node)
        // Node2 (non-participating full node) should be pruned
        if node_ids[0] == state.self_id {
            assert!(state.connection_info.contains_key(&node_ids[1]));
            assert!(state.routing_info.contains_key(&node_ids[1]));
            assert!(!state.connection_info.contains_key(&node_ids[2]));
            assert!(!state.routing_info.contains_key(&node_ids[2]));
        }

        // Node1 (participating full node) should have connection to Node0 (validator)
        // Node2 (non-participating full node) should be pruned
        if node_ids[1] == state.self_id {
            assert!(state.connection_info.contains_key(&node_ids[0]));
            assert!(state.routing_info.contains_key(&node_ids[0]));
            assert!(!state.connection_info.contains_key(&node_ids[2]));
            assert!(!state.routing_info.contains_key(&node_ids[2]));
        }

        // Node2 (non-participating full node) should have connection to Node0 (validator)
        if node_ids[2] == state.self_id {
            assert!(state.connection_info.contains_key(&node_ids[0]));
            assert!(state.routing_info.contains_key(&node_ids[0]));
            assert!(!state.connection_info.contains_key(&node_ids[1]));
            assert!(!state.routing_info.contains_key(&node_ids[1]));
        }
    }
}
