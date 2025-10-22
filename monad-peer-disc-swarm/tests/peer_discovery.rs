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
        GAUGE_PEER_DISC_DROP_PONG, GAUGE_PEER_DISC_LOOKUP_TIMEOUT,
        GAUGE_PEER_DISC_NUM_DOWNSTREAM_FULLNODES, GAUGE_PEER_DISC_NUM_UPSTREAM_VALIDATORS,
        GAUGE_PEER_DISC_PING_TIMEOUT, GAUGE_PEER_DISC_RECV_LOOKUP_REQUEST,
        GAUGE_PEER_DISC_RECV_PING, GAUGE_PEER_DISC_RECV_PONG,
        GAUGE_PEER_DISC_RECV_TARGETED_LOOKUP_REQUEST, GAUGE_PEER_DISC_SEND_LOOKUP_REQUEST,
        GAUGE_PEER_DISC_SEND_PING, GAUGE_PEER_DISC_SEND_PONG, PeerDiscovery, PeerDiscoveryBuilder,
        SecondaryRaptorcastConnectionStatus,
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

/// TestConfig can be used to configure different bootstrap peers and pinned_full_nodes for each node
/// E.g. a bootstrap_peers of {0: {1,2}, 1: {0}, 2: {0,1}} means that
/// node0 has node1 and node2 in its bootstrap_peers
/// node1 has node0 in its bootstrap_peers
/// node2 has node0 and node1 in its bootstrap_peers
/// TODO: in the future, we can support different configs like different outbound delay for different nodes too
#[derive(Clone)]
struct TestConfig {
    pub num_nodes: u32,
    pub current_round: Round,
    pub current_epoch: Epoch,
    pub epoch_validators: BTreeMap<Epoch, BTreeSet<usize>>,
    pub pinned_full_nodes: BTreeMap<usize, BTreeSet<usize>>,
    pub roles: BTreeMap<usize, bool>, // true for secondary raptorcast enabled, false for disabled
    pub bootstrap_peers: BTreeMap<usize, BTreeSet<usize>>,
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
            roles: BTreeMap::from([(0, false), (1, false)]),
            bootstrap_peers: BTreeMap::from([(0, BTreeSet::from([1])), (1, BTreeSet::from([0]))]),
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

    let name_record = NameRecord::new(ipaddr_v4, 8000, 0);
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
    let epoch_validators: BTreeMap<Epoch, BTreeSet<NodeId<NopPubKey>>> = config
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
                let secondary_raptorcast_enabled = config.roles.get(&i).cloned().unwrap_or(false);
                let bootstrap_peers = config
                    .bootstrap_peers
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
                    addr: generate_name_record(key).udp_address(),
                    algo_builder: PeerDiscoveryBuilder {
                        self_id,
                        self_record: generate_name_record(key),
                        current_round: config.current_round,
                        current_epoch: config.current_epoch,
                        epoch_validators: epoch_validators.clone(),
                        pinned_full_nodes,
                        bootstrap_peers,
                        refresh_period: config.refresh_period,
                        request_timeout: config.request_timeout,
                        unresponsive_prune_threshold: config.unresponsive_prune_threshold,
                        last_participation_prune_threshold: config
                            .last_participation_prune_threshold,
                        min_num_peers: config.min_num_peers,
                        max_num_peers: config.max_num_peers,
                        enable_publisher: secondary_raptorcast_enabled,
                        enable_client: secondary_raptorcast_enabled,
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
    let (keys, swarm_builder) = setup_keys_and_swarm_builder(config);
    let node_ids = keys
        .iter()
        .map(|k| NodeId::new(k.pubkey()))
        .collect::<Vec<_>>();
    let mut nodes = swarm_builder.build();

    while nodes.step_until(Duration::from_secs(0)) {}

    for state in nodes.states().values() {
        let state = state.peer_disc_driver.get_peer_disc_state();

        for node_id in node_ids.iter() {
            if node_id == &state.self_id {
                continue;
            }
            assert!(state.pending_queue.contains_key(node_id));
        }
        assert!(state.routing_info.is_empty());
    }

    while nodes.step_until(Duration::from_secs(2)) {}

    // ping is sent out at t=0. we expect receiving pong at t=2
    // and thus adding the name record to routing_info
    // other metrics should be 4 due to message delay
    for state in nodes.states().values() {
        let state = state.peer_disc_driver.get_peer_disc_state();
        let metrics = state.metrics();
        assert_eq!(metrics[GAUGE_PEER_DISC_SEND_PING], 1);
        assert_eq!(metrics[GAUGE_PEER_DISC_SEND_PONG], 1);
        assert_eq!(metrics[GAUGE_PEER_DISC_RECV_PING], 1);
        assert_eq!(metrics[GAUGE_PEER_DISC_RECV_PONG], 1);

        for node_id in node_ids.iter() {
            if node_id == &state.self_id {
                continue;
            }
            assert!(state.routing_info.contains_key(node_id));
        }
        assert!(state.pending_queue.is_empty());
    }
}

#[traced_test]
#[test]
fn test_new_node_joining() {
    // 3 nodes: Node0, Node1, Node2
    // two bootstrap nodes where addresses are known to each other
    // one new joining node where it knows the bootstrap nodes addresses but not vice versa
    // initialize bootstrap peers of the three nodes
    // Node0 name record: Node1
    // Node1 name record: Node0
    // Node2 name record: Node0, Node1
    let config = TestConfig {
        num_nodes: 3,
        epoch_validators: BTreeMap::from([(Epoch(1), BTreeSet::from([0, 1, 2]))]),
        bootstrap_peers: BTreeMap::from([
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
    let new_name_record = NameRecord::new(
        *SocketAddrV4::from_str("2.2.2.2:8000").unwrap().ip(),
        8000,
        1,
    );
    let mut encoded = Vec::new();
    new_name_record.encode(&mut encoded);
    let signature = SignatureType::sign::<signing_domain::NameRecord>(&encoded, node_0_key);
    let new_name_record = MonadNameRecord {
        name_record: new_name_record,
        signature,
    };

    let new_node_0_builder = NodeBuilder {
        id: node_0,
        addr: new_name_record.udp_address(),
        algo_builder: PeerDiscoveryBuilder {
            self_id: node_0,
            self_record: new_name_record.clone(),
            current_round: config.current_round,
            current_epoch: config.current_epoch,
            epoch_validators: BTreeMap::new(),
            pinned_full_nodes: BTreeSet::new(),
            bootstrap_peers: BTreeMap::from([(node_1, generate_name_record(node_1_key))]),
            refresh_period: config.refresh_period,
            request_timeout: config.request_timeout,
            unresponsive_prune_threshold: config.unresponsive_prune_threshold,
            last_participation_prune_threshold: config.last_participation_prune_threshold,
            min_num_peers: config.min_num_peers,
            max_num_peers: config.max_num_peers,
            enable_publisher: false,
            enable_client: false,
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
fn test_unresponsive_pings() {
    let config = TestConfig {
        refresh_period: Duration::from_secs(10),
        epoch_validators: BTreeMap::from([(Epoch(1), BTreeSet::from([1]))]),
        ..Default::default()
    };
    let (keys, swarm_builder) = setup_keys_and_swarm_builder(config);
    let mut nodes = swarm_builder.build();

    // a node goes offline
    let offline_key = &keys[0];
    let offline_node = NodeId::new(offline_key.pubkey());
    nodes.remove_state(&offline_node);

    // the offline node should not be added to routing info
    while nodes.step_until(Duration::from_secs(5)) {}
    assert_eq!(nodes.states().len(), 1);
    for state in nodes.states().values() {
        let state = state.peer_disc_driver.get_peer_disc_state();
        let metrics = state.metrics();

        assert_eq!(metrics[GAUGE_PEER_DISC_SEND_PING], 3);
        assert_eq!(metrics[GAUGE_PEER_DISC_PING_TIMEOUT], 3);
        assert!(state.routing_info.is_empty());
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
        bootstrap_peers: BTreeMap::from([
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
    while nodes.step_until(Duration::from_secs(5)) {}

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

    // check that Node2 and Node3 now has name record of Node1
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
        bootstrap_peers: BTreeMap::from([
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

    while nodes.step_until(Duration::from_secs(10)) {}

    // Node1 has number of peers larger than min number of peers but still missing validator Node2
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
            // Both Node1 and Node2 sends targeted peer lookup request to Node0
            assert_eq!(metrics[GAUGE_PEER_DISC_RECV_LOOKUP_REQUEST], 2);
            assert_eq!(metrics[GAUGE_PEER_DISC_RECV_TARGETED_LOOKUP_REQUEST], 2);
        }
    }
}

#[traced_test]
#[test]
fn test_peer_lookup_retry() {
    // 3 nodes: Node0, Node1, Node2
    // Node0 name record: Node1, Node2
    // Node1 name record: Node0
    // Node2 name record: <empty>
    // Node1 send peer lookup request to Node0, requesting for Node2
    let config = TestConfig {
        num_nodes: 3,
        bootstrap_peers: BTreeMap::from([
            (0, BTreeSet::from([1, 2])),
            (1, BTreeSet::from([0])),
            (2, BTreeSet::new()),
        ]),
        epoch_validators: BTreeMap::from([(Epoch(1), BTreeSet::from([0, 1, 2]))]),
        ..Default::default()
    };
    let (keys, swarm_builder) = setup_keys_and_swarm_builder(config.clone());
    let node_ids = keys
        .iter()
        .map(|k| NodeId::new(k.pubkey()))
        .collect::<Vec<_>>();
    let mut nodes = swarm_builder.build();

    while nodes.step_until(Duration::from_secs(0)) {}

    for (node, state) in nodes.states() {
        let state = state.peer_disc_driver.get_peer_disc_state();
        if node == &node_ids[0] {
            // Node0 has name records of Node1 and Node2
            assert!(state.routing_info.contains_key(&node_ids[1]));
            assert!(state.routing_info.contains_key(&node_ids[2]));
        } else {
            // Node1 and Node2 have name record of Node0
            assert!(state.routing_info.contains_key(&node_ids[0]));
        }
    }

    // Node1 and Node2 should send peer lookup request to Node0 asking for each other
    // Node0 is offline and unable to respond
    nodes.remove_state(&node_ids[0]);
    while nodes.step_until(config.refresh_period + Duration::from_secs(5)) {}

    for (node, state) in nodes.states() {
        let state = state.peer_disc_driver.get_peer_disc_state();
        let metrics = state.metrics();

        // Node1 and Node2 should send targeted peer lookup request to Node0
        assert_eq!(metrics[GAUGE_PEER_DISC_SEND_LOOKUP_REQUEST], 4);
        assert_eq!(metrics[GAUGE_PEER_DISC_LOOKUP_TIMEOUT], 4);

        // Node1 and Node2 does not contain each other in routing info
        if node == &node_ids[1] {
            assert!(!state.routing_info.contains_key(&node_ids[2]));
        } else if node == &node_ids[2] {
            assert!(!state.routing_info.contains_key(&node_ids[1]));
        }
        assert!(state.routing_info.contains_key(&node_ids[0]));
    }
}

#[traced_test]
#[test]
fn test_ping_timeout() {
    // 2 nodes: Node0, Node1
    let config = TestConfig {
        refresh_period: Duration::from_secs(20),
        request_timeout: Duration::from_secs(1),
        epoch_validators: BTreeMap::default(),
        last_participation_prune_threshold: Round(3),
        outbound_pipeline: vec![GenericTransformer::Latency(LatencyTransformer::new(
            Duration::from_secs(2),
        ))],
        ..Default::default()
    };
    let (_, swarm_builder) = setup_keys_and_swarm_builder(config);
    let mut nodes = swarm_builder.build();

    while nodes.step_until(Duration::from_secs(4)) {}

    // message latency of 2 seconds
    // ping timeout of 1 second
    // verify that ping timeout event is recorded correctly and subsequent pong is dropped
    // unresponsive_pings accumulate until being pruned when threshold is reached
    for state in nodes.states().values() {
        let state = state.peer_disc_driver.get_peer_disc_state();
        let metrics = state.metrics();
        // prune threshold is three, so it's pruned after 3 unresponsive pings
        assert_eq!(metrics[GAUGE_PEER_DISC_SEND_PING], 5);
        assert_eq!(metrics[GAUGE_PEER_DISC_PING_TIMEOUT], 4);
        assert_eq!(metrics[GAUGE_PEER_DISC_RECV_PONG], 1);
        assert_eq!(metrics[GAUGE_PEER_DISC_DROP_PONG], 1);

        // name record not added to routing info
        assert!(state.routing_info.is_empty());
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
        bootstrap_peers: BTreeMap::from([
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

    while nodes.step_until(Duration::from_secs(30)) {}

    for (node_id, state) in nodes.states() {
        let state = state.peer_disc_driver.get_peer_disc_state();
        let metrics = state.metrics();

        // Node1, Node2, and Node3 should send lookup request during refresh to Node0 and discover each other
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
        bootstrap_peers: BTreeMap::from([
            (0, BTreeSet::from([1, 2, 3, 4])),
            (1, BTreeSet::from([0, 2, 3, 4])),
            (2, BTreeSet::from([0, 1, 3, 4])),
            (3, BTreeSet::from([0, 1, 2, 4])),
            (4, BTreeSet::from([0, 1, 2, 3])),
        ]),
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

    while nodes.step_until(Duration::from_secs(0)) {}

    for state in nodes.states().values() {
        let state = state.peer_disc_driver.get_peer_disc_state();
        // all nodes should have other nodes in routing info
        for peer_id in node_ids.iter() {
            if peer_id == &state.self_id {
                continue;
            }
            assert!(state.routing_info.contains_key(peer_id));
        }
    }

    // remove Node4 to simulate inactive node
    nodes.remove_state(&node_ids[4]);

    while nodes.step_until(config.refresh_period) {}

    for (node_id, state) in nodes.states() {
        let state = state.peer_disc_driver.get_peer_disc_state();

        // Node4 is inactive, should be pruned by Node1, Node2 and Node3, but should not be pruned by Node0 (due to pinned full node)
        // Node4 is pinned full node for Node0, should not be pruned in routing info of Node0
        if node_id == &node_ids[0] {
            assert!(state.routing_info.contains_key(&node_ids[4]));
        }

        // additional full nodes above max_num_peers are pruned
        assert!(state.routing_info.len() == 2);
    }
}

#[traced_test]
#[test]
fn test_full_nodes_connections() {
    // 3 nodes: Node0, Node1, Node2
    // Node0 is a Validator, Node1 is a ValidatorPublisher, Node2 is a FullNodeClient
    let config = TestConfig {
        num_nodes: 3,
        epoch_validators: BTreeMap::from([(Epoch(1), BTreeSet::from([0, 1]))]),
        roles: BTreeMap::from([(0, false), (1, true), (2, true)]),
        bootstrap_peers: BTreeMap::from([
            (0, BTreeSet::from([1])),
            (1, BTreeSet::from([0])),
            (2, BTreeSet::from([0, 1])),
        ]),
        last_participation_prune_threshold: Round(5),
        refresh_period: Duration::from_secs(15),
        request_timeout: Duration::from_secs(5),
        outbound_pipeline: vec![GenericTransformer::Latency(LatencyTransformer::new(
            Duration::from_secs(1),
        ))],
        ..Default::default()
    };
    let (keys, swarm_builder) = setup_keys_and_swarm_builder(config);
    let node_ids = keys
        .iter()
        .map(|k| NodeId::new(k.pubkey()))
        .collect::<Vec<_>>();
    let mut nodes = swarm_builder.build();

    while nodes.step_until(Duration::from_secs(5)) {}

    for state in nodes.states().values() {
        let state = state.peer_disc_driver.get_peer_disc_state();
        // all nodes should have other nodes in routing info
        for peer_id in node_ids.iter() {
            if peer_id == &state.self_id {
                continue;
            }
            assert!(state.routing_info.contains_key(peer_id));
        }

        if state.self_id == node_ids[0] {
            // Node0 is a validator, no connected secondary raptorcast full nodes
            assert_eq!(
                state.participation_info.get(&node_ids[1]).unwrap().status,
                SecondaryRaptorcastConnectionStatus::None
            );
            assert_eq!(
                state.participation_info.get(&node_ids[2]).unwrap().status,
                SecondaryRaptorcastConnectionStatus::None
            );
        } else if state.self_id == node_ids[1] {
            // Node1 is a validator publisher, should be connected to Node2 which is a full node client
            assert_eq!(
                state.participation_info.get(&node_ids[0]).unwrap().status,
                SecondaryRaptorcastConnectionStatus::None
            );
            assert_eq!(
                state.participation_info.get(&node_ids[2]).unwrap().status,
                SecondaryRaptorcastConnectionStatus::Connected
            );
        } else if state.self_id == node_ids[2] {
            // Node2 is a full node client, should be connected to Node1 which is a validator publisher
            assert_eq!(
                state.participation_info.get(&node_ids[0]).unwrap().status,
                SecondaryRaptorcastConnectionStatus::Pending
            );
            assert_eq!(
                state.participation_info.get(&node_ids[1]).unwrap().status,
                SecondaryRaptorcastConnectionStatus::Connected
            );
        }
    }

    // round increases, non participating full node is pruned
    let round_change_event = PeerDiscoveryEvent::UpdateCurrentRound {
        round: Round(10),
        epoch: Epoch(1),
    };
    for node_id in &node_ids {
        nodes.insert_test_event(node_id, Duration::from_secs(10), round_change_event.clone());
    }
    while nodes.step_until(Duration::from_secs(15)) {}
    for state in nodes.states().values() {
        let state = state.peer_disc_driver.get_peer_disc_state();

        if state.self_id == node_ids[0] {
            assert!(state.routing_info.contains_key(&node_ids[1]));
            assert_eq!(
                state.participation_info.get(&node_ids[1]).unwrap().status,
                SecondaryRaptorcastConnectionStatus::None
            );
            assert!(!state.routing_info.contains_key(&node_ids[2]));
            assert!(!state.participation_info.contains_key(&node_ids[2]));
        } else if state.self_id == node_ids[1] {
            assert!(state.routing_info.contains_key(&node_ids[0]));
            assert_eq!(
                state.participation_info.get(&node_ids[0]).unwrap().status,
                SecondaryRaptorcastConnectionStatus::None
            );
            assert!(!state.routing_info.contains_key(&node_ids[2]));
            assert!(!state.participation_info.contains_key(&node_ids[2]));
        } else if state.self_id == node_ids[2] {
            assert!(state.routing_info.contains_key(&node_ids[0]));
            assert_eq!(
                state.participation_info.get(&node_ids[0]).unwrap().status,
                SecondaryRaptorcastConnectionStatus::Pending
            );
            assert!(state.routing_info.contains_key(&node_ids[1]));
            assert_eq!(
                state.participation_info.get(&node_ids[1]).unwrap().status,
                SecondaryRaptorcastConnectionStatus::Pending
            );
        }
    }

    // during refresh, full node will try to look for upstream again
    let participation_event = PeerDiscoveryEvent::UpdateConfirmGroup {
        end_round: Round(20),
        peers: BTreeSet::from([node_ids[1]]),
    };
    nodes.insert_test_event(&node_ids[2], Duration::from_secs(20), participation_event);
    while nodes.step_until(Duration::from_secs(30)) {}
    for state in nodes.states().values() {
        let state = state.peer_disc_driver.get_peer_disc_state();
        let metrics = state.metrics();

        if state.self_id == node_ids[0] {
            assert_eq!(metrics[GAUGE_PEER_DISC_NUM_UPSTREAM_VALIDATORS], 0);
            assert_eq!(metrics[GAUGE_PEER_DISC_NUM_DOWNSTREAM_FULLNODES], 0);
        } else if state.self_id == node_ids[1] {
            assert!(state.routing_info.contains_key(&node_ids[2]));
            assert_eq!(
                state.participation_info.get(&node_ids[2]).unwrap().status,
                SecondaryRaptorcastConnectionStatus::Connected
            );
            assert_eq!(metrics[GAUGE_PEER_DISC_NUM_UPSTREAM_VALIDATORS], 0);
            assert_eq!(metrics[GAUGE_PEER_DISC_NUM_DOWNSTREAM_FULLNODES], 1);
        } else if state.self_id == node_ids[2] {
            assert!(state.routing_info.contains_key(&node_ids[0]));
            assert_eq!(
                state.participation_info.get(&node_ids[0]).unwrap().status,
                SecondaryRaptorcastConnectionStatus::Pending
            );
            assert!(state.routing_info.contains_key(&node_ids[1]));
            assert_eq!(
                state.participation_info.get(&node_ids[1]).unwrap().status,
                SecondaryRaptorcastConnectionStatus::Connected
            );
            assert_eq!(metrics[GAUGE_PEER_DISC_NUM_UPSTREAM_VALIDATORS], 1);
            assert_eq!(metrics[GAUGE_PEER_DISC_NUM_DOWNSTREAM_FULLNODES], 0);
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
        bootstrap_peers: BTreeMap::from([
            (0, BTreeSet::from([1, 2, 3, 4])),
            (1, BTreeSet::from([0, 2, 3, 4])),
            (2, BTreeSet::from([0, 1, 3, 4])),
            (3, BTreeSet::from([0, 1, 2, 4])),
            (4, BTreeSet::from([0, 1, 2, 3])),
            (5, BTreeSet::from([0, 1, 2])),
        ]),
        refresh_period: Duration::from_secs(5),
        min_num_peers: 3,
        max_num_peers: 10,
        ..Default::default()
    };
    let (keys, swarm_builder) = setup_keys_and_swarm_builder(config.clone());
    let node_ids = keys
        .iter()
        .map(|k| NodeId::new(k.pubkey()))
        .collect::<Vec<_>>();
    let mut nodes = swarm_builder.build();

    while nodes.step_until(Duration::from_secs(0)) {}

    for (node_id, state) in nodes.states() {
        let state = state.peer_disc_driver.get_peer_disc_state();

        // Node0, Node1, and Node2 have name records of Node5
        if node_id == &node_ids[0] || node_id == &node_ids[1] || node_id == &node_ids[2] {
            assert!(state.routing_info.contains_key(&node_ids[5]));
            assert_eq!(state.routing_info.len(), 5);
        }

        // Node3 and Node4 does not have name records of Node5
        if node_id == &node_ids[3] || node_id == &node_ids[4] {
            assert!(!state.routing_info.contains_key(&node_ids[5]));
            assert_eq!(state.routing_info.len(), 4);
        }

        // Node5 have name records of Node0, Node1, and Node2
        if node_id == &node_ids[5] {
            assert_eq!(state.routing_info.len(), 3);
        }
    }

    // Node5 is promoted to validator
    let epoch_change_event = PeerDiscoveryEvent::UpdateCurrentRound {
        round: Round(2),
        epoch: Epoch(2),
    };
    nodes.insert_test_event(&node_ids[5], Duration::from_secs(0), epoch_change_event);

    while nodes.step_until(config.refresh_period) {}

    // Node5 sends ping to all validators
    for state in nodes.states().values() {
        let state = state.peer_disc_driver.get_peer_disc_state();

        // All nodes should have name records of each other
        for peer_id in node_ids.iter() {
            if peer_id == &state.self_id {
                continue;
            }
            assert!(state.routing_info.contains_key(peer_id));
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
        roles: BTreeMap::from([(0, true), (1, true), (2, true), (3, false), (4, true)]),
        epoch_validators: BTreeMap::from([
            (Epoch(1), BTreeSet::from([0, 1, 2, 3, 4])),
            (Epoch(2), BTreeSet::from([0, 1, 2, 3])),
        ]),
        bootstrap_peers: BTreeMap::from([
            (0, BTreeSet::from([1, 2, 3, 4])),
            (1, BTreeSet::from([0, 2, 3, 4])),
            (2, BTreeSet::from([0, 1, 3, 4])),
            (3, BTreeSet::from([0, 1, 2, 4])),
            (4, BTreeSet::from([0, 1, 2, 3])),
        ]),
        refresh_period: Duration::from_secs(5),
        last_participation_prune_threshold: Round(5),
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

    let mut second_epoch_validators = BTreeSet::new();
    for state in nodes.states().values() {
        let state = state.peer_disc_driver.get_peer_disc_state();

        // all nodes have name records of each other
        for peer_id in node_ids.iter() {
            if peer_id == &state.self_id {
                continue;
            }
            assert!(state.routing_info.contains_key(peer_id));
        }
        assert!(state.pending_queue.is_empty());

        if state.self_id == node_ids[4] {
            second_epoch_validators = state.epoch_validators.get(&Epoch(2)).cloned().unwrap();
        }
    }

    // Node4 is demoted to a full node
    let validator_set_change_event = PeerDiscoveryEvent::UpdateValidatorSet {
        epoch: Epoch(2),
        validators: second_epoch_validators,
    };
    nodes.insert_test_event(
        &node_ids[4],
        Duration::from_secs(0),
        validator_set_change_event,
    );

    while nodes.step_until(Duration::from_secs(10)) {}

    // Node4 should now be only connected to three upstream validators
    for (node_id, state) in nodes.states() {
        let state = state.peer_disc_driver.get_peer_disc_state();

        if node_id == &node_ids[4] {
            assert_eq!(state.routing_info.len(), 4);
            let connected_upstream = state
                .participation_info
                .iter()
                .filter(|(_, info)| info.status == SecondaryRaptorcastConnectionStatus::Connected)
                .count();
            assert_eq!(connected_upstream, 3);
            continue;
        }

        // other nodes should still have connections to each other
        for peer_id in node_ids.iter() {
            if peer_id == &state.self_id {
                continue;
            }
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
        bootstrap_peers: BTreeMap::from([
            (0, BTreeSet::new()),
            (1, BTreeSet::from([0])),
            (2, BTreeSet::from([0, 1])),
        ]),
        epoch_validators: BTreeMap::from([(Epoch(1), BTreeSet::from([0]))]),
        last_participation_prune_threshold: Round(5),
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
            assert!(state.routing_info.contains_key(&node_ids[1]));
            assert!(!state.routing_info.contains_key(&node_ids[2]));
        }

        // Node1 (participating full node) should have connection to Node0 (validator)
        // Node2 (non-participating full node) should be pruned
        if node_ids[1] == state.self_id {
            assert!(state.routing_info.contains_key(&node_ids[0]));
            assert!(!state.routing_info.contains_key(&node_ids[2]));
        }

        // Node2 (non-participating full node) should have connection to Node0 (validator)
        if node_ids[2] == state.self_id {
            assert!(state.routing_info.contains_key(&node_ids[0]));
            assert!(!state.routing_info.contains_key(&node_ids[1]));
        }
    }
}
