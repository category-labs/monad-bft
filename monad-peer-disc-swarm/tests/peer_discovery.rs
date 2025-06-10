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
        PeerDiscovery, PeerDiscoveryBuilder, PeerInfo,
    },
};
use monad_router_scheduler::{NoSerRouterConfig, NoSerRouterScheduler, RouterSchedulerBuilder};
use monad_testutil::signing::create_keys;
use monad_transformer::{GenericTransformer, GenericTransformerPipeline, LatencyTransformer};
use monad_types::{Epoch, NodeId};
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
    let signature = SignatureType::sign(&encoded, keypair);
    MonadNameRecord {
        name_record,
        signature,
    }
}

#[traced_test]
#[test]
fn test_ping_pong() {
    let keys = create_keys::<SignatureType>(2);
    let all_peers: BTreeMap<NodeId<PubKeyType>, PeerInfo<SignatureType>> = keys
        .iter()
        .map(|k| {
            (NodeId::new(k.pubkey()), PeerInfo {
                last_ping: None,
                unresponsive_pings: 0,
                name_record: generate_name_record(k),
            })
        })
        .collect();
    let swarm_builder = PeerDiscSwarmBuilder::<PeerDiscSwarm, PeerDiscoveryBuilder<SignatureType>> {
        builders: keys
            .iter()
            .enumerate()
            .map(|(i, key)| {
                let self_id = NodeId::new(key.pubkey());
                let peer_info = all_peers
                    .clone()
                    .into_iter()
                    .filter(|(id, _)| id != &self_id)
                    .collect::<BTreeMap<_, _>>();
                NodeBuilder {
                    id: NodeId::new(key.pubkey()),
                    addr: generate_name_record(key).address(),
                    algo_builder: PeerDiscoveryBuilder {
                        self_id,
                        self_record: generate_name_record(key),
                        current_epoch: Epoch(1),
                        epoch_validators: BTreeMap::new(),
                        dedicated_full_nodes: BTreeSet::new(),
                        peer_info,
                        ping_period: Duration::from_secs(5),
                        refresh_period: Duration::from_secs(30),
                        request_timeout: Duration::from_secs(1),
                        prune_threshold: 3,
                        min_active_connections: 5,
                        max_active_connections: 50,
                        rng_seed: 123456,
                    },
                    router_scheduler: NoSerRouterConfig::new(all_peers.keys().cloned().collect())
                        .build(),
                    seed: i.try_into().unwrap(),
                    outbound_pipeline: vec![GenericTransformer::Latency(LatencyTransformer::new(
                        Duration::from_secs(1),
                    ))],
                }
            })
            .collect(),
        seed: 7,
    };

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
    let keys = create_keys::<SignatureType>(3);

    // two bootstrap nodes where addresses are known to each other
    // one new joining node where it knows the bootstrap nodes addresses but not vice versa
    let (bootstrap_keys, third_key) = (&keys[0..2], &keys[2]);

    // initialize peer info of the three nodes
    // NodeA name record: NodeB
    // NodeB name record: NodeA
    // NodeC name record: NodeA, NodeB
    // (we can assume that NodeA and NodeB are the bootstrap nodes in a network)
    let bootstrap_peers: BTreeMap<NodeId<PubKeyType>, PeerInfo<SignatureType>> = bootstrap_keys
        .iter()
        .map(|k| {
            (NodeId::new(k.pubkey()), PeerInfo {
                last_ping: None,
                unresponsive_pings: 0,
                name_record: generate_name_record(k),
            })
        })
        .collect();
    let all_peers: BTreeMap<NodeId<PubKeyType>, PeerInfo<SignatureType>> = keys
        .iter()
        .map(|k| {
            (NodeId::new(k.pubkey()), PeerInfo {
                last_ping: None,
                unresponsive_pings: 0,
                name_record: generate_name_record(k),
            })
        })
        .collect();
    let swarm_builder = PeerDiscSwarmBuilder::<PeerDiscSwarm, PeerDiscoveryBuilder<SignatureType>> {
        builders: keys
            .iter()
            .enumerate()
            .map(|(i, key)| {
                let self_id = NodeId::new(key.pubkey());
                let base_peers = if key.pubkey() == third_key.pubkey() {
                    &all_peers
                } else {
                    &bootstrap_peers
                };
                let peer_info = base_peers
                    .clone()
                    .into_iter()
                    .filter(|(id, _)| id != &self_id)
                    .collect::<BTreeMap<_, _>>();
                NodeBuilder {
                    id: NodeId::new(key.pubkey()),
                    addr: generate_name_record(key).address(),
                    algo_builder: PeerDiscoveryBuilder {
                        self_id,
                        self_record: generate_name_record(key),
                        current_epoch: Epoch(1),
                        epoch_validators: BTreeMap::new(),
                        dedicated_full_nodes: BTreeSet::new(),
                        peer_info,
                        ping_period: Duration::from_secs(2),
                        refresh_period: Duration::from_secs(4),
                        request_timeout: Duration::from_secs(1),
                        prune_threshold: 3,
                        min_active_connections: 5,
                        max_active_connections: 50,
                        rng_seed: 123456,
                    },
                    router_scheduler: NoSerRouterConfig::new(all_peers.keys().cloned().collect())
                        .build(),
                    seed: i.try_into().unwrap(),
                    outbound_pipeline: vec![],
                }
            })
            .collect(),
        seed: 7,
    };
    let mut nodes = swarm_builder.build();
    while nodes.step_until(Duration::from_secs(0)) {}

    // NodeA, NodeB and NodeC should now have peer_info of each other
    for state in nodes.states().values() {
        let state = state.peer_disc_driver.get_peer_disc_state();
        for node_id in all_peers.keys() {
            if node_id == &state.self_id {
                continue;
            }
            assert!(state.peer_info.contains_key(node_id));
        }
    }
}

#[traced_test]
#[test]
fn test_update_name_record() {
    let keys = create_keys::<SignatureType>(2);
    let node_a_key = &keys[0];
    let node_a = NodeId::new(node_a_key.pubkey());
    let node_b_key = &keys[1];
    let node_b = NodeId::new(node_b_key.pubkey());
    let all_peers: BTreeMap<NodeId<PubKeyType>, PeerInfo<SignatureType>> = keys
        .iter()
        .map(|k| {
            (NodeId::new(k.pubkey()), PeerInfo {
                last_ping: None,
                unresponsive_pings: 0,
                name_record: generate_name_record(k),
            })
        })
        .collect();
    let swarm_builder = PeerDiscSwarmBuilder::<PeerDiscSwarm, PeerDiscoveryBuilder<SignatureType>> {
        builders: keys
            .iter()
            .enumerate()
            .map(|(i, key)| {
                let self_id = NodeId::new(key.pubkey());
                let peer_info = all_peers
                    .clone()
                    .into_iter()
                    .filter(|(id, _)| id != &self_id)
                    .collect::<BTreeMap<_, _>>();
                NodeBuilder {
                    id: NodeId::new(key.pubkey()),
                    addr: generate_name_record(key).address(),
                    algo_builder: PeerDiscoveryBuilder {
                        self_id,
                        self_record: generate_name_record(key),
                        current_epoch: Epoch(1),
                        epoch_validators: BTreeMap::new(),
                        dedicated_full_nodes: BTreeSet::new(),
                        peer_info,
                        ping_period: Duration::from_secs(2),
                        refresh_period: Duration::from_secs(4),
                        request_timeout: Duration::from_secs(1),
                        prune_threshold: 3,
                        min_active_connections: 5,
                        max_active_connections: 50,
                        rng_seed: 123456,
                    },
                    router_scheduler: NoSerRouterConfig::new(all_peers.keys().cloned().collect())
                        .build(),
                    seed: i.try_into().unwrap(),
                    outbound_pipeline: vec![],
                }
            })
            .collect(),
        seed: 7,
    };

    let mut nodes = swarm_builder.build();

    while nodes.step_until(Duration::from_secs(0)) {}

    // NodeA, NodeB should have peer info of each other
    for state in nodes.states().values() {
        let state = state.peer_disc_driver.get_peer_disc_state();
        for node_id in all_peers.keys() {
            if node_id == &state.self_id {
                continue;
            }
            assert!(state.peer_info.contains_key(node_id));
        }
    }

    // NodeA restarts with new name record
    // which will then initiate connections with other nodes to remain connected
    let _old_node_a_state = nodes
        .remove_state(&node_a)
        .expect("Node A state should exist");

    // create new name record for NodeA with new IP and incremented seq number
    let new_name_record = NameRecord {
        address: SocketAddrV4::from_str("2.2.2.2:8000").unwrap(),
        seq: 1,
    };
    let mut encoded = Vec::new();
    new_name_record.encode(&mut encoded);
    let signature = SignatureType::sign(&encoded, node_a_key);
    let new_name_record = MonadNameRecord {
        name_record: new_name_record,
        signature,
    };
    let mut new_peer_info = all_peers.clone();
    new_peer_info.get_mut(&node_a).unwrap().name_record = new_name_record;

    let new_node_a_builder = NodeBuilder {
        id: node_a,
        addr: new_name_record.address(),
        algo_builder: PeerDiscoveryBuilder {
            self_id: node_a,
            self_record: new_name_record,
            current_epoch: Epoch(1),
            epoch_validators: BTreeMap::new(),
            dedicated_full_nodes: BTreeSet::new(),
            peer_info: new_peer_info.clone(),
            ping_period: Duration::from_secs(2),
            refresh_period: Duration::from_secs(4),
            request_timeout: Duration::from_secs(1),
            prune_threshold: 3,
            min_active_connections: 5,
            max_active_connections: 50,
            rng_seed: 123456,
        },
        router_scheduler: NoSerRouterConfig::new(all_peers.keys().cloned().collect()).build(),
        seed: 1,
        outbound_pipeline: vec![],
    };

    nodes.add_state(new_node_a_builder);

    while nodes.step_until(Duration::from_secs(0)) {}

    // Node B should have the peer info of NodeA updated
    let node_b_state = nodes
        .states()
        .get(&node_b)
        .expect("Node B state should exist");
    let peer_info = &node_b_state
        .peer_disc_driver
        .get_peer_disc_state()
        .peer_info;
    let node_a_record = peer_info
        .get(&node_a)
        .expect("Node B should have node A name record")
        .name_record;

    assert_eq!(node_a_record, new_name_record);
}

#[traced_test]
#[test]
fn test_prune_nodes() {
    let keys = create_keys::<SignatureType>(2);
    let all_peers: BTreeMap<NodeId<PubKeyType>, PeerInfo<SignatureType>> = keys
        .iter()
        .map(|k| {
            (NodeId::new(k.pubkey()), PeerInfo {
                last_ping: None,
                unresponsive_pings: 0,
                name_record: generate_name_record(k),
            })
        })
        .collect();
    let swarm_builder = PeerDiscSwarmBuilder::<PeerDiscSwarm, PeerDiscoveryBuilder<SignatureType>> {
        builders: keys
            .iter()
            .enumerate()
            .map(|(i, key)| {
                let self_id = NodeId::new(key.pubkey());
                let peer_info = all_peers
                    .clone()
                    .into_iter()
                    .filter(|(id, _)| id != &self_id)
                    .collect::<BTreeMap<_, _>>();
                NodeBuilder {
                    id: NodeId::new(key.pubkey()),
                    addr: generate_name_record(key).address(),
                    algo_builder: PeerDiscoveryBuilder {
                        self_id,
                        self_record: generate_name_record(key),
                        current_epoch: Epoch(1),
                        epoch_validators: BTreeMap::new(),
                        dedicated_full_nodes: BTreeSet::new(),
                        peer_info,
                        ping_period: Duration::from_secs(2),
                        refresh_period: Duration::from_secs(10),
                        request_timeout: Duration::from_secs(1),
                        prune_threshold: 3,
                        min_active_connections: 5,
                        max_active_connections: 50,
                        rng_seed: 123456,
                    },
                    router_scheduler: NoSerRouterConfig::new(all_peers.keys().cloned().collect())
                        .build(),
                    seed: i.try_into().unwrap(),
                    outbound_pipeline: vec![],
                }
            })
            .collect(),
        seed: 7,
    };

    let mut nodes = swarm_builder.build();

    // prune period is set to 10 seconds (i.e. prune every 10 seconds)
    // refresh is called at t=0 and t=10
    while nodes.step_until(Duration::from_secs(10)) {}
    for state in nodes.states().values() {
        let metrics = state.peer_disc_driver.get_peer_disc_state().metrics();
        assert!(metrics[GAUGE_PEER_DISC_REFRESH] == 2);
    }

    // a node goes offline
    let offline_key = &keys[0];
    let offline_node = NodeId::new(offline_key.pubkey());
    nodes.remove_state(&offline_node);

    // the offline node should be pruned
    while nodes.step_until(Duration::from_secs(20)) {}
    assert_eq!(nodes.states().len(), 1);
    for state in nodes.states().values() {
        let state = state.peer_disc_driver.get_peer_disc_state();
        let metrics = state.metrics();
        assert!(metrics[GAUGE_PEER_DISC_REFRESH] == 3);
        assert!(state.peer_info.is_empty());
    }
}

#[traced_test]
#[test]
fn test_peer_lookup_random_nodes() {
    let keys = create_keys::<SignatureType>(5);

    let subset_keys = &keys[0..1];
    let all_keys = &keys[0..4];
    let non_existent_key = &keys[4];

    let node_a = NodeId::new(all_keys[0].pubkey());
    let node_b = NodeId::new(all_keys[1].pubkey());
    let node_c = NodeId::new(all_keys[2].pubkey());
    let node_d = NodeId::new(all_keys[3].pubkey());
    let nonexistent_id = NodeId::new(non_existent_key.pubkey());

    // initialize peer info
    // NodeA name record: NodeB, NodeC, NodeD
    // NodeB name record: NodeA
    // NodeC name record: <empty>
    // NodeD name record: <empty>
    let subset_peers: BTreeMap<NodeId<PubKeyType>, PeerInfo<SignatureType>> = subset_keys
        .iter()
        .map(|k| {
            (NodeId::new(k.pubkey()), PeerInfo {
                last_ping: None,
                unresponsive_pings: 0,
                name_record: generate_name_record(k),
            })
        })
        .collect();
    let all_peers: BTreeMap<NodeId<PubKeyType>, PeerInfo<SignatureType>> = all_keys
        .iter()
        .map(|k| {
            (NodeId::new(k.pubkey()), PeerInfo {
                last_ping: None,
                unresponsive_pings: 0,
                name_record: generate_name_record(k),
            })
        })
        .collect();
    let swarm_builder = PeerDiscSwarmBuilder::<PeerDiscSwarm, PeerDiscoveryBuilder<SignatureType>> {
        builders: all_keys
            .iter()
            .enumerate()
            .map(|(i, key)| {
                let self_id = NodeId::new(key.pubkey());
                let peers = if key.pubkey() == node_a.pubkey() {
                    all_peers.clone()
                } else if key.pubkey() == node_b.pubkey() {
                    subset_peers.clone()
                } else {
                    BTreeMap::new()
                };
                let peer_info = peers
                    .into_iter()
                    .filter(|(id, _)| id != &self_id)
                    .collect::<BTreeMap<_, _>>();
                NodeBuilder {
                    id: NodeId::new(key.pubkey()),
                    addr: generate_name_record(key).address(),
                    algo_builder: PeerDiscoveryBuilder {
                        self_id,
                        self_record: generate_name_record(key),
                        current_epoch: Epoch(1),
                        epoch_validators: BTreeMap::from([(
                            Epoch(1),
                            BTreeSet::from([node_a, node_b, node_c, node_d]),
                        )]),
                        dedicated_full_nodes: BTreeSet::new(),
                        peer_info,
                        ping_period: Duration::from_secs(2),
                        refresh_period: Duration::from_secs(60),
                        request_timeout: Duration::from_secs(1),
                        prune_threshold: 3,
                        min_active_connections: 5,
                        max_active_connections: 50,
                        rng_seed: 123456,
                    },
                    router_scheduler: NoSerRouterConfig::new(all_peers.keys().cloned().collect())
                        .build(),
                    seed: i.try_into().unwrap(),
                    outbound_pipeline: vec![],
                }
            })
            .collect(),
        seed: 7,
    };
    let mut nodes = swarm_builder.build();
    while nodes.step_until(Duration::from_secs(0)) {}

    // NodeB send lookup request to NodeA for nonexistent node
    let lookup_event = PeerDiscoveryEvent::SendPeerLookup {
        to: node_a,
        target: nonexistent_id,
        open_discovery: true,
    };
    nodes.insert_test_event(&node_b, Duration::from_secs(0), lookup_event);

    while nodes.step_until(Duration::from_secs(0)) {}

    // NodeB should know about NodeC and NodeD through the random response
    let node_b_state = nodes
        .states()
        .get(&node_b)
        .expect("Node B state should exist")
        .peer_disc_driver
        .get_peer_disc_state();
    for node_id in all_peers.keys() {
        if node_id == &node_b_state.self_id {
            continue;
        }
        assert!(node_b_state.peer_info.contains_key(node_id));
    }

    // check that NodeB sends pings to new nodes
    let metrics = node_b_state.metrics();
    assert_eq!(metrics[GAUGE_PEER_DISC_SEND_PING], 3);

    // check that NodeC and NodeD now has name record of NodeB
    for (node_id, state) in nodes.states() {
        let state = state.peer_disc_driver.get_peer_disc_state();
        if node_id == &node_b {
            continue;
        }
        assert!(state.peer_info.contains_key(&node_b));
    }
}

#[traced_test]
#[test]
fn test_peer_lookup_targeted_nodes() {
    let keys = create_keys::<SignatureType>(3);

    let subset_keys = &keys[0..2];

    let node_a = NodeId::new(keys[0].pubkey());
    let node_b = NodeId::new(keys[1].pubkey());
    let node_c = NodeId::new(keys[2].pubkey());

    // initialize peer info
    // NodeA name record: NodeB, NodeC
    // NodeB name record: NodeA
    // All three nodes are validators, NodeB is missing NodeC name record
    let all_peers: BTreeMap<NodeId<PubKeyType>, PeerInfo<SignatureType>> = keys
        .iter()
        .map(|k| {
            (NodeId::new(k.pubkey()), PeerInfo {
                last_ping: None,
                unresponsive_pings: 0,
                name_record: generate_name_record(k),
            })
        })
        .collect();
    let node_b_peers: BTreeMap<NodeId<PubKeyType>, PeerInfo<SignatureType>> = keys[0..2]
        .iter()
        .map(|k| {
            (NodeId::new(k.pubkey()), PeerInfo {
                last_ping: None,
                unresponsive_pings: 0,
                name_record: generate_name_record(k),
            })
        })
        .collect();
    let swarm_builder = PeerDiscSwarmBuilder::<PeerDiscSwarm, PeerDiscoveryBuilder<SignatureType>> {
        builders: subset_keys
            .iter()
            .enumerate()
            .map(|(i, key)| {
                let self_id = NodeId::new(key.pubkey());
                NodeBuilder {
                    id: NodeId::new(key.pubkey()),
                    addr: generate_name_record(key).address(),
                    algo_builder: PeerDiscoveryBuilder {
                        self_id,
                        self_record: generate_name_record(key),
                        current_epoch: Epoch(1),
                        epoch_validators: BTreeMap::from([(
                            Epoch(1),
                            BTreeSet::from([node_a, node_b, node_c]),
                        )]),
                        dedicated_full_nodes: BTreeSet::new(),
                        peer_info: if self_id == node_a {
                            all_peers
                                .clone()
                                .into_iter()
                                .filter(|(id, _)| id != &self_id)
                                .collect::<BTreeMap<_, _>>()
                        } else {
                            node_b_peers
                                .clone()
                                .into_iter()
                                .filter(|(id, _)| id != &self_id)
                                .collect::<BTreeMap<_, _>>()
                        },
                        ping_period: Duration::from_secs(2),
                        refresh_period: Duration::from_secs(10),
                        request_timeout: Duration::from_secs(1),
                        prune_threshold: 3,
                        min_active_connections: 1,
                        max_active_connections: 50,
                        rng_seed: 123456,
                    },
                    router_scheduler: NoSerRouterConfig::new(all_peers.keys().cloned().collect())
                        .build(),
                    seed: i.try_into().unwrap(),
                    outbound_pipeline: vec![],
                }
            })
            .collect(),
        seed: 7,
    };
    let mut nodes = swarm_builder.build();
    while nodes.step_until(Duration::from_secs(10)) {}

    // NodeB has number of peers larger than min active connections but still missing validator NodeC
    // NodeB should send targeted lookup request to NodeA asking for NodeC
    for state in nodes.states().values() {
        let state = state.peer_disc_driver.get_peer_disc_state();
        let metrics = state.metrics();
        for node_id in all_peers.keys() {
            if node_id == &state.self_id {
                continue;
            }
            assert!(state.peer_info.contains_key(node_id));
        }

        if state.self_id == node_a {
            assert_eq!(metrics[GAUGE_PEER_DISC_RECV_LOOKUP_REQUEST], 1);
            assert_eq!(metrics[GAUGE_PEER_DISC_RECV_TARGETED_LOOKUP_REQUEST], 1);
        }
    }
}

#[traced_test]
#[test]
fn test_peer_lookup_retry() {
    let keys = create_keys::<SignatureType>(3);
    let node_a_key = &keys[0];
    let node_a = NodeId::new(node_a_key.pubkey());
    let node_b_key = &keys[1];
    let node_b = NodeId::new(node_b_key.pubkey());
    let node_c_key = &keys[2];
    let node_c = NodeId::new(node_c_key.pubkey());
    let (subset_keys, _) = (&keys[0..2], &keys[2]);
    let all_peers: BTreeMap<NodeId<PubKeyType>, PeerInfo<SignatureType>> = subset_keys
        .iter()
        .map(|k| {
            (NodeId::new(k.pubkey()), PeerInfo {
                last_ping: None,
                unresponsive_pings: 0,
                name_record: generate_name_record(k),
            })
        })
        .collect();
    let mut epoch_validators = BTreeMap::new();
    epoch_validators.insert(Epoch(1), BTreeSet::from([node_a, node_b, node_c]));
    let swarm_builder = PeerDiscSwarmBuilder::<PeerDiscSwarm, PeerDiscoveryBuilder<SignatureType>> {
        builders: subset_keys
            .iter()
            .enumerate()
            .map(|(i, key)| {
                let self_id = NodeId::new(key.pubkey());
                NodeBuilder {
                    id: NodeId::new(key.pubkey()),
                    addr: generate_name_record(key).address(),
                    algo_builder: PeerDiscoveryBuilder {
                        self_id,
                        self_record: generate_name_record(key),
                        current_epoch: Epoch(1),
                        epoch_validators: epoch_validators.clone(),
                        dedicated_full_nodes: BTreeSet::new(),
                        peer_info: if self_id == node_a {
                            let mut info = BTreeMap::new();
                            info.insert(node_b, PeerInfo {
                                last_ping: None,
                                unresponsive_pings: 0,
                                name_record: generate_name_record(node_b_key),
                            });
                            info.insert(node_c, PeerInfo {
                                last_ping: None,
                                unresponsive_pings: 0,
                                name_record: generate_name_record(node_c_key),
                            });
                            info
                        } else {
                            let mut info = BTreeMap::new();
                            info.insert(node_a, PeerInfo {
                                last_ping: None,
                                unresponsive_pings: 0,
                                name_record: generate_name_record(node_a_key),
                            });
                            info
                        },
                        ping_period: Duration::from_secs(15),
                        refresh_period: Duration::from_secs(30),
                        request_timeout: Duration::from_secs(1),
                        prune_threshold: 3,
                        min_active_connections: 5,
                        max_active_connections: 50,
                        rng_seed: 123456,
                    },
                    router_scheduler: NoSerRouterConfig::new(all_peers.keys().cloned().collect())
                        .build(),
                    seed: i.try_into().unwrap(),
                    outbound_pipeline: vec![GenericTransformer::Latency(LatencyTransformer::new(
                        Duration::from_secs(2),
                    ))],
                }
            })
            .collect(),
        seed: 7,
    };

    let mut nodes = swarm_builder.build();

    // initialize peer info
    // NodeA name record: NodeB, NodeC
    // NodeB name record: NodeA
    // NodeB send peer lookup request to NodeA, requesting for NodeC
    // Outbound message latency of NodeB is 2 seconds while timeout is 1 second
    let lookup_event = PeerDiscoveryEvent::SendPeerLookup {
        to: node_a,
        target: node_c,
        open_discovery: false,
    };
    nodes.insert_test_event(&node_b, Duration::from_secs(0), lookup_event);

    while nodes.step_until(Duration::from_secs(10)) {}

    for (node, state) in nodes.states() {
        if node == &node_b {
            let state = state.peer_disc_driver.get_peer_disc_state();
            let metrics = state.metrics();
            assert_eq!(metrics[GAUGE_PEER_DISC_LOOKUP_TIMEOUT], 8);
            assert_eq!(metrics[GAUGE_PEER_DISC_DROP_LOOKUP_RESPONSE], 8);

            // Due to lookup timeout, NodeB still does not have name record of NodeC
            assert!(!state.peer_info.contains_key(&node_c));
        }
    }
}

#[traced_test]
#[test]
fn test_ping_timeout() {
    let keys = create_keys::<SignatureType>(2);
    let all_peers: BTreeMap<NodeId<PubKeyType>, PeerInfo<SignatureType>> = keys
        .iter()
        .map(|k| {
            (NodeId::new(k.pubkey()), PeerInfo {
                last_ping: None,
                unresponsive_pings: 0,
                name_record: generate_name_record(k),
            })
        })
        .collect();
    let swarm_builder = PeerDiscSwarmBuilder::<PeerDiscSwarm, PeerDiscoveryBuilder<SignatureType>> {
        builders: keys
            .iter()
            .enumerate()
            .map(|(i, key)| {
                let self_id = NodeId::new(key.pubkey());
                let peer_info = all_peers
                    .clone()
                    .into_iter()
                    .filter(|(id, _)| id != &self_id)
                    .collect::<BTreeMap<_, _>>();
                NodeBuilder {
                    id: NodeId::new(key.pubkey()),
                    addr: generate_name_record(key).address(),
                    algo_builder: PeerDiscoveryBuilder {
                        self_id,
                        self_record: generate_name_record(key),
                        current_epoch: Epoch(1),
                        epoch_validators: BTreeMap::new(),
                        dedicated_full_nodes: BTreeSet::new(),
                        peer_info,
                        ping_period: Duration::from_secs(5),
                        refresh_period: Duration::from_secs(20),
                        request_timeout: Duration::from_secs(1),
                        prune_threshold: 3,
                        min_active_connections: 5,
                        max_active_connections: 50,
                        rng_seed: 123456,
                    },
                    router_scheduler: NoSerRouterConfig::new(all_peers.keys().cloned().collect())
                        .build(),
                    seed: i.try_into().unwrap(),
                    outbound_pipeline: vec![GenericTransformer::Latency(LatencyTransformer::new(
                        Duration::from_secs(2),
                    ))],
                }
            })
            .collect(),
        seed: 7,
    };

    let mut nodes = swarm_builder.build();

    while nodes.step_until(Duration::from_secs(20)) {}

    // message latency of 2 seconds
    // ping timeout of 1 second
    // verify that ping timeout event is recorded correctly and subsequent pong is dropped
    // unresponsive_pings accumulate until being pruned
    for state in nodes.states().values() {
        let state = state.peer_disc_driver.get_peer_disc_state();
        let metrics = state.metrics();
        assert_eq!(metrics[GAUGE_PEER_DISC_SEND_PING], 5);
        assert_eq!(metrics[GAUGE_PEER_DISC_PING_TIMEOUT], 4);
        assert_eq!(metrics[GAUGE_PEER_DISC_RECV_PONG], 4);
        assert_eq!(metrics[GAUGE_PEER_DISC_DROP_PONG], 4);

        assert!(state.peer_info.is_empty());
    }
}

#[traced_test]
#[test]
fn test_min_watermark() {
    // 4 nodes: NodeA, NodeB, NodeC, NodeD
    // NodeA does not have any peer in the beginning
    // NodeB, NodeC, and NodeD has NodeA as their peer in the beginning
    let keys = create_keys::<SignatureType>(4);
    let node_a_key = &keys[0];
    let node_a = NodeId::new(node_a_key.pubkey());
    let node_b_key = &keys[1];
    let node_b = NodeId::new(node_b_key.pubkey());
    let node_c_key = &keys[2];
    let node_c = NodeId::new(node_c_key.pubkey());
    let node_d_key = &keys[3];
    let node_d = NodeId::new(node_d_key.pubkey());
    let all_peers: BTreeMap<NodeId<PubKeyType>, PeerInfo<SignatureType>> = keys
        .iter()
        .map(|k| {
            (NodeId::new(k.pubkey()), PeerInfo {
                last_ping: None,
                unresponsive_pings: 0,
                name_record: generate_name_record(k),
            })
        })
        .collect();
    let swarm_builder = PeerDiscSwarmBuilder::<PeerDiscSwarm, PeerDiscoveryBuilder<SignatureType>> {
        builders: keys
            .iter()
            .enumerate()
            .map(|(i, key)| {
                let self_id = NodeId::new(key.pubkey());
                NodeBuilder {
                    id: NodeId::new(key.pubkey()),
                    addr: generate_name_record(key).address(),
                    algo_builder: PeerDiscoveryBuilder {
                        self_id,
                        self_record: generate_name_record(key),
                        current_epoch: Epoch(1),
                        epoch_validators: BTreeMap::from([(
                            Epoch(1),
                            BTreeSet::from([node_a, node_b, node_c, node_d]),
                        )]),
                        dedicated_full_nodes: BTreeSet::new(),
                        peer_info: if self_id == node_a {
                            BTreeMap::new()
                        } else {
                            BTreeMap::from([(node_a, PeerInfo {
                                last_ping: None,
                                unresponsive_pings: 0,
                                name_record: generate_name_record(node_a_key),
                            })])
                        },
                        ping_period: Duration::from_secs(2),
                        refresh_period: Duration::from_secs(5),
                        request_timeout: Duration::from_secs(1),
                        prune_threshold: 1,
                        min_active_connections: 2,
                        max_active_connections: 10,
                        rng_seed: 123456,
                    },
                    router_scheduler: NoSerRouterConfig::new(all_peers.keys().cloned().collect())
                        .build(),
                    seed: i.try_into().unwrap(),
                    outbound_pipeline: vec![],
                }
            })
            .collect(),
        seed: 7,
    };

    let mut nodes = swarm_builder.build();

    while nodes.step_until(Duration::from_secs(0)) {}

    for (node_id, state) in nodes.states() {
        let state = state.peer_disc_driver.get_peer_disc_state();
        let metrics = state.metrics();

        // NodeB, NodeC, and NodeD should send lookup request to NodeA and discover each other
        for peer_id in all_peers.keys() {
            if peer_id == &state.self_id {
                continue;
            }
            assert!(state.peer_info.contains_key(peer_id));
        }

        if node_id == &node_a {
            assert_eq!(metrics[GAUGE_PEER_DISC_RECV_LOOKUP_REQUEST], 3);
        } else {
            assert_eq!(metrics[GAUGE_PEER_DISC_SEND_LOOKUP_REQUEST], 1);
        }
    }
}

#[traced_test]
#[test]
fn test_max_watermark() {
    // 5 nodes: NodeA, NodeB, NodeC, NodeD, NodeE
    // NodeA is a validator, NodeB, NodeC, NodeD, NodeE are full nodes
    // NodeE is a dedicated full node for NodeA
    let keys = create_keys::<SignatureType>(5);
    let node_a_key = &keys[0];
    let node_a = NodeId::new(node_a_key.pubkey());
    let node_e_key = &keys[4];
    let node_e = NodeId::new(node_e_key.pubkey());
    let (subset_keys, _) = (&keys[0..4], &keys[4]);
    let all_peers: BTreeMap<NodeId<PubKeyType>, PeerInfo<SignatureType>> = keys
        .iter()
        .map(|k| {
            (NodeId::new(k.pubkey()), PeerInfo {
                last_ping: None,
                unresponsive_pings: 0,
                name_record: generate_name_record(k),
            })
        })
        .collect();
    let refresh_period = Duration::from_secs(5);
    let swarm_builder = PeerDiscSwarmBuilder::<PeerDiscSwarm, PeerDiscoveryBuilder<SignatureType>> {
        builders: subset_keys
            .iter()
            .enumerate()
            .map(|(i, key)| {
                let self_id = NodeId::new(key.pubkey());
                let peer_info = all_peers
                    .clone()
                    .into_iter()
                    .filter(|(id, _)| id != &self_id)
                    .collect::<BTreeMap<_, _>>();
                NodeBuilder {
                    id: NodeId::new(key.pubkey()),
                    addr: generate_name_record(key).address(),
                    algo_builder: PeerDiscoveryBuilder {
                        self_id,
                        self_record: generate_name_record(key),
                        current_epoch: Epoch(1),
                        epoch_validators: BTreeMap::from([(Epoch(1), BTreeSet::from([node_a]))]),
                        dedicated_full_nodes: if self_id == node_a {
                            BTreeSet::from([node_e])
                        } else {
                            BTreeSet::new()
                        },
                        peer_info,
                        ping_period: Duration::from_secs(2),
                        refresh_period,
                        request_timeout: Duration::from_secs(1),
                        prune_threshold: 1,
                        min_active_connections: 1,
                        max_active_connections: 2,
                        rng_seed: 123456,
                    },
                    router_scheduler: NoSerRouterConfig::new(all_peers.keys().cloned().collect())
                        .build(),
                    seed: i.try_into().unwrap(),
                    outbound_pipeline: vec![],
                }
            })
            .collect(),
        seed: 7,
    };

    let mut nodes = swarm_builder.build();

    while nodes.step_until(refresh_period) {}

    for (node_id, state) in nodes.states() {
        let state = state.peer_disc_driver.get_peer_disc_state();

        // NodeE is inactive, should be pruned by NodeB, NodeC and NodeD, but should not be pruned by NodeA
        if node_id == &node_a {
            assert!(state.peer_info.contains_key(&node_e));
        } else {
            assert!(!state.peer_info.contains_key(&node_e));
        }

        // additional full nodes above max_active_connections are pruned
        assert!(state.peer_info.len() <= 2);
    }
}
