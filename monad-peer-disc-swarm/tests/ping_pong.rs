use std::{
    collections::BTreeMap,
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
    MonadNameRecord, NameRecord, PeerDiscoveryAlgo, PeerDiscoveryMessage,
    discovery::{PeerDiscovery, PeerDiscoveryBuilder, PeerInfo},
};
use monad_router_scheduler::{NoSerRouterConfig, NoSerRouterScheduler, RouterSchedulerBuilder};
use monad_testutil::signing::create_keys;
use monad_transformer::{GenericTransformer, GenericTransformerPipeline, LatencyTransformer};
use monad_types::NodeId;
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
                        peer_info,
                        ping_period: Duration::from_secs(5),
                        prune_period: Duration::from_secs(30),
                        request_timeout: Duration::from_secs(1),
                        prune_threshold: 3,
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
        assert_eq!(metrics["send_ping"], 5);
        assert_eq!(metrics["send_pong"], 4);
        assert_eq!(metrics["recv_ping"], 4);
        assert_eq!(metrics["recv_pong"], 4);
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
    // NodeB name record: NodeA,
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
                        peer_info,
                        ping_period: Duration::from_secs(2),
                        prune_period: Duration::from_secs(4),
                        request_timeout: Duration::from_secs(1),
                        prune_threshold: 3,
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
                        peer_info,
                        ping_period: Duration::from_secs(2),
                        prune_period: Duration::from_secs(4),
                        request_timeout: Duration::from_secs(1),
                        prune_threshold: 3,
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
            peer_info: new_peer_info.clone(),
            ping_period: Duration::from_secs(2),
            prune_period: Duration::from_secs(4),
            request_timeout: Duration::from_secs(1),
            prune_threshold: 3,
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
                        peer_info,
                        ping_period: Duration::from_secs(2),
                        prune_period: Duration::from_secs(10),
                        request_timeout: Duration::from_secs(1),
                        prune_threshold: 3,
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
    while nodes.step_until(Duration::from_secs(10)) {}
    for state in nodes.states().values() {
        let metrics = state.peer_disc_driver.get_peer_disc_state().metrics();
        assert!(metrics["prune"] == 1);
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
        assert!(metrics["prune"] == 2);
        assert!(state.peer_info.is_empty());
    }
}

#[traced_test]
#[test]
fn test_peer_lookup_retry() {
    // TODO: handle case when the peer lookup request is not being handled
}
