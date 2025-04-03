use std::{
    collections::BTreeMap,
    net::{SocketAddr, SocketAddrV4},
    str::FromStr,
    time::Duration,
};

use alloy_rlp::Encodable;
use monad_crypto::{
    NopKeyPair, NopPubKey, NopSignature,
    certificate_signature::{CertificateKeyPair, CertificateSignature},
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
}

type KeyPairType = NopKeyPair;
type PubKeyType = NopPubKey;
type SignatureType = NopSignature;

fn generate_name_record(keypair: &KeyPairType) -> MonadNameRecord<SignatureType> {
    let name_record = NameRecord {
        address: SocketAddr::V4(SocketAddrV4::from_str("1.1.1.1:8000").unwrap()),
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
                last_seen: None,
                name_record: generate_name_record(k),
            })
        })
        .collect();
    let swarm_builder = PeerDiscSwarmBuilder::<PeerDiscSwarm, PeerDiscoveryBuilder<SignatureType>> {
        builders: keys
            .iter()
            .enumerate()
            .map(|(i, key)| NodeBuilder {
                id: NodeId::new(key.pubkey()),
                algo_builder: PeerDiscoveryBuilder {
                    local_record_seq: 0,
                    self_id: NodeId::new(key.pubkey()),
                    peer_info: all_peers.clone(),
                    ping_period: Duration::from_secs(1),
                    rng_seed: 123456,
                },
                router_scheduler: NoSerRouterConfig::new(all_peers.keys().cloned().collect())
                    .build(),
                seed: i.try_into().unwrap(),
            })
            .collect(),
        seed: 7,
    };

    let mut nodes = swarm_builder.build();

    while nodes.step_until(Duration::from_secs(10)) {}

    // first ping is sent out at t=0. we expect >=10 at t=10
    for state in nodes.states().values() {
        let metrics = state.peer_disc_driver.get_peer_disc_state().metrics();
        assert!(metrics["send_ping"] >= 10);
        assert!(metrics["send_pong"] >= 10);
        assert!(metrics["recv_ping"] >= 10);
        assert!(metrics["recv_pong"] >= 10);
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
    // NodeA name record: NodeA, NodeB
    // NodeB name record: NodeA, NodeB
    // NodeC name record: NodeA, NodeB, NodeC
    // (we can assume that NodeA and NodeB are the bootstrap nodes in a network)
    let bootstrap_peers: BTreeMap<NodeId<PubKeyType>, PeerInfo<SignatureType>> = bootstrap_keys
        .iter()
        .map(|k| {
            (NodeId::new(k.pubkey()), PeerInfo {
                last_ping: None,
                last_seen: None,
                name_record: generate_name_record(k),
            })
        })
        .collect();
    let all_peers: BTreeMap<NodeId<PubKeyType>, PeerInfo<SignatureType>> = keys
        .iter()
        .map(|k| {
            (NodeId::new(k.pubkey()), PeerInfo {
                last_ping: None,
                last_seen: None,
                name_record: generate_name_record(k),
            })
        })
        .collect();
    let swarm_builder = PeerDiscSwarmBuilder::<PeerDiscSwarm, PeerDiscoveryBuilder<SignatureType>> {
        builders: keys
            .iter()
            .enumerate()
            .map(|(i, key)| NodeBuilder {
                id: NodeId::new(key.pubkey()),
                algo_builder: PeerDiscoveryBuilder {
                    local_record_seq: 0,
                    self_id: NodeId::new(key.pubkey()),
                    peer_info: if key.pubkey() == third_key.pubkey() {
                        all_peers.clone()
                    } else {
                        bootstrap_peers.clone()
                    },
                    ping_period: Duration::from_secs(1),
                    rng_seed: 123456,
                },
                router_scheduler: NoSerRouterConfig::new(all_peers.keys().cloned().collect())
                    .build(),
                seed: i.try_into().unwrap(),
            })
            .collect(),
        seed: 7,
    };
    let mut nodes = swarm_builder.build();
    while nodes.step_until(Duration::from_secs(1)) {}

    // NodeA, NodeB and NodeC should now have peer_info of each other
    for state in nodes.states().values() {
        let state = state.peer_disc_driver.get_peer_disc_state();
        for node_id in all_peers.keys() {
            assert!(state.peer_info.contains_key(node_id));
        }
    }
}
