use std::time::Duration;

use monad_crypto::{
    NopSignature,
    certificate_signature::{CertificateKeyPair, CertificateSignaturePubKey},
};
use monad_peer_disc_swarm::{
    NodeBuilder, PeerDiscSwarmRelation, SwarmPubKeyType, builder::PeerDiscSwarmBuilder,
};
use monad_peer_discovery::{
    algo::{PeerDiscoveryAlgo, PeerDiscoveryMessage},
    mock::{PingPongDiscovery, PingPongDiscoveryBuilder},
};
use monad_router_scheduler::{NoSerRouterConfig, NoSerRouterScheduler, RouterSchedulerBuilder};
use monad_testutil::signing::create_keys;
use monad_types::NodeId;

struct PingPongPeerDiscSwarm {}

impl PeerDiscSwarmRelation for PingPongPeerDiscSwarm {
    type SignatureType = NopSignature;

    type PeerDiscoveryAlgoType = PingPongDiscovery<SwarmPubKeyType<Self>>;

    type TransportMessage = PeerDiscoveryMessage<SwarmPubKeyType<Self>>;

    type RouterSchedulerType = NoSerRouterScheduler<
        SwarmPubKeyType<Self>,
        PeerDiscoveryMessage<SwarmPubKeyType<Self>>,
        PeerDiscoveryMessage<SwarmPubKeyType<Self>>,
    >;
}

type SignatureType = NopSignature;

#[test]
fn test_ping_pong() {
    let keys = create_keys::<SignatureType>(2);
    let all_peers = keys
        .iter()
        .map(|k| NodeId::new(k.pubkey()))
        .collect::<Vec<_>>();
    let swarm_builder = PeerDiscSwarmBuilder::<
        PingPongPeerDiscSwarm,
        PingPongDiscoveryBuilder<CertificateSignaturePubKey<SignatureType>>,
    > {
        builders: keys
            .iter()
            .enumerate()
            .map(|(i, key)| NodeBuilder {
                id: NodeId::new(key.pubkey()),
                algo_builder: PingPongDiscoveryBuilder {
                    self_id: NodeId::new(key.pubkey()),
                    peers: all_peers.clone(),
                    ping_period: Duration::from_secs(1),
                },
                router_scheduler: NoSerRouterConfig::new(all_peers.clone().into_iter().collect())
                    .build(),
                seed: i.try_into().unwrap(),
            })
            .collect(),
        seed: 7,
    };

    let mut nodes = swarm_builder.build();

    while nodes.step_until(Duration::from_secs(10)) {}

    // first ping is sent out at t=0. we expect >=10 at t=10
    for (node_id, state) in nodes.states() {
        let metrics = state.peer_disc_driver.get_peer_disc_state().metrics();
        assert!(metrics["send_ping"] >= 10, "send_ping < 10 {}", node_id);
        assert!(metrics["send_pong"] >= 10, "send_pong < 10 {}", node_id);
        assert!(metrics["recv_ping"] >= 10, "recv_ping < 10 {}", node_id);
        assert!(metrics["recv_pong"] >= 10, "recv_pong < 10 {}", node_id);
    }
}
