use std::time::Duration;

use monad_crypto::{
    certificate_signature::{CertificateSignaturePubKey, CertificateSignatureRecoverable, PubKey},
    hasher::{Hasher, HasherType},
    NopSignature,
};
use monad_gossip::{
    broadcasttree::{BroadcastTree, BroadcastTreeConfig},
    gossipsub::{UnsafeGossipsub, UnsafeGossipsubConfig},
    mock::{MockGossip, MockGossipConfig},
    testutil::{make_swarm, test_broadcast, test_direct, Swarm},
    Gossip,
};
use monad_transformer::{BytesTransformer, LatencyTransformer, PacerTransformer};
use monad_types::NodeId;
use rand::SeedableRng;
use rand_chacha::ChaCha20Rng;

const BROADCAST_PAYLOAD_SIZE_BYTES: usize = 5000 * 32;
const DIRECT_PAYLOAD_SIZE_BYTES: usize = 1024;
const UP_BANDWIDTH_MBIT: usize = 100;
const GOSSIPSUB_FANOUT: usize = 7;
const BROADCASTTREE_ARITY: usize = 6;
const BROADCASTTREE_NUM_ROUTES: usize = 1;

fn testnet<
    ST: CertificateSignatureRecoverable,
    G: Gossip<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
>(
    make_gossip: impl Fn(&[NodeId<G::NodeIdPubKey>], &NodeId<G::NodeIdPubKey>) -> G,
) -> Swarm<G> {
    const NUM_NODES: u16 = 100;
    make_swarm::<ST, _>(NUM_NODES, make_gossip, |_all_peers, _me| {
        vec![
            BytesTransformer::Latency(LatencyTransformer::new(Duration::from_millis(50))),
            BytesTransformer::Pacer(PacerTransformer::new(UP_BANDWIDTH_MBIT, 8 * 1450)),
        ]
    })
}

fn make_mock_gossip<PT: PubKey>(all_peers: &[NodeId<PT>], me: &NodeId<PT>) -> MockGossip<PT> {
    MockGossipConfig {
        all_peers: all_peers.to_vec(),
        me: *me,
    }
    .build()
}

fn testnet_mock_gossip_broadcast() -> u128 {
    let mut swarm = testnet::<NopSignature, _>(make_mock_gossip);
    let mut rng = ChaCha20Rng::from_seed([0; 32]);
    test_broadcast(
        &mut rng,
        &mut swarm,
        Duration::from_secs(10),
        BROADCAST_PAYLOAD_SIZE_BYTES,
        1,
        1.0,
    )
    .as_millis()
}

fn testnet_mock_gossip_direct() -> u128 {
    let mut swarm = testnet::<NopSignature, _>(make_mock_gossip);
    let mut rng = ChaCha20Rng::from_seed([0; 32]);
    test_direct(
        &mut rng,
        &mut swarm,
        Duration::from_secs(10),
        DIRECT_PAYLOAD_SIZE_BYTES,
    )
    .as_millis()
}

fn make_gossipsub<PT: PubKey>(all_peers: &[NodeId<PT>], me: &NodeId<PT>) -> UnsafeGossipsub<PT> {
    UnsafeGossipsubConfig {
        seed: {
            let mut hasher = HasherType::new();
            hasher.update(&me.pubkey().bytes());
            hasher.hash().0
        },
        me: *me,
        all_peers: all_peers.to_vec(),
        fanout: GOSSIPSUB_FANOUT,
    }
    .build()
}

fn testnet_gossipsub_broadcast() -> u128 {
    let mut swarm = testnet::<NopSignature, _>(make_gossipsub);
    let mut rng = ChaCha20Rng::from_seed([0; 32]);
    test_broadcast(
        &mut rng,
        &mut swarm,
        Duration::from_secs(10),
        BROADCAST_PAYLOAD_SIZE_BYTES,
        1,
        1.0,
    )
    .as_millis()
}

fn testnet_gossipsub_direct() -> u128 {
    let mut swarm = testnet::<NopSignature, _>(make_gossipsub);
    let mut rng = ChaCha20Rng::from_seed([0; 32]);
    test_direct(
        &mut rng,
        &mut swarm,
        Duration::from_secs(10),
        DIRECT_PAYLOAD_SIZE_BYTES,
    )
    .as_millis()
}

fn make_broadcasttree<PT: PubKey>(all_peers: &[NodeId<PT>], me: &NodeId<PT>) -> BroadcastTree<PT> {
    BroadcastTreeConfig {
        all_peers: all_peers.to_vec(),
        my_id: *me,
        tree_arity: BROADCASTTREE_ARITY,
        num_routes: BROADCASTTREE_NUM_ROUTES,
    }
    .build()
}

fn testnet_broadcasttree_broadcast() -> u128 {
    let mut swarm = testnet::<NopSignature, _>(make_broadcasttree);
    let mut rng = ChaCha20Rng::from_seed([0; 32]);
    test_broadcast(
        &mut rng,
        &mut swarm,
        Duration::from_secs(10),
        BROADCAST_PAYLOAD_SIZE_BYTES,
        1,
        1.0,
    )
    .as_millis()
}

fn testnet_broadcasttree_direct() -> u128 {
    let mut swarm = testnet::<NopSignature, _>(make_broadcasttree);
    let mut rng = ChaCha20Rng::from_seed([0; 32]);
    test_direct(
        &mut rng,
        &mut swarm,
        Duration::from_secs(10),
        DIRECT_PAYLOAD_SIZE_BYTES,
    )
    .as_millis()
}

monad_virtual_bench::virtual_bench_main! {
    testnet_mock_gossip_broadcast,
    testnet_mock_gossip_direct,
    testnet_gossipsub_broadcast,
    testnet_gossipsub_direct,
    testnet_broadcasttree_broadcast,
    testnet_broadcasttree_direct,
}
