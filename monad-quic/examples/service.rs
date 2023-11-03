use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    num::ParseIntError,
    time::{Duration, Instant},
};

use clap::Parser;
use futures_util::{FutureExt, StreamExt};
use monad_crypto::secp256k1::KeyPair;
use monad_executor::Executor;
use monad_executor_glue::{Identifiable, Message, PeerId, RouterCommand, RouterTarget};
use monad_gossip::mock::{MockGossip, MockGossipConfig};
use monad_quic::service::{Service, ServiceConfig, UnsafeNoAuthQuinnConfig};
use monad_types::{Deserializable, Serializable};

#[derive(Parser, Debug)]
struct Args {
    /// addresses
    #[arg(short, long, required=true, num_args=1..)]
    addresses: Vec<String>,
}

pub fn main() {
    let args = Args::parse();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(service(args.addresses, 1, 5_000 * 32))
}

async fn service(addresses: Vec<String>, num_broadcast: u8, message_len: usize) {
    assert!(
        addresses.len() <= 100,
        "peer_id generation only supports <= 100 peers"
    );
    assert!(message_len >= 1);
    let num_peers = addresses.len() as u8;
    let peers: Vec<PeerId> = (0..num_peers)
        .map(|idx| {
            let mut privkey: [u8; 32] = [1 + idx; 32];
            let keypair = KeyPair::from_bytes(&mut privkey).unwrap();
            PeerId(keypair.pubkey())
        })
        .collect();

    let known_addresses: HashMap<PeerId, SocketAddr> = peers
        .iter()
        .copied()
        .zip(addresses.into_iter())
        .map(|(peer, address)| (peer, address.parse().unwrap()))
        .collect();

    const MAX_RTT: Duration = Duration::from_millis(110);
    const BANDWIDTH_Mbps: u16 = 1_000;

    let mut services = peers
        .iter()
        .copied()
        .map(|me| {
            let server_address = *known_addresses.get(&me).unwrap();
            Service::new(
                ServiceConfig {
                    zero_instant: Instant::now(),
                    me,
                    known_addresses: known_addresses.clone(),
                    server_address,
                    quinn_config: UnsafeNoAuthQuinnConfig::new(me, MAX_RTT, BANDWIDTH_Mbps),
                },
                MockGossipConfig {
                    all_peers: peers.clone(),
                }
                .build(),
            )
        })
        .collect::<Vec<Service<_, MockGossip, MockMessage, MockMessage>>>();

    // broadcast from first peer
    services[0].exec(
        (0..num_broadcast)
            .map(|idx| RouterCommand::Publish {
                target: RouterTarget::Broadcast,
                message: MockMessage::new(idx, message_len),
            })
            .collect(),
    );
    // messages from first peer expected
    let expected_messages: HashSet<(PeerId, u8)> = peers
        .first()
        .iter()
        .copied()
        .flat_map(|peer| (0..num_broadcast).map(|message_id| (*peer, message_id)))
        .collect();

    let (wg, _) = tokio::sync::broadcast::channel::<()>(num_peers as usize);
    futures_util::future::join_all(
        services
            .into_iter()
            .enumerate()
            .map(|(idx, mut service)| {
                let mut expected_messages = expected_messages.clone();
                let wg = wg.clone();
                let mut rx = wg.subscribe();
                async move {
                    let start = Instant::now();
                    while !expected_messages.is_empty() {
                        let message = service.next().await.expect("never terminates");
                        assert!(expected_messages.remove(&message));
                    }
                    eprintln!(
                        "took {:?} for node={} to rx all messages",
                        start.elapsed(),
                        idx
                    );
                    wg.send(()).unwrap();
                    let mut num_done = 0;
                    while num_done < num_peers {
                        if let futures_util::future::Either::Left((result, _)) =
                            futures_util::future::select(rx.recv().boxed(), service.next().boxed())
                                .await
                        {
                            result.unwrap();
                            num_done += 1;
                        }
                    }
                }
            })
            .map(tokio::spawn),
    )
    .await
    .into_iter()
    .collect::<Result<Vec<_>, _>>()
    .unwrap();
}

#[derive(Clone)]
struct MockMessage(Vec<u8>);

impl MockMessage {
    fn new(id: u8, message_len: usize) -> Self {
        let mut message = vec![0; message_len];
        message[0] = id;
        Self(message)
    }
}

impl AsRef<Self> for MockMessage {
    fn as_ref(&self) -> &Self {
        self
    }
}

impl Identifiable for MockMessage {
    type Id = u8;

    fn id(&self) -> Self::Id {
        self.0[0]
    }
}

impl Message for MockMessage {
    type Event = (PeerId, u8);

    fn event(self, from: PeerId) -> Self::Event {
        (from, self.0[0])
    }
}

impl Serializable<Vec<u8>> for MockMessage {
    fn serialize(&self) -> Vec<u8> {
        self.0.clone()
    }
}

impl Deserializable<[u8]> for MockMessage {
    type ReadError = ParseIntError;

    fn deserialize(message: &[u8]) -> Result<Self, Self::ReadError> {
        Ok(Self(message.to_vec()))
    }
}
