use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    num::ParseIntError,
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};

use bytes::{Bytes, BytesMut};
use clap::Parser;
use futures_util::StreamExt;
use monad_crypto::secp256k1::KeyPair;
use monad_executor::Executor;
use monad_executor_glue::{Message, RouterCommand};
use monad_gossip::mock::{MockGossip, MockGossipConfig};
use monad_quic::{SafeQuinnConfig, Service, ServiceConfig};
use monad_types::{Deserializable, NodeId, RouterTarget, Serializable};

#[derive(Parser, Debug)]
struct Args {
    /// addresses
    #[arg(short, long, required=true, num_args=1..)]
    addresses: Vec<String>,
}

pub fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format_timestamp_micros()
        .init();
    let args = Args::parse();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(service(args.addresses, 1, 5_000 * 32));
}

async fn service(addresses: Vec<String>, num_broadcast: u8, message_len: usize) {
    assert!(
        addresses.len() <= 100,
        "peer_id generation only supports <= 100 peers"
    );
    assert!(message_len >= 1);
    let num_peers = addresses.len() as u8;
    let keys: Vec<KeyPair> = (0..num_peers)
        .map(|idx| {
            let mut privkey: [u8; 32] = [1 + idx; 32];
            KeyPair::from_bytes(&mut privkey).unwrap()
        })
        .collect();

    let peers: Vec<NodeId> = keys
        .iter()
        .map(|keypair| NodeId(keypair.pubkey()))
        .collect();

    let known_addresses: HashMap<NodeId, SocketAddr> = peers
        .iter()
        .copied()
        .zip(addresses.into_iter())
        .map(|(peer, address)| (peer, address.parse().unwrap()))
        .collect();

    const MAX_RTT: Duration = Duration::from_millis(110);
    const BANDWIDTH_Mbps: u16 = 1_000;

    let mut services = keys
        .iter()
        .map(|key| {
            let me = NodeId(key.pubkey());
            let server_address = *known_addresses.get(&me).unwrap();
            Service::new(
                ServiceConfig {
                    me,
                    known_addresses: known_addresses.clone(),
                    server_address,
                    quinn_config: SafeQuinnConfig::new(key, MAX_RTT, BANDWIDTH_Mbps),
                },
                MockGossipConfig {
                    all_peers: peers.clone(),
                    me,
                }
                .build(),
            )
        })
        .collect::<Vec<Service<_, MockGossip, MockMessage, MockMessage>>>();

    // broadcast from first peer
    services[0].exec(vec![RouterCommand::Publish {
        target: RouterTarget::Broadcast,
        message: MockMessage::new(0, 8),
    }]);

    let broadcast_commands: Vec<_> = (1..=num_broadcast)
        .map(|idx| RouterCommand::Publish {
            target: RouterTarget::Broadcast,
            message: MockMessage::new(idx, message_len),
        })
        .collect();
    // messages from first peer expected
    let expected_messages: HashSet<(NodeId, u8)> = peers
        .first()
        .iter()
        .copied()
        .flat_map(|peer| (1..=num_broadcast).map(|message_id| (*peer, message_id)))
        .collect();

    let mux = Arc::new(RwLock::new(0_u8));
    let start_mux = Arc::new(RwLock::new(None));
    services
        .into_iter()
        .enumerate()
        .for_each(|(idx, mut service)| {
            let mut broadcast_commands = Some(broadcast_commands.clone());
            let mux = mux.clone();
            let start_mux = start_mux.clone();
            let mut expected_messages = expected_messages.clone();
            std::thread::spawn(move || {
                let rt = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .unwrap();

                rt.block_on(async move {
                    let mut num_ack_rx = 0;
                    while !expected_messages.is_empty() {
                        let message = service.next().await.expect("never terminates");
                        if message.1 == 0 {
                            if idx != 0 {
                                // send ack
                                service.exec(vec![RouterCommand::Publish {
                                    target: RouterTarget::PointToPoint(message.0),
                                    message: MockMessage::new(0, 5),
                                }]);
                            } else {
                                num_ack_rx += 1;
                                assert!(num_ack_rx <= num_peers);
                                if num_ack_rx == num_peers {
                                    // all acks received, warmed up, broadcast payload
                                    tracing::info!("START");
                                    let now = Instant::now();
                                    *start_mux.write().unwrap() = Some(now);
                                    tracing::info!("exec broadcast payload");
                                    service.exec(broadcast_commands.take().unwrap());
                                    tracing::info!(
                                        "done broadcast payload, elapsed={:?}",
                                        now.elapsed()
                                    );
                                }
                            }
                        } else {
                            assert!(expected_messages.remove(&message));
                        }
                    }
                    tracing::info!(
                        "took {:?} for node={} to rx all messages",
                        (*start_mux.read().unwrap()).unwrap().elapsed(),
                        idx
                    );
                    *mux.write().unwrap() += 1;
                    service.next().await;
                    unreachable!("should yield forever");
                });
            });
        });

    while *mux.read().unwrap() < num_peers {
        std::thread::sleep(Duration::from_millis(100));
    }
    std::process::exit(0)
}

#[derive(Clone)]
struct MockMessage {
    id: u8,
    message_len: usize,
}

impl MockMessage {
    fn new(id: u8, message_len: usize) -> Self {
        Self { id, message_len }
    }
}

impl Message for MockMessage {
    type Event = (NodeId, u8);

    fn event(self, from: NodeId) -> Self::Event {
        (from, self.id)
    }
}

impl Serializable<Bytes> for MockMessage {
    fn serialize(&self) -> Bytes {
        let mut message = BytesMut::zeroed(self.message_len);
        message[0] = self.id;
        message.into()
    }
}

impl Deserializable<Bytes> for MockMessage {
    type ReadError = ParseIntError;

    fn deserialize(message: &Bytes) -> Result<Self, Self::ReadError> {
        Ok(Self::new(message[0], message.len()))
    }
}
