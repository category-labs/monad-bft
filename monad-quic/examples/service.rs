use std::{
    collections::{BTreeMap, HashMap},
    net::SocketAddr,
    num::ParseIntError,
    time::{Duration, Instant},
};

use bytes::{Bytes, BytesMut};
use clap::Parser;
use futures_util::StreamExt;
use monad_crypto::{
    certificate_signature::{CertificateKeyPair, CertificateSignature, CertificateSignaturePubKey},
    NopSignature,
};
use monad_executor::Executor;
use monad_executor_glue::{Message, RouterCommand};
use monad_gossip::mock::MockGossipConfig;
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
    rt.block_on(service(args.addresses, 1, 10_000 * 400));
}

type SignatureType = NopSignature;
type PubKeyType = CertificateSignaturePubKey<SignatureType>;

async fn service(addresses: Vec<String>, num_broadcast: u8, message_len: usize) {
    assert!(
        addresses.len() <= 100,
        "peer_id generation only supports <= 100 peers"
    );
    assert!(message_len >= 1);
    let num_peers = addresses.len() as u8;
    let keys: Vec<_> = (0..num_peers)
        .map(|idx| {
            let mut privkey: [u8; 32] = [1 + idx; 32];
            <<SignatureType as CertificateSignature>::KeyPairType as CertificateKeyPair>::from_bytes(&mut privkey).unwrap()
        })
        .collect();

    let peers: Vec<NodeId<PubKeyType>> = keys
        .iter()
        .map(|keypair| NodeId::new(keypair.pubkey()))
        .collect();

    let known_addresses: HashMap<NodeId<PubKeyType>, SocketAddr> = peers
        .iter()
        .copied()
        .zip(addresses.into_iter())
        .map(|(peer, address)| (peer, address.parse().unwrap()))
        .collect();

    const MAX_RTT: Duration = Duration::from_millis(110);
    const BANDWIDTH_Mbps: u16 = 1_000;

    let services = keys
        .iter()
        .map(|key| {
            let me = NodeId::new(key.pubkey());
            let server_address = *known_addresses.get(&me).unwrap();
            Service::<_, _, MockMessage, MockMessage>::new(
                ServiceConfig {
                    me,
                    known_addresses: known_addresses.clone(),
                    server_address,
                    quinn_config: SafeQuinnConfig::<SignatureType>::new(
                        key,
                        MAX_RTT,
                        BANDWIDTH_Mbps,
                    ),
                },
                MockGossipConfig {
                    all_peers: peers.clone(),
                    me,
                }
                .build(),
            )
        })
        .collect::<Vec<_>>();

    let (tx_writer, tx_reader): (BTreeMap<_, _>, Vec<_>) = peers
        .iter()
        .copied()
        .map(|peer| {
            let (sender, receiver) =
                tokio::sync::mpsc::unbounded_channel::<RouterCommand<PubKeyType, MockMessage>>();
            ((peer, sender), receiver)
        })
        .unzip();
    let (rx_writer, rx_reader) =
        std::sync::mpsc::channel::<(NodeId<PubKeyType>, <MockMessage as Message>::Event)>();

    services
        .into_iter()
        .zip(tx_reader)
        .for_each(|(mut service, mut tx_reader)| {
            let rx_writer = rx_writer.clone();
            std::thread::spawn(move || {
                let rt = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .unwrap();

                rt.block_on(async move {
                    loop {
                        tokio::select! {
                            maybe_message = service.next() => {
                                let message = maybe_message.expect("never terminates");
                                rx_writer
                                    .send((service.me(), message))
                                    .expect("rx_reader should never be dropped");
                            }
                            maybe_tx = tx_reader.recv() => {
                                let tx = maybe_tx.expect("tx_writer should never be dropped");
                                // TODO batch these?
                                service.exec(vec![tx]);
                            }
                        };
                    }
                });
            });
        });

    let (tx_peer, tx_router) = tx_writer.first_key_value().expect("at least 1 tx");
    let start = Instant::now();
    let mut id = 0_u8;
    'warmup: loop {
        let mut expected_rx_count = num_peers;
        tracing::info!("warming up, sending msg id={}", id);
        let message = MockMessage::new(id, 8);
        tx_router
            .send(RouterCommand::Publish {
                target: RouterTarget::Broadcast,
                message,
            })
            .expect("reader should never be dropped");
        while let Ok((_, (tx, msg_id))) = rx_reader.recv_timeout(Duration::from_secs(1)) {
            if &tx == tx_peer && msg_id == message.id {
                expected_rx_count -= 1;
                if expected_rx_count == 0 {
                    break 'warmup;
                }
            }
        }
        id += 1;
        assert!(id <= u8::MAX - num_broadcast);
    }
    tracing::info!("took {:?} to warmup!", start.elapsed());

    let start = Instant::now();
    let mut expected_message_ids = HashMap::new();
    for broadcast_id in id..id + num_broadcast {
        let message = MockMessage::new(broadcast_id, message_len);
        tx_router
            .send(RouterCommand::Publish {
                target: RouterTarget::Broadcast,
                message,
            })
            .expect("reader should never be dropped");
        expected_message_ids.insert(message.id, num_peers);
    }
    while let Ok((_, (tx, msg_id))) = rx_reader.recv_timeout(Duration::from_secs(10)) {
        if &tx == tx_peer {
            let num_left = expected_message_ids
                .get_mut(&msg_id)
                .expect("msg_id must exist");
            *num_left -= 1;
            if num_left == &0 {
                expected_message_ids.remove(&msg_id);
            }
            if expected_message_ids.is_empty() {
                tracing::info!(
                    "took {:?} to broadcast/receive {} messages!",
                    start.elapsed(),
                    num_broadcast
                );
                std::process::exit(0);
            }
        }
    }
    unreachable!("timed out!");
}

#[derive(Clone, Copy)]
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
    type NodeIdPubKey = PubKeyType;
    type Event = (NodeId<Self::NodeIdPubKey>, u8);

    fn event(self, from: NodeId<Self::NodeIdPubKey>) -> Self::Event {
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
