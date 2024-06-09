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
use monad_gossip::{
    seeder::{Raptor, SeederConfig},
    Gossip,
};
use monad_quic::{SafeQuinnConfig, Service, ServiceConfig};
use monad_types::{Deserializable, Epoch, NodeId, Round, RouterTarget, Serializable};
use tracing_subscriber::fmt::format::FmtSpan;

#[derive(Parser, Debug)]
struct Args {
    /// addresses
    #[arg(short, long, required=true, num_args=1..)]
    addresses: Vec<String>,
}

pub fn main() {
    tracing_subscriber::fmt::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_span_events(FmtSpan::CLOSE)
        .init();
    let args = Args::parse();

    let payload_size = 10_000 * 400;

    let num_rt = 2;
    let threads_per_rt = 2;

    const MAX_RTT: Duration = Duration::from_millis(250);
    const BANDWIDTH_Mbps: u16 = 1_000;

    std::thread::sleep(Duration::from_secs(5));
    service(
        num_rt,
        threads_per_rt,
        args.addresses,
        1,
        payload_size,
        MAX_RTT,
        BANDWIDTH_Mbps,
    );
}

type SignatureType = NopSignature;
type PubKeyType = CertificateSignaturePubKey<SignatureType>;

fn service(
    num_rt: usize,
    threads_per_rt: usize,
    addresses: Vec<String>,
    num_broadcast: u32,
    message_len: usize,
    max_rtt: Duration,
    bandwidth_Mbps: u16,
) {
    assert!(message_len >= 4);
    let num_peers = addresses.len() as u32;
    let keys: Vec<_> = (0..num_peers)
        .map(|idx| {
            let mut privkey: [u8; 32] = [1; 32];
            let idx_bytes = idx.to_le_bytes();
            privkey[0] = idx_bytes[0];
            privkey[1] = idx_bytes[1];
            privkey[2] = idx_bytes[2];
            privkey[3] = idx_bytes[3];
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
        .zip(addresses)
        .map(|(peer, address)| (peer, address.parse().unwrap()))
        .collect();

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

    let rts: Vec<_> = std::iter::repeat_with(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(threads_per_rt)
            .build()
            .unwrap()
    })
    .take(num_rt)
    .collect();

    rts.iter()
        .cycle()
        .zip(keys.into_iter().zip(tx_reader))
        .for_each(|(rt, (key, mut tx_reader))| {
            let rx_writer = rx_writer.clone();
            let me = NodeId::new(key.pubkey());
            let all_peers = peers.clone();
            let server_address = *known_addresses.get(&me).unwrap();
            let known_addresses = known_addresses.clone();

            rt.spawn(async move {
                let service_config = ServiceConfig {
                    me,
                    known_addresses,
                    server_address,
                    quinn_config: SafeQuinnConfig::<SignatureType>::new(
                        &key,
                        max_rtt,
                        bandwidth_Mbps,
                    ),
                };

                let gossip = SeederConfig::<Raptor<SignatureType>> {
                    all_peers,
                    key: unsafe {
                        // FIXME find workaround for this transmute
                        // This is required right now because Service::new requires a 'static
                        // future for spawning the helper task.
                        //
                        // This can be resolved once we have a preferred secret management solution
                        std::mem::transmute(&key)
                    },

                    timeout: Duration::from_millis(700),
                    up_bandwidth_Mbps: bandwidth_Mbps,
                    chunker_poll_interval: Duration::from_millis(10),
                }
                .build()
                .boxed(); // TODO get rid of boxing
                let mut service =
                    Service::<_, _, MockMessage, MockMessage>::new(service_config, gossip);
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

    let (tx_peer, tx_router) = tx_writer.first_key_value().expect("at least 1 tx");
    let start = Instant::now();
    let required_successful_sends_in_row = 5;
    let mut successful_sends = 0;
    let mut id = 0;
    'warmup: loop {
        let mut expected_rx_count = num_peers;
        tracing::info!("warming up, sending msg id={}", id);
        let message = MockMessage::new(id, message_len);
        tx_router
            .send(RouterCommand::Publish {
                target: RouterTarget::Broadcast(Epoch(0), Round(0)),
                message,
            })
            .expect("reader should never be dropped");
        while let Ok((_, (tx, msg_id))) = rx_reader.recv_timeout(Duration::from_millis(1000)) {
            if &tx == tx_peer && msg_id == message.id {
                expected_rx_count -= 1;
                if expected_rx_count == 0 {
                    successful_sends += 1;
                    tracing::info!("successful_sends = {}", successful_sends);
                    if successful_sends == required_successful_sends_in_row {
                        break 'warmup;
                    } else {
                        id += 1;
                        assert!(id <= u32::MAX - num_broadcast);
                        std::thread::sleep(Duration::from_secs(1));
                        continue 'warmup;
                    }
                }
            }
        }
        tracing::info!(
            "failed to recv warmup msg, num_left = {}",
            expected_rx_count
        );
        successful_sends = 0;
        id += 1;
        assert!(id <= u32::MAX - num_broadcast);
    }
    tracing::info!("took {:?} to warmup!", start.elapsed());
    std::thread::sleep(Duration::from_secs(1));

    let start = Instant::now();
    let mut expected_message_ids = HashMap::new();
    for broadcast_id in id..id + num_broadcast {
        let message = MockMessage::new(broadcast_id, message_len);
        tx_router
            .send(RouterCommand::Publish {
                target: RouterTarget::Broadcast(Epoch(0), Round(0)),
                message,
            })
            .expect("reader should never be dropped");
        expected_message_ids.insert(message.id, num_peers);
    }
    while let Ok((_, (tx, msg_id))) = rx_reader.recv_timeout(Duration::from_secs(1)) {
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
                std::thread::sleep(Duration::from_secs(3));
                std::process::exit(0);
            }
        }
    }
    unreachable!("timed out!");
}

#[derive(Clone, Copy)]
struct MockMessage {
    id: u32,
    message_len: usize,
}

impl MockMessage {
    fn new(id: u32, message_len: usize) -> Self {
        Self { id, message_len }
    }
}

impl Message for MockMessage {
    type NodeIdPubKey = PubKeyType;
    type Event = (NodeId<Self::NodeIdPubKey>, u32);

    fn event(self, from: NodeId<Self::NodeIdPubKey>) -> Self::Event {
        (from, self.id)
    }
}

impl Serializable<Bytes> for MockMessage {
    fn serialize(&self) -> Bytes {
        let mut message = BytesMut::zeroed(self.message_len);
        let id_bytes = self.id.to_le_bytes();
        message[0] = id_bytes[0];
        message[1] = id_bytes[1];
        message[2] = id_bytes[2];
        message[3] = id_bytes[3];
        message.into()
    }
}

impl Deserializable<Bytes> for MockMessage {
    type ReadError = ParseIntError;

    fn deserialize(message: &Bytes) -> Result<Self, Self::ReadError> {
        Ok(Self::new(
            u32::from_le_bytes(message[..4].try_into().unwrap()),
            message.len(),
        ))
    }
}
