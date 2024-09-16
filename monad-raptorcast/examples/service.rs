use std::{
    collections::{BTreeMap, HashMap},
    net::SocketAddr,
    num::ParseIntError,
    time::{Duration, Instant},
};

use bytes::{Bytes, BytesMut};
use clap::Parser;
use futures_util::StreamExt;
use monad_compressible::Compressible;
use monad_crypto::certificate_signature::{
    CertificateKeyPair, CertificateSignature, CertificateSignaturePubKey,
};
use monad_executor::Executor;
use monad_executor_glue::{Message, RouterCommand};
use monad_raptorcast::{RaptorCast, RaptorCastConfig};
use monad_secp::SecpSignature;
use monad_types::{Deserializable, Epoch, NodeId, RouterTarget, Serializable, Stake};
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

    service(num_rt, threads_per_rt, args.addresses, 1, payload_size);
}

type SignatureType = SecpSignature;
type PubKeyType = CertificateSignaturePubKey<SignatureType>;

fn service(
    num_rt: usize,
    threads_per_rt: usize,
    addresses: Vec<String>,
    num_broadcast: u32,
    message_len: usize,
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
                let service_config = RaptorCastConfig {
                    key,
                    known_addresses,
                    redundancy: 2,
                    local_addr: server_address.to_string(),
                };

                let mut service =
                    RaptorCast::<SignatureType, MockMessage, MockMessage>::new(service_config);
                service.exec(vec![RouterCommand::AddEpochValidatorSet {
                    epoch: Epoch(0),
                    validator_set: all_peers.iter().map(|peer| (*peer, Stake(0))).collect(),
                }]);
                loop {
                    tokio::select! {
                        maybe_message = service.next() => {
                            let message = maybe_message.expect("never terminates");
                            rx_writer
                                .send((me, message))
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

    std::thread::sleep(Duration::from_secs(1));

    let start = Instant::now();
    let mut expected_message_ids = HashMap::new();
    for broadcast_id in 0..num_broadcast {
        let message = MockMessage::new(broadcast_id, message_len);
        tx_router
            .send(RouterCommand::Publish {
                target: RouterTarget::Broadcast(Epoch(0)),
                message,
            })
            .expect("reader should never be dropped");
        expected_message_ids.insert(message.id, num_peers);
    }
    while let Ok((_, (tx, msg_id))) = rx_reader.recv_timeout(Duration::from_secs(100)) {
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

impl Compressible for MockMessage {
    type Output = MockMessage;

    fn compress(&self) -> Self::Output {
        todo!()
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
