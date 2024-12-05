use std::{
    collections::{BTreeMap, HashMap},
    net::{SocketAddr, UdpSocket},
    num::ParseIntError,
    sync::Once,
    time::Duration,
};

use bytes::{Bytes, BytesMut};
use futures_util::StreamExt;
use monad_crypto::certificate_signature::{
    CertificateKeyPair, CertificateSignature, CertificateSignaturePubKey, PubKey,
};
use monad_executor::Executor;
use monad_executor_glue::{Message, RouterCommand};
use monad_raptor::SOURCE_SYMBOLS_MAX;
use monad_raptorcast::{
    udp::{build_messages, build_messages_with_length},
    util::{BuildTarget, EpochValidators, FullNodes, Validator},
    RaptorCast, RaptorCastConfig, RaptorCastEvent,
};
use monad_secp::{KeyPair, SecpSignature};
use monad_types::{Deserializable, Epoch, NodeId, Serializable, Stake};
use tracing_subscriber::fmt::format::FmtSpan;

type SignatureType = SecpSignature;
type PubKeyType = CertificateSignaturePubKey<SignatureType>;

// Try to crash the R10 managed decoder by feeding it encoded symbols of different sizes.
// A previous version of the R10 managed decoder did not handle this correctly and would panic.
#[test]
pub fn different_symbol_sizes() {
    let rx_addr = "127.0.0.1:10000".parse().unwrap();
    let tx_addr = "127.0.0.1:10001".parse().unwrap();

    let (rx_nodeid, tx_nodeid, tx_keypair, known_addresses) = set_up_test(&rx_addr, &tx_addr);

    let message: Bytes = vec![0; 100 * 1000].into();

    let tx_socket = UdpSocket::bind(tx_addr).unwrap();

    // Generate differently-sized encoded symbols that look like they are part of the same
    // message.  For the RaptorCast receiver to think that they are part of the same message,
    // they should have the same:
    // - unix_ts_ms: we use 0 for both messages;
    // - author: we use the same tx keypair/nodeid for both messages;
    // - app_message_{hash,len}: we use identical message bodies for the two messages.
    for i in 0..=1 {
        let gso_size = match i {
            0 => 1460,
            1 => 1480,
            _ => panic!(),
        };

        let mut validators = EpochValidators {
            validators: BTreeMap::from([
                (rx_nodeid, Validator { stake: Stake(1) }),
                (tx_nodeid, Validator { stake: Stake(1) }),
            ]),
        };

        let epoch_validators = validators.view_without(vec![&tx_nodeid]);
        let full_nodes = FullNodes::new(Vec::new());

        let messages = build_messages::<SignatureType>(
            &tx_keypair,
            gso_size,
            message.clone(),
            2, // redundancy,
            0, // epoch_no
            0, // unix_ts_ms
            BuildTarget::Raptorcast((epoch_validators, full_nodes.view())),
            &known_addresses,
        );

        // Send only the first symbol of the first message, and send all of the symbols
        // in the second message.
        if i == 0 {
            tx_socket
                .send_to(&messages[0].1[0..usize::from(gso_size)], messages[0].0)
                .unwrap();
        } else {
            for message in messages {
                for chunk in message.1.chunks(usize::from(gso_size)) {
                    tx_socket.send_to(chunk, message.0).unwrap();
                }
            }
        }
    }

    // Wait for RaptorCast instance to catch up.
    std::thread::sleep(Duration::from_millis(100));
}

// Try to crash the R10 decoder by feeding it more than 2^16 encoded symbols.
// A previous version of the R10 managed decoder allowed the same symbol to be passed
// multiple times to the underlying R10 decoder, which could exhaust the maximum number
// of buffer indices in the decoder and panic the decoder.
#[test]
pub fn buffer_count_overflow() {
    let rx_addr = "127.0.0.1:10002".parse().unwrap();
    let tx_addr = "127.0.0.1:10003".parse().unwrap();

    let (rx_nodeid, tx_nodeid, tx_keypair, known_addresses) = set_up_test(&rx_addr, &tx_addr);

    let message: Bytes = vec![0; 4 * 1000].into();

    let tx_socket = UdpSocket::bind(tx_addr).unwrap();

    let gso_size = 1460;

    let mut validators = EpochValidators {
        validators: BTreeMap::from([
            (rx_nodeid, Validator { stake: Stake(1) }),
            (tx_nodeid, Validator { stake: Stake(1) }),
        ]),
    };

    let epoch_validators = validators.view_without(vec![&tx_nodeid]);
    let full_nodes = FullNodes::new(Vec::new());

    let messages = build_messages::<SignatureType>(
        &tx_keypair,
        gso_size,
        message,
        2, // redundancy,
        0, // epoch_no
        0, // unix_ts_ms
        BuildTarget::Raptorcast((epoch_validators, full_nodes.view())),
        &known_addresses,
    );

    // Send 70_000 copies of the first symbol of the first message, which will overflow
    // the buffer array in the decoder unless it implements replay protection.
    for _ in 0..1000 {
        for _ in 0..70 {
            tx_socket
                .send_to(&messages[0].1[0..usize::from(gso_size)], messages[0].0)
                .unwrap();
        }

        std::thread::sleep(Duration::from_millis(1));
    }

    // Wait for RaptorCast instance to catch up.
    std::thread::sleep(Duration::from_millis(100));
}

// Try to crash the RaptorCast receive path by feeding it (part of) an oversized encoded
// message.  A previous version of the RaptorCast receive path would unwrap() an Err when
// it would receive an invalid (e.g. oversized) message for which ManagedDecoder::new()
// would fail.
#[test]
pub fn oversized_message() {
    let rx_addr = "127.0.0.1:10004".parse().unwrap();
    let tx_addr = "127.0.0.1:10005".parse().unwrap();

    let (rx_nodeid, tx_nodeid, tx_keypair, known_addresses) = set_up_test(&rx_addr, &tx_addr);

    let message: Bytes = vec![0; 4 * 1000].into();

    let tx_socket = UdpSocket::bind(tx_addr).unwrap();

    let gso_size = 1460;

    let mut validators = EpochValidators {
        validators: BTreeMap::from([
            (rx_nodeid, Validator { stake: Stake(1) }),
            (tx_nodeid, Validator { stake: Stake(1) }),
        ]),
    };

    let epoch_validators = validators.view_without(vec![&tx_nodeid]);
    let full_nodes = FullNodes::new(Vec::new());

    let messages = build_messages_with_length::<SignatureType>(
        &tx_keypair,
        gso_size,
        message,
        ((SOURCE_SYMBOLS_MAX + 1) * usize::from(gso_size))
            .try_into()
            .unwrap(),
        2, // redundancy,
        0, // epoch_no
        0, // unix_ts_ms
        BuildTarget::Raptorcast((epoch_validators, full_nodes.view())),
        &known_addresses,
    );

    // Sending a single packet of an oversized message is sufficient to crash the
    // receiver if it is vulnerable to this issue.
    tx_socket
        .send_to(&messages[0].1[0..usize::from(gso_size)], messages[0].0)
        .unwrap();

    // Wait for RaptorCast instance to catch up.
    std::thread::sleep(Duration::from_millis(100));
}

// Try to crash RaptorCast receive path by feeding it a zero-sized packet. A previous
// version of the RaptorCast receive path would crash via handle_message() when receiving
// a zero-sized packet due to being invoked with message.payload.len() == message.stride == 0
// which would then call .step_by(0) on (0..0), which panics.
#[test]
pub fn zero_sized_packet() {
    let rx_addr = "127.0.0.1:10006".parse().unwrap();
    let tx_addr = "127.0.0.1:10007".parse().unwrap();

    let (_rx_nodeid, _tx_nodeid, _tx_keypair, _known_addresses) = set_up_test(&rx_addr, &tx_addr);

    let message = [0; 10];

    let tx_socket = UdpSocket::bind(tx_addr).unwrap();

    // Sending a single zero-sized packet is sufficient to crash the receiver
    // if it is vulnerable to this issue.
    tx_socket.send_to(&message[0..0], rx_addr).unwrap();

    // Wait for RaptorCast instance to catch up.
    std::thread::sleep(Duration::from_millis(100));
}

static ONCE_SETUP: Once = Once::new();

pub fn set_up_test(
    rx_addr: &SocketAddr,
    tx_addr: &SocketAddr,
) -> (
    NodeId<PubKeyType>,
    NodeId<PubKeyType>,
    KeyPair,
    HashMap<NodeId<PubKeyType>, SocketAddr>,
) {
    ONCE_SETUP.call_once(|| {
        tracing_subscriber::fmt::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_span_events(FmtSpan::CLOSE)
            .init();

        // Cause the test to fail if any of the tokio runtime threads panic.  Taken from:
        // https://stackoverflow.com/questions/35988775/how-can-i-cause-a-panic-on-a-thread-to-immediately-end-the-main-thread/36031130#36031130
        let orig_panic_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |panic_info| {
            orig_panic_hook(panic_info);
            std::process::exit(1);
        }));
    });

    let rx_keypair = {
        <<SignatureType as CertificateSignature>::KeyPairType as CertificateKeyPair>::from_bytes(
            &mut [2; 32],
        )
        .unwrap()
    };
    let rx_nodeid = NodeId::new(rx_keypair.pubkey());

    let tx_keypair = {
        <<SignatureType as CertificateSignature>::KeyPairType as CertificateKeyPair>::from_bytes(
            &mut [1; 32],
        )
        .unwrap()
    };
    let tx_nodeid = NodeId::new(tx_keypair.pubkey());

    let known_addresses: HashMap<NodeId<PubKeyType>, SocketAddr> =
        HashMap::from([(rx_nodeid, *rx_addr), (tx_nodeid, *tx_addr)]);

    let validator_set = vec![(rx_nodeid, Stake(1)), (tx_nodeid, Stake(1))];

    {
        let known_addresses = known_addresses.clone();
        let rx_addr = rx_addr.to_owned();

        // We want the runtime not to be destroyed after we exit this function.
        let rt = Box::leak(Box::new(
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap(),
        ));

        rt.spawn(async move {
            let service_config = RaptorCastConfig {
                key: rx_keypair,
                full_nodes: Default::default(),
                known_addresses,
                redundancy: 2,
                local_addr: rx_addr,
                up_bandwidth_mbps: 1_000,
                mtu: 1480,
            };

            let mut service = RaptorCast::<
                SignatureType,
                MockMessage,
                MockMessage,
                <MockMessage as Message>::Event,
            >::new(service_config);

            service.exec(vec![RouterCommand::AddEpochValidatorSet {
                epoch: Epoch(0),
                validator_set,
            }]);

            loop {
                let message = service.next().await.expect("never terminates");

                println!("received message: {:?}", message);
            }
        });
    }

    // Wait for RaptorCast instance to set itself up.
    std::thread::sleep(Duration::from_millis(100));

    (rx_nodeid, tx_nodeid, tx_keypair, known_addresses)
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
    type Event = MockEvent<Self::NodeIdPubKey>;

    fn event(self, from: NodeId<Self::NodeIdPubKey>) -> Self::Event {
        MockEvent((from, self.id))
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

#[derive(Clone, Copy, Debug)]
struct MockEvent<P: PubKey>((NodeId<P>, u32));

impl<P> From<RaptorCastEvent<MockEvent<P>, P>> for MockEvent<P>
where
    P: PubKey,
{
    fn from(value: RaptorCastEvent<MockEvent<P>, P>) -> Self {
        match value {
            RaptorCastEvent::Message(event) => event,
            RaptorCastEvent::PeerManagerResponse(_peer_manager_response) => {
                unimplemented!()
            }
        }
    }
}
