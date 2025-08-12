// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::{
    collections::{BTreeMap, HashMap},
    io::ErrorKind,
    net::{Ipv4Addr, SocketAddr, SocketAddrV4, UdpSocket},
    num::ParseIntError,
    sync::{Arc, Once},
    time::Duration,
};

use alloy_rlp::{RlpDecodable, RlpEncodable};
use bytes::{Bytes, BytesMut};
use futures_util::StreamExt;
use monad_crypto::certificate_signature::{
    CertificateKeyPair, CertificateSignature, CertificateSignaturePubKey,
    CertificateSignatureRecoverable, PubKey,
};
use monad_dataplane::{udp::DEFAULT_SEGMENT_SIZE, DataplaneBuilder};
use monad_executor::Executor;
use monad_executor_glue::{Message, RouterCommand};
use monad_peer_discovery::{
    discovery::{PeerDiscovery, PeerDiscoveryBuilder},
    driver::PeerDiscoveryDriver,
    MonadNameRecord, NameRecord, Port, PortTag,
};
use monad_raptor::SOURCE_SYMBOLS_MAX;
use monad_raptorcast::{
    config::{
        RaptorCastConfig, RaptorCastConfigPrimary, RaptorCastConfigSecondary,
        SecondaryRaptorCastModeConfig,
    },
    message::OutboundRouterMessage,
    new_defaulted_raptorcast_for_tests,
    udp::{build_messages, build_messages_with_length, MAX_REDUNDANCY},
    util::{BuildTarget, EpochValidators, FullNodes, Redundancy, Validator},
    RaptorCast, RaptorCastEvent,
};
use monad_secp::{KeyPair, SecpSignature};
use monad_types::{Deserializable, Epoch, NodeId, Round, RouterTarget, Serializable, Stake};
use rand_chacha::{rand_core::SeedableRng, ChaCha8Rng};
use serial_test::serial;
use tokio::{net::UdpSocket as TokioUdpSocket, time::timeout};
use tracing_subscriber::fmt::format::FmtSpan;

type SignatureType = SecpSignature;
type PubKeyType = CertificateSignaturePubKey<SignatureType>;

fn init_test_logger() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .try_init();
}

fn create_keypair(seed: u8) -> Arc<KeyPair> {
    Arc::new(
        <<SignatureType as CertificateSignature>::KeyPairType as CertificateKeyPair>::from_bytes(
            &mut [seed; 32],
        )
        .unwrap(),
    )
}

fn create_name_record(ip: Ipv4Addr, ports: Vec<Port>) -> NameRecord {
    let mut tcp_port = None;
    let mut udp_port = None;
    let mut direct_udp_port = None;

    for port in ports {
        match port.tag_enum() {
            Some(PortTag::TCP) => tcp_port = Some(port.port),
            Some(PortTag::UDP) => udp_port = Some(port.port),
            Some(PortTag::DirectUdp) => direct_udp_port = Some(port.port),
            _ => {}
        }
    }

    NameRecord {
        ip,
        tcp_port: tcp_port.unwrap_or(0),
        udp_port: udp_port.unwrap_or(0),
        direct_udp_port,
        capabilities: 0,
        seq: 0,
    }
}

fn create_peer_discovery_builder<ST: CertificateSignatureRecoverable>(
    self_id: NodeId<CertificateSignaturePubKey<ST>>,
    self_record: MonadNameRecord<ST>,
    routing_info: BTreeMap<NodeId<CertificateSignaturePubKey<ST>>, MonadNameRecord<ST>>,
) -> PeerDiscoveryBuilder<ST> {
    PeerDiscoveryBuilder {
        self_id,
        self_record,
        current_round: Round(0),
        current_epoch: Epoch(0),
        epoch_validators: std::collections::BTreeMap::new(),
        pinned_full_nodes: std::collections::BTreeSet::new(),
        routing_info,
        ping_period: Duration::from_millis(1000),
        refresh_period: Duration::from_millis(5000),
        request_timeout: Duration::from_millis(500),
        unresponsive_prune_threshold: 5,
        last_participation_prune_threshold: Round(100),
        min_num_peers: 3,
        max_num_peers: 50,
        rng: ChaCha8Rng::from_seed([0u8; 32]),
    }
}

fn create_raptorcast_config(shared_key: Arc<KeyPair>) -> RaptorCastConfig<SignatureType> {
    RaptorCastConfig {
        shared_key,
        mtu: 1450,
        udp_message_max_age_ms: 5000,
        primary_instance: RaptorCastConfigPrimary::default(),
        secondary_instance: RaptorCastConfigSecondary {
            raptor10_redundancy: 2,
            mode: SecondaryRaptorCastModeConfig::None,
        },
    }
}

// Try to crash the R10 managed decoder by feeding it encoded symbols of different sizes.
// A previous version of the R10 managed decoder did not handle this correctly and would panic.
#[test]
pub fn different_symbol_sizes() {
    let tx_addr = "127.0.0.1:10000".parse().unwrap();
    let rx_addr = "127.0.0.1:10001".parse().unwrap();
    let rebroadcast_addr = "127.0.0.1:10002".parse().unwrap();

    let (tx_nodeid, tx_keypair, rx_nodeid, known_addresses) =
        set_up_test(&tx_addr, &rx_addr, Some(&rebroadcast_addr));

    let message: Bytes = vec![0; 100 * 1000].into();

    let tx_socket = UdpSocket::bind(tx_addr).unwrap();

    let rebroadcast_socket = UdpSocket::bind(rebroadcast_addr).unwrap();
    rebroadcast_socket
        .set_read_timeout(Some(Duration::from_millis(100)))
        .unwrap();

    // Generate differently-sized encoded symbols that look like they are part of the same
    // message.  For the RaptorCast receiver to think that they are part of the same message,
    // they should have the same:
    // - unix_ts_ms: we use 0 for both messages;
    // - author: we use the same tx keypair/nodeid for both messages;
    // - app_message_{hash,len}: we use identical message bodies for the two messages.
    for i in 0..=1 {
        let segment_size = match i {
            0 => DEFAULT_SEGMENT_SIZE - 20,
            1 => DEFAULT_SEGMENT_SIZE,
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
            segment_size,
            message.clone(),
            Redundancy::from_u8(2),
            0, // epoch_no
            0, // unix_ts_ms
            BuildTarget::Raptorcast((epoch_validators, full_nodes.view())),
            &known_addresses,
        );

        // Send only the first symbol of the first message, and send all of the symbols
        // in the second message.
        if i == 0 {
            tx_socket
                .send_to(&messages[0].1[0..usize::from(segment_size)], messages[0].0)
                .unwrap();
        } else {
            for message in messages {
                for chunk in message.1.chunks(usize::from(segment_size)) {
                    tx_socket.send_to(chunk, message.0).unwrap();
                }
            }
        }
    }

    // Wait for RaptorCast instance to catch up.
    std::thread::sleep(Duration::from_millis(100));

    // Verify that the rebroadcast target receives the first symbol.
    let _ = rebroadcast_socket.recv(&mut []).unwrap();

    // Verify that the rebroadcast target never receives another symbol of different length.
    assert_eq!(
        rebroadcast_socket.recv(&mut []).unwrap_err().kind(),
        ErrorKind::WouldBlock
    );
}

// Try to crash the R10 decoder by feeding it more than 2^16 encoded symbols.
// A previous version of the R10 managed decoder allowed the same symbol to be passed
// multiple times to the underlying R10 decoder, which could exhaust the maximum number
// of buffer indices in the decoder and panic the decoder.
#[test]
pub fn buffer_count_overflow() {
    let tx_addr = "127.0.0.1:10003".parse().unwrap();
    let rx_addr = "127.0.0.1:10004".parse().unwrap();

    let (tx_nodeid, tx_keypair, rx_nodeid, known_addresses) = set_up_test(&tx_addr, &rx_addr, None);

    let message: Bytes = vec![0; 4 * 1000].into();

    let tx_socket = UdpSocket::bind(tx_addr).unwrap();

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
        DEFAULT_SEGMENT_SIZE,
        message,
        Redundancy::from_u8(2),
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
                .send_to(
                    &messages[0].1[0..usize::from(DEFAULT_SEGMENT_SIZE)],
                    messages[0].0,
                )
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
    let tx_addr = "127.0.0.1:10005".parse().unwrap();
    let rx_addr = "127.0.0.1:10006".parse().unwrap();

    let (tx_nodeid, tx_keypair, rx_nodeid, known_addresses) = set_up_test(&tx_addr, &rx_addr, None);

    let message: Bytes = vec![0; 4 * 1000].into();

    let tx_socket = UdpSocket::bind(tx_addr).unwrap();

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
        DEFAULT_SEGMENT_SIZE,
        message,
        ((SOURCE_SYMBOLS_MAX + 1) * usize::from(DEFAULT_SEGMENT_SIZE))
            .try_into()
            .unwrap(),
        Redundancy::from_u8(2),
        0, // epoch_no
        0, // unix_ts_ms
        BuildTarget::Raptorcast((epoch_validators, full_nodes.view())),
        &known_addresses,
    );

    // Sending a single packet of an oversized message is sufficient to crash the
    // receiver if it is vulnerable to this issue.
    tx_socket
        .send_to(
            &messages[0].1[0..usize::from(DEFAULT_SEGMENT_SIZE)],
            messages[0].0,
        )
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
    let tx_addr = "127.0.0.1:10007".parse().unwrap();
    let rx_addr = "127.0.0.1:10008".parse().unwrap();

    let (_tx_nodeid, _tx_keypair, _rx_nodeid, _known_addresses) =
        set_up_test(&tx_addr, &rx_addr, None);

    let message = [0; 10];

    let tx_socket = UdpSocket::bind(tx_addr).unwrap();

    // Sending a single zero-sized packet is sufficient to crash the receiver
    // if it is vulnerable to this issue.
    tx_socket.send_to(&message[0..0], rx_addr).unwrap();

    // Wait for RaptorCast instance to catch up.
    std::thread::sleep(Duration::from_millis(100));
}

// Verify that all received encoded symbols that are valid are rebroadcast
// exactly once.
#[test]
pub fn valid_rebroadcast() {
    let tx_addr = "127.0.0.1:10009".parse().unwrap();
    let rx_addr = "127.0.0.1:10010".parse().unwrap();
    let rebroadcast_addr = "127.0.0.1:10011".parse().unwrap();

    let (tx_nodeid, tx_keypair, rx_nodeid, known_addresses) =
        set_up_test(&tx_addr, &rx_addr, Some(&rebroadcast_addr));

    let message: Bytes = vec![0; 4 * 1000].into();

    let tx_socket = UdpSocket::bind(tx_addr).unwrap();

    let rebroadcast_socket = UdpSocket::bind(rebroadcast_addr).unwrap();
    rebroadcast_socket
        .set_read_timeout(Some(Duration::from_millis(100)))
        .unwrap();

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
        DEFAULT_SEGMENT_SIZE,
        message,
        MAX_REDUNDANCY, // redundancy,
        0,              // epoch_no
        0,              // unix_ts_ms
        BuildTarget::Raptorcast((epoch_validators, full_nodes.view())),
        &known_addresses,
    );

    for i in 0..=1 {
        let mut num_chunks = 0;
        for message in &messages {
            for chunk in message.1.chunks(usize::from(DEFAULT_SEGMENT_SIZE)) {
                tx_socket.send_to(chunk, message.0).unwrap();

                num_chunks += 1;
            }
        }

        // Wait for all rebroadcasting activity to complete.
        std::thread::sleep(Duration::from_millis(100));

        if i == 0 {
            for _ in 0..num_chunks {
                // Verify that the rebroadcast target receives a copy of every symbol.
                let _ = rebroadcast_socket.recv(&mut []).unwrap();
            }
        } else {
            // Verify that the rebroadcast target has nothing more to receive.
            assert_eq!(
                rebroadcast_socket.recv(&mut []).unwrap_err().kind(),
                ErrorKind::WouldBlock
            );
        }
    }
}

static ONCE_SETUP: Once = Once::new();

#[cfg(test)]
pub fn set_up_test(
    tx_addr: &SocketAddr,
    rx_addr: &SocketAddr,
    rebroadcast_addr: Option<&SocketAddr>,
) -> (
    NodeId<PubKeyType>,
    KeyPair,
    NodeId<PubKeyType>,
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

    let tx_keypair = {
        <<SignatureType as CertificateSignature>::KeyPairType as CertificateKeyPair>::from_bytes(
            &mut [1; 32],
        )
        .unwrap()
    };
    let tx_nodeid = NodeId::new(tx_keypair.pubkey());

    let rx_keypair = {
        <<SignatureType as CertificateSignature>::KeyPairType as CertificateKeyPair>::from_bytes(
            &mut [2; 32],
        )
        .unwrap()
    };
    let rx_nodeid = NodeId::new(rx_keypair.pubkey());

    let mut known_addresses: HashMap<NodeId<PubKeyType>, SocketAddr> =
        HashMap::from([(tx_nodeid, *tx_addr), (rx_nodeid, *rx_addr)]);

    let mut validator_set = vec![(tx_nodeid, Stake(1)), (rx_nodeid, Stake(1))];

    if let Some(rebroadcast_addr) = rebroadcast_addr {
        let rebroadcast_keypair = {
            <<SignatureType as CertificateSignature>::KeyPairType as CertificateKeyPair>::from_bytes(
                &mut [3; 32],
            )
            .unwrap()
        };
        let rebroadcast_nodeid = NodeId::new(rebroadcast_keypair.pubkey());

        known_addresses.insert(rebroadcast_nodeid, *rebroadcast_addr);

        validator_set.push((rebroadcast_nodeid, Stake(1)));
    }

    {
        let peer_addresses: HashMap<NodeId<PubKeyType>, SocketAddrV4> = known_addresses
            .clone()
            .into_iter()
            .map(|(id, addr)| {
                let addr = match addr {
                    SocketAddr::V4(addr) => addr,
                    SocketAddr::V6(_) => panic!("IPv6 addresses not supported"),
                };
                (id, addr)
            })
            .collect();
        let rx_addr = rx_addr.to_owned();

        // We want the runtime not to be destroyed after we exit this function.
        let rt = Box::leak(Box::new(
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap(),
        ));

        rt.spawn(async move {
            let mut service = new_defaulted_raptorcast_for_tests::<
                SignatureType,
                MockMessage,
                MockMessage,
                <MockMessage as Message>::Event,
            >(rx_addr, peer_addresses, Arc::new(rx_keypair));

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

    (tx_nodeid, tx_keypair, rx_nodeid, known_addresses)
}

#[derive(Clone, Copy, RlpEncodable, RlpDecodable)]
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

impl<ST> From<RaptorCastEvent<MockEvent<CertificateSignaturePubKey<ST>>, ST>>
    for MockEvent<CertificateSignaturePubKey<ST>>
where
    ST: CertificateSignatureRecoverable,
{
    fn from(value: RaptorCastEvent<MockEvent<CertificateSignaturePubKey<ST>>, ST>) -> Self {
        match value {
            RaptorCastEvent::Message(event) => event,
            RaptorCastEvent::PeerManagerResponse(_peer_manager_response) => {
                unimplemented!()
            }
        }
    }
}

#[tokio::test]
#[serial]
async fn test_direct_udp_support() {
    init_test_logger();

    let tx_addr: SocketAddr = "127.0.0.1:10020".parse().unwrap();
    let direct_udp_port = 11021u16;

    let direct_udp_socket = TokioUdpSocket::bind(("127.0.0.1", direct_udp_port))
        .await
        .unwrap();

    let non_direct_udp_socket = TokioUdpSocket::bind(("127.0.0.1", 10022)).await.unwrap();

    let tx_keypair = create_keypair(50);
    let rx_with_direct_keypair = create_keypair(51);
    let rx_no_direct_keypair = create_keypair(52);

    let tx_nodeid = NodeId::new(tx_keypair.pubkey());
    let rx_with_direct_nodeid = NodeId::new(rx_with_direct_keypair.pubkey());
    let rx_no_direct_nodeid = NodeId::new(rx_no_direct_keypair.pubkey());

    let tx_name_record = create_name_record(
        Ipv4Addr::new(127, 0, 0, 1),
        vec![
            Port::new(PortTag::TCP, 10020),
            Port::new(PortTag::UDP, 10020),
        ],
    );

    let rx_with_direct_name_record = create_name_record(
        Ipv4Addr::new(127, 0, 0, 1),
        vec![
            Port::new(PortTag::TCP, 10021),
            Port::new(PortTag::UDP, 10021),
            Port::new(PortTag::DirectUdp, direct_udp_port),
        ],
    );

    let rx_no_direct_name_record = create_name_record(
        Ipv4Addr::new(127, 0, 0, 1),
        vec![
            Port::new(PortTag::TCP, 10022),
            Port::new(PortTag::UDP, 10022),
        ],
    );

    let mut routing_info = std::collections::BTreeMap::new();
    routing_info.insert(
        tx_nodeid,
        MonadNameRecord::new(tx_name_record.clone(), &*tx_keypair),
    );
    routing_info.insert(
        rx_with_direct_nodeid,
        MonadNameRecord::new(rx_with_direct_name_record.clone(), &*rx_with_direct_keypair),
    );
    routing_info.insert(
        rx_no_direct_nodeid,
        MonadNameRecord::new(rx_no_direct_name_record.clone(), &*rx_no_direct_keypair),
    );

    let self_record = MonadNameRecord::new(tx_name_record, &*tx_keypair);

    let up_bandwidth_mbps = 1_000;
    let dp_builder = DataplaneBuilder::new(&tx_addr, up_bandwidth_mbps);
    let dataplane = dp_builder.build();
    let (dataplane_reader, dataplane_writer) = dataplane.split();

    let peer_discovery_builder =
        create_peer_discovery_builder(tx_nodeid.clone(), self_record, routing_info);

    let config = create_raptorcast_config(tx_keypair.clone());

    let pd = PeerDiscoveryDriver::new(peer_discovery_builder);
    let shared_pd = Arc::new(std::sync::Mutex::new(pd));

    let mut tx_service = RaptorCast::<
        SignatureType,
        MockMessage,
        MockMessage,
        <MockMessage as Message>::Event,
        PeerDiscovery<SignatureType>,
    >::new(config, dataplane_reader, dataplane_writer, shared_pd);

    tx_service.exec(vec![RouterCommand::AddEpochValidatorSet {
        epoch: Epoch(0),
        validator_set: vec![
            (tx_nodeid, Stake(1)),
            (rx_with_direct_nodeid, Stake(1)),
            (rx_no_direct_nodeid, Stake(1)),
        ],
    }]);

    let message_to_direct = MockMessage::new(1, 100);
    let message_to_no_direct = MockMessage::new(2, 100);

    tx_service.exec(vec![RouterCommand::Publish {
        target: RouterTarget::PointToPoint(rx_with_direct_nodeid),
        message: message_to_direct,
    }]);

    tx_service.exec(vec![RouterCommand::Publish {
        target: RouterTarget::PointToPoint(rx_no_direct_nodeid),
        message: message_to_no_direct,
    }]);

    let mut buf = [0u8; 2048];
    match timeout(
        Duration::from_secs(2),
        direct_udp_socket.recv_from(&mut buf),
    )
    .await
    {
        Ok(Ok((_size, from))) => {
            assert_eq!(from, tx_addr);
        }
        Ok(Err(e)) => panic!("Error receiving on direct UDP socket: {}", e),
        Err(_) => {
            panic!("FAILED: Should receive data on direct UDP socket when name record has DirectUdp port");
        }
    }

    match timeout(
        Duration::from_secs(2),
        non_direct_udp_socket.recv_from(&mut buf),
    )
    .await
    {
        Ok(Ok((_size, from))) => {
            assert_eq!(from, tx_addr);
        }
        Ok(Err(e)) => panic!("Error receiving on non-direct UDP socket: {}", e),
        Err(_) => {
            panic!("FAILED: Should receive data for non-direct node on regular UDP socket");
        }
    }
}

#[tokio::test]
#[serial]
async fn test_direct_udp_data_reception() {
    init_test_logger();

    let rx_addr: SocketAddr = "127.0.0.1:10030".parse().unwrap();
    let direct_udp_port = 11030u16;

    let tx_keypair = create_keypair(60);
    let rx_keypair = create_keypair(61);

    let tx_nodeid = NodeId::new(tx_keypair.pubkey());
    let rx_nodeid = NodeId::new(rx_keypair.pubkey());

    let rx_name_record = create_name_record(
        Ipv4Addr::new(127, 0, 0, 1),
        vec![
            Port::new(PortTag::TCP, rx_addr.port()),
            Port::new(PortTag::UDP, rx_addr.port()),
            Port::new(PortTag::DirectUdp, direct_udp_port),
        ],
    );

    let rx_name_record_clone = rx_name_record.clone();
    let rx_keypair_clone = rx_keypair.clone();

    let routing_info = std::collections::BTreeMap::new();
    let self_record = MonadNameRecord::new(rx_name_record_clone.clone(), &*rx_keypair_clone);

    let up_bandwidth_mbps = 1_000;
    let mut dp_builder = DataplaneBuilder::new(&rx_addr, up_bandwidth_mbps);
    dp_builder = dp_builder.with_direct_socket(direct_udp_port);
    let dataplane = dp_builder.build();
    assert!(dataplane.block_until_ready(Duration::from_secs(1)));
    let (dataplane_reader, dataplane_writer) = dataplane.split();
    let peer_discovery_builder =
        create_peer_discovery_builder(rx_nodeid.clone(), self_record, routing_info);

    let config = create_raptorcast_config(rx_keypair_clone.clone());

    let pd = PeerDiscoveryDriver::new(peer_discovery_builder);
    let shared_pd = Arc::new(std::sync::Mutex::new(pd));

    let mut rx_service = RaptorCast::<
        SignatureType,
        MockMessage,
        MockMessage,
        <MockMessage as Message>::Event,
        PeerDiscovery<SignatureType>,
    >::new(config, dataplane_reader, dataplane_writer, shared_pd);

    let external_tx_socket = TokioUdpSocket::bind("127.0.0.1:10032").await.unwrap();

    let test_message = MockMessage::new(42, 200);
    let outbound_msg =
        OutboundRouterMessage::<MockMessage, SignatureType>::AppMessage(test_message);
    let test_message_bytes = match outbound_msg.try_serialize() {
        Ok(bytes) => bytes,
        Err(e) => panic!("Failed to serialize message: {:?}", e),
    };

    let mut direct_known_addresses = HashMap::new();
    direct_known_addresses.insert(
        rx_nodeid,
        SocketAddr::V4(rx_name_record.direct_udp_socket().unwrap()),
    );

    let messages = build_messages::<SignatureType>(
        &tx_keypair,
        DEFAULT_SEGMENT_SIZE,
        test_message_bytes,
        Redundancy::from_u8(2),
        0,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64,
        BuildTarget::PointToPoint(&rx_nodeid),
        &direct_known_addresses,
    );

    for (_, (dest, payload)) in messages.iter().enumerate() {
        for chunk in payload.chunks(usize::from(DEFAULT_SEGMENT_SIZE)) {
            external_tx_socket.send_to(chunk, dest).await.unwrap();
        }
    }

    match timeout(Duration::from_secs(2), rx_service.next()).await {
        Ok(Some(event)) => {
            let MockEvent((from, msg_id)) = event;
            assert_eq!(from, tx_nodeid);
            assert_eq!(msg_id, 42);
        }
        Ok(None) => panic!("terminated unexpectedly"),
        Err(_) => panic!("timed out"),
    }
}
