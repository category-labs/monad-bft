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
    net::{SocketAddr, SocketAddrV4, UdpSocket},
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
use monad_dataplane::udp::DEFAULT_SEGMENT_SIZE;
use monad_executor::Executor;
use monad_executor_glue::{Message, OutboundForwardTxs, RouterCommand};
use monad_peer_discovery::mock::NopDiscovery;
use monad_raptorcast::{
    create_dataplane_for_tests, new_defaulted_raptorcast_for_tests,
    packet::build_messages,
    raptorcast_secondary::{group_message::FullNodesGroupMessage, SecondaryOutboundMessage},
    udp::{GroupId, MAX_REDUNDANCY},
    util::{BuildTarget, EpochValidators, Group, Redundancy},
    DataplaneHandles, RaptorCast, RaptorCastEvent,
};
use monad_secp::{KeyPair, SecpSignature};
use monad_types::{
    Deserializable, Epoch, NodeId, Round, RoundSpan, Serializable, Stake, UdpPriority,
};
use monad_validator::validator_set::ValidatorSet;
use tokio::sync::mpsc::unbounded_channel;
use tracing_subscriber::fmt::format::FmtSpan;

type SignatureType = SecpSignature;
type PubKeyType = CertificateSignaturePubKey<SignatureType>;

// Try to crash the R10 managed decoder by feeding it encoded symbols of different sizes.
// A previous version of the R10 managed decoder did not handle this correctly and would panic.
#[test]
pub fn different_symbol_sizes() {
    let rebroadcast_socket = UdpSocket::bind("127.0.0.1:0").unwrap();
    let rebroadcast_addr = rebroadcast_socket.local_addr().unwrap();

    let (tx_nodeid, tx_keypair, rx_nodeid, known_addresses, _rx_addr) =
        set_up_test(Some(rebroadcast_addr));

    let message: Bytes = vec![0; 100 * 1000].into();

    let tx_socket = UdpSocket::bind("127.0.0.1:0").unwrap();
    rebroadcast_socket
        .set_read_timeout(Some(Duration::from_millis(200)))
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

        let valset = BTreeMap::from([(rx_nodeid, Stake::ONE), (tx_nodeid, Stake::ONE)]);
        let validators = EpochValidators {
            validators: ValidatorSet::new_unchecked(valset),
        };

        let epoch_validators = validators.view_without(vec![&tx_nodeid]);

        let messages = build_messages::<SignatureType>(
            &tx_keypair,
            segment_size,
            message.clone(),
            Redundancy::from_u8(2),
            GroupId::Primary(Epoch(0)), // epoch_no
            0,                          // unix_ts_ms
            BuildTarget::Raptorcast(epoch_validators),
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
    std::thread::sleep(Duration::from_millis(200));

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
    let (tx_nodeid, tx_keypair, rx_nodeid, known_addresses, _rx_addr) = set_up_test(None);

    let message: Bytes = vec![0; 4 * 1000].into();

    let tx_socket = UdpSocket::bind("127.0.0.1:0").unwrap();

    let valset = BTreeMap::from([(rx_nodeid, Stake::ONE), (tx_nodeid, Stake::ONE)]);
    let validators = EpochValidators {
        validators: ValidatorSet::new_unchecked(valset),
    };

    let epoch_validators = validators.view_without(vec![&tx_nodeid]);

    let messages = build_messages::<SignatureType>(
        &tx_keypair,
        DEFAULT_SEGMENT_SIZE,
        message,
        Redundancy::from_u8(2),
        GroupId::Primary(Epoch(0)), // epoch_no
        0,                          // unix_ts_ms
        BuildTarget::Raptorcast(epoch_validators),
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

// Try to crash RaptorCast receive path by feeding it a zero-sized packet. A previous
// version of the RaptorCast receive path would crash via handle_message() when receiving
// a zero-sized packet due to being invoked with message.payload.len() == message.stride == 0
// which would then call .step_by(0) on (0..0), which panics.
#[test]
pub fn zero_sized_packet() {
    let (_tx_nodeid, _tx_keypair, _rx_nodeid, _known_addresses, rx_addr) = set_up_test(None);

    let message = [0; 10];

    let tx_socket = UdpSocket::bind("127.0.0.1:0").unwrap();

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
    let rebroadcast_socket = UdpSocket::bind("127.0.0.1:0").unwrap();
    let rebroadcast_addr = rebroadcast_socket.local_addr().unwrap();

    let (tx_nodeid, tx_keypair, rx_nodeid, known_addresses, _rx_addr) =
        set_up_test(Some(rebroadcast_addr));

    let message: Bytes = vec![0; 4 * 1000].into();

    let tx_socket = UdpSocket::bind("127.0.0.1:0").unwrap();
    rebroadcast_socket
        .set_read_timeout(Some(Duration::from_millis(100)))
        .unwrap();

    let valset = BTreeMap::from([(rx_nodeid, Stake::ONE), (tx_nodeid, Stake::ONE)]);
    let validators = EpochValidators {
        validators: ValidatorSet::new_unchecked(valset),
    };

    let epoch_validators = validators.view_without(vec![&tx_nodeid]);

    let messages = build_messages::<SignatureType>(
        &tx_keypair,
        DEFAULT_SEGMENT_SIZE,
        message,
        MAX_REDUNDANCY,             // redundancy,
        GroupId::Primary(Epoch(0)), // epoch_no
        0,                          // unix_ts_ms
        BuildTarget::Raptorcast(epoch_validators),
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
    rebroadcast_addr: Option<SocketAddr>,
) -> (
    NodeId<PubKeyType>,
    KeyPair,
    NodeId<PubKeyType>,
    HashMap<NodeId<PubKeyType>, SocketAddr>,
    SocketAddr, // rx_addr
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

    let dataplane = create_dataplane_for_tests(false);
    let rx_addr = SocketAddr::V4(dataplane.non_auth_addr);

    let tx_addr = UdpSocket::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap();

    let mut known_addresses: HashMap<NodeId<PubKeyType>, SocketAddr> =
        HashMap::from([(tx_nodeid, tx_addr), (rx_nodeid, rx_addr)]);

    let mut validator_set = vec![(tx_nodeid, Stake::ONE), (rx_nodeid, Stake::ONE)];

    if let Some(rebroadcast_addr) = rebroadcast_addr {
        let rebroadcast_keypair = {
            <<SignatureType as CertificateSignature>::KeyPairType as CertificateKeyPair>::from_bytes(
                &mut [3; 32],
            )
            .unwrap()
        };
        let rebroadcast_nodeid = NodeId::new(rebroadcast_keypair.pubkey());

        known_addresses.insert(rebroadcast_nodeid, rebroadcast_addr);

        validator_set.push((rebroadcast_nodeid, Stake::ONE));
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
            >(dataplane, peer_addresses, Arc::new(rx_keypair));

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

    (tx_nodeid, tx_keypair, rx_nodeid, known_addresses, rx_addr)
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

impl OutboundForwardTxs for MockMessage {
    fn forward_txs(_txs: Vec<Bytes>) -> Self {
        MockMessage::new(0, 0)
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
            RaptorCastEvent::SecondaryRaptorcastPeersUpdate { .. } => {
                unimplemented!()
            }
            RaptorCastEvent::LeanUdpTx { .. } => {
                unimplemented!()
            }
            RaptorCastEvent::LeanUdpForwardTxs { .. } => {
                unimplemented!()
            }
        }
    }
}

fn keypair(seed: u8) -> KeyPair {
    <<SignatureType as CertificateSignature>::KeyPairType as CertificateKeyPair>::from_bytes(
        &mut [seed; 32],
    )
    .unwrap()
}

type MockRaptorCast = RaptorCast<
    SignatureType,
    MockMessage,
    MockMessage,
    MockEvent<CertificateSignaturePubKey<SignatureType>>,
    NopDiscovery<SignatureType>,
    monad_raptorcast::auth::NoopAuthProtocol<CertificateSignaturePubKey<SignatureType>>,
    monad_raptorcast::auth::NopScore<NodeId<CertificateSignaturePubKey<SignatureType>>>,
>;

fn setup_raptorcast_service(
    keypair: KeyPair,
    dataplane: DataplaneHandles,
    known_addresses: HashMap<NodeId<PubKeyType>, SocketAddrV4>,
) -> MockRaptorCast {
    new_defaulted_raptorcast_for_tests::<
        SignatureType,
        MockMessage,
        MockMessage,
        <MockMessage as Message>::Event,
    >(dataplane, known_addresses, Arc::new(keypair))
}

#[cfg(test)]
#[tokio::test]
async fn publish_to_full_nodes() {
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

    // 1. Set up nodes
    let validator_keypair = keypair(1);
    let validator_nodeid = NodeId::new(validator_keypair.pubkey());

    let full_node1_keypair = keypair(2);
    let full_node1_id = NodeId::new(full_node1_keypair.pubkey());

    let full_node2_keypair = keypair(3);
    let full_node2_id = NodeId::new(full_node2_keypair.pubkey());

    let validator_set = vec![(validator_nodeid, Stake::ONE)];

    // 2. Create dataplanes first to get bound addresses
    let validator_dp = create_dataplane_for_tests(false);
    let full_node1_dp = create_dataplane_for_tests(false);
    let full_node2_dp = create_dataplane_for_tests(false);

    let known_addresses: HashMap<NodeId<PubKeyType>, SocketAddrV4> = [
        (validator_nodeid, validator_dp.non_auth_addr),
        (full_node1_id, full_node1_dp.non_auth_addr),
        (full_node2_id, full_node2_dp.non_auth_addr),
    ]
    .into_iter()
    .collect();

    // 3. Create services with known addresses
    let mut validator_rc =
        setup_raptorcast_service(validator_keypair, validator_dp, known_addresses.clone());
    validator_rc.set_dedicated_full_nodes(vec![full_node1_id, full_node2_id]);

    let mut full_node1_rc =
        setup_raptorcast_service(full_node1_keypair, full_node1_dp, known_addresses.clone());

    let mut full_node2_rc =
        setup_raptorcast_service(full_node2_keypair, full_node2_dp, known_addresses.clone());

    // 4. Set validator set for all nodes
    for service in [&mut validator_rc, &mut full_node1_rc, &mut full_node2_rc] {
        service.exec(vec![RouterCommand::AddEpochValidatorSet {
            epoch: Epoch(0),
            validator_set: validator_set.clone(),
        }]);
    }

    // 5. Exhaust any remaining events
    loop {
        tokio::select! {
            biased;
            _ = validator_rc.next() => {},
            _ = full_node1_rc.next() => {},
            _ = full_node2_rc.next() => {},
            _ = std::future::ready(()) => break,
        }
    }

    // 6. Publish message from validator to full nodes
    let message = MockMessage::new(42, 10000);
    let command = RouterCommand::PublishToFullNodes {
        epoch: Epoch(0),
        round: Round(0),
        message,
    };
    validator_rc.exec(vec![command]);

    // 7. Assert full nodes receive the message
    let timeout = Duration::from_secs(1);
    let event1 = tokio::time::timeout(timeout, full_node1_rc.next())
        .await
        .expect("timeout")
        .expect("stream ended");
    let MockEvent((from, id)) = event1;
    assert_eq!(from, validator_nodeid);
    assert_eq!(id, 42);

    let event2 = tokio::time::timeout(timeout, full_node2_rc.next())
        .await
        .expect("timeout")
        .expect("stream ended");
    let MockEvent((from, id)) = event2;
    assert_eq!(from, validator_nodeid);
    assert_eq!(id, 42);
}

#[cfg(test)]
#[tokio::test]
async fn delete_expired_groups() {
    let node_keypair = keypair(1);
    let node_id = NodeId::new(node_keypair.pubkey());

    let dataplane = create_dataplane_for_tests(false);
    let mut raptorcast = setup_raptorcast_service(node_keypair, dataplane, HashMap::new());
    raptorcast.exec(vec![RouterCommand::UpdateCurrentRound(Epoch(1), Round(1))]);

    // setup
    let (send_net_messages, _) = unbounded_channel::<FullNodesGroupMessage<SignatureType>>();
    let (send_group_infos, recv_group_infos) = unbounded_channel::<Group<PubKeyType>>();
    let (_, recv_outbound_from_secondary) =
        unbounded_channel::<SecondaryOutboundMessage<PubKeyType>>();
    raptorcast.set_is_dynamic_full_node(true);
    raptorcast.bind_channel_to_secondary_raptorcast(
        send_net_messages,
        recv_group_infos,
        recv_outbound_from_secondary,
    );

    // populate raptorcast group
    let group = Group::new_fullnode_group(
        vec![],
        &node_id,
        node_id,
        RoundSpan {
            start: Round(1),
            end: Round(10),
        },
    );
    send_group_infos.send(group).unwrap();

    loop {
        tokio::select! {
            biased;
            _ = raptorcast.next() => {},
            _ = std::future::ready(()) => break,
        }
    }

    let rebroadcast_map = raptorcast.get_rebroadcast_groups().get_fullnode_map();
    assert_eq!(
        rebroadcast_map.len(),
        1,
        "Expected one group in rebroadcast map"
    );

    // round increment beyond group end round
    raptorcast.exec(vec![RouterCommand::UpdateCurrentRound(Epoch(1), Round(11))]);
    let rebroadcast_map = raptorcast.get_rebroadcast_groups().get_fullnode_map();

    // expired group should be deleted
    assert!(rebroadcast_map.is_empty(), "Expected empty rebroadcast map");
}

#[tokio::test]
async fn test_priority_messages() {
    let mut tx_secret = [1u8; 32];
    let mut rx_secret = [2u8; 32];
    let tx_key = Arc::new(KeyPair::from_bytes(&mut tx_secret).unwrap());
    let rx_key = Arc::new(KeyPair::from_bytes(&mut rx_secret).unwrap());

    let tx_nodeid = NodeId::new(tx_key.pubkey());
    let rx_nodeid = NodeId::new(rx_key.pubkey());

    let tx_dataplane = create_dataplane_for_tests(false);
    let rx_dataplane = create_dataplane_for_tests(false);

    let known_addresses = HashMap::from([
        (tx_nodeid, tx_dataplane.non_auth_addr),
        (rx_nodeid, rx_dataplane.non_auth_addr),
    ]);

    let mut tx_rc = new_defaulted_raptorcast_for_tests::<
        SignatureType,
        MockMessage,
        MockMessage,
        MockEvent<PubKeyType>,
    >(tx_dataplane, known_addresses.clone(), tx_key.clone());

    let mut rx_rc = new_defaulted_raptorcast_for_tests::<
        SignatureType,
        MockMessage,
        MockMessage,
        MockEvent<PubKeyType>,
    >(rx_dataplane, known_addresses.clone(), rx_key.clone());

    let epoch = Epoch(0);
    let validator_set = vec![(tx_nodeid, Stake::ONE), (rx_nodeid, Stake::ONE)];

    tx_rc.exec(vec![RouterCommand::AddEpochValidatorSet {
        epoch,
        validator_set: validator_set.clone(),
    }]);

    rx_rc.exec(vec![RouterCommand::AddEpochValidatorSet {
        epoch,
        validator_set: validator_set.clone(),
    }]);

    const HIGH_PRIORITY_START: u32 = 1000;
    const REGULAR_PRIORITY_START: u32 = 2000;
    const MESSAGE_COUNT: usize = 100;
    const MESSAGE_SIZE: usize = 10000;

    for i in 0..MESSAGE_COUNT {
        let high_priority_msg = MockMessage::new(HIGH_PRIORITY_START + i as u32, MESSAGE_SIZE);
        tx_rc.exec(vec![RouterCommand::PublishWithPriority {
            target: monad_types::RouterTarget::PointToPoint(rx_nodeid),
            message: high_priority_msg,
            priority: UdpPriority::High,
        }]);
    }

    for i in 0..MESSAGE_COUNT {
        let regular_priority_msg =
            MockMessage::new(REGULAR_PRIORITY_START + i as u32, MESSAGE_SIZE);
        tx_rc.exec(vec![RouterCommand::PublishWithPriority {
            target: monad_types::RouterTarget::PointToPoint(rx_nodeid),
            message: regular_priority_msg,
            priority: UdpPriority::Regular,
        }]);
    }

    let mut received_messages = Vec::new();
    let timeout = Duration::from_secs(10);
    let start = std::time::Instant::now();

    while received_messages.len() < MESSAGE_COUNT * 2 && start.elapsed() < timeout {
        match tokio::time::timeout(Duration::from_millis(100), rx_rc.next()).await {
            Ok(Some(event)) => {
                let MockEvent((_, msg_id)) = event;
                received_messages.push(msg_id);
            }
            Ok(None) => break,
            Err(_) => continue,
        }
    }

    assert_eq!(
        received_messages.len(),
        MESSAGE_COUNT * 2,
        "Should receive all messages (got {} in {:?})",
        received_messages.len(),
        start.elapsed()
    );

    let high_priority_received = received_messages[0..MESSAGE_COUNT].to_vec();
    let regular_priority_received = received_messages[MESSAGE_COUNT..].to_vec();

    for (i, msg_id) in high_priority_received.iter().enumerate() {
        assert_eq!(
            *msg_id,
            HIGH_PRIORITY_START + i as u32,
            "High priority messages should be received first and in order"
        );
    }

    for (i, msg_id) in regular_priority_received.iter().enumerate() {
        assert_eq!(
            *msg_id,
            REGULAR_PRIORITY_START + i as u32,
            "Regular priority messages should be received after high priority"
        );
    }
}

#[tokio::test]
async fn test_raptorcast_forwarding_priority() {
    let mut validator1_secret = [1u8; 32];
    let mut validator2_secret = [2u8; 32];
    let mut validator_fullnode_secret = [3u8; 32];

    let validator1_key = Arc::new(KeyPair::from_bytes(&mut validator1_secret).unwrap());
    let validator2_key = Arc::new(KeyPair::from_bytes(&mut validator2_secret).unwrap());
    let validator_fullnode_key =
        Arc::new(KeyPair::from_bytes(&mut validator_fullnode_secret).unwrap());

    let validator1_nodeid = NodeId::new(validator1_key.pubkey());
    let validator2_nodeid = NodeId::new(validator2_key.pubkey());
    let validator_fullnode_nodeid = NodeId::new(validator_fullnode_key.pubkey());

    let validator1_dataplane = create_dataplane_for_tests(false);
    let validator2_dataplane = create_dataplane_for_tests(false);
    let validator_fullnode_dataplane = create_dataplane_for_tests(false);

    let known_addresses = HashMap::from([
        (validator1_nodeid, validator1_dataplane.non_auth_addr),
        (validator2_nodeid, validator2_dataplane.non_auth_addr),
        (
            validator_fullnode_nodeid,
            validator_fullnode_dataplane.non_auth_addr,
        ),
    ]);

    let mut validator1_rc = new_defaulted_raptorcast_for_tests::<
        SignatureType,
        MockMessage,
        MockMessage,
        MockEvent<PubKeyType>,
    >(
        validator1_dataplane,
        known_addresses.clone(),
        validator1_key.clone(),
    );

    let mut validator2_rc = new_defaulted_raptorcast_for_tests::<
        SignatureType,
        MockMessage,
        MockMessage,
        MockEvent<PubKeyType>,
    >(
        validator2_dataplane,
        known_addresses.clone(),
        validator2_key.clone(),
    );

    let mut validator_fullnode_rc = new_defaulted_raptorcast_for_tests::<
        SignatureType,
        MockMessage,
        MockMessage,
        MockEvent<PubKeyType>,
    >(
        validator_fullnode_dataplane,
        known_addresses.clone(),
        validator_fullnode_key.clone(),
    );

    let epoch = Epoch(0);
    let validator_set = vec![
        (validator1_nodeid, Stake::ONE),
        (validator2_nodeid, Stake::ONE),
        (validator_fullnode_nodeid, Stake::ONE),
    ];

    validator1_rc.exec(vec![RouterCommand::AddEpochValidatorSet {
        epoch,
        validator_set: validator_set.clone(),
    }]);

    validator2_rc.exec(vec![RouterCommand::AddEpochValidatorSet {
        epoch,
        validator_set: validator_set.clone(),
    }]);

    validator_fullnode_rc.exec(vec![RouterCommand::AddEpochValidatorSet {
        epoch,
        validator_set: validator_set.clone(),
    }]);

    validator2_rc.exec(vec![RouterCommand::UpdateFullNodes {
        dedicated_full_nodes: vec![validator_fullnode_nodeid],
        prioritized_full_nodes: vec![],
    }]);

    const MESSAGE_SIZE: usize = 128 << 20;

    let high_priority_msg = MockMessage::new(0xAA, MESSAGE_SIZE);
    validator1_rc.exec(vec![RouterCommand::PublishWithPriority {
        target: monad_types::RouterTarget::Broadcast(epoch),
        message: high_priority_msg,
        priority: UdpPriority::High,
    }]);

    let regular_priority_msg = MockMessage::new(0xBB, MESSAGE_SIZE);
    validator1_rc.exec(vec![RouterCommand::PublishWithPriority {
        target: monad_types::RouterTarget::Broadcast(epoch),
        message: regular_priority_msg,
        priority: UdpPriority::Regular,
    }]);

    let mut received_messages = Vec::new();
    let timeout = Duration::from_secs(2);
    let start = std::time::Instant::now();

    while received_messages.len() < 2 && start.elapsed() < timeout {
        if let Some(event) = validator_fullnode_rc.next().await {
            let MockEvent((from, msg_id)) = event;
            received_messages.push((from, msg_id));
        }
    }

    assert_eq!(received_messages.len(), 2);

    assert_eq!(
        received_messages[0].1, 0xAA,
        "high priority message (0xAA) should be received first"
    );
    assert_eq!(
        received_messages[1].1, 0xBB,
        "regular priority message (0xBB) should be received second"
    );
}
