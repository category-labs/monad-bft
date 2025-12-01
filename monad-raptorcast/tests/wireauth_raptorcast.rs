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

mod common;

use std::{
    collections::HashMap,
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    num::ParseIntError,
    sync::Arc,
    time::Duration,
};

use alloy_rlp::{RlpDecodable, RlpEncodable};
use bytes::{Bytes, BytesMut};
use common::{find_tcp_free_port, find_udp_free_port};
use futures_util::StreamExt;
use itertools::Itertools;
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable, PubKey,
};
use monad_executor::Executor;
use monad_executor_glue::{Message, RouterCommand};
use monad_peer_discovery::{MonadNameRecord, NameRecord};
use monad_raptorcast::RaptorCastEvent;
use monad_secp::{KeyPair, SecpSignature};
use monad_types::{Deserializable, Epoch, NodeId, Serializable, Stake};
use rstest::rstest;
use tracing_subscriber::EnvFilter;

const UP_BANDWIDTH_MBPS: u64 = 1_000;
const CONNECTION_TIMEOUT: Duration = Duration::from_secs(5);
const MESSAGE_TIMEOUT: Duration = Duration::from_secs(10);
const NUM_NODES: usize = 10;

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .try_init();
}

fn keypair(seed: u8) -> KeyPair {
    KeyPair::from_bytes(&mut [seed; 32]).unwrap()
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
    type NodeIdPubKey = CertificateSignaturePubKey<SecpSignature>;
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
            RaptorCastEvent::PeerManagerResponse(_) => unimplemented!(),
            RaptorCastEvent::SecondaryRaptorcastPeersUpdate { .. } => unimplemented!(),
        }
    }
}

struct ValidatorChannels {
    cmd_tx: tokio::sync::mpsc::UnboundedSender<RouterCommand<SecpSignature, MockMessage>>,
    event_rx:
        tokio::sync::mpsc::UnboundedReceiver<MockEvent<CertificateSignaturePubKey<SecpSignature>>>,
    ready_rx: tokio::sync::oneshot::Receiver<()>,
}

#[derive(Clone)]
struct ValidatorInfo {
    keypair: Arc<KeyPair>,
    nodeid: NodeId<CertificateSignaturePubKey<SecpSignature>>,
    pubkey: monad_secp::PubKey,
    tcp_addr: SocketAddrV4,
    auth_addr: SocketAddrV4,
    non_auth_addr: SocketAddrV4,
}

impl ValidatorInfo {
    fn new(seed: u8) -> Self {
        let kp = keypair(seed);
        let nodeid = NodeId::new(kp.pubkey());
        let pubkey = kp.pubkey();
        let tcp_addr = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), find_tcp_free_port());
        let auth_addr = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), find_udp_free_port());
        let non_auth_addr = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), find_udp_free_port());
        Self {
            keypair: Arc::new(kp),
            nodeid,
            pubkey,
            tcp_addr,
            auth_addr,
            non_auth_addr,
        }
    }

    fn create_name_record(&self, with_auth: bool) -> MonadNameRecord<SecpSignature> {
        let name_record = if with_auth {
            NameRecord::new_with_authentication(
                Ipv4Addr::new(127, 0, 0, 1),
                self.tcp_addr.port(),
                self.non_auth_addr.port(),
                self.auth_addr.port(),
                1,
            )
        } else {
            NameRecord::new(Ipv4Addr::new(127, 0, 0, 1), self.non_auth_addr.port(), 1)
        };
        MonadNameRecord::new(name_record, &*self.keypair)
    }
}

const DEFAULT_SIG_VERIFICATION_RATE_LIMIT: u32 = 10_000;

fn create_raptorcast_config(
    keypair: Arc<KeyPair>,
    sig_verification_rate_limit: u32,
) -> monad_raptorcast::config::RaptorCastConfig<SecpSignature> {
    monad_raptorcast::config::RaptorCastConfig {
        shared_key: keypair,
        mtu: monad_dataplane::udp::DEFAULT_MTU,
        udp_message_max_age_ms: u64::MAX,
        sig_verification_rate_limit,
        primary_instance: Default::default(),
        secondary_instance: monad_node_config::FullNodeRaptorCastConfig {
            enable_publisher: false,
            enable_client: false,
            raptor10_fullnode_redundancy_factor: 2f32,
            full_nodes_prioritized: monad_node_config::FullNodeConfig { identities: vec![] },
            round_span: monad_types::Round(10),
            invite_lookahead: monad_types::Round(5),
            max_invite_wait: monad_types::Round(3),
            deadline_round_dist: monad_types::Round(3),
            init_empty_round_span: monad_types::Round(1),
            max_group_size: 10,
            max_num_group: 5,
            invite_future_dist_min: monad_types::Round(1),
            invite_future_dist_max: monad_types::Round(5),
            invite_accept_heartbeat_ms: 100,
        },
    }
}

fn create_dataplane(
    tcp_addr: SocketAddrV4,
    auth_addr: SocketAddrV4,
    non_auth_addr: SocketAddrV4,
) -> (
    monad_dataplane::TcpSocketHandle,
    monad_dataplane::UdpSocketHandle,
    monad_dataplane::UdpSocketHandle,
    monad_dataplane::DataplaneControl,
) {
    let dp = monad_dataplane::DataplaneBuilder::new(&SocketAddr::V4(tcp_addr), UP_BANDWIDTH_MBPS)
        .extend_udp_sockets(vec![
            monad_dataplane::UdpSocketConfig {
                socket_addr: SocketAddr::V4(auth_addr),
                label: monad_raptorcast::AUTHENTICATED_RAPTORCAST_SOCKET.to_string(),
            },
            monad_dataplane::UdpSocketConfig {
                socket_addr: SocketAddr::V4(non_auth_addr),
                label: monad_raptorcast::RAPTORCAST_SOCKET.to_string(),
            },
        ])
        .build();
    assert!(dp.block_until_ready(Duration::from_secs(1)));

    let (tcp_socket, mut udp_dataplane, control) = dp.split();
    let authenticated_socket = udp_dataplane
        .take_socket(monad_raptorcast::AUTHENTICATED_RAPTORCAST_SOCKET)
        .expect("authenticated socket");
    let non_authenticated_socket = udp_dataplane
        .take_socket(monad_raptorcast::RAPTORCAST_SOCKET)
        .expect("non-authenticated socket");

    (
        tcp_socket,
        authenticated_socket,
        non_authenticated_socket,
        control,
    )
}

fn create_peer_discovery(
    known_addresses: HashMap<NodeId<CertificateSignaturePubKey<SecpSignature>>, SocketAddrV4>,
    name_records: HashMap<
        NodeId<CertificateSignaturePubKey<SecpSignature>>,
        MonadNameRecord<SecpSignature>,
    >,
) -> Arc<
    std::sync::Mutex<
        monad_peer_discovery::driver::PeerDiscoveryDriver<
            monad_peer_discovery::mock::NopDiscovery<SecpSignature>,
        >,
    >,
> {
    let builder = monad_peer_discovery::mock::NopDiscoveryBuilder {
        known_addresses,
        name_records,
        ..Default::default()
    };
    let pd = monad_peer_discovery::driver::PeerDiscoveryDriver::new(builder);
    Arc::new(std::sync::Mutex::new(pd))
}

fn spawn_noop_validator(
    keypair: Arc<KeyPair>,
    tcp_addr: SocketAddrV4,
    auth_addr: SocketAddrV4,
    non_auth_addr: SocketAddrV4,
    known_addresses: HashMap<NodeId<CertificateSignaturePubKey<SecpSignature>>, SocketAddrV4>,
    name_records: HashMap<
        NodeId<CertificateSignaturePubKey<SecpSignature>>,
        MonadNameRecord<SecpSignature>,
    >,
) -> ValidatorChannels {
    let (cmd_tx, cmd_rx) = tokio::sync::mpsc::unbounded_channel();
    let (event_tx, event_rx) = tokio::sync::mpsc::unbounded_channel();
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();

    tokio::task::spawn_local(async move {
        let shared_pd = create_peer_discovery(known_addresses, name_records);
        let (tcp_socket, _authenticated_socket, non_authenticated_socket, control) =
            create_dataplane(tcp_addr, auth_addr, non_auth_addr);
        let (tcp_reader, tcp_writer) = tcp_socket.split();
        let config = create_raptorcast_config(keypair, DEFAULT_SIG_VERIFICATION_RATE_LIMIT);
        let auth_protocol = monad_raptorcast::auth::NoopAuthProtocol::new();

        let mut validator_rc = monad_raptorcast::RaptorCast::<
            SecpSignature,
            MockMessage,
            MockMessage,
            MockEvent<CertificateSignaturePubKey<SecpSignature>>,
            monad_peer_discovery::mock::NopDiscovery<SecpSignature>,
            _,
        >::new(
            config,
            monad_raptorcast::raptorcast_secondary::SecondaryRaptorCastModeConfig::None,
            tcp_reader,
            tcp_writer,
            None,
            non_authenticated_socket,
            control,
            shared_pd,
            Epoch(0),
            auth_protocol,
        );

        let mut cmd_rx = cmd_rx;
        let _ = ready_tx.send(());

        loop {
            tokio::select! {
                Some(cmd) = cmd_rx.recv() => {
                    validator_rc.exec(vec![cmd]);
                }
                Some(event) = validator_rc.next() => {
                    if event_tx.send(event).is_err() {
                        break;
                    }
                }
            }
        }
    });

    ValidatorChannels {
        cmd_tx,
        event_rx,
        ready_rx,
    }
}

fn spawn_wireauth_validator(
    keypair: Arc<KeyPair>,
    tcp_addr: SocketAddrV4,
    auth_addr: SocketAddrV4,
    non_auth_addr: SocketAddrV4,
    known_addresses: HashMap<NodeId<CertificateSignaturePubKey<SecpSignature>>, SocketAddrV4>,
    name_records: HashMap<
        NodeId<CertificateSignaturePubKey<SecpSignature>>,
        MonadNameRecord<SecpSignature>,
    >,
    peers_to_check: Vec<(SocketAddrV4, monad_secp::PubKey)>,
    sig_verification_rate_limit: u32,
) -> ValidatorChannels {
    let (cmd_tx, cmd_rx) = tokio::sync::mpsc::unbounded_channel();
    let (event_tx, event_rx) = tokio::sync::mpsc::unbounded_channel();
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();

    tokio::task::spawn_local(async move {
        let shared_pd = create_peer_discovery(known_addresses, name_records);
        let (tcp_socket, authenticated_socket, non_authenticated_socket, control) =
            create_dataplane(tcp_addr, auth_addr, non_auth_addr);
        let (tcp_reader, tcp_writer) = tcp_socket.split();
        let config = create_raptorcast_config(keypair.clone(), sig_verification_rate_limit);
        let wireauth_config = monad_wireauth::Config::default();
        let auth_protocol =
            monad_raptorcast::auth::WireAuthProtocol::new(wireauth_config, keypair.clone());

        let mut validator_rc = monad_raptorcast::RaptorCast::<
            SecpSignature,
            MockMessage,
            MockMessage,
            MockEvent<CertificateSignaturePubKey<SecpSignature>>,
            monad_peer_discovery::mock::NopDiscovery<SecpSignature>,
            _,
        >::new(
            config,
            monad_raptorcast::raptorcast_secondary::SecondaryRaptorCastModeConfig::None,
            tcp_reader,
            tcp_writer,
            Some(authenticated_socket),
            non_authenticated_socket,
            control,
            shared_pd,
            Epoch(0),
            auth_protocol,
        );

        let mut cmd_rx = cmd_rx;
        let check_connections = !peers_to_check.is_empty();
        let mut ready_tx = Some(ready_tx);
        let mut check_interval = tokio::time::interval(Duration::from_millis(100));

        loop {
            tokio::select! {
                Some(cmd) = cmd_rx.recv() => {
                    validator_rc.exec(vec![cmd]);
                }
                Some(event) = validator_rc.next() => {
                    if event_tx.send(event).is_err() {
                        break;
                    }
                }
                _ = check_interval.tick(), if check_connections => {
                    if let Some(tx) = ready_tx.take() {
                        let all_connected = peers_to_check.iter().all(|(addr, pubkey)| {
                            validator_rc.is_connected_to(&SocketAddr::V4(*addr), pubkey)
                        });

                        if all_connected {
                            let _ = tx.send(());
                        } else {
                            ready_tx = Some(tx);
                        }
                    }
                }
            }
        }
    });

    ValidatorChannels {
        cmd_tx,
        event_rx,
        ready_rx,
    }
}

async fn establish_connections(
    cmd_txs: &[&tokio::sync::mpsc::UnboundedSender<RouterCommand<SecpSignature, MockMessage>>],
    ready_rxs: Vec<tokio::sync::oneshot::Receiver<()>>,
    epoch: Epoch,
    validator_set: Vec<(NodeId<CertificateSignaturePubKey<SecpSignature>>, Stake)>,
    event_rxs: &mut [&mut tokio::sync::mpsc::UnboundedReceiver<
        MockEvent<CertificateSignaturePubKey<SecpSignature>>,
    >],
) {
    for cmd_tx in cmd_txs {
        cmd_tx
            .send(RouterCommand::AddEpochValidatorSet {
                epoch,
                validator_set: validator_set.clone(),
            })
            .unwrap();
    }

    let setup_message = MockMessage::new(1, 100);
    for cmd_tx in cmd_txs {
        cmd_tx
            .send(RouterCommand::Publish {
                target: monad_types::RouterTarget::Broadcast(epoch),
                message: setup_message,
            })
            .unwrap();
    }

    for ready_rx in ready_rxs {
        tokio::time::timeout(CONNECTION_TIMEOUT, ready_rx)
            .await
            .expect("connection timeout")
            .expect("ready channel closed");
    }

    for event_rx in event_rxs {
        while event_rx.try_recv().is_ok() {}
    }
}

#[derive(Clone, Copy)]
enum RoutingType {
    PointToPoint,
    Raptorcast,
    Broadcast,
}

async fn run_test_scenario(num_auth_nodes: usize, routing_type: RoutingType, message_size: usize) {
    let validator_infos: Vec<_> = (1..=NUM_NODES as u8).map(ValidatorInfo::new).collect();

    let name_records: HashMap<_, _> = validator_infos
        .iter()
        .enumerate()
        .map(|(i, v)| (v.nodeid, v.create_name_record(i < num_auth_nodes)))
        .collect();

    let known_addresses: HashMap<_, _> = validator_infos
        .iter()
        .map(|v| (v.nodeid, v.non_auth_addr))
        .collect();

    let peers_for_check: Vec<_> = validator_infos
        .iter()
        .enumerate()
        .filter(|(i, _)| *i < num_auth_nodes)
        .map(|(_, v)| (v.auth_addr, v.pubkey))
        .collect();

    let validators: Vec<_> = validator_infos
        .iter()
        .enumerate()
        .map(|(i, v)| {
            if i < num_auth_nodes {
                let peers = peers_for_check
                    .iter()
                    .enumerate()
                    .filter(|(j, _)| *j != i)
                    .map(|(_, p)| *p)
                    .collect();
                spawn_wireauth_validator(
                    v.keypair.clone(),
                    v.tcp_addr,
                    v.auth_addr,
                    v.non_auth_addr,
                    known_addresses.clone(),
                    name_records.clone(),
                    peers,
                    DEFAULT_SIG_VERIFICATION_RATE_LIMIT,
                )
            } else {
                spawn_noop_validator(
                    v.keypair.clone(),
                    v.tcp_addr,
                    v.auth_addr,
                    v.non_auth_addr,
                    known_addresses.clone(),
                    name_records.clone(),
                )
            }
        })
        .collect();

    let epoch = Epoch(0);
    let validator_set: Vec<_> = validator_infos
        .iter()
        .map(|v| (v.nodeid, Stake::ONE))
        .collect();

    let (cmd_txs, ready_rxs, mut event_rxs): (Vec<_>, Vec<_>, Vec<_>) = validators
        .into_iter()
        .map(|v| (v.cmd_tx, v.ready_rx, v.event_rx))
        .multiunzip();

    let cmd_tx_refs: Vec<_> = cmd_txs.iter().collect();
    let mut event_rx_refs: Vec<_> = event_rxs.iter_mut().collect();

    establish_connections(
        &cmd_tx_refs,
        ready_rxs,
        epoch,
        validator_set,
        &mut event_rx_refs,
    )
    .await;

    let sender_idx = 0;
    let sender_nodeid = validator_infos[sender_idx].nodeid;

    match routing_type {
        RoutingType::PointToPoint => {
            for receiver_idx in 1..NUM_NODES {
                let message = MockMessage::new(1000 + receiver_idx as u32, message_size);
                cmd_txs[sender_idx]
                    .send(RouterCommand::Publish {
                        target: monad_types::RouterTarget::PointToPoint(
                            validator_infos[receiver_idx].nodeid,
                        ),
                        message,
                    })
                    .unwrap();

                let event = tokio::time::timeout(MESSAGE_TIMEOUT, event_rxs[receiver_idx].recv())
                    .await
                    .expect("timeout waiting for message")
                    .expect("channel closed");

                let MockEvent((from, msg_id)) = event;
                assert_eq!(from, sender_nodeid);
                assert_eq!(msg_id, 1000 + receiver_idx as u32);
            }
        }
        RoutingType::Raptorcast | RoutingType::Broadcast => {
            let message = MockMessage::new(1000, message_size);
            let target = match routing_type {
                RoutingType::Raptorcast => monad_types::RouterTarget::Raptorcast(epoch),
                RoutingType::Broadcast => monad_types::RouterTarget::Broadcast(epoch),
                _ => unreachable!(),
            };

            cmd_txs[sender_idx]
                .send(RouterCommand::Publish { target, message })
                .unwrap();

            for event_rx in event_rxs.iter_mut().take(NUM_NODES) {
                let event = tokio::time::timeout(MESSAGE_TIMEOUT, event_rx.recv())
                    .await
                    .expect("timeout waiting for message")
                    .expect("channel closed");

                let MockEvent((from, msg_id)) = event;
                assert_eq!(from, sender_nodeid);
                assert_eq!(msg_id, 1000);
            }
        }
    }
}

async fn test_rate_limiting_basic() {
    const NUM_TEST_NODES: usize = 3;
    const RATE_LIMIT: u32 = 10;
    const MESSAGE_SIZE: usize = 1_000;
    const NUM_MESSAGES: u32 = 20;

    let validator_infos: Vec<_> = (1..=NUM_TEST_NODES as u8).map(ValidatorInfo::new).collect();

    let name_records: HashMap<_, _> = validator_infos
        .iter()
        .map(|v| (v.nodeid, v.create_name_record(true)))
        .collect();

    let known_addresses: HashMap<_, _> = validator_infos
        .iter()
        .map(|v| (v.nodeid, v.non_auth_addr))
        .collect();

    let peers_for_check: Vec<_> = validator_infos
        .iter()
        .map(|v| (v.auth_addr, v.pubkey))
        .collect();

    let validators: Vec<_> = validator_infos
        .iter()
        .enumerate()
        .map(|(i, v)| {
            let peers = peers_for_check
                .iter()
                .enumerate()
                .filter(|(j, _)| *j != i)
                .map(|(_, p)| *p)
                .collect();
            spawn_wireauth_validator(
                v.keypair.clone(),
                v.tcp_addr,
                v.auth_addr,
                v.non_auth_addr,
                known_addresses.clone(),
                name_records.clone(),
                peers,
                RATE_LIMIT,
            )
        })
        .collect();

    let epoch = Epoch(0);
    // first 2 nodes are validators
    let validator_set: Vec<_> = validator_infos
        .iter()
        .take(2)
        .map(|v| (v.nodeid, Stake::ONE))
        .collect();

    let (cmd_txs, ready_rxs, mut event_rxs): (Vec<_>, Vec<_>, Vec<_>) = validators
        .into_iter()
        .map(|v| (v.cmd_tx, v.ready_rx, v.event_rx))
        .multiunzip();

    let cmd_tx_refs: Vec<_> = cmd_txs.iter().collect();
    let mut event_rx_refs: Vec<_> = event_rxs.iter_mut().collect();

    establish_connections(
        &cmd_tx_refs,
        ready_rxs,
        epoch,
        validator_set,
        &mut event_rx_refs,
    )
    .await;

    for event_rx in event_rxs.iter_mut() {
        while event_rx.try_recv().is_ok() {}
    }

    let validator_sender_idx = 0;
    let validator_receiver_idx = 1;
    let validator_sender_nodeid = validator_infos[validator_sender_idx].nodeid;

    for i in 0..NUM_MESSAGES {
        let message = MockMessage::new(1000 + i, MESSAGE_SIZE);
        cmd_txs[validator_sender_idx]
            .send(RouterCommand::Publish {
                target: monad_types::RouterTarget::PointToPoint(
                    validator_infos[validator_receiver_idx].nodeid,
                ),
                message,
            })
            .unwrap();
    }

    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut validator_received = 0;
    while let Ok(event) = event_rxs[validator_receiver_idx].try_recv() {
        let MockEvent((from, _)) = event;
        assert_eq!(from, validator_sender_nodeid);
        validator_received += 1;
    }

    assert_eq!(
        validator_received, NUM_MESSAGES as usize,
        "all {} messages from validator should be received",
        NUM_MESSAGES,
    );

    let non_validator_idx = 2;
    let non_validator_nodeid = validator_infos[non_validator_idx].nodeid;
    let target_validator_idx = 0;

    for i in 0..NUM_MESSAGES {
        let message = MockMessage::new(2000 + i, MESSAGE_SIZE);
        cmd_txs[non_validator_idx]
            .send(RouterCommand::Publish {
                target: monad_types::RouterTarget::PointToPoint(
                    validator_infos[target_validator_idx].nodeid,
                ),
                message,
            })
            .unwrap();
    }

    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut non_validator_received = 0;
    while let Ok(event) = event_rxs[target_validator_idx].try_recv() {
        let MockEvent((from, _)) = event;
        assert_eq!(from, non_validator_nodeid);
        non_validator_received += 1;
    }

    assert!(
        non_validator_received > 0,
        "at least some messages from non-validator should be received"
    );
    assert!(
        non_validator_received < NUM_MESSAGES as usize,
        "all {} messages from non-validator were received, rate limiting did not work",
        NUM_MESSAGES
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_rate_limiting_p2p() {
    init_tracing();

    tokio::task::LocalSet::new()
        .run_until(test_rate_limiting_basic())
        .await;
}

#[rstest]
#[case(10, RoutingType::Raptorcast, 2_000_000)]
#[case(5, RoutingType::Raptorcast, 2_000_000)]
#[case(0, RoutingType::Raptorcast, 2_000_000)]
#[case(5, RoutingType::Broadcast, 10_000)]
#[case(5, RoutingType::PointToPoint, 1_000)]
#[serial_test::file_serial]
#[tokio::test(flavor = "current_thread")]
async fn test_wireauth_matrix(
    #[case] num_auth_nodes: usize,
    #[case] routing_type: RoutingType,
    #[case] message_size: usize,
) {
    init_tracing();

    tokio::task::LocalSet::new()
        .run_until(run_test_scenario(
            num_auth_nodes,
            routing_type,
            message_size,
        ))
        .await;
}
