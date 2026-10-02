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
    collections::HashMap,
    future::Future,
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    sync::{Arc, Mutex},
    time::Duration,
};

use alloy_rlp::{RlpDecodable, RlpEncodable};
use bytes::Bytes;
use futures::{FutureExt, StreamExt};
use monad_crypto::certificate_signature::CertificateSignaturePubKey;
use monad_dataplane::{
    DataplaneBuilder, DataplaneControl, TcpSocketHandle, TcpSocketId, UdpSocketHandle, UdpSocketId,
};
use monad_executor::Executor;
use monad_peer_discovery::{
    driver::PeerDiscoveryDriver,
    mock::{NopDiscovery, NopDiscoveryBuilder},
    MonadNameRecord, NameRecord,
};
use monad_secp::{KeyPair, SecpSignature};
use monad_types::{Epoch, NodeId};
use monad_wireauth::Config;
use tracing_subscriber::EnvFilter;

use super::{DataplaneHandles, RaptorCast, RaptorCastEvent};
use crate::auth::{
    metrics::TCP_METRICS, protocol::WireAuthProtocol, tcp_socket::wireauth_config,
    AuthenticationProtocol, DataplaneCompletion,
};

fn keypair(seed: u8) -> KeyPair {
    KeyPair::from_bytes(&mut [seed; 32]).unwrap()
}

type PublicKey = CertificateSignaturePubKey<SecpSignature>;
type Discovery = NopDiscovery<SecpSignature>;
type DiscoveryDriver = Arc<Mutex<PeerDiscoveryDriver<Discovery>>>;
type TestRouter = RaptorCast<
    SecpSignature,
    TestMessage,
    TestMessage,
    RaptorCastEvent<(NodeId<PublicKey>, u32), SecpSignature>,
    Discovery,
    WireAuthProtocol,
    crate::auth::NopScore<NodeId<PublicKey>>,
>;

#[derive(Clone, RlpEncodable, RlpDecodable)]
struct TestMessage {
    id: u32,
}

impl monad_executor_glue::Message for TestMessage {
    type NodeIdPubKey = PublicKey;
    type Event = (NodeId<PublicKey>, u32);

    fn event(self, from: NodeId<PublicKey>) -> Self::Event {
        (from, self.id)
    }
}

type NameRecords = HashMap<NodeId<PublicKey>, MonadNameRecord<SecpSignature>>;

struct NodeInfo {
    keypair: Arc<KeyPair>,
    node_id: NodeId<PublicKey>,
    sigauth_tcp_addr: SocketAddrV4,
    wireauth_tcp_addr: SocketAddrV4,
    sigauth_tcp: Option<TcpSocketHandle>,
    wireauth_tcp: Option<TcpSocketHandle>,
    authenticated_udp: Option<UdpSocketHandle>,
    legacy_udp: Option<UdpSocketHandle>,
    control: Option<DataplaneControl>,
}

impl NodeInfo {
    fn new(seed: u8) -> Self {
        let kp = keypair(seed);
        let node_id = NodeId::new(kp.pubkey());
        let bind_addr = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
        let mut dp = DataplaneBuilder::new(1000)
            .with_tcp_sockets([
                (TcpSocketId::AuthenticatedRaptorcast, bind_addr),
                (TcpSocketId::Raptorcast, bind_addr),
            ])
            .with_udp_sockets([
                (UdpSocketId::AuthenticatedRaptorcast, bind_addr),
                (UdpSocketId::Raptorcast, bind_addr),
            ])
            .build();
        assert!(dp.block_until_ready(Duration::from_secs(1)));

        let wireauth_tcp = dp
            .tcp_sockets
            .take(TcpSocketId::AuthenticatedRaptorcast)
            .expect("wireauth tcp socket");
        let sigauth_tcp = dp
            .tcp_sockets
            .take(TcpSocketId::Raptorcast)
            .expect("sigauth tcp socket");
        let SocketAddr::V4(wireauth_tcp_addr) = wireauth_tcp.local_addr() else {
            panic!("expected IPv4 wireauth address");
        };
        let SocketAddr::V4(sigauth_tcp_addr) = sigauth_tcp.local_addr() else {
            panic!("expected IPv4 sigauth address");
        };

        Self {
            keypair: Arc::new(kp),
            node_id,
            sigauth_tcp_addr,
            wireauth_tcp_addr,
            sigauth_tcp: Some(sigauth_tcp),
            wireauth_tcp: Some(wireauth_tcp),
            authenticated_udp: dp.udp_sockets.take(UdpSocketId::AuthenticatedRaptorcast),
            legacy_udp: dp.udp_sockets.take(UdpSocketId::Raptorcast),
            control: Some(dp.control),
        }
    }

    fn create_name_record(&self, with_wireauth_tcp: bool) -> MonadNameRecord<SecpSignature> {
        self.create_name_record_with_tcp_port(with_wireauth_tcp, self.sigauth_tcp_addr.port())
    }

    fn create_name_record_with_tcp_port(
        &self,
        with_wireauth_tcp: bool,
        tcp_port: u16,
    ) -> MonadNameRecord<SecpSignature> {
        let name_record = NameRecord::new_with_ports(
            Ipv4Addr::LOCALHOST,
            tcp_port,
            Some(self.sigauth_tcp_addr.port()),
            self.sigauth_tcp_addr.port(),
            None,
            if with_wireauth_tcp {
                Some(self.wireauth_tcp_addr.port())
            } else {
                None
            },
            1,
        );
        MonadNameRecord::new(name_record, &*self.keypair)
    }
}

fn create_router(
    node: &mut NodeInfo,
    peer_discovery: DiscoveryDriver,
    config: Config,
) -> (TestRouter, DataplaneControl) {
    let authenticated_udp = node.authenticated_udp.take().unwrap();
    let legacy_udp = node.legacy_udp.take().unwrap();
    let SocketAddr::V4(auth_addr) = authenticated_udp.local_addr() else {
        panic!("expected IPv4 UDP address");
    };
    let SocketAddr::V4(non_auth_addr) = legacy_udp.local_addr() else {
        panic!("expected IPv4 UDP address");
    };
    let control = node.control.take().unwrap();
    let handles = DataplaneHandles {
        tcp_socket: node.sigauth_tcp.take().unwrap(),
        authenticated_tcp_socket: node.wireauth_tcp.take(),
        authenticated_socket: authenticated_udp,
        direct_udp_socket: None,
        non_authenticated_socket: legacy_udp,
        control: control.clone(),
        tcp_addr: node.sigauth_tcp_addr,
        auth_addr,
        direct_udp_addr: None,
        non_auth_addr,
    };
    let mut router: TestRouter = crate::new_wireauth_raptorcast_for_tests(
        handles,
        HashMap::new(),
        node.keypair.clone(),
        Epoch(1),
    );
    router.peer_discovery_driver = peer_discovery;
    router.authenticated_tcp.as_mut().unwrap().auth_protocol =
        WireAuthProtocol::new(&TCP_METRICS, config, node.keypair.clone());
    (router, control)
}

fn create_peer_discovery(name_records: &NameRecords) -> DiscoveryDriver {
    let builder = NopDiscoveryBuilder {
        known_addresses: HashMap::new(),
        name_records: name_records.clone(),
        ..Default::default()
    };
    Arc::new(Mutex::new(PeerDiscoveryDriver::new(builder)))
}

struct TestPair {
    sender_id: NodeId<PublicKey>,
    receiver_id: NodeId<PublicKey>,
    sender: TestRouter,
    receiver: TestRouter,
    _controls: [DataplaneControl; 2],
}

impl TestPair {
    fn new(with_wireauth_tcp: bool) -> Self {
        Self::with_options(with_wireauth_tcp, wireauth_config(), None)
    }

    fn with_options(
        with_wireauth_tcp: bool,
        sender_config: Config,
        receiver_tcp_port: Option<u16>,
    ) -> Self {
        let mut sender = NodeInfo::new(1);
        let mut receiver = NodeInfo::new(2);
        let mut name_records =
            HashMap::from([(sender.node_id, sender.create_name_record(with_wireauth_tcp))]);
        let receiver_record = receiver_tcp_port.map_or_else(
            || receiver.create_name_record(with_wireauth_tcp),
            |port| receiver.create_name_record_with_tcp_port(with_wireauth_tcp, port),
        );
        name_records.insert(receiver.node_id, receiver_record);

        let (sender_socket, sender_control) = create_router(
            &mut sender,
            create_peer_discovery(&name_records),
            sender_config,
        );
        let (receiver_socket, receiver_control) = create_router(
            &mut receiver,
            create_peer_discovery(&name_records),
            wireauth_config(),
        );
        Self {
            sender_id: sender.node_id,
            receiver_id: receiver.node_id,
            sender: sender_socket,
            receiver: receiver_socket,
            _controls: [sender_control, receiver_control],
        }
    }
}

fn run_test(test: impl Future<Output = ()>) {
    init_tracing();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    tokio::task::LocalSet::new().block_on(&runtime, test);
}

fn messages(prefix: &str, count: usize) -> Vec<Bytes> {
    (0..count)
        .map(|i| Bytes::from(format!("{prefix}_{i}")))
        .collect()
}

async fn send_and_receive(
    mut sender: TestRouter,
    mut receiver: TestRouter,
    receiver_id: NodeId<PublicKey>,
    outbound: Vec<(Bytes, DataplaneCompletion)>,
) -> Vec<Bytes> {
    let message_count = outbound.len();
    let drive_sender = async {
        for (payload, completion) in outbound {
            sender.tcp_build_and_send(&receiver_id, || payload, completion);
        }
        loop {
            if let Err(err) = sender.recv_tcp().await {
                tracing::warn!(?err, "sender recv error");
            }
        }
    };
    let collect = async {
        let mut received = Vec::with_capacity(message_count);
        while received.len() < message_count {
            match receiver.recv_tcp().await {
                Ok(message) => received.push(message.payload),
                Err(err) => tracing::warn!(?err, "receiver recv error"),
            }
        }
        received
    };

    tokio::time::timeout(Duration::from_secs(10), async {
        tokio::select! {
            received = collect => received,
            () = drive_sender => unreachable!("sender receive loop exited"),
        }
    })
    .await
    .expect("timed out receiving tcp messages")
}

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .try_init();
}

#[test]
fn test_tcp_sigauth_only() {
    const NUM_MESSAGES: usize = 10;
    run_test(async {
        let TestPair {
            sender,
            receiver,
            receiver_id,
            _controls,
            ..
        } = TestPair::new(false);
        let expected = messages("sigauth_message", NUM_MESSAGES);
        let outbound = expected
            .iter()
            .cloned()
            .map(|message| (message, None))
            .collect();

        let received = send_and_receive(sender, receiver, receiver_id, outbound).await;
        assert_eq!(received, expected);
    });
}

#[test]
fn test_tcp_wireauth_only() {
    const NUM_MESSAGES: usize = 10;
    run_test(async {
        let TestPair {
            sender,
            receiver,
            receiver_id,
            _controls,
            ..
        } = TestPair::new(true);
        let expected = messages("wireauth_message", NUM_MESSAGES);
        let outbound = expected
            .iter()
            .cloned()
            .map(|message| (message, None))
            .collect();

        let received = send_and_receive(sender, receiver, receiver_id, outbound).await;
        assert_eq!(received, expected);
    });
}

#[test]
fn test_tcp_wireauth_bidirectional() {
    run_test(async {
        let TestPair {
            sender_id: node_a_id,
            receiver_id: node_b_id,
            sender: mut socket_a,
            receiver: mut socket_b,
            _controls,
        } = TestPair::new(true);
        let exchange = async {
            let node_a = async {
                socket_a.tcp_build_and_send(&node_b_id, || Bytes::from("init_from_a"), None);
                loop {
                    match socket_a.recv_tcp().await {
                        Ok(message) => break message.payload,
                        Err(err) => tracing::warn!(?err, "node_a recv error"),
                    }
                }
            };
            let node_b = async {
                loop {
                    match socket_b.recv_tcp().await {
                        Ok(message) => {
                            socket_b.tcp_build_and_send(
                                &node_a_id,
                                || Bytes::from("reply_from_b"),
                                None,
                            );
                            break message.payload;
                        }
                        Err(err) => tracing::warn!(?err, "node_b recv error"),
                    }
                }
            };
            tokio::join!(node_a, node_b)
        };

        let (received_by_a, received_by_b) =
            tokio::time::timeout(Duration::from_secs(10), exchange)
                .await
                .expect("test timed out");
        assert_eq!(received_by_a, Bytes::from_static(b"reply_from_b"));
        assert_eq!(received_by_b, Bytes::from_static(b"init_from_a"));
    });
}

#[test]
fn test_wireauth_preserves_completion() {
    run_test(async {
        let TestPair {
            sender,
            receiver,
            receiver_id,
            _controls,
            ..
        } = TestPair::with_options(true, wireauth_config(), Some(1));
        let payload = Bytes::from_static(b"completion_message");
        let (completion_tx, completion_rx) = futures::channel::oneshot::channel();

        let received = send_and_receive(
            sender,
            receiver,
            receiver_id,
            vec![(payload.clone(), Some(completion_tx))],
        )
        .await;

        assert_eq!(received, [payload]);
        completion_rx.await.expect("tcp write should complete");
    });
}

#[test]
fn test_wireauth_buffer_failure_does_not_fallback() {
    run_test(async {
        let sender_config = Config {
            max_buffered_bytes_per_session: 0,
            ..wireauth_config()
        };
        let TestPair {
            sender: mut sender_socket,
            receiver: mut receiver_socket,
            receiver_id,
            _controls,
            ..
        } = TestPair::with_options(true, sender_config, None);
        let payload = Bytes::from_static(b"rejected_message");
        let (completion_tx, completion_rx) = futures::channel::oneshot::channel();

        sender_socket.tcp_build_and_send(&receiver_id, || payload, Some(completion_tx));

        assert!(completion_rx.await.is_err());
        assert!(
            tokio::time::timeout(Duration::from_millis(100), receiver_socket.recv_tcp())
                .await
                .is_err(),
            "buffer failure must not send via sigauth"
        );
    });
}

#[test]
fn test_tcp_selects_transport_by_both_capabilities() {
    run_test(async {
        for local_wireauth in [false, true] {
            for peer_wireauth in [false, true] {
                let TestPair {
                    mut sender,
                    mut receiver,
                    sender_id,
                    receiver_id,
                    _controls,
                    ..
                } = TestPair::new(peer_wireauth);
                if !local_wireauth {
                    sender.authenticated_tcp = None;
                }
                if !peer_wireauth {
                    receiver.authenticated_tcp = None;
                }
                let payload = Bytes::from_static(b"capability matrix");
                let (tx, rx) = futures::channel::oneshot::channel();
                sender.tcp_build_and_send(&receiver_id, || payload.clone(), Some(tx));
                let metrics = &mut sender.tcp_metrics;
                let wireauth = local_wireauth && peer_wireauth;
                assert_eq!(
                    metrics
                        .gauge(crate::auth::GAUGE_RAPTORCAST_AUTH_WIREAUTH_TCP_BYTES_WRITTEN)
                        .get(),
                    if wireauth { payload.len() as u64 } else { 0 }
                );
                assert_eq!(
                    metrics
                        .gauge(crate::auth::GAUGE_RAPTORCAST_AUTH_SIGAUTH_TCP_BYTES_WRITTEN)
                        .get(),
                    if wireauth { 0 } else { payload.len() as u64 }
                );
                let message = tokio::select! {
                    result = tokio::time::timeout(Duration::from_secs(3), receiver.recv_tcp()) => result.unwrap().unwrap(),
                    _ = sender.recv_tcp() => panic!("unexpected response data"),
                };
                assert_eq!(message.payload, payload);
                assert_eq!(message.from, sender_id.pubkey());
                rx.await
                    .expect("selected transport must complete the write");
            }
        }
    });
}

#[test]
fn test_tcp_recv_errors_preserve_address_and_cause() {
    run_test(async {
        use std::io::{Read, Write};
        for wireauth in [false, true] {
            let mut node = NodeInfo::new(32);
            let addr = if wireauth {
                node.wireauth_tcp_addr
            } else {
                node.sigauth_tcp_addr
            };
            let (mut socket, control) = create_router(
                &mut node,
                create_peer_discovery(&HashMap::new()),
                wireauth_config(),
            );
            let mut peer = std::net::TcpStream::connect(addr).unwrap();
            peer.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
            let source = peer.local_addr().unwrap();
            let mut frame = Vec::new();
            frame.extend_from_slice(&0x434e5353u32.to_le_bytes());
            frame.extend_from_slice(&1u32.to_le_bytes());
            frame.extend_from_slice(&1u64.to_le_bytes());
            frame.push(0);
            peer.write_all(&frame).unwrap();
            let error = tokio::time::timeout(Duration::from_secs(2), socket.recv_tcp())
                .await
                .unwrap()
                .err()
                .expect("malformed authentication must fail");
            assert_eq!(error.src_addr(), source);
            match error {
                crate::auth::TcpRecvError::WireAuth(error) if wireauth => {
                    assert!(matches!(error.error, monad_wireauth::Error::Message(_)));
                }
                crate::auth::TcpRecvError::SigAuth(error) if !wireauth => {
                    assert!(matches!(
                        error.error,
                        crate::auth::SigAuthError::MessageTooShort
                    ));
                }
                other => panic!("unexpected error: {other:?}"),
            }
            control.disconnect(source);
            let mut byte = [0];
            assert_eq!(peer.read(&mut byte).unwrap(), 0);
        }
    });
}

#[test]
fn test_wireauth_connect_failure_does_not_fallback() {
    run_test(async {
        let TestPair {
            mut sender,
            mut receiver,
            receiver_id,
            _controls,
            ..
        } = TestPair::with_options(
            true,
            Config {
                max_initiated_sessions: 0,
                ..wireauth_config()
            },
            None,
        );
        let (tx, rx) = futures::channel::oneshot::channel();
        sender.tcp_build_and_send(
            &receiver_id,
            || Bytes::from_static(b"rejected connect"),
            Some(tx),
        );
        assert!(rx.await.is_err());
        assert!(
            tokio::time::timeout(Duration::from_millis(100), receiver.recv_tcp())
                .await
                .is_err(),
            "connect failure must not send via sigauth"
        );
        assert_eq!(
            sender
                .tcp_metrics
                .gauge(crate::auth::GAUGE_RAPTORCAST_AUTH_WIREAUTH_TCP_BYTES_WRITTEN)
                .get(),
            0
        );
        assert_eq!(
            sender
                .tcp_metrics
                .gauge(crate::auth::GAUGE_RAPTORCAST_AUTH_SIGAUTH_TCP_BYTES_WRITTEN)
                .get(),
            0
        );
    });
}

#[test]
fn test_tcp_mixed_transports_dispatch_and_export_metrics() {
    run_test(async {
        let mut legacy_node = NodeInfo::new(40);
        let mut wireauth_node = NodeInfo::new(41);
        let mut receiver_node = NodeInfo::new(42);
        let records = HashMap::from([
            (legacy_node.node_id, legacy_node.create_name_record(false)),
            (
                wireauth_node.node_id,
                wireauth_node.create_name_record(true),
            ),
            (
                receiver_node.node_id,
                receiver_node.create_name_record(true),
            ),
        ]);
        let (mut legacy, _legacy_control) = create_router(
            &mut legacy_node,
            create_peer_discovery(&records),
            wireauth_config(),
        );
        legacy.authenticated_tcp = None;
        let (mut wireauth, _wireauth_control) = create_router(
            &mut wireauth_node,
            create_peer_discovery(&records),
            wireauth_config(),
        );
        let (mut receiver, _receiver_control) = create_router(
            &mut receiver_node,
            create_peer_discovery(&records),
            wireauth_config(),
        );
        let legacy_payload =
            crate::message::OutboundRouterMessage::<_, SecpSignature>::AppMessage(TestMessage {
                id: 1,
            })
            .try_serialize()
            .unwrap();
        let wireauth_payload =
            crate::message::OutboundRouterMessage::<_, SecpSignature>::AppMessage(TestMessage {
                id: 2,
            })
            .try_serialize()
            .unwrap();
        let legacy_len = legacy_payload.len() as u64;
        let wireauth_len = wireauth_payload.len() as u64;
        let (legacy_tx, legacy_rx) = futures::channel::oneshot::channel();
        let (wireauth_tx, wireauth_rx) = futures::channel::oneshot::channel();
        legacy.tcp_build_and_send(&receiver_node.node_id, || legacy_payload, Some(legacy_tx));
        wireauth.tcp_build_and_send(
            &receiver_node.node_id,
            || wireauth_payload,
            Some(wireauth_tx),
        );

        let collect = async {
            let mut messages = Vec::new();
            while messages.len() < 2 {
                match receiver.next().await.unwrap() {
                    RaptorCastEvent::Message(message) => messages.push(message),
                    _ => panic!("unexpected router event"),
                }
            }
            messages.sort_by_key(|(_, id)| *id);
            messages
        };
        let received = tokio::time::timeout(Duration::from_secs(3), async {
            tokio::select! {
                messages = collect => messages,
                _ = legacy.next() => panic!("unexpected legacy response"),
                _ = wireauth.next() => panic!("unexpected wireauth response"),
            }
        })
        .await
        .unwrap();
        assert_eq!(
            received,
            [(legacy_node.node_id, 1), (wireauth_node.node_id, 2)]
        );
        legacy_rx.await.unwrap();
        wireauth_rx.await.unwrap();

        let exported = receiver.metrics().into_inner();
        for (metric, expected) in [
            (
                crate::auth::GAUGE_RAPTORCAST_AUTH_SIGAUTH_TCP_BYTES_READ,
                legacy_len,
            ),
            (
                crate::auth::GAUGE_RAPTORCAST_AUTH_WIREAUTH_TCP_BYTES_READ,
                wireauth_len,
            ),
        ] {
            let value = exported
                .iter()
                .find(|(name, _, _)| *name == metric.name)
                .unwrap()
                .1;
            assert_eq!(value, expected, "{}", metric.name);
        }
        for (router, metric, expected) in [
            (
                &legacy,
                crate::auth::GAUGE_RAPTORCAST_AUTH_SIGAUTH_TCP_BYTES_WRITTEN,
                legacy_len,
            ),
            (
                &wireauth,
                crate::auth::GAUGE_RAPTORCAST_AUTH_WIREAUTH_TCP_BYTES_WRITTEN,
                wireauth_len,
            ),
        ] {
            let exported = router.metrics().into_inner();
            let value = exported
                .iter()
                .find(|(name, _, _)| *name == metric.name)
                .unwrap()
                .1;
            assert_eq!(value, expected, "{}", metric.name);
        }
    });
}

#[test]
fn test_tcp_router_disconnects_on_authentication_error() {
    run_test(async {
        use std::io::Write;

        use tokio::io::AsyncReadExt;

        for wireauth in [false, true] {
            let mut node = NodeInfo::new(43);
            let addr = if wireauth {
                node.wireauth_tcp_addr
            } else {
                node.sigauth_tcp_addr
            };
            let (mut router, _control) = create_router(
                &mut node,
                create_peer_discovery(&HashMap::new()),
                wireauth_config(),
            );
            let mut peer = std::net::TcpStream::connect(addr).unwrap();
            let mut frame = Vec::new();
            frame.extend_from_slice(&0x434e5353u32.to_le_bytes());
            frame.extend_from_slice(&1u32.to_le_bytes());
            frame.extend_from_slice(&1u64.to_le_bytes());
            frame.push(0);
            peer.write_all(&frame).unwrap();
            peer.set_nonblocking(true).unwrap();
            let mut peer = tokio::net::TcpStream::from_std(peer).unwrap();
            let mut byte = [0];
            let read = tokio::time::timeout(Duration::from_secs(2), async {
                tokio::select! {
                    result = peer.read(&mut byte) => result.unwrap(),
                    _ = router.next() => panic!("malformed authentication must not produce an event"),
                }
            }).await.unwrap();
            assert_eq!(read, 0, "router must close the offending connection");
        }
    });
}

#[test]
fn test_tcp_wireauth_buffer_limit() {
    let mut protocol =
        WireAuthProtocol::new(&TCP_METRICS, wireauth_config(), Arc::new(keypair(50)));
    let peer = keypair(51).pubkey();
    protocol
        .connect(&peer, "127.0.0.1:10000".parse().unwrap(), 0)
        .unwrap();
    for _ in 0..2 {
        protocol
            .buffer_message(&peer, Bytes::from(vec![0; 5 * 1024 * 1024]), None)
            .unwrap();
    }
    let (tx, rx) = futures::channel::oneshot::channel();
    let error = protocol
        .buffer_message(&peer, Bytes::from_static(b"x"), Some(tx))
        .unwrap_err();
    assert!(matches!(error, monad_wireauth::Error::BufferLimitExceeded {
        size, limit,
    } if size == 10 * 1024 * 1024 + 1 && limit == 10 * 1024 * 1024));
    assert!(rx.now_or_never().unwrap().is_err());
}

#[test]
fn test_tcp_wireauth_initiator_limit() {
    let mut protocol =
        WireAuthProtocol::new(&TCP_METRICS, wireauth_config(), Arc::new(keypair(202)));
    for seed in 1..=200 {
        let peer = keypair(seed).pubkey();
        let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, 10000 + u16::from(seed)));
        protocol.connect(&peer, addr, 0).unwrap();
    }
    let error = protocol
        .connect(
            &keypair(201).pubkey(),
            "127.0.0.1:10201".parse().unwrap(),
            0,
        )
        .unwrap_err();
    assert!(matches!(
        error,
        monad_wireauth::Error::TooManyInitiatedSessions { limit: 200 }
    ));
}

#[test]
fn test_tcp_wireauth_buffers_large_message_during_handshake() {
    run_test(async {
        let TestPair {
            sender,
            receiver,
            receiver_id,
            _controls,
            ..
        } = TestPair::with_options(true, wireauth_config(), Some(1));
        let payload = Bytes::from(vec![0x42; 2 * 1024 * 1024]);
        let (tx, rx) = futures::channel::oneshot::channel();
        let received = send_and_receive(
            sender,
            receiver,
            receiver_id,
            vec![(payload.clone(), Some(tx))],
        )
        .await;
        assert_eq!(received, [payload]);
        rx.await.expect("buffered large TCP message must complete");
    });
}
