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
    net::SocketAddr,
    pin::Pin,
    sync::{Arc, Mutex},
    time::Instant,
};

use bytes::{Bytes, BytesMut};
use monad_crypto::{
    certificate_signature::{
        CertificateSignature, CertificateSignaturePubKey, CertificateSignatureRecoverable,
    },
    signing_domain,
};
use monad_dataplane::{TcpMsg, TcpSocketReader, TcpSocketWriter};
use monad_executor::{ExecutorMetrics, ExecutorMetricsChain};
use monad_peer_discovery::{driver::PeerDiscoveryDriver, PeerDiscoveryAlgo};
use monad_types::NodeId;
use tokio::time::Sleep;
use tracing::{debug, warn};

use super::{
    common::{encrypt_packet, AuthenticatedTimerFuture},
    metrics::{
        init_socket_executor_metrics, GAUGE_RAPTORCAST_AUTH_SIGAUTH_TCP_BYTES_READ,
        GAUGE_RAPTORCAST_AUTH_SIGAUTH_TCP_BYTES_WRITTEN,
        GAUGE_RAPTORCAST_AUTH_WIREAUTH_TCP_BYTES_READ,
        GAUGE_RAPTORCAST_AUTH_WIREAUTH_TCP_BYTES_WRITTEN,
    },
    protocol::AuthenticationProtocol,
    DataplaneCompletion,
};
use crate::SIGNATURE_SIZE;

#[derive(Clone)]
pub struct AuthRecvTcpMsg<P> {
    pub src_addr: SocketAddr,
    pub payload: Bytes,
    pub from: P,
}

#[derive(Debug)]
pub enum SigAuthError {
    MessageTooShort,
    InvalidSignature,
    PubkeyRecoveryFailed,
}

#[derive(Debug)]
pub enum DualTcpRecvError {
    WireAuth(SocketAddr),
    SigAuth(SocketAddr, SigAuthError),
}

impl DualTcpRecvError {
    pub fn src_addr(&self) -> SocketAddr {
        match self {
            DualTcpRecvError::WireAuth(addr) => *addr,
            DualTcpRecvError::SigAuth(addr, _) => *addr,
        }
    }
}

pub struct SigAuthTcpSocket<ST>
where
    ST: CertificateSignatureRecoverable,
{
    reader: TcpSocketReader,
    writer: TcpSocketWriter,
    signing_key: Arc<ST::KeyPairType>,
}

impl<ST> SigAuthTcpSocket<ST>
where
    ST: CertificateSignatureRecoverable,
{
    pub fn new(
        reader: TcpSocketReader,
        writer: TcpSocketWriter,
        signing_key: Arc<ST::KeyPairType>,
    ) -> Self {
        Self {
            reader,
            writer,
            signing_key,
        }
    }

    pub fn write(&mut self, addr: SocketAddr, payload: Bytes, completion: DataplaneCompletion) {
        let mut signed_message = BytesMut::zeroed(SIGNATURE_SIZE + payload.len());
        let signature =
            <ST as CertificateSignature>::serialize(&ST::sign::<
                signing_domain::RaptorcastAppMessage,
            >(&payload, &self.signing_key));
        debug_assert_eq!(signature.len(), SIGNATURE_SIZE);
        signed_message[..SIGNATURE_SIZE].copy_from_slice(&signature);
        signed_message[SIGNATURE_SIZE..].copy_from_slice(&payload);

        self.writer.write(
            addr,
            TcpMsg {
                msg: signed_message.freeze(),
                completion,
            },
        );
    }

    pub async fn recv(
        &mut self,
    ) -> Result<AuthRecvTcpMsg<CertificateSignaturePubKey<ST>>, (SocketAddr, SigAuthError)> {
        let msg = self.reader.recv().await;
        let payload = msg.payload;
        let src_addr = msg.src_addr;

        if payload.len() < SIGNATURE_SIZE {
            return Err((src_addr, SigAuthError::MessageTooShort));
        }

        let signature_bytes = &payload[..SIGNATURE_SIZE];
        let signature = <ST as CertificateSignature>::deserialize(signature_bytes)
            .map_err(|_| (src_addr, SigAuthError::InvalidSignature))?;

        let app_message_bytes = payload.slice(SIGNATURE_SIZE..);
        let from = signature
            .recover_pubkey::<signing_domain::RaptorcastAppMessage>(app_message_bytes.as_ref())
            .map_err(|_| (src_addr, SigAuthError::PubkeyRecoveryFailed))?;

        Ok(AuthRecvTcpMsg {
            src_addr,
            payload: app_message_bytes,
            from,
        })
    }
}

pub struct DualTcpSocketHandle<ST, AP, PD>
where
    ST: CertificateSignatureRecoverable,
    AP: AuthenticationProtocol<PublicKey = CertificateSignaturePubKey<ST>>,
    PD: PeerDiscoveryAlgo<SignatureType = ST>,
{
    authenticated: Option<AuthenticatedTcpSocketHandle<AP>>,
    signature_based: SigAuthTcpSocket<ST>,
    peer_discovery: Arc<Mutex<PeerDiscoveryDriver<PD>>>,
    metrics: ExecutorMetrics,
}

impl<ST, AP, PD> DualTcpSocketHandle<ST, AP, PD>
where
    ST: CertificateSignatureRecoverable,
    AP: AuthenticationProtocol<PublicKey = CertificateSignaturePubKey<ST>>,
    PD: PeerDiscoveryAlgo<SignatureType = ST>,
{
    pub fn new(
        authenticated: Option<AuthenticatedTcpSocketHandle<AP>>,
        signature_based: SigAuthTcpSocket<ST>,
        peer_discovery: Arc<Mutex<PeerDiscoveryDriver<PD>>>,
    ) -> Self {
        Self {
            authenticated,
            signature_based,
            peer_discovery,
            metrics: init_socket_executor_metrics(),
        }
    }

    pub fn write_to_peer(
        &mut self,
        peer_id: &NodeId<CertificateSignaturePubKey<ST>>,
        payload: Bytes,
        completion: DataplaneCompletion,
    ) {
        let (tcp_addr, wireauth_tcp_addr) = {
            let pd_driver = self.peer_discovery.lock().unwrap();
            let Some(name_record) = pd_driver.get_name_record(peer_id) else {
                warn!(?peer_id, "tcp write_to_peer: name record unknown");
                return;
            };
            let tcp_addr = SocketAddr::V4(name_record.name_record.tcp_socket());
            let wireauth_addr = name_record
                .name_record
                .authenticated_tcp_socket()
                .map(SocketAddr::V4);
            (tcp_addr, wireauth_addr)
        };

        if let Some(wireauth_addr) = wireauth_tcp_addr {
            let public_key = peer_id.pubkey();
            let Some(authenticated) = &mut self.authenticated else {
                warn!(
                    ?peer_id,
                    "tcp write_to_peer: authenticated tcp is not configured"
                );
                return;
            };
            let payload_len = payload.len() as u64;
            let written = if let Some(session_addr) = authenticated
                .auth_protocol
                .get_socket_by_public_key(&public_key)
            {
                authenticated.write(session_addr, payload, completion)
            } else {
                authenticated.try_send_via_wireauth(&public_key, wireauth_addr, payload, completion)
            };
            if written {
                self.metrics
                    .gauge(GAUGE_RAPTORCAST_AUTH_WIREAUTH_TCP_BYTES_WRITTEN)
                    .add(payload_len);
            }
            return;
        }

        self.metrics
            .gauge(GAUGE_RAPTORCAST_AUTH_SIGAUTH_TCP_BYTES_WRITTEN)
            .add(payload.len() as u64);
        self.signature_based.write(tcp_addr, payload, completion);
    }

    pub async fn recv(&mut self) -> Result<AuthRecvTcpMsg<AP::PublicKey>, DualTcpRecvError> {
        if let Some(authenticated) = &mut self.authenticated {
            tokio::select! {
                result = authenticated.recv() => {
                    match result {
                        Ok(msg) => {
                            self.metrics
                                .gauge(GAUGE_RAPTORCAST_AUTH_WIREAUTH_TCP_BYTES_READ)
                                .add(msg.payload.len() as u64);
                            Ok(msg)
                        }
                        Err(src_addr) => Err(DualTcpRecvError::WireAuth(src_addr)),
                    }
                },
                result = self.signature_based.recv() => {
                    match result {
                        Ok(msg) => {
                            self.metrics
                                .gauge(GAUGE_RAPTORCAST_AUTH_SIGAUTH_TCP_BYTES_READ)
                                .add(msg.payload.len() as u64);
                            Ok(msg)
                        }
                        Err((src_addr, err)) => Err(DualTcpRecvError::SigAuth(src_addr, err)),
                    }
                },
            }
        } else {
            match self.signature_based.recv().await {
                Ok(msg) => {
                    self.metrics
                        .gauge(GAUGE_RAPTORCAST_AUTH_SIGAUTH_TCP_BYTES_READ)
                        .add(msg.payload.len() as u64);
                    Ok(msg)
                }
                Err((src_addr, err)) => Err(DualTcpRecvError::SigAuth(src_addr, err)),
            }
        }
    }

    pub fn metrics(&self) -> ExecutorMetricsChain<'_> {
        let mut chain = ExecutorMetricsChain::default().push(self.metrics.as_ref());
        if let Some(authenticated) = &self.authenticated {
            chain = chain.chain(authenticated.auth_protocol.metrics());
        }
        chain
    }
}

pub struct AuthenticatedTcpSocketHandle<AP>
where
    AP: AuthenticationProtocol,
{
    reader: TcpSocketReader,
    writer: TcpSocketWriter,
    pub(crate) auth_protocol: AP,
    auth_timer: Option<(Pin<Box<Sleep>>, Instant)>,
}

impl<AP> AuthenticatedTcpSocketHandle<AP>
where
    AP: AuthenticationProtocol,
    AP::PublicKey: Clone,
{
    pub fn new(reader: TcpSocketReader, writer: TcpSocketWriter, auth_protocol: AP) -> Self {
        Self {
            reader,
            writer,
            auth_protocol,
            auth_timer: None,
        }
    }

    pub async fn recv(&mut self) -> Result<AuthRecvTcpMsg<AP::PublicKey>, SocketAddr> {
        loop {
            let timer =
                AuthenticatedTimerFuture::new(&mut self.auth_protocol, &mut self.auth_timer);

            tokio::select! {
                () = timer => {
                    self.flush();
                    continue;
                }
                message = self.reader.recv() => {
                    let mut packet_buf = message.payload.to_vec();
                    match self.auth_protocol.dispatch(&mut packet_buf, message.src_addr) {
                        Ok(Some((plaintext, Some(from)))) => {
                            return Ok(AuthRecvTcpMsg {
                                src_addr: message.src_addr,
                                payload: plaintext,
                                from,
                            })
                        }
                        Ok(Some((_, None))) => {
                            warn!(addr=?message.src_addr, "received tcp data packet without public key");
                            self.flush();
                            continue;
                        }
                        Ok(None) => {
                            self.flush();
                            continue;
                        }
                        Err(e) => {
                            debug!(addr=?message.src_addr, error=?e, "failed to decrypt tcp message");
                            return Err(message.src_addr);
                        }
                    }
                }
            }
        }
    }

    pub fn write(
        &mut self,
        addr: SocketAddr,
        payload: Bytes,
        completion: DataplaneCompletion,
    ) -> bool {
        if let Some(encrypted) = self.encrypt_packet(addr, payload) {
            self.writer.write(
                encrypted.0,
                TcpMsg {
                    msg: encrypted.1,
                    completion,
                },
            );
            true
        } else {
            false
        }
    }

    pub fn flush(&mut self) {
        while let Some((addr, packet, completion)) = self.auth_protocol.next_packet() {
            self.write_auth_packet(addr, packet, completion);
        }
    }

    pub fn try_send_via_wireauth(
        &mut self,
        public_key: &AP::PublicKey,
        wireauth_addr: SocketAddr,
        payload: Bytes,
        completion: DataplaneCompletion,
    ) -> bool {
        if !self
            .auth_protocol
            .has_initiator_session_by_public_key(public_key)
        {
            if let Err(err) = self.auth_protocol.connect(
                public_key,
                wireauth_addr,
                monad_wireauth::DEFAULT_RETRY_ATTEMPTS,
            ) {
                warn!(?err, "failed to connect via wireauth");
                return false;
            }
            self.flush();
        }

        if let Err(err) = self
            .auth_protocol
            .buffer_message(public_key, payload, completion)
        {
            warn!(?err, "failed to buffer tcp message");
            return false;
        }
        true
    }

    fn encrypt_packet(
        &mut self,
        addr: SocketAddr,
        plaintext: Bytes,
    ) -> Option<(SocketAddr, Bytes)> {
        encrypt_packet(&mut self.auth_protocol, &addr, plaintext)
    }

    fn write_auth_packet(&self, addr: SocketAddr, packet: Bytes, completion: DataplaneCompletion) {
        self.writer.write(
            addr,
            TcpMsg {
                msg: packet,
                completion,
            },
        );
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        future::Future,
        net::{Ipv4Addr, SocketAddr, SocketAddrV4},
        sync::{Arc, Mutex},
        time::Duration,
    };

    use bytes::Bytes;
    use monad_crypto::certificate_signature::CertificateSignaturePubKey;
    use monad_dataplane::{DataplaneBuilder, DataplaneControl, TcpSocketHandle, TcpSocketId};
    use monad_peer_discovery::{
        driver::PeerDiscoveryDriver,
        mock::{NopDiscovery, NopDiscoveryBuilder},
        MonadNameRecord, NameRecord,
    };
    use monad_secp::{KeyPair, SecpSignature};
    use monad_types::NodeId;
    use monad_wireauth::Config;
    use tracing_subscriber::EnvFilter;

    use super::{AuthenticatedTcpSocketHandle, DualTcpSocketHandle, SigAuthTcpSocket};
    use crate::auth::{metrics::TCP_METRICS, protocol::WireAuthProtocol, DataplaneCompletion};

    fn keypair(seed: u8) -> KeyPair {
        KeyPair::from_bytes(&mut [seed; 32]).unwrap()
    }

    type PublicKey = CertificateSignaturePubKey<SecpSignature>;
    type Discovery = NopDiscovery<SecpSignature>;
    type DiscoveryDriver = Arc<Mutex<PeerDiscoveryDriver<Discovery>>>;
    type TestSocket = DualTcpSocketHandle<SecpSignature, WireAuthProtocol, Discovery>;
    type NameRecords = HashMap<NodeId<PublicKey>, MonadNameRecord<SecpSignature>>;

    struct NodeInfo {
        keypair: Arc<KeyPair>,
        node_id: NodeId<PublicKey>,
        sigauth_tcp_addr: SocketAddrV4,
        wireauth_tcp_addr: SocketAddrV4,
        sigauth_tcp: Option<TcpSocketHandle>,
        wireauth_tcp: Option<TcpSocketHandle>,
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

    fn create_dual_tcp_socket(
        node: &mut NodeInfo,
        peer_discovery: DiscoveryDriver,
        config: Config,
    ) -> (TestSocket, DataplaneControl) {
        let wireauth_tcp = node.wireauth_tcp.take().expect("unused wireauth socket");
        let sigauth_tcp = node.sigauth_tcp.take().expect("unused sigauth socket");

        let (wireauth_reader, wireauth_writer) = wireauth_tcp.split();
        let (sigauth_reader, sigauth_writer) = sigauth_tcp.split();

        let auth_protocol = WireAuthProtocol::new(&TCP_METRICS, config, node.keypair.clone());
        let authenticated_handle =
            AuthenticatedTcpSocketHandle::new(wireauth_reader, wireauth_writer, auth_protocol);
        let sig_auth = SigAuthTcpSocket::<SecpSignature>::new(
            sigauth_reader,
            sigauth_writer,
            node.keypair.clone(),
        );

        let socket = DualTcpSocketHandle::new(Some(authenticated_handle), sig_auth, peer_discovery);
        (
            socket,
            node.control.take().expect("unused dataplane control"),
        )
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
        sender: TestSocket,
        receiver: TestSocket,
        _controls: [DataplaneControl; 2],
    }

    impl TestPair {
        fn new(with_wireauth_tcp: bool) -> Self {
            Self::with_options(with_wireauth_tcp, Config::default(), None)
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

            let (sender_socket, sender_control) = create_dual_tcp_socket(
                &mut sender,
                create_peer_discovery(&name_records),
                sender_config,
            );
            let (receiver_socket, receiver_control) = create_dual_tcp_socket(
                &mut receiver,
                create_peer_discovery(&name_records),
                Config::default(),
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
        mut sender: TestSocket,
        mut receiver: TestSocket,
        receiver_id: NodeId<PublicKey>,
        outbound: Vec<(Bytes, DataplaneCompletion)>,
    ) -> Vec<Bytes> {
        let message_count = outbound.len();
        let drive_sender = async {
            for (payload, completion) in outbound {
                sender.write_to_peer(&receiver_id, payload, completion);
            }
            loop {
                if let Err(err) = sender.recv().await {
                    tracing::warn!(?err, "sender recv error");
                }
            }
        };
        let collect = async {
            let mut received = Vec::with_capacity(message_count);
            while received.len() < message_count {
                match receiver.recv().await {
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
    fn test_dual_tcp_sigauth_only() {
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
    fn test_dual_tcp_wireauth_only() {
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
    fn test_dual_tcp_wireauth_bidirectional() {
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
                    socket_a.write_to_peer(&node_b_id, Bytes::from("init_from_a"), None);
                    loop {
                        match socket_a.recv().await {
                            Ok(message) => break message.payload,
                            Err(err) => tracing::warn!(?err, "node_a recv error"),
                        }
                    }
                };
                let node_b = async {
                    loop {
                        match socket_b.recv().await {
                            Ok(message) => {
                                socket_b.write_to_peer(
                                    &node_a_id,
                                    Bytes::from("reply_from_b"),
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
            } = TestPair::with_options(true, Config::default(), Some(1));
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
                ..Config::default()
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

            sender_socket.write_to_peer(&receiver_id, payload, Some(completion_tx));

            assert!(completion_rx.await.is_err());
            assert!(
                tokio::time::timeout(Duration::from_millis(100), receiver_socket.recv())
                    .await
                    .is_err(),
                "buffer failure must not send via sigauth"
            );
        });
    }
}
