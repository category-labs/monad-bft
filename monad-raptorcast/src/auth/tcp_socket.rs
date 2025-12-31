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
    future::Future,
    net::SocketAddr,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
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
use tracing::{debug, trace, warn};
use zerocopy::IntoBytes;

use super::{
    metrics::{
        GAUGE_RAPTORCAST_AUTH_SIGAUTH_TCP_BYTES_READ,
        GAUGE_RAPTORCAST_AUTH_SIGAUTH_TCP_BYTES_WRITTEN,
        GAUGE_RAPTORCAST_AUTH_WIREAUTH_TCP_BYTES_READ,
        GAUGE_RAPTORCAST_AUTH_WIREAUTH_TCP_BYTES_WRITTEN,
    },
    protocol::AuthenticationProtocol,
};

const SIGNATURE_SIZE: usize = 65;

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

    pub fn write(
        &mut self,
        addr: SocketAddr,
        payload: Bytes,
        completion: Option<futures::channel::oneshot::Sender<()>>,
    ) {
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
            metrics: ExecutorMetrics::default(),
        }
    }

    pub fn write_to_peer(
        &mut self,
        peer_id: &NodeId<CertificateSignaturePubKey<ST>>,
        payload: Bytes,
        completion: Option<futures::channel::oneshot::Sender<()>>,
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

        let public_key = peer_id.pubkey();
        if let (Some(authenticated), Some(wireauth_addr)) =
            (&mut self.authenticated, wireauth_tcp_addr)
        {
            if authenticated
                .auth_protocol
                .is_connected_public_key(&public_key)
            {
                self.metrics[GAUGE_RAPTORCAST_AUTH_WIREAUTH_TCP_BYTES_WRITTEN] +=
                    payload.len() as u64;
                authenticated.write(wireauth_addr, payload);
                return;
            }

            match authenticated.try_send_via_wireauth(&public_key, wireauth_addr, payload) {
                Ok(()) => return,
                Err(returned_payload) => {
                    self.metrics[GAUGE_RAPTORCAST_AUTH_SIGAUTH_TCP_BYTES_WRITTEN] +=
                        returned_payload.len() as u64;
                    self.signature_based
                        .write(tcp_addr, returned_payload, completion);
                    return;
                }
            }
        }

        self.metrics[GAUGE_RAPTORCAST_AUTH_SIGAUTH_TCP_BYTES_WRITTEN] += payload.len() as u64;
        self.signature_based.write(tcp_addr, payload, completion);
    }

    pub async fn recv(&mut self) -> Result<AuthRecvTcpMsg<AP::PublicKey>, DualTcpRecvError> {
        if let Some(authenticated) = &mut self.authenticated {
            tokio::select! {
                result = authenticated.recv() => {
                    match result {
                        Ok(msg) => {
                            self.metrics[GAUGE_RAPTORCAST_AUTH_WIREAUTH_TCP_BYTES_READ] += msg.payload.len() as u64;
                            Ok(msg)
                        }
                        Err(src_addr) => Err(DualTcpRecvError::WireAuth(src_addr)),
                    }
                },
                result = self.signature_based.recv() => {
                    match result {
                        Ok(msg) => {
                            self.metrics[GAUGE_RAPTORCAST_AUTH_SIGAUTH_TCP_BYTES_READ] += msg.payload.len() as u64;
                            Ok(msg)
                        }
                        Err((src_addr, err)) => Err(DualTcpRecvError::SigAuth(src_addr, err)),
                    }
                },
            }
        } else {
            match self.signature_based.recv().await {
                Ok(msg) => {
                    self.metrics[GAUGE_RAPTORCAST_AUTH_SIGAUTH_TCP_BYTES_READ] +=
                        msg.payload.len() as u64;
                    Ok(msg)
                }
                Err((src_addr, err)) => Err(DualTcpRecvError::SigAuth(src_addr, err)),
            }
        }
    }

    pub fn metrics(&self) -> ExecutorMetricsChain {
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
            let timer = AuthenticatedTimerFuture {
                auth_protocol: &mut self.auth_protocol,
                auth_timer: &mut self.auth_timer,
            };

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

    pub fn write(&mut self, addr: SocketAddr, payload: Bytes) {
        if let Some(encrypted) = self.encrypt_packet(addr, payload) {
            self.writer.write(
                encrypted.0,
                TcpMsg {
                    msg: encrypted.1,
                    completion: None,
                },
            );
        }
    }

    pub fn flush(&mut self) {
        while let Some((addr, packet)) = self.auth_protocol.next_packet() {
            self.write_auth_packet(addr, packet);
        }
    }

    pub fn try_send_via_wireauth(
        &mut self,
        public_key: &AP::PublicKey,
        wireauth_addr: SocketAddr,
        payload: Bytes,
    ) -> Result<(), Bytes> {
        if !self
            .auth_protocol
            .has_initiating_session_by_public_key(public_key)
        {
            if let Err(err) = self.auth_protocol.connect(
                public_key,
                wireauth_addr,
                monad_wireauth::DEFAULT_RETRY_ATTEMPTS,
            ) {
                warn!(?err, "failed to connect via wireauth");
                return Err(payload);
            }
            self.flush();
        }

        if let Err(err) = self.auth_protocol.buffer_message(public_key, payload) {
            warn!(?err, "failed to buffer tcp message");
        }
        Ok(())
    }

    fn encrypt_packet(
        &mut self,
        addr: SocketAddr,
        plaintext: Bytes,
    ) -> Option<(SocketAddr, Bytes)> {
        let header_size = AP::HEADER_SIZE as usize;
        let mut packet = BytesMut::with_capacity(header_size + plaintext.len());
        packet.resize(header_size, 0);
        packet.extend_from_slice(&plaintext);

        match self
            .auth_protocol
            .encrypt_by_socket(&addr, &mut packet[header_size..])
        {
            Ok(header) => {
                let header_bytes = header.as_bytes();
                packet[..header_size].copy_from_slice(header_bytes);
                Some((addr, packet.freeze()))
            }
            Err(e) => {
                warn!(addr=?addr, error=?e, "failed to encrypt tcp message");
                None
            }
        }
    }

    fn write_auth_packet(&self, addr: SocketAddr, packet: Bytes) {
        self.writer.write(
            addr,
            TcpMsg {
                msg: packet,
                completion: None,
            },
        );
    }
}

pub struct AuthenticatedTimerFuture<'a, AP: AuthenticationProtocol> {
    auth_protocol: &'a mut AP,
    auth_timer: &'a mut Option<(Pin<Box<Sleep>>, Instant)>,
}

impl<AP: AuthenticationProtocol> Future for AuthenticatedTimerFuture<'_, AP> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        trace!("polling tcp wireauth timer");

        loop {
            let Some(deadline) = self.auth_protocol.next_deadline() else {
                return Poll::Pending;
            };

            let now = Instant::now();
            if deadline <= now {
                if let Some(d) = now.checked_duration_since(deadline) {
                    if d > std::time::Duration::from_millis(100) {
                        warn!(delta_ms = d.as_millis(), "slow polling tcp wireauth timer");
                    }
                }

                self.auth_protocol.tick();
                return Poll::Ready(());
            }

            let should_update_timer = self
                .auth_timer
                .as_ref()
                .is_none_or(|(_, stored_deadline)| deadline < *stored_deadline);
            if should_update_timer {
                *self.auth_timer = Some((
                    Box::pin(tokio::time::sleep_until(deadline.into())),
                    deadline,
                ));
            }

            match self.auth_timer.as_mut() {
                Some((sleep, _)) => match sleep.as_mut().poll(cx) {
                    Poll::Ready(()) => *self.auth_timer = None,
                    Poll::Pending => return Poll::Pending,
                },
                None => return Poll::Pending,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        net::{Ipv4Addr, SocketAddr, SocketAddrV4, TcpListener},
        sync::{Arc, Mutex},
        time::Duration,
    };

    use bytes::Bytes;
    use monad_dataplane::DataplaneBuilder;
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
    use crate::auth::protocol::WireAuthProtocol;

    fn keypair(seed: u8) -> KeyPair {
        KeyPair::from_bytes(&mut [seed; 32]).unwrap()
    }

    fn find_tcp_free_port() -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").expect("failed to bind");
        listener.local_addr().expect("failed to get addr").port()
    }

    struct NodeInfo {
        keypair: Arc<KeyPair>,
        node_id:
            NodeId<monad_crypto::certificate_signature::CertificateSignaturePubKey<SecpSignature>>,
        sigauth_tcp_addr: SocketAddrV4,
        wireauth_tcp_addr: SocketAddrV4,
    }

    impl NodeInfo {
        fn new(seed: u8) -> Self {
            let kp = keypair(seed);
            let node_id = NodeId::new(kp.pubkey());
            Self {
                keypair: Arc::new(kp),
                node_id,
                sigauth_tcp_addr: SocketAddrV4::new(Ipv4Addr::LOCALHOST, find_tcp_free_port()),
                wireauth_tcp_addr: SocketAddrV4::new(Ipv4Addr::LOCALHOST, find_tcp_free_port()),
            }
        }

        fn create_name_record(&self, with_wireauth_tcp: bool) -> MonadNameRecord<SecpSignature> {
            let name_record = NameRecord::new_with_authentication(
                Ipv4Addr::LOCALHOST,
                self.sigauth_tcp_addr.port(),
                self.sigauth_tcp_addr.port(),
                self.sigauth_tcp_addr.port(),
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
        node: &NodeInfo,
        peer_discovery: Arc<Mutex<PeerDiscoveryDriver<NopDiscovery<SecpSignature>>>>,
    ) -> (
        DualTcpSocketHandle<SecpSignature, WireAuthProtocol, NopDiscovery<SecpSignature>>,
        monad_dataplane::DataplaneControl,
    ) {
        let mut dp = DataplaneBuilder::new(1000)
            .with_tcp_sockets([
                (
                    monad_dataplane::TcpSocketId::AuthenticatedRaptorcast,
                    SocketAddr::V4(node.wireauth_tcp_addr),
                ),
                (
                    monad_dataplane::TcpSocketId::Raptorcast,
                    SocketAddr::V4(node.sigauth_tcp_addr),
                ),
            ])
            .build();

        assert!(dp.block_until_ready(Duration::from_secs(1)));

        let wireauth_tcp = dp
            .tcp_sockets
            .take(monad_dataplane::TcpSocketId::AuthenticatedRaptorcast)
            .expect("wireauth tcp socket");
        let sigauth_tcp = dp
            .tcp_sockets
            .take(monad_dataplane::TcpSocketId::Raptorcast)
            .expect("sigauth tcp socket");

        let (wireauth_reader, wireauth_writer) = wireauth_tcp.split();
        let (sigauth_reader, sigauth_writer) = sigauth_tcp.split();

        let config = Config::default();
        let auth_protocol = WireAuthProtocol::new(config, node.keypair.clone());
        let authenticated_handle =
            AuthenticatedTcpSocketHandle::new(wireauth_reader, wireauth_writer, auth_protocol);
        let sig_auth = SigAuthTcpSocket::<SecpSignature>::new(
            sigauth_reader,
            sigauth_writer,
            node.keypair.clone(),
        );

        let socket = DualTcpSocketHandle::new(Some(authenticated_handle), sig_auth, peer_discovery);
        (socket, dp.control)
    }

    fn init_tracing() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(EnvFilter::from_default_env())
            .try_init();
    }

    #[test]
    fn test_dual_tcp_sigauth_only() {
        init_tracing();
        const NUM_MESSAGES: usize = 10;

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let local = tokio::task::LocalSet::new();

        local.block_on(&rt, async {
            let sender = NodeInfo::new(1);
            let receiver = NodeInfo::new(2);

            let mut name_records = HashMap::new();
            name_records.insert(sender.node_id.clone(), sender.create_name_record(false));
            name_records.insert(receiver.node_id.clone(), receiver.create_name_record(false));

            let create_pd = |records: HashMap<_, _>| {
                let builder = NopDiscoveryBuilder {
                    known_addresses: HashMap::new(),
                    name_records: records,
                    ..Default::default()
                };
                Arc::new(Mutex::new(PeerDiscoveryDriver::new(builder)))
            };

            let pd_sender = create_pd(name_records.clone());
            let pd_receiver = create_pd(name_records.clone());

            let (mut sender_socket, _sender_control) = create_dual_tcp_socket(&sender, pd_sender);
            let (mut receiver_socket, _receiver_control) =
                create_dual_tcp_socket(&receiver, pd_receiver);

            let (tx, mut rx) = tokio::sync::mpsc::channel::<Bytes>(NUM_MESSAGES);
            let receiver_id = receiver.node_id.clone();

            tokio::task::spawn_local(async move {
                for i in 0..NUM_MESSAGES {
                    let payload = Bytes::from(format!("sigauth_message_{}", i));
                    sender_socket.write_to_peer(&receiver_id, payload, None);
                }
            });

            tokio::task::spawn_local(async move {
                loop {
                    match receiver_socket.recv().await {
                        Ok(msg) => {
                            if tx.send(msg.payload).await.is_err() {
                                break;
                            }
                        }
                        Err(e) => {
                            tracing::warn!(?e, "receiver recv error");
                        }
                    }
                }
            });

            let mut received = Vec::new();
            let timeout = tokio::time::timeout(Duration::from_secs(5), async {
                while received.len() < NUM_MESSAGES {
                    if let Some(msg) = rx.recv().await {
                        received.push(msg);
                    }
                }
            })
            .await;

            assert!(timeout.is_ok(), "timeout: received={}", received.len());
            assert_eq!(received.len(), NUM_MESSAGES);

            for (i, msg) in received.iter().enumerate() {
                let payload = String::from_utf8_lossy(msg);
                assert!(
                    payload.starts_with("sigauth_message_"),
                    "unexpected payload {}: {}",
                    i,
                    payload
                );
            }
        });
    }

    #[test]
    fn test_dual_tcp_wireauth_only() {
        init_tracing();
        const NUM_MESSAGES: usize = 10;

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let local = tokio::task::LocalSet::new();

        local.block_on(&rt, async {
            let sender = NodeInfo::new(1);
            let receiver = NodeInfo::new(2);

            let mut name_records = HashMap::new();
            name_records.insert(sender.node_id.clone(), sender.create_name_record(true));
            name_records.insert(receiver.node_id.clone(), receiver.create_name_record(true));

            let create_pd = |records: HashMap<_, _>| {
                let builder = NopDiscoveryBuilder {
                    known_addresses: HashMap::new(),
                    name_records: records,
                    ..Default::default()
                };
                Arc::new(Mutex::new(PeerDiscoveryDriver::new(builder)))
            };

            let pd_sender = create_pd(name_records.clone());
            let pd_receiver = create_pd(name_records.clone());

            let (mut sender_socket, _sender_control) = create_dual_tcp_socket(&sender, pd_sender);
            let (mut receiver_socket, _receiver_control) =
                create_dual_tcp_socket(&receiver, pd_receiver);

            let (tx, mut rx) = tokio::sync::mpsc::channel::<Bytes>(NUM_MESSAGES);
            let (tx_stop, mut rx_stop) = tokio::sync::mpsc::channel::<()>(1);
            let receiver_id = receiver.node_id.clone();

            tokio::task::spawn_local(async move {
                for i in 0..NUM_MESSAGES {
                    let payload = Bytes::from(format!("wireauth_message_{}", i));
                    sender_socket.write_to_peer(&receiver_id, payload, None);
                }

                loop {
                    tokio::select! {
                        _ = rx_stop.recv() => break,
                        result = sender_socket.recv() => {
                            if let Err(e) = result {
                                tracing::warn!(?e, "sender recv error");
                            }
                        }
                    }
                }
            });

            tokio::task::spawn_local(async move {
                loop {
                    match receiver_socket.recv().await {
                        Ok(msg) => {
                            if tx.send(msg.payload).await.is_err() {
                                break;
                            }
                        }
                        Err(e) => {
                            tracing::warn!(?e, "receiver recv error");
                        }
                    }
                }
            });

            let mut received = Vec::new();
            let timeout = tokio::time::timeout(Duration::from_secs(10), async {
                while received.len() < NUM_MESSAGES {
                    if let Some(msg) = rx.recv().await {
                        received.push(msg);
                    }
                }
            })
            .await;

            let _ = tx_stop.send(()).await;

            assert!(timeout.is_ok(), "timeout: received={}", received.len());
            assert_eq!(received.len(), NUM_MESSAGES);

            for (i, msg) in received.iter().enumerate() {
                let payload = String::from_utf8_lossy(msg);
                assert!(
                    payload.starts_with("wireauth_message_"),
                    "unexpected payload {}: {}",
                    i,
                    payload
                );
            }
        });
    }
}
