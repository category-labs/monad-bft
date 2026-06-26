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
    task::{Context, Poll},
    time::Instant,
};

use bytes::{Bytes, BytesMut};
use monad_crypto::certificate_signature::PubKey;
use monad_dataplane::{UdpSocketHandle, UnicastMsg};
use monad_executor::{ExecutorMetrics, ExecutorMetricsChain};
use monad_peer_discovery::NameRecord;
use monad_types::{NodeId, UdpPriority};
use thiserror::Error;
use tokio::time::Sleep;
use tracing::{debug, trace, warn};
use zerocopy::IntoBytes;

use super::{
    framing::AuthPacketFramer,
    metrics::{
        init_socket_executor_metrics, GAUGE_RAPTORCAST_AUTH_AUTHENTICATED_UDP_BYTES_READ,
        GAUGE_RAPTORCAST_AUTH_AUTHENTICATED_UDP_BYTES_WRITTEN,
        GAUGE_RAPTORCAST_AUTH_NON_AUTHENTICATED_UDP_BYTES_READ,
        GAUGE_RAPTORCAST_AUTH_NON_AUTHENTICATED_UDP_BYTES_WRITTEN,
    },
    protocol::AuthenticationProtocol,
};

// Wireauth always sends the initial connect packet. This disables additional
// timer retries because later sends can initiate another connection attempt.
pub(crate) const AUTH_SESSION_CONNECT_RETRY_ATTEMPTS: u64 = 0;

#[derive(Clone)]
pub struct AuthRecvMsg<P: PubKey> {
    pub src_addr: SocketAddr,
    pub payload: Bytes,
    pub stride: u16,
    pub sender: Option<NodeId<P>>,
}

pub struct DualSocketHandle<AP>
where
    AP: AuthenticationProtocol,
{
    authenticated: AuthenticatedSocketHandle<AP>,
    non_authenticated: Option<UdpSocketHandle>,
    metrics: ExecutorMetrics,
}

enum NameRecordDeliveryMethod {
    Authenticated,
    NonAuthenticatedFallback { udp_addr: SocketAddr },
}

impl<AP> DualSocketHandle<AP>
where
    AP: AuthenticationProtocol,
{
    pub fn new(
        authenticated: AuthenticatedSocketHandle<AP>,
        non_authenticated: Option<UdpSocketHandle>,
    ) -> Self {
        Self {
            authenticated,
            non_authenticated,
            metrics: init_socket_executor_metrics(),
        }
    }

    pub fn write_unicast_with_priority(&mut self, msg: UnicastMsg, priority: UdpPriority) {
        let mut auth_msgs = Vec::new();
        let mut non_auth_msgs = Vec::new();
        let mut auth_bytes = 0u64;
        let mut non_auth_bytes = 0u64;

        for (addr, payload) in msg.msgs {
            if self.authenticated.is_connected_socket(&addr) {
                auth_bytes += payload.len() as u64;
                auth_msgs.push((addr, payload));
                continue;
            }
            non_auth_bytes += payload.len() as u64;
            non_auth_msgs.push((addr, payload));
        }

        if !auth_msgs.is_empty() {
            self.metrics
                .gauge(GAUGE_RAPTORCAST_AUTH_AUTHENTICATED_UDP_BYTES_WRITTEN)
                .add(auth_bytes);
            self.authenticated.write_unicast_with_priority(
                UnicastMsg {
                    msgs: auth_msgs,
                    stride: msg.stride,
                },
                priority,
            );
        }

        if !non_auth_msgs.is_empty() {
            if let Some(non_authenticated) = &self.non_authenticated {
                self.metrics
                    .gauge(GAUGE_RAPTORCAST_AUTH_NON_AUTHENTICATED_UDP_BYTES_WRITTEN)
                    .add(non_auth_bytes);
                non_authenticated.write_unicast_with_priority(
                    UnicastMsg {
                        msgs: non_auth_msgs,
                        stride: msg.stride,
                    },
                    priority,
                );
            } else {
                debug!(
                    dropped = non_auth_msgs.len(),
                    "dropping UDP messages without a non-authenticated socket"
                );
            }
        }
    }

    pub fn write_to_name_record(
        &mut self,
        public_key: &AP::PublicKey,
        name_record: &NameRecord,
        msg: Bytes,
        stride: u16,
        priority: UdpPriority,
    ) {
        let auth_addr = SocketAddr::V4(name_record.authenticated_udp_socket());
        let has_authenticated_socket =
            self.is_connected_socket_and_public_key(&auth_addr, public_key);
        let has_initiator_session = self
            .authenticated
            .auth_protocol
            .has_initiator_session_by_socket_and_public_key(&auth_addr, public_key);
        let non_authenticated_addr = self
            .non_authenticated
            .is_some()
            .then(|| name_record.udp_socket().map(SocketAddr::V4))
            .flatten();
        let needs_connect = !has_authenticated_socket && !has_initiator_session;
        let delivery_method = match non_authenticated_addr.filter(|_| !has_authenticated_socket) {
            Some(udp_addr) => NameRecordDeliveryMethod::NonAuthenticatedFallback { udp_addr },
            None => NameRecordDeliveryMethod::Authenticated,
        };

        match delivery_method {
            NameRecordDeliveryMethod::NonAuthenticatedFallback { udp_addr } => {
                self.write_unicast_with_priority(
                    UnicastMsg {
                        stride,
                        msgs: vec![(udp_addr, msg)],
                    },
                    priority,
                );

                if needs_connect {
                    if let Err(error) =
                        self.connect(public_key, auth_addr, AUTH_SESSION_CONNECT_RETRY_ATTEMPTS)
                    {
                        warn!(
                            ?public_key,
                            auth_addr = ?auth_addr,
                            ?error,
                            "failed to initiate authenticated UDP session"
                        );
                    }
                    self.flush();
                }
            }
            NameRecordDeliveryMethod::Authenticated => {
                if needs_connect {
                    if let Err(error) =
                        self.connect(public_key, auth_addr, AUTH_SESSION_CONNECT_RETRY_ATTEMPTS)
                    {
                        warn!(
                            ?public_key,
                            auth_addr = ?auth_addr,
                            ?error,
                            "failed to initiate authenticated UDP session"
                        );
                    }
                    self.flush();
                }

                let msg_len = msg.len() as u64;
                if self
                    .authenticated
                    .write_with_buffering(public_key, msg, stride, priority)
                    .is_err()
                {
                    warn!(
                        ?public_key,
                        "failed to write or buffer authenticated UDP packet"
                    );
                } else {
                    self.metrics
                        .gauge(GAUGE_RAPTORCAST_AUTH_AUTHENTICATED_UDP_BYTES_WRITTEN)
                        .add(msg_len);
                }
            }
        }
    }
    pub fn connect(
        &mut self,
        remote_public_key: &AP::PublicKey,
        remote_addr: SocketAddr,
        retry_attempts: u64,
    ) -> Result<(), AP::Error> {
        self.authenticated
            .connect(remote_public_key, remote_addr, retry_attempts)
    }

    pub fn disconnect(&mut self, remote_public_key: &AP::PublicKey) {
        self.authenticated.disconnect(remote_public_key);
    }

    pub fn flush(&mut self) {
        self.authenticated.flush();
    }

    pub async fn recv(&mut self) -> Result<AuthRecvMsg<AP::PublicKey>, AP::Error> {
        if let Some(non_authenticated) = &mut self.non_authenticated {
            tokio::select! {
                result = self.authenticated.recv() => {
                    if let Ok(ref msg) = result {
                        self.metrics
                            .gauge(GAUGE_RAPTORCAST_AUTH_AUTHENTICATED_UDP_BYTES_READ)
                            .add(msg.payload.len() as u64);
                    }
                    result
                }
                msg = non_authenticated.recv() => {
                    self.metrics
                        .gauge(GAUGE_RAPTORCAST_AUTH_NON_AUTHENTICATED_UDP_BYTES_READ)
                        .add(msg.payload.len() as u64);
                    Ok(AuthRecvMsg {
                        src_addr: msg.src_addr,
                        payload: msg.payload,
                        stride: msg.stride,
                        sender: None,
                    })
                }
            }
        } else {
            let result = self.authenticated.recv().await;
            if let Ok(ref msg) = result {
                self.metrics
                    .gauge(GAUGE_RAPTORCAST_AUTH_AUTHENTICATED_UDP_BYTES_READ)
                    .add(msg.payload.len() as u64);
            }
            result
        }
    }

    pub fn is_connected_socket_and_public_key(
        &self,
        socket_addr: &SocketAddr,
        public_key: &AP::PublicKey,
    ) -> bool {
        self.authenticated
            .is_connected_socket_and_public_key(socket_addr, public_key)
    }

    pub fn get_socket_by_public_key(&self, public_key: &AP::PublicKey) -> Option<SocketAddr> {
        self.authenticated.get_socket_by_public_key(public_key)
    }

    pub fn has_any_session_by_public_key(&self, public_key: &AP::PublicKey) -> bool {
        self.authenticated.has_any_session_by_public_key(public_key)
    }

    pub fn segment_size(&self, mtu: u16) -> u16 {
        monad_dataplane::udp::segment_size_for_mtu(mtu) - AP::HEADER_SIZE
    }

    pub fn metrics(&self) -> ExecutorMetricsChain<'_> {
        ExecutorMetricsChain::default()
            .push(self.metrics.as_ref())
            .chain(self.authenticated.metrics())
    }
}

pub struct AuthenticatedSocketHandle<AP>
where
    AP: AuthenticationProtocol,
{
    socket: UdpSocketHandle,
    auth_protocol: AP,
    auth_timer: Option<(Pin<Box<Sleep>>, Instant)>,
}

impl<AP> AuthenticatedSocketHandle<AP>
where
    AP: AuthenticationProtocol,
{
    pub fn new(socket: UdpSocketHandle, auth_protocol: AP) -> Self {
        Self {
            socket,
            auth_protocol,
            auth_timer: None,
        }
    }

    pub async fn recv(&mut self) -> Result<AuthRecvMsg<AP::PublicKey>, AP::Error> {
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
                message = self.socket.recv() => {
                    let mut packet_buf = message.payload.to_vec();
                    match self.auth_protocol.dispatch(&mut packet_buf, message.src_addr) {
                        Ok(Some((plaintext, auth_public_key))) => {
                            return Ok(AuthRecvMsg {
                                src_addr: message.src_addr,
                                payload: plaintext,
                                stride: message.stride,
                                sender: auth_public_key.map(NodeId::new),
                            })
                        }
                        Ok(None) => {
                            self.flush();
                            continue;
                        }
                        Err(e) => {
                            debug!(addr=?message.src_addr, error=?e, "failed to decrypt message");
                            return Err(e);
                        }
                    }
                }
            }
        }
    }

    pub fn write_unicast_with_priority(&mut self, msg: UnicastMsg, priority: UdpPriority) {
        let stride = msg.stride as usize;
        for (addr, mut chunk) in msg.msgs {
            while !chunk.is_empty() {
                let piece = chunk.split_to(chunk.len().min(stride));
                let piece_len = piece.len() as u16;
                if let Some(encrypted) = self.encrypt_packet(addr, piece) {
                    self.socket.write_unicast_with_priority(
                        UnicastMsg {
                            msgs: vec![encrypted],
                            stride: piece_len + AP::HEADER_SIZE,
                        },
                        priority,
                    );
                }
            }
        }
    }

    /// Returns `Err(())` if the message was not written.
    #[allow(clippy::result_unit_err)]
    pub fn write_by_public_key_with_priority(
        &mut self,
        public_key: &AP::PublicKey,
        plaintext: Bytes,
        priority: UdpPriority,
    ) -> Result<(), ()> {
        if !self.auth_protocol.is_connected_public_key(public_key) {
            return Err(());
        }

        let Some(addr) = self.auth_protocol.get_socket_by_public_key(public_key) else {
            warn!("failed to find socket for connected public key");
            return Err(());
        };

        let Some(packet) = self.encrypt_packet_by_public_key(public_key, plaintext) else {
            return Err(());
        };

        debug_assert!(
            !packet.is_empty(),
            "authenticated packets should never be empty"
        );
        debug_assert!(
            packet.len() <= u16::MAX as usize,
            "authenticated packets should fit in u16 stride"
        );
        let stride = packet.len() as u16;
        self.socket.write_unicast_with_priority(
            UnicastMsg {
                msgs: vec![(addr, packet)],
                stride,
            },
            priority,
        );
        Ok(())
    }

    /// Returns `Err(())` if the message was not written or buffered.
    #[allow(clippy::result_unit_err)]
    pub fn write_with_buffering(
        &mut self,
        public_key: &AP::PublicKey,
        mut plaintext: Bytes,
        stride: u16,
        priority: UdpPriority,
    ) -> Result<(), ()> {
        let has_authenticated_socket = self.auth_protocol.is_connected_public_key(public_key);
        let has_initiator_session = self
            .auth_protocol
            .has_initiator_session_by_public_key(public_key);

        if !has_authenticated_socket && !has_initiator_session {
            return Err(());
        }

        if plaintext.is_empty() {
            return Ok(());
        }

        let stride = usize::from(stride);
        while !plaintext.is_empty() {
            let piece = plaintext.split_to(plaintext.len().min(stride));

            if has_authenticated_socket {
                self.write_by_public_key_with_priority(public_key, piece, priority)?;
                continue;
            }

            if let Err(err) = self.auth_protocol.buffer_message(public_key, piece) {
                warn!(error=?err, "failed to buffer message");
                return Err(());
            }
        }

        Ok(())
    }

    pub fn is_connected_socket(&self, socket_addr: &SocketAddr) -> bool {
        self.auth_protocol.is_connected_socket(socket_addr)
    }

    pub fn is_connected_socket_and_public_key(
        &self,
        socket_addr: &SocketAddr,
        public_key: &AP::PublicKey,
    ) -> bool {
        self.auth_protocol
            .is_connected_socket_and_public_key(socket_addr, public_key)
    }

    pub fn get_socket_by_public_key(&self, public_key: &AP::PublicKey) -> Option<SocketAddr> {
        self.auth_protocol.get_socket_by_public_key(public_key)
    }

    pub fn has_any_session_by_public_key(&self, public_key: &AP::PublicKey) -> bool {
        self.auth_protocol.has_any_session_by_public_key(public_key)
    }

    pub fn connect(
        &mut self,
        remote_public_key: &AP::PublicKey,
        remote_addr: SocketAddr,
        retry_attempts: u64,
    ) -> Result<(), AP::Error> {
        self.auth_protocol
            .connect(remote_public_key, remote_addr, retry_attempts)
    }

    pub fn disconnect(&mut self, remote_public_key: &AP::PublicKey) {
        self.auth_protocol.disconnect(remote_public_key);
    }

    pub fn flush(&mut self) {
        while let Some((addr, packet)) = self.auth_protocol.next_packet() {
            self.write_auth_packet(addr, packet);
        }
    }

    pub fn timer(&mut self) -> AuthenticatedTimerFuture<'_, AP> {
        AuthenticatedTimerFuture {
            auth_protocol: &mut self.auth_protocol,
            auth_timer: &mut self.auth_timer,
        }
    }

    pub fn metrics(&self) -> ExecutorMetricsChain<'_> {
        self.auth_protocol.metrics()
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
            Err(err) => {
                warn!(addr=?addr, error=?err, "failed to encrypt message");
                None
            }
        }
    }

    fn encrypt_packet_by_public_key(
        &mut self,
        public_key: &AP::PublicKey,
        plaintext: Bytes,
    ) -> Option<Bytes> {
        let header_size = AP::HEADER_SIZE as usize;
        let mut packet = BytesMut::with_capacity(header_size + plaintext.len());
        packet.resize(header_size, 0);
        packet.extend_from_slice(&plaintext);

        match self
            .auth_protocol
            .encrypt_by_public_key(public_key, &mut packet[header_size..])
        {
            Ok(header) => {
                let header_bytes = header.as_bytes();
                packet[..header_size].copy_from_slice(header_bytes);
                Some(packet.freeze())
            }
            Err(err) => {
                warn!(error=?err, "failed to encrypt message");
                None
            }
        }
    }

    fn write_auth_packet(&self, addr: SocketAddr, packet: Bytes) {
        let stride = packet.len() as u16;
        self.socket.write_unicast_with_priority(
            UnicastMsg {
                msgs: vec![(addr, packet)],
                stride,
            },
            UdpPriority::High,
        );
    }
}

pub struct FramedAuthenticatedSocketHandle<AP, F>
where
    AP: AuthenticationProtocol,
    F: AuthPacketFramer<AP::PublicKey, Decoded = Bytes>,
{
    socket: AuthenticatedSocketHandle<AP>,
    framer: F,
}

#[derive(Debug, Error)]
pub enum FramedRecvError<E: std::fmt::Debug> {
    #[error("authenticated socket error: {0:?}")]
    Auth(E),
    #[error("received unauthenticated message on framed socket")]
    MissingAuthPublicKey,
}

impl<AP, F> FramedAuthenticatedSocketHandle<AP, F>
where
    AP: AuthenticationProtocol,
    F: AuthPacketFramer<AP::PublicKey, Decoded = Bytes>,
{
    pub fn new(socket: AuthenticatedSocketHandle<AP>, framer: F) -> Self {
        Self { socket, framer }
    }

    pub async fn recv(&mut self) -> Result<AuthRecvMsg<AP::PublicKey>, FramedRecvError<AP::Error>> {
        loop {
            let msg = self.socket.recv().await.map_err(FramedRecvError::Auth)?;
            let Some(sender) = msg.sender else {
                return Err(FramedRecvError::MissingAuthPublicKey);
            };

            match self.framer.deframe(sender.pubkey(), msg.payload) {
                Ok(Some(payload)) => {
                    return Ok(AuthRecvMsg {
                        src_addr: msg.src_addr,
                        payload,
                        sender: Some(sender),
                        stride: msg.stride,
                    });
                }
                Ok(None) => continue,
                Err(err) => {
                    debug!(addr=?msg.src_addr, error=?err, "failed to decode authenticated packet");
                    continue;
                }
            }
        }
    }

    /// Returns `Err(())` if the message was not written.
    #[allow(clippy::result_unit_err)]
    pub fn write(
        &mut self,
        public_key: &AP::PublicKey,
        plaintext: Bytes,
        priority: UdpPriority,
    ) -> Result<(), ()> {
        let packets = match self.framer.frame(plaintext) {
            Ok(packets) => packets,
            Err(err) => {
                warn!(error=?err, "failed to encode message");
                return Err(());
            }
        };

        for packet in packets {
            self.socket
                .write_by_public_key_with_priority(public_key, packet, priority)?;
        }

        Ok(())
    }

    /// Returns `Err(())` if the message was not written or buffered.
    #[allow(clippy::result_unit_err)]
    pub fn write_buffered(
        &mut self,
        public_key: &AP::PublicKey,
        plaintext: Bytes,
        priority: UdpPriority,
    ) -> Result<(), ()> {
        let packets = match self.framer.frame(plaintext) {
            Ok(packets) => packets,
            Err(err) => {
                warn!(error=?err, "failed to encode message");
                return Err(());
            }
        };

        for packet in packets {
            let stride = packet.len() as u16;
            self.socket
                .write_with_buffering(public_key, packet, stride, priority)?;
        }

        Ok(())
    }

    pub fn is_connected_socket(&self, socket_addr: &SocketAddr) -> bool {
        self.socket.is_connected_socket(socket_addr)
    }

    pub fn is_connected_socket_and_public_key(
        &self,
        socket_addr: &SocketAddr,
        public_key: &AP::PublicKey,
    ) -> bool {
        self.socket
            .is_connected_socket_and_public_key(socket_addr, public_key)
    }

    pub fn get_socket_by_public_key(&self, public_key: &AP::PublicKey) -> Option<SocketAddr> {
        self.socket.get_socket_by_public_key(public_key)
    }

    pub fn connect(
        &mut self,
        remote_public_key: &AP::PublicKey,
        remote_addr: SocketAddr,
        retry_attempts: u64,
    ) -> Result<(), AP::Error> {
        self.socket
            .connect(remote_public_key, remote_addr, retry_attempts)
    }

    pub fn disconnect(&mut self, remote_public_key: &AP::PublicKey) {
        self.socket.disconnect(remote_public_key);
    }

    pub fn flush(&mut self) {
        self.socket.flush();
    }

    pub fn timer(&mut self) -> AuthenticatedTimerFuture<'_, AP> {
        self.socket.timer()
    }

    pub fn has_any_session_by_public_key(&self, public_key: &AP::PublicKey) -> bool {
        self.socket.has_any_session_by_public_key(public_key)
    }

    pub fn metrics(&self) -> ExecutorMetricsChain<'_> {
        self.socket.metrics().chain(self.framer.metrics())
    }
}

pub struct AuthenticatedTimerFuture<'a, AP: AuthenticationProtocol> {
    auth_protocol: &'a mut AP,
    auth_timer: &'a mut Option<(Pin<Box<Sleep>>, Instant)>,
}

impl<AP: AuthenticationProtocol> Future for AuthenticatedTimerFuture<'_, AP> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        trace!("polling wireauth timer");

        loop {
            let Some(deadline) = self.auth_protocol.next_deadline() else {
                return Poll::Pending;
            };

            let now = Instant::now();
            if deadline <= now {
                if let Some(d) = now.checked_duration_since(deadline) {
                    if d > std::time::Duration::from_millis(100) {
                        warn!(delta_ms = d.as_millis(), "slow polling wireauth timer");
                    }
                }

                self.auth_protocol.tick();
                return Poll::Ready(());
            }

            // wireauth internal timers are expected to be updated
            // for example initially session with have long timer set to session_timeout
            // after fully establishing session, keapalive_interval will be set to a shorter duration
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
        convert::Infallible,
        net::{Ipv4Addr, SocketAddr, SocketAddrV4},
        pin::pin,
        sync::Arc,
        task::Poll,
        time::Duration,
    };

    use bytes::Bytes;
    use futures::poll;
    use monad_dataplane::{DataplaneBuilder, UnicastMsg};
    use monad_peer_discovery::NameRecord;
    use monad_secp::KeyPair;
    use monad_types::UdpPriority;
    use monad_wireauth::{Config, DEFAULT_RETRY_ATTEMPTS};
    use tracing_subscriber::EnvFilter;

    use super::{
        AuthenticatedSocketHandle, DualSocketHandle, FramedAuthenticatedSocketHandle,
        FramedRecvError,
    };
    use crate::auth::{
        framing::AuthPacketFramer,
        metrics::GAUGE_RAPTORCAST_AUTH_AUTHENTICATED_UDP_BYTES_WRITTEN,
        protocol::{NoopAuthProtocol, WireAuthProtocol},
    };

    struct PanicFramer;

    impl AuthPacketFramer<monad_secp::PubKey> for PanicFramer {
        type Decoded = Bytes;
        type Error = Infallible;

        fn frame(&mut self, _payload: Bytes) -> Result<impl Iterator<Item = Bytes>, Self::Error> {
            Ok(std::iter::empty())
        }

        fn deframe(
            &mut self,
            _public_key: monad_secp::PubKey,
            _packet: Bytes,
        ) -> Result<Option<Self::Decoded>, Self::Error> {
            unreachable!("framer should not be called for unauthenticated messages");
        }
    }

    fn init_tracing() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(EnvFilter::from_default_env())
            .try_init();
    }

    fn keypair(seed: u8) -> KeyPair {
        KeyPair::from_bytes(&mut [seed; 32]).unwrap()
    }

    struct PeerNode {
        socket: DualSocketHandle<WireAuthProtocol>,
        auth_addr: SocketAddr,
        non_auth_addr: Option<SocketAddr>,
        public_key: monad_secp::PubKey,
        _tcp_socket: monad_dataplane::TcpSocketHandle,
        _control: monad_dataplane::DataplaneControl,
    }

    impl PeerNode {
        fn new(seed: u8, with_non_authenticated: bool) -> Self {
            let bind_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();

            let mut udp_sockets = vec![(
                monad_dataplane::UdpSocketId::AuthenticatedRaptorcast,
                bind_addr,
            )];
            if with_non_authenticated {
                udp_sockets.push((monad_dataplane::UdpSocketId::Raptorcast, bind_addr));
            }

            let mut dp = DataplaneBuilder::new(1000)
                .with_tcp_sockets([(monad_dataplane::TcpSocketId::Raptorcast, bind_addr)])
                .with_udp_sockets(udp_sockets)
                .build();

            let tcp_socket = dp
                .tcp_sockets
                .take(monad_dataplane::TcpSocketId::Raptorcast)
                .expect("tcp socket");
            let authenticated_socket = dp
                .udp_sockets
                .take(monad_dataplane::UdpSocketId::AuthenticatedRaptorcast)
                .expect("authenticated socket");
            let non_authenticated_socket = dp
                .udp_sockets
                .take(monad_dataplane::UdpSocketId::Raptorcast);
            let control = dp.control.clone();

            let auth_addr = authenticated_socket.local_addr();
            let non_auth_addr = non_authenticated_socket
                .as_ref()
                .map(monad_dataplane::UdpSocketHandle::local_addr);

            let keypair = keypair(seed);
            let public_key = keypair.pubkey();
            let config = Config::default();
            let auth_protocol = WireAuthProtocol::new(
                &crate::auth::metrics::UDP_METRICS,
                config,
                Arc::new(keypair),
            );
            let authenticated_handle =
                AuthenticatedSocketHandle::new(authenticated_socket, auth_protocol);
            let socket = DualSocketHandle::new(authenticated_handle, non_authenticated_socket);

            Self {
                socket,
                auth_addr,
                non_auth_addr,
                public_key,
                _tcp_socket: tcp_socket,
                _control: control,
            }
        }

        fn connect(&mut self, peer_public: &monad_secp::PubKey, peer_addr: SocketAddr) {
            self.socket
                .connect(peer_public, peer_addr, DEFAULT_RETRY_ATTEMPTS)
                .expect("connect failed");
            self.socket.flush();
        }

        fn write_message(&mut self, dest: SocketAddr, message: &[u8]) {
            self.socket.write_unicast_with_priority(
                UnicastMsg {
                    msgs: vec![(dest, Bytes::copy_from_slice(message))],
                    stride: message.len() as u16,
                },
                UdpPriority::Regular,
            );
        }
    }

    async fn exchange_handshake(peer1: &mut PeerNode, peer2: &mut PeerNode) {
        let timeout = Duration::from_secs(3);
        let start = std::time::Instant::now();

        while start.elapsed() < timeout {
            tokio::select! {
                result1 = tokio::time::timeout(Duration::from_millis(100), peer1.socket.recv()) => {
                    if let Ok(Ok(msg)) = result1 {
                        tracing::info!(src=?msg.src_addr, len=msg.payload.len(), "peer1 received");
                    }
                }
                result2 = tokio::time::timeout(Duration::from_millis(100), peer2.socket.recv()) => {
                    if let Ok(Ok(msg)) = result2 {
                        tracing::info!(src=?msg.src_addr, len=msg.payload.len(), "peer2 received");
                    }
                }
                _ = tokio::time::sleep(Duration::from_millis(100)) => {
                    if peer1.socket.is_connected_socket_and_public_key(&peer2.auth_addr, &peer2.public_key) {
                        tracing::info!("handshake complete");
                        break;
                    }
                }
            }
        }
    }

    #[tokio::test]
    async fn test_e2e_bidirectional() {
        init_tracing();

        let mut alice = PeerNode::new(1, true);
        let mut bob = PeerNode::new(2, true);

        let bob_addr = bob.auth_addr;
        let alice_addr = alice.auth_addr;

        alice.connect(&bob.public_key, bob_addr);

        exchange_handshake(&mut alice, &mut bob).await;

        alice.write_message(bob_addr, b"hello from alice");

        let received_bob = tokio::time::timeout(Duration::from_secs(2), bob.socket.recv())
            .await
            .expect("timeout waiting for bob")
            .expect("bob received");
        assert_eq!(&received_bob.payload[..], b"hello from alice");
        assert_eq!(received_bob.src_addr, alice_addr);

        bob.write_message(alice_addr, b"hello from bob");

        let received_alice = tokio::time::timeout(Duration::from_secs(2), alice.socket.recv())
            .await
            .expect("timeout waiting for alice")
            .expect("alice received");
        assert_eq!(&received_alice.payload[..], b"hello from bob");
        assert_eq!(received_alice.src_addr, bob_addr);
    }

    #[tokio::test]
    async fn test_e2e_bidirectional_without_non_authenticated_socket() {
        init_tracing();

        let mut alice = PeerNode::new(1, false);
        let mut bob = PeerNode::new(2, false);

        assert!(alice.non_auth_addr.is_none());
        assert!(bob.non_auth_addr.is_none());

        let bob_addr = bob.auth_addr;
        let alice_addr = alice.auth_addr;

        alice.connect(&bob.public_key, bob_addr);

        exchange_handshake(&mut alice, &mut bob).await;

        alice.write_message(bob_addr, b"hello from alice");

        let received_bob = tokio::time::timeout(Duration::from_secs(2), bob.socket.recv())
            .await
            .expect("timeout waiting for bob")
            .expect("bob received");
        assert_eq!(&received_bob.payload[..], b"hello from alice");
        assert_eq!(received_bob.src_addr, alice_addr);

        bob.write_message(alice_addr, b"hello from bob");

        let received_alice = tokio::time::timeout(Duration::from_secs(2), alice.socket.recv())
            .await
            .expect("timeout waiting for alice")
            .expect("alice received");
        assert_eq!(&received_alice.payload[..], b"hello from bob");
        assert_eq!(received_alice.src_addr, bob_addr);
    }

    #[tokio::test]
    async fn test_write_to_name_record_tracks_authenticated_bytes() {
        init_tracing();

        let mut alice = PeerNode::new(1, false);
        let mut bob = PeerNode::new(2, false);

        alice.connect(&bob.public_key, bob.auth_addr);
        exchange_handshake(&mut alice, &mut bob).await;

        let SocketAddr::V4(bob_auth_addr) = bob.auth_addr else {
            panic!("test uses IPv4 addresses");
        };
        let bob_name_record = NameRecord::new_with_ports(
            *bob_auth_addr.ip(),
            bob_auth_addr.port(),
            None,
            bob_auth_addr.port(),
            None,
            None,
            1,
        );
        let payload = Bytes::from_static(b"authenticated path");

        alice.socket.write_to_name_record(
            &bob.public_key,
            &bob_name_record,
            payload.clone(),
            payload.len() as u16,
            UdpPriority::Regular,
        );

        let authenticated_bytes = alice
            .socket
            .metrics()
            .into_inner()
            .into_iter()
            .find_map(|(name, value, _)| {
                (name == GAUGE_RAPTORCAST_AUTH_AUTHENTICATED_UDP_BYTES_WRITTEN.name)
                    .then_some(value)
            })
            .unwrap_or_default();
        assert_eq!(authenticated_bytes, payload.len() as u64);
    }

    #[tokio::test]
    async fn test_write_to_name_record_segments_authenticated_payload_by_stride() {
        init_tracing();

        let mut alice = PeerNode::new(1, false);
        let mut bob = PeerNode::new(2, false);

        alice.connect(&bob.public_key, bob.auth_addr);
        exchange_handshake(&mut alice, &mut bob).await;

        let SocketAddr::V4(bob_auth_addr) = bob.auth_addr else {
            panic!("test uses IPv4 addresses");
        };
        let bob_name_record = NameRecord::new_with_ports(
            *bob_auth_addr.ip(),
            bob_auth_addr.port(),
            None,
            bob_auth_addr.port(),
            None,
            None,
            1,
        );

        alice.socket.write_to_name_record(
            &bob.public_key,
            &bob_name_record,
            Bytes::from_static(b"helloworld"),
            5,
            UdpPriority::Regular,
        );

        let first = tokio::time::timeout(Duration::from_secs(2), bob.socket.recv())
            .await
            .expect("timeout waiting for first chunk")
            .expect("bob received first chunk");
        let second = tokio::time::timeout(Duration::from_secs(2), bob.socket.recv())
            .await
            .expect("timeout waiting for second chunk")
            .expect("bob received second chunk");

        assert_eq!(&first.payload[..], b"hello");
        assert_eq!(&second.payload[..], b"world");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_timer_deadline() {
        init_tracing();

        let bind_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();

        let mut dp = DataplaneBuilder::new(1000)
            .with_udp_sockets([(
                monad_dataplane::UdpSocketId::AuthenticatedRaptorcast,
                bind_addr,
            )])
            .build();

        let authenticated_socket = dp
            .udp_sockets
            .take(monad_dataplane::UdpSocketId::AuthenticatedRaptorcast)
            .expect("authenticated socket");

        let local_keypair = keypair(1);
        let config = Config {
            handshake_rate_reset_interval: Duration::from_millis(10),
            session_timeout: Duration::from_millis(4),
            session_timeout_jitter: Duration::ZERO,
            ..Default::default()
        };

        let auth_protocol = WireAuthProtocol::new(
            &crate::auth::metrics::UDP_METRICS,
            config,
            Arc::new(local_keypair),
        );
        let mut handle = AuthenticatedSocketHandle::new(authenticated_socket, auth_protocol);

        assert_eq!(poll!(pin!(handle.timer())), Poll::Pending);

        tokio::time::sleep(Duration::from_millis(11)).await;

        assert_eq!(poll!(pin!(handle.timer())), Poll::Ready(()));
        assert_eq!(poll!(pin!(handle.timer())), Poll::Pending);

        // this ensures that timer is updated with a shorter deadline
        let remote_keypair = keypair(2);
        let remote_addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 19004));
        handle
            .connect(&remote_keypair.pubkey(), remote_addr, 1)
            .expect("connect failed");
        tokio::time::sleep(Duration::from_millis(5)).await;
        assert_eq!(poll!(pin!(handle.timer())), Poll::Ready(()));
    }

    #[tokio::test]
    async fn test_framed_socket_rejects_unauthenticated_messages() {
        init_tracing();

        let bind_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();

        let mut recv_dp = DataplaneBuilder::new(1000)
            .with_udp_sockets([(
                monad_dataplane::UdpSocketId::AuthenticatedRaptorcast,
                bind_addr,
            )])
            .build();
        let recv_socket = recv_dp
            .udp_sockets
            .take(monad_dataplane::UdpSocketId::AuthenticatedRaptorcast)
            .expect("recv socket");
        let recv_addr = recv_socket.local_addr();

        let mut send_dp = DataplaneBuilder::new(1000)
            .with_udp_sockets([(monad_dataplane::UdpSocketId::Raptorcast, bind_addr)])
            .build();
        let send_socket = send_dp
            .udp_sockets
            .take(monad_dataplane::UdpSocketId::Raptorcast)
            .expect("send socket");

        let auth_socket = AuthenticatedSocketHandle::new(
            recv_socket,
            NoopAuthProtocol::<monad_secp::PubKey>::new(),
        );
        let mut framed_socket = FramedAuthenticatedSocketHandle::new(auth_socket, PanicFramer);

        send_socket.write_unicast_with_priority(
            UnicastMsg {
                msgs: vec![(recv_addr, Bytes::from_static(b"hello"))],
                stride: 5,
            },
            UdpPriority::Regular,
        );

        let result = tokio::time::timeout(Duration::from_secs(2), framed_socket.recv())
            .await
            .expect("timeout waiting for framed recv");
        assert!(matches!(result, Err(FramedRecvError::MissingAuthPublicKey)));
    }
}
