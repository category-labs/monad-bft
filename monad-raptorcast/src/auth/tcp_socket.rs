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
    socket::AUTH_SESSION_CONNECT_RETRY_ATTEMPTS,
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
                .encrypted_tcp_socket()
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
                AUTH_SESSION_CONNECT_RETRY_ATTEMPTS,
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
