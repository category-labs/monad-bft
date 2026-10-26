// Copyright (C) 2026 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use std::{net::SocketAddr, pin::Pin, sync::Arc, time::Instant};

use bytes::{Bytes, BytesMut};
use monad_crypto::{
    certificate_signature::{
        CertificateSignature, CertificateSignaturePubKey, CertificateSignatureRecoverable,
    },
    signing_domain,
};
use monad_dataplane::{TcpMsg, TcpProtocol, TcpSocketHandle, TcpSocketReader, TcpSocketWriter};
use monad_executor::{ExecutorMetrics, ExecutorMetricsChain};
use tokio::time::Sleep;
use zerocopy::IntoBytes;

use super::{
    protocol::AuthenticationProtocol, socket::AuthenticatedTimerFuture, DataplaneCompletion,
};
use crate::SIGNATURE_SIZE;

monad_executor::metric_consts! {
    TCP_SIGNATURE_BYTES_SENT { name: "monad.raptorcast.tcp.signature_bytes_sent", help: "Application bytes submitted on signed TCP" }
    TCP_WIREAUTH_BYTES_SENT { name: "monad.raptorcast.tcp.wireauth_bytes_sent", help: "Application bytes submitted on encrypted TCP" }
    TCP_SIGNATURE_BYTES_RECEIVED { name: "monad.raptorcast.tcp.signature_bytes_received", help: "Application bytes received on signed TCP" }
    TCP_WIREAUTH_BYTES_RECEIVED { name: "monad.raptorcast.tcp.wireauth_bytes_received", help: "Application bytes received on encrypted TCP" }
}

pub fn wireauth_config() -> monad_wireauth::Config {
    monad_wireauth::Config {
        max_initiated_sessions: 200,
        max_buffered_bytes_per_session: 4 * 1024 * 1024,
        ..Default::default()
    }
}

#[derive(Debug)]
pub enum TcpRecvError<E> {
    Signature,
    WireAuth(E),
    MissingIdentity,
}

pub struct AuthRecvTcpMsg<P> {
    pub src_addr: SocketAddr,
    pub payload: Bytes,
    pub from: P,
}

/// Both TCP protocols share this reader, writer, listener and connection registry.
/// WireAuth owns session lifetimes independently of TCP connection lifetimes.
pub struct SharedTcpSocket<ST, AP>
where
    ST: CertificateSignatureRecoverable,
    AP: AuthenticationProtocol<PublicKey = CertificateSignaturePubKey<ST>>,
{
    reader: TcpSocketReader,
    writer: TcpSocketWriter,
    signing_key: Arc<ST::KeyPairType>,
    auth_protocol: AP,
    auth_timer: Option<(Pin<Box<Sleep>>, Instant)>,
    metrics: ExecutorMetrics,
}

impl<ST, AP> SharedTcpSocket<ST, AP>
where
    ST: CertificateSignatureRecoverable,
    AP: AuthenticationProtocol<PublicKey = CertificateSignaturePubKey<ST>>,
{
    pub fn new(
        socket: TcpSocketHandle,
        auth_protocol: AP,
        signing_key: Arc<ST::KeyPairType>,
    ) -> Self {
        let (reader, writer) = socket.split();
        Self {
            reader,
            writer,
            signing_key,
            auth_protocol,
            auth_timer: None,
            metrics: ExecutorMetrics::with_metric_defs(&[
                TCP_SIGNATURE_BYTES_SENT,
                TCP_WIREAUTH_BYTES_SENT,
                TCP_SIGNATURE_BYTES_RECEIVED,
                TCP_WIREAUTH_BYTES_RECEIVED,
            ]),
        }
    }

    pub fn write(
        &mut self,
        addr: SocketAddr,
        peer: &AP::PublicKey,
        protocol: TcpProtocol,
        payload: Bytes,
        completion: DataplaneCompletion,
    ) -> Result<(), AP::Error> {
        let len = payload.len() as u64;
        match protocol {
            TcpProtocol::Signature => {
                let signature =
                    ST::sign::<signing_domain::RaptorcastAppMessage>(&payload, &self.signing_key);
                let signature = <ST as CertificateSignature>::serialize(&signature);
                let mut packet = BytesMut::with_capacity(signature.len() + payload.len());
                packet.extend_from_slice(&signature);
                packet.extend_from_slice(&payload);
                self.writer.write(
                    addr,
                    TcpMsg {
                        protocol,
                        msg: packet.freeze(),
                        completion,
                    },
                );
                self.metrics.gauge(TCP_SIGNATURE_BYTES_SENT).add(len);
            }
            TcpProtocol::WireAuth => {
                if let Some(session_addr) = self.auth_protocol.get_socket_by_public_key(peer) {
                    let header_size = AP::HEADER_SIZE as usize;
                    let mut packet = BytesMut::zeroed(header_size);
                    packet.extend_from_slice(&payload);
                    let header = self
                        .auth_protocol
                        .encrypt_by_public_key(peer, &mut packet[header_size..])?;
                    packet[..header_size].copy_from_slice(header.as_bytes());
                    self.writer.write(
                        session_addr,
                        TcpMsg {
                            protocol,
                            msg: packet.freeze(),
                            completion,
                        },
                    );
                } else {
                    if !self.auth_protocol.has_initiator_session_by_public_key(peer) {
                        self.auth_protocol.connect(peer, addr, 0)?;
                    }
                    self.auth_protocol
                        .buffer_message(peer, payload, completion)?;
                    self.flush();
                }
                self.metrics.gauge(TCP_WIREAUTH_BYTES_SENT).add(len);
            }
        }
        Ok(())
    }

    fn flush(&mut self) {
        while let Some((addr, msg, completion)) = self.auth_protocol.next_packet() {
            self.writer.write(
                addr,
                TcpMsg {
                    protocol: TcpProtocol::WireAuth,
                    msg,
                    completion,
                },
            );
        }
    }

    pub async fn recv(
        &mut self,
    ) -> Result<AuthRecvTcpMsg<AP::PublicKey>, (SocketAddr, TcpRecvError<AP::Error>)> {
        loop {
            let timer =
                AuthenticatedTimerFuture::new(&mut self.auth_protocol, &mut self.auth_timer);
            let message = tokio::select! {
                () = timer => { self.flush(); continue; }
                message = self.reader.recv() => message,
            };
            let addr = message.src_addr;
            let (payload, from, metric) = match message.protocol {
                TcpProtocol::Signature => {
                    let signature = message
                        .payload
                        .get(..SIGNATURE_SIZE)
                        .and_then(|bytes| <ST as CertificateSignature>::deserialize(bytes).ok())
                        .ok_or((addr, TcpRecvError::Signature))?;
                    let payload = message.payload.slice(SIGNATURE_SIZE..);
                    let from = signature
                        .recover_pubkey::<signing_domain::RaptorcastAppMessage>(&payload)
                        .map_err(|_| (addr, TcpRecvError::Signature))?;
                    (payload, from, TCP_SIGNATURE_BYTES_RECEIVED)
                }
                TcpProtocol::WireAuth => {
                    let mut packet = message.payload.to_vec();
                    let result = self
                        .auth_protocol
                        .dispatch(&mut packet, addr)
                        .map_err(|error| (addr, TcpRecvError::WireAuth(error)))?;
                    self.flush();
                    match result {
                        None => continue,
                        Some((payload, Some(from))) => (payload, from, TCP_WIREAUTH_BYTES_RECEIVED),
                        Some((_, None)) => return Err((addr, TcpRecvError::MissingIdentity)),
                    }
                }
            };
            self.metrics.gauge(metric).add(payload.len() as u64);
            return Ok(AuthRecvTcpMsg {
                src_addr: addr,
                payload,
                from,
            });
        }
    }

    pub fn metrics(&self) -> ExecutorMetricsChain<'_> {
        self.auth_protocol.metrics().push(&self.metrics)
    }
}
