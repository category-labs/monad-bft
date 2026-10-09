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

use std::{net::SocketAddr, pin::Pin, sync::Arc, time::Instant};

use bytes::{Bytes, BytesMut};
use monad_crypto::{
    certificate_signature::{
        CertificateSignature, CertificateSignaturePubKey, CertificateSignatureRecoverable,
    },
    signing_domain,
};
use monad_dataplane::{TcpMsg, TcpSocketReader, TcpSocketWriter};
use tokio::time::Sleep;
use tracing::warn;

use super::{
    common::{dispatch_packet, encrypt_packet, AuthRecvError, AuthenticatedTimerFuture},
    protocol::AuthenticationProtocol,
    DataplaneCompletion,
};
use crate::SIGNATURE_SIZE;

/// TCP Wireauth allows 200 pending initiators with up to 10 MiB buffered per session.
pub fn wireauth_config() -> monad_wireauth::Config {
    monad_wireauth::Config {
        max_initiated_sessions: 200,
        max_buffered_bytes_per_session: 10 * 1024 * 1024,
        ..Default::default()
    }
}

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
pub enum TcpRecvError<E: std::fmt::Debug> {
    WireAuth(AuthRecvError<E>),
    SigAuth(AuthRecvError<SigAuthError>),
}

impl<E: std::fmt::Debug> TcpRecvError<E> {
    pub fn src_addr(&self) -> SocketAddr {
        match self {
            Self::WireAuth(error) => error.src_addr,
            Self::SigAuth(error) => error.src_addr,
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
    ) -> Result<AuthRecvTcpMsg<CertificateSignaturePubKey<ST>>, AuthRecvError<SigAuthError>> {
        let msg = self.reader.recv().await;
        let payload = msg.payload;
        let src_addr = msg.src_addr;

        if payload.len() < SIGNATURE_SIZE {
            return Err(AuthRecvError {
                src_addr,
                error: SigAuthError::MessageTooShort,
            });
        }

        let signature_bytes = &payload[..SIGNATURE_SIZE];
        let signature =
            <ST as CertificateSignature>::deserialize(signature_bytes).map_err(|_| {
                AuthRecvError {
                    src_addr,
                    error: SigAuthError::InvalidSignature,
                }
            })?;

        let app_message_bytes = payload.slice(SIGNATURE_SIZE..);
        let from = signature
            .recover_pubkey::<signing_domain::RaptorcastAppMessage>(app_message_bytes.as_ref())
            .map_err(|_| AuthRecvError {
                src_addr,
                error: SigAuthError::PubkeyRecoveryFailed,
            })?;

        Ok(AuthRecvTcpMsg {
            src_addr,
            payload: app_message_bytes,
            from,
        })
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

    pub async fn recv(
        &mut self,
    ) -> Result<AuthRecvTcpMsg<AP::PublicKey>, AuthRecvError<AP::Error>> {
        loop {
            let timer =
                AuthenticatedTimerFuture::new(&mut self.auth_protocol, &mut self.auth_timer);

            tokio::select! {
                () = timer => {
                    self.flush();
                    continue;
                }
                message = self.reader.recv() => {
                    match dispatch_packet(&mut self.auth_protocol, message.payload, message.src_addr) {
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
                        Err(error) => return Err(error),
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
        if let Some((addr, msg)) = encrypt_packet(&mut self.auth_protocol, &addr, payload) {
            self.writer.write(addr, TcpMsg { msg, completion });
            true
        } else {
            false
        }
    }

    pub fn flush(&mut self) {
        while let Some((addr, msg, completion)) = self.auth_protocol.next_packet() {
            self.writer.write(addr, TcpMsg { msg, completion });
        }
    }
}
