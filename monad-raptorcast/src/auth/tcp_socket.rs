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

use std::{net::SocketAddr, sync::Arc};

use bytes::{Bytes, BytesMut};
use monad_crypto::{
    certificate_signature::{
        CertificateSignature, CertificateSignaturePubKey, CertificateSignatureRecoverable,
    },
    signing_domain,
};
use monad_dataplane::{TcpMsg, TcpSocketReader, TcpSocketWriter};

use super::DataplaneCompletion;
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
