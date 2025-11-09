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

use std::net::SocketAddr;

use thiserror::Error as ThisError;

use crate::{
    protocol::errors::{CookieError, CryptoError, HandshakeError, MessageError},
    session::{SessionError, SessionIndex},
};

#[derive(ThisError, Debug)]
pub enum Error {
    #[error("MAC1 verification failed from {addr}: {source}")]
    Mac1VerificationFailed {
        addr: SocketAddr,
        #[source]
        source: CryptoError,
    },

    #[error("MAC2 verification failed from {addr}: {source}")]
    Mac2VerificationFailed {
        addr: SocketAddr,
        #[source]
        source: CryptoError,
    },

    #[error("static key decryption failed from {addr}: {source}")]
    StaticKeyDecryptionFailed {
        addr: SocketAddr,
        #[source]
        source: CryptoError,
    },

    #[error("timestamp decryption failed from {addr}: {source}")]
    TimestampDecryptionFailed {
        addr: SocketAddr,
        #[source]
        source: CryptoError,
    },

    #[error("empty message decryption failed from {addr}: {source}")]
    EmptyMessageDecryptionFailed {
        addr: SocketAddr,
        #[source]
        source: CryptoError,
    },

    #[error(
        "timestamp replay detected from {addr}: received timestamp is not newer than expected"
    )]
    TimestampReplay { addr: SocketAddr },

    #[error("invalid message type {msg_type:#04x} from {addr}")]
    InvalidMessageType { msg_type: u32, addr: SocketAddr },

    #[error("invalid receiver index {index} from {addr}")]
    InvalidReceiverIndex {
        index: SessionIndex,
        addr: SocketAddr,
    },

    #[error("buffer too small for message from {addr}: need {required} bytes, got {actual}")]
    BufferTooSmall {
        addr: SocketAddr,
        required: usize,
        actual: usize,
    },

    #[error("invalid packet header: malformed or unrecognized format from {addr}")]
    InvalidPacketHeader { addr: SocketAddr },

    #[error("MAC verification failed: data packet integrity check failed from {addr}")]
    DataMacVerificationFailed { addr: SocketAddr },

    #[error("cookie decryption failed from {addr}: {source}")]
    CookieDecryptionFailed {
        addr: SocketAddr,
        #[source]
        source: CryptoError,
    },

    #[error("invalid cookie MAC from {addr}: {source}")]
    InvalidCookieMac {
        addr: SocketAddr,
        #[source]
        source: CryptoError,
    },

    #[error("invalid key from {addr}: {error}")]
    InvalidKey { addr: SocketAddr, error: String },

    #[error("ECDH operation failed: unable to compute shared secret")]
    EcdhFailed,

    #[error("session not found")]
    SessionNotFound,

    #[error("session index exhausted")]
    SessionIndexExhausted,

    #[error("session not established for address {addr}")]
    SessionNotEstablishedForAddress { addr: SocketAddr },

    #[error("session timeout from {addr}")]
    SessionTimeout { addr: SocketAddr },

    #[error("replay attack detected: packet counter already seen from {addr}")]
    ReplayAttack { addr: SocketAddr },

    #[error("invalid timestamp format: unable to parse TAI64N from {size} bytes")]
    InvalidTimestamp { size: usize },

    #[error("empty packet from {addr}")]
    EmptyPacket { addr: SocketAddr },

    #[error("session index not found: {index}")]
    SessionIndexNotFound { index: SessionIndex },
}

pub type Result<T> = std::result::Result<T, Error>;

pub trait SessionErrorContext {
    fn with_addr(self, addr: SocketAddr) -> Error;
}

impl SessionErrorContext for SessionError {
    fn with_addr(self, addr: SocketAddr) -> Error {
        match self {
            SessionError::InvalidHandshake(e) => {
                use crate::protocol::errors::ProtocolError;
                match e {
                    ProtocolError::Handshake(h) => h.with_addr(addr),
                    ProtocolError::Crypto(c) => c.with_addr(addr),
                    ProtocolError::Message(m) => m.with_addr(addr),
                    ProtocolError::Cookie(c) => c.with_addr(addr),
                }
            }
            SessionError::CryptoError(e) => e.with_addr(addr),
            SessionError::InvalidMac(e) => match e {
                CryptoError::MacVerificationFailed => Error::DataMacVerificationFailed { addr },
                CryptoError::InvalidKey(err) => Error::InvalidKey {
                    addr,
                    error: err.to_string(),
                },
                CryptoError::EcdhFailed => Error::EcdhFailed,
            },
            SessionError::InvalidCookie(e) => e.with_addr(addr),
            SessionError::ReplayAttack { .. } => Error::ReplayAttack { addr },
        }
    }
}

pub trait ProtocolErrorContext {
    fn with_addr(self, addr: SocketAddr) -> Error;
}

impl ProtocolErrorContext for HandshakeError {
    fn with_addr(self, addr: SocketAddr) -> Error {
        match self {
            HandshakeError::StaticKeyDecryptionFailed(source) => {
                Error::StaticKeyDecryptionFailed { addr, source }
            }
            HandshakeError::TimestampDecryptionFailed(source) => {
                Error::TimestampDecryptionFailed { addr, source }
            }
            HandshakeError::EmptyMessageDecryptionFailed(source) => {
                Error::EmptyMessageDecryptionFailed { addr, source }
            }
            HandshakeError::InvalidTimestamp { size } => Error::InvalidTimestamp { size },
        }
    }
}

impl ProtocolErrorContext for CryptoError {
    fn with_addr(self, addr: SocketAddr) -> Error {
        match self {
            CryptoError::MacVerificationFailed => Error::DataMacVerificationFailed { addr },
            CryptoError::InvalidKey(e) => Error::InvalidKey {
                addr,
                error: e.to_string(),
            },
            CryptoError::EcdhFailed => Error::EcdhFailed,
        }
    }
}

impl ProtocolErrorContext for MessageError {
    fn with_addr(self, addr: SocketAddr) -> Error {
        match self {
            MessageError::BufferTooSmall { required, actual } => Error::BufferTooSmall {
                addr,
                required,
                actual,
            },
            MessageError::InvalidMessageType(msg_type) => {
                Error::InvalidMessageType { msg_type, addr }
            }
            MessageError::InvalidHeader => Error::InvalidPacketHeader { addr },
            MessageError::InvalidDataPacketHeader => Error::InvalidPacketHeader { addr },
        }
    }
}

impl ProtocolErrorContext for CookieError {
    fn with_addr(self, addr: SocketAddr) -> Error {
        match self {
            CookieError::CookieDecryptionFailed(source) => {
                Error::CookieDecryptionFailed { addr, source }
            }
            CookieError::InvalidCookieMac(source) => Error::InvalidCookieMac { addr, source },
        }
    }
}
