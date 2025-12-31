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
    #[error("session error: {0}")]
    Session(#[from] SessionError),

    #[error("handshake error: {0}")]
    Handshake(#[from] HandshakeError),

    #[error("message error: {0}")]
    Message(#[from] MessageError),

    #[error("crypto error: {0}")]
    Crypto(#[from] CryptoError),

    #[error("cookie error: {0}")]
    Cookie(#[from] CookieError),

    #[error("session not found")]
    SessionNotFound,

    #[error("session index exhausted")]
    SessionIndexExhausted,

    #[error("session not established for address {addr}")]
    SessionNotEstablishedForAddress { addr: SocketAddr },

    #[error("invalid receiver index {index}")]
    InvalidReceiverIndex { index: SessionIndex },

    #[error("timestamp replay detected: received timestamp is not newer than expected")]
    TimestampReplay,

    #[error("session index not found: {index}")]
    SessionIndexNotFound { index: SessionIndex },

    #[error("too many initiated sessions: limit is {limit}")]
    TooManyInitiatedSessions { limit: usize },

    #[error("buffer limit exceeded: {size} bytes exceeds limit of {limit} bytes")]
    BufferLimitExceeded { size: usize, limit: usize },
}

pub type Result<T> = std::result::Result<T, Error>;
