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

use thiserror::Error;

#[derive(Error, Debug)]
pub enum HandshakeError {
    #[error("MAC1 verification failed: {0}")]
    Mac1VerificationFailed(#[source] CryptoError),

    #[error("static key decryption failed: {0}")]
    StaticKeyDecryptionFailed(#[source] CryptoError),

    #[error("timestamp decryption failed: {0}")]
    TimestampDecryptionFailed(#[source] CryptoError),

    #[error("invalid timestamp format: unable to parse TAI64N from {size} bytes")]
    InvalidTimestamp { size: usize },

    #[error("empty message decryption failed: {0}")]
    EmptyMessageDecryptionFailed(#[source] CryptoError),
}

#[derive(Error, Debug)]
pub enum MessageError {
    #[error("buffer too small: need at least {required} bytes, got {actual}")]
    BufferTooSmall { required: usize, actual: usize },

    #[error("invalid message type: {0:#04x} is not a recognized protocol message")]
    InvalidMessageType(u32),

    #[error("invalid message header: unable to parse or malformed structure")]
    InvalidHeader,

    #[error("invalid data packet header: unable to parse or malformed structure")]
    InvalidDataPacketHeader,
}

#[derive(Error, Debug)]
pub enum CryptoError {
    #[error("MAC verification failed: message authentication code does not match")]
    MacVerificationFailed,

    #[error("invalid key: {0}")]
    InvalidKey(#[from] monad_secp::Error),

    #[error("ECDH operation failed: unable to compute shared secret")]
    EcdhFailed,
}

#[derive(Error, Debug)]
pub enum CookieError {
    #[error("cookie decryption failed: {0}")]
    CookieDecryptionFailed(#[source] CryptoError),

    #[error("invalid cookie MAC: {0}")]
    InvalidCookieMac(#[source] CryptoError),
}

#[derive(Error, Debug)]
pub enum ProtocolError {
    #[error("handshake error: {0}")]
    Handshake(#[from] HandshakeError),

    #[error("message error: {0}")]
    Message(#[from] MessageError),

    #[error("crypto error: {0}")]
    Crypto(#[from] CryptoError),

    #[error("cookie error: {0}")]
    Cookie(#[from] CookieError),
}
