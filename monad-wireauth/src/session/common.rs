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

use std::{net::SocketAddr, time::Duration};

use tracing::debug;

use crate::{
    config::RETRY_ALWAYS,
    protocol::{common::*, cookies, tai64::Tai64N},
};

#[derive(Debug, Clone)]
pub struct TerminatedEvent {
    pub remote_public_key: monad_secp::PubKey,
    pub remote_addr: SocketAddr,
}

#[derive(Debug, Clone)]
pub struct SessionTimeoutResult {
    pub terminated: TerminatedEvent,
    pub rekey: Option<RekeyEvent>,
}

#[derive(Debug, Clone)]
pub struct RekeyEvent {
    pub remote_public_key: monad_secp::PubKey,
    pub remote_addr: SocketAddr,
    pub retry_attempts: u64,
}

#[derive(Clone)]
pub struct MessageEvent {
    pub remote_addr: SocketAddr,
    pub header: crate::protocol::messages::DataPacketHeader,
}

#[derive(Debug, Clone, Copy)]
pub struct RenewedTimer {
    pub previous: Option<Duration>,
    pub current: Duration,
}

#[derive(Debug, thiserror::Error)]
pub enum SessionError {
    #[error("handshake validation failed: {0}")]
    HandshakeError(#[source] crate::protocol::errors::HandshakeError),
    #[error("message error: {0}")]
    MessageError(#[source] crate::protocol::errors::MessageError),
    #[error("cryptographic operation failed: {0}")]
    CryptoError(#[source] crate::protocol::errors::CryptoError),
    #[error("MAC verification failed: {0}")]
    InvalidMac(#[source] crate::protocol::errors::CryptoError),
    #[error("cookie validation failed: {0}")]
    InvalidCookie(#[source] crate::protocol::errors::CookieError),
    #[error("counter {counter} is outside replay window (next={next})")]
    NonceOutsideWindow { counter: u64, next: u64 },
    #[error("duplicate counter {counter} detected")]
    NonceDuplicate { counter: u64 },
}

pub struct SessionState {
    pub keepalive_deadline: Option<Duration>,
    pub rekey_deadline: Option<Duration>,
    pub session_timeout_deadline: Option<Duration>,
    pub max_session_duration_deadline: Option<Duration>,
    pub gc_deadline: Option<Duration>,
    pub last_handshake_mac1: Option<[u8; 16]>,
    cookie_received: bool,
    pub retry_attempts: u64,
    pub initiator_timestamp: Option<Tai64N>,
    pub remote_addr: SocketAddr,
    pub remote_public_key: monad_secp::PubKey,
    pub local_index: SessionIndex,
    pub created: Duration,
    pub is_initiator: bool,
}

impl SessionState {
    pub fn new(
        remote_addr: SocketAddr,
        remote_public_key: monad_secp::PubKey,
        local_index: SessionIndex,
        created: Duration,
        retry_attempts: u64,
        initiator_timestamp: Option<Tai64N>,
        is_initiator: bool,
    ) -> Self {
        Self {
            keepalive_deadline: None,
            rekey_deadline: None,
            session_timeout_deadline: None,
            max_session_duration_deadline: None,
            gc_deadline: None,
            last_handshake_mac1: None,
            cookie_received: false,
            retry_attempts,
            initiator_timestamp,
            remote_addr,
            remote_public_key,
            local_index,
            created,
            is_initiator,
        }
    }

    pub fn reset_keepalive(
        &mut self,
        duration_since_start: Duration,
        timer_duration: Duration,
    ) -> RenewedTimer {
        let previous = self.keepalive_deadline;
        let current = duration_since_start + timer_duration;
        self.keepalive_deadline = Some(current);
        RenewedTimer { previous, current }
    }

    pub fn reset_rekey(&mut self, duration_since_start: Duration, timer_duration: Duration) {
        self.rekey_deadline = Some(duration_since_start + timer_duration);
    }

    pub fn reset_session_timeout(
        &mut self,
        duration_since_start: Duration,
        timer_duration: Duration,
    ) -> RenewedTimer {
        let previous = self.session_timeout_deadline;
        let current = duration_since_start + timer_duration;
        self.session_timeout_deadline = Some(current);
        RenewedTimer { previous, current }
    }

    pub fn clear_keepalive(&mut self) {
        self.keepalive_deadline = None;
    }

    pub fn clear_rekey(&mut self) {
        self.rekey_deadline = None;
    }

    pub fn clear_session_timeout(&mut self) {
        self.session_timeout_deadline = None;
    }

    pub fn set_max_session_duration(
        &mut self,
        duration_since_start: Duration,
        timer_duration: Duration,
    ) {
        self.max_session_duration_deadline = Some(duration_since_start + timer_duration);
    }

    pub fn clear_max_session_duration(&mut self) {
        self.max_session_duration_deadline = None;
    }

    pub fn reset_gc_deadline(&mut self, duration_since_start: Duration, timer_duration: Duration) {
        self.gc_deadline = Some(duration_since_start + timer_duration);
    }

    pub fn clear_gc_deadline(&mut self) {
        self.gc_deadline = None;
    }

    pub fn get_next_deadline(&self) -> Option<Duration> {
        [
            self.keepalive_deadline,
            self.rekey_deadline,
            self.session_timeout_deadline,
            self.max_session_duration_deadline,
            self.gc_deadline,
        ]
        .iter()
        .filter_map(|&timer| timer)
        .min()
    }

    pub fn initiator_timestamp(&self) -> Option<Tai64N> {
        self.initiator_timestamp
    }

    pub fn handle_cookie(
        &mut self,
        cookie_reply: &mut crate::protocol::messages::CookieReply,
    ) -> Result<Option<[u8; 16]>, SessionError> {
        if self.cookie_received {
            return Ok(None);
        }

        let Some(mac1) = self.last_handshake_mac1 else {
            debug!("no last_handshake_mac1 stored");
            return Err(SessionError::InvalidCookie(
                crate::protocol::errors::CookieError::InvalidCookieMac(
                    crate::protocol::errors::CryptoError::MacVerificationFailed,
                ),
            ));
        };

        let cookie = cookies::accept_cookie_reply(&self.remote_public_key, cookie_reply, &mac1)
            .map_err(|e| {
                debug!(error=?e, "failed to accept cookie reply");
                SessionError::InvalidCookie(e)
            })?;

        // Invalid replies must not consume this handshake's opportunity to
        // accept a cookie. Each retry creates a new pending handshake.
        self.cookie_received = true;
        debug!("cookie accepted successfully");
        Ok(Some(cookie))
    }

    pub fn handle_session_timeout(&mut self) -> (TerminatedEvent, Option<RekeyEvent>) {
        debug!(
            retry_attempts = self.retry_attempts,
            remote_addr = ?self.remote_addr,
            is_initiator = self.is_initiator,
            "handling session timeout"
        );

        let terminated = TerminatedEvent {
            remote_public_key: self.remote_public_key,
            remote_addr: self.remote_addr,
        };

        if !self.is_initiator {
            return (terminated, None);
        }

        let should_retry = self.retry_attempts > 0 || self.retry_attempts == RETRY_ALWAYS;
        if self.retry_attempts > 0 && self.retry_attempts != RETRY_ALWAYS {
            self.retry_attempts -= 1;
        }

        let rekey = should_retry.then_some(RekeyEvent {
            remote_public_key: self.remote_public_key,
            remote_addr: self.remote_addr,
            retry_attempts: self.retry_attempts,
        });

        (terminated, rekey)
    }
}

pub(crate) fn add_jitter<R: secp256k1::rand::Rng>(
    rng: &mut R,
    base: Duration,
    jitter: Duration,
) -> Duration {
    let jitter_millis = jitter.as_millis() as u64;
    let random_jitter = rng.next_u64() % (jitter_millis + 1);
    base + Duration::from_millis(random_jitter)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_first_valid_cookie_is_accepted_per_handshake() {
        let remote_key = monad_secp::KeyPair::from_ikm(b"cookie-once")
            .unwrap()
            .pubkey();
        let mac1 = [3; 16];
        let mut session = SessionState::new(
            "127.0.0.1:9000".parse().unwrap(),
            remote_key,
            SessionIndex::new(1),
            Duration::ZERO,
            0,
            None,
            true,
        );
        session.last_handshake_mac1 = Some(mac1);
        let cookie_reply = |nonce, cookie| {
            cookies::send_cookie_reply(&[4; 32], nonce, &remote_key, 1, &mac1, &cookie)
        };

        let mut invalid = cookie_reply(0, [5; 16]);
        invalid.encrypted_cookie[0] ^= 1;
        assert!(session.handle_cookie(&mut invalid).is_err());
        assert!(!session.cookie_received);

        let mut valid = cookie_reply(1, [5; 16]);
        assert_eq!(
            session.handle_cookie(&mut valid.clone()).unwrap(),
            Some([5; 16])
        );
        assert!(session.cookie_received);
        assert_eq!(session.handle_cookie(&mut valid).unwrap(), None);
        assert_eq!(
            session
                .handle_cookie(&mut cookie_reply(2, [6; 16]))
                .unwrap(),
            None
        );
    }
}
