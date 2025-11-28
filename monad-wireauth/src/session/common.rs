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
    time::{Duration, SystemTime},
};

use tracing::debug;

use crate::{
    config::RETRY_ALWAYS,
    protocol::{common::*, cookies},
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
    pub stored_cookie: Option<[u8; 16]>,
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
    #[error("replay detected: packet counter {counter} already seen")]
    NonceReplay { counter: u64 },
}

pub struct SessionState {
    pub keepalive_deadline: Option<Duration>,
    pub rekey_deadline: Option<Duration>,
    pub session_timeout_deadline: Option<Duration>,
    pub max_session_duration_deadline: Option<Duration>,
    pub stored_cookie: Option<[u8; 16]>,
    pub last_handshake_mac1: Option<[u8; 16]>,
    pub retry_attempts: u64,
    pub initiator_system_time: Option<SystemTime>,
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
        initiator_system_time: Option<SystemTime>,
        is_initiator: bool,
    ) -> Self {
        Self {
            keepalive_deadline: None,
            rekey_deadline: None,
            session_timeout_deadline: None,
            max_session_duration_deadline: None,
            stored_cookie: None,
            last_handshake_mac1: None,
            retry_attempts,
            initiator_system_time,
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

    pub fn get_next_deadline(&self) -> Option<Duration> {
        [
            self.keepalive_deadline,
            self.rekey_deadline,
            self.session_timeout_deadline,
            self.max_session_duration_deadline,
        ]
        .iter()
        .filter_map(|&timer| timer)
        .min()
    }

    pub fn stored_cookie(&self) -> Option<[u8; 16]> {
        self.stored_cookie
    }

    pub fn initiator_system_time(&self) -> Option<SystemTime> {
        self.initiator_system_time
    }

    pub fn handle_cookie(
        &mut self,
        cookie_reply: &mut crate::protocol::messages::CookieReply,
    ) -> Result<(), SessionError> {
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

        self.stored_cookie = Some(cookie);
        debug!("cookie stored successfully");
        Ok(())
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
            stored_cookie: self.stored_cookie,
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
