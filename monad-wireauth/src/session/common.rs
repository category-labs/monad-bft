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
use zeroize::Zeroizing;

use crate::protocol::{common::*, cookies};

pub const RETRY_ALWAYS: u64 = u64::MAX;
pub const DEFAULT_RETRY_ATTEMPTS: u64 = 3;

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

#[derive(Clone)]
pub struct Config {
    pub session_timeout: Duration,
    pub session_timeout_jitter: Duration,
    pub keepalive_interval: Duration,
    pub keepalive_jitter: Duration,
    pub rekey_interval: Duration,
    pub rekey_jitter: Duration,
    pub max_session_duration: Duration,
    pub handshake_rate_limit: u64,
    pub handshake_rate_reset_interval: Duration,
    pub cookie_refresh_duration: Duration,
    pub low_watermark_sessions: usize,
    pub high_watermark_sessions: usize,
    pub max_sessions_per_ip: usize,
    pub ip_rate_limit_window: Duration,
    pub max_requests_per_ip: usize,
    pub ip_history_capacity: usize,
    pub psk: Zeroizing<[u8; 32]>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            session_timeout: Duration::from_secs(10),
            session_timeout_jitter: Duration::from_secs(1),
            keepalive_interval: Duration::from_secs(3),
            keepalive_jitter: Duration::from_millis(300),
            rekey_interval: Duration::from_secs(6 * 60 * 60),
            rekey_jitter: Duration::from_secs(60),
            max_session_duration: Duration::from_secs(7 * 60 * 60),
            handshake_rate_limit: 2000,
            handshake_rate_reset_interval: Duration::from_secs(1),
            cookie_refresh_duration: Duration::from_secs(120),
            low_watermark_sessions: 10_000,
            high_watermark_sessions: 100_000,
            max_sessions_per_ip: 10,
            ip_rate_limit_window: Duration::from_secs(10),
            max_requests_per_ip: 10,
            ip_history_capacity: 1_000_000,
            psk: Zeroizing::new([0u8; 32]),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SessionError {
    #[error("handshake validation failed: {0}")]
    InvalidHandshake(#[source] crate::protocol::errors::ProtocolError),
    #[error("cryptographic operation failed: {0}")]
    CryptoError(#[source] crate::protocol::errors::CryptoError),
    #[error("MAC verification failed: {0}")]
    InvalidMac(#[source] crate::protocol::errors::CryptoError),
    #[error("cookie validation failed: {0}")]
    InvalidCookie(#[source] crate::protocol::errors::CookieError),
    #[error("replay attack detected: packet counter {counter} already seen")]
    ReplayAttack { counter: u64 },
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

    pub fn reset_keepalive(&mut self, duration_since_start: Duration, timer_duration: Duration) {
        self.keepalive_deadline = Some(duration_since_start + timer_duration);
    }

    pub fn reset_rekey(&mut self, duration_since_start: Duration, timer_duration: Duration) {
        self.rekey_deadline = Some(duration_since_start + timer_duration);
    }

    pub fn reset_session_timeout(
        &mut self,
        duration_since_start: Duration,
        timer_duration: Duration,
    ) {
        self.session_timeout_deadline = Some(duration_since_start + timer_duration);
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
                use crate::protocol::errors::ProtocolError;
                match e {
                    ProtocolError::Cookie(c) => SessionError::InvalidCookie(c),
                    ProtocolError::Crypto(c) => SessionError::CryptoError(c),
                    _ => SessionError::InvalidHandshake(e),
                }
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
