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

use std::time::Duration;

use zeroize::Zeroizing;

pub const RETRY_ALWAYS: u64 = u64::MAX;
pub const DEFAULT_RETRY_ATTEMPTS: u64 = 3;

#[derive(Clone)]
pub struct Config {
    /// idle time before session expires (reset on any packet exchange)
    pub session_timeout: Duration,
    /// randomization to prevent thundering herd on timeout
    pub session_timeout_jitter: Duration,
    /// send empty packet after this idle time to maintain session
    pub keepalive_interval: Duration,
    /// randomization to spread keepalive traffic
    pub keepalive_jitter: Duration,
    /// time before initiating new handshake to rotate keys
    pub rekey_interval: Duration,
    /// randomization to avoid synchronized rekey storms
    pub rekey_jitter: Duration,
    /// absolute session lifetime regardless of activity (forces rekey)
    pub max_session_duration: Duration,
    /// global rate limit (per reset interval) for handshake initiations without a valid cookie.
    /// this budget is intended for "unproven" sources and prevents them from starving cookie-valid
    /// handshakes.
    pub handshake_cookie_unverified_rate_limit: u64,
    /// global rate limit (per reset interval) for handshake initiations with a valid cookie.
    /// this budget is intended for "proven" sources.
    pub handshake_cookie_verified_rate_limit: u64,
    /// window for handshake rate limiting
    pub handshake_rate_reset_interval: Duration,
    /// max outbound connect attempts per second (dos protection)
    pub connect_rate_limit: u64,
    /// window for outbound connect rate limiting
    pub connect_rate_reset_interval: Duration,
    /// cookie validity period (responder rotates cookie key)
    pub cookie_refresh_duration: Duration,
    /// nonzero lru capacity for cookies received from peers, keyed by public key.
    /// size alongside handshake/session limits to retain cookies until retries.
    pub cookie_cache_capacity: usize,
    /// time window for counting cookie-valid handshake requests per ip
    pub ip_rate_limit_window: Duration,
    /// lru cache size for tracking recent cookie-valid handshake requests per ip
    pub ip_history_capacity: usize,
    /// maximum established transport sessions; also drop handshakes at this threshold
    pub total_transport_sessions: usize,
    /// max concurrent accepted handshakes waiting for the first authenticated packet
    pub max_pending_accepted_sessions: usize,
    /// max concurrent initiated handshakes admitted by connect()
    /// timer-driven retries and rekeys currently bypass this threshold
    pub max_pending_initiated_sessions: usize,
    /// limit distinct established peer public keys from a single ip
    pub max_established_peers_per_ip: usize,
    /// optional pre-shared key mixed into handshake for additional auth
    pub psk: Zeroizing<[u8; 32]>,
    /// max bytes of buffered messages per initiated session
    pub max_buffered_bytes_per_session: usize,
    /// idle time (without useful data) before session is garbage collected
    pub gc_idle_timeout: Duration,
    /// cap how many expired timers are processed per tick (dos protection)
    pub max_expired_timers_per_tick: usize,
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
            max_session_duration: Duration::from_secs(6 * 60 * 60 + 5 * 60),
            handshake_cookie_unverified_rate_limit: 500,
            handshake_cookie_verified_rate_limit: 1000,
            handshake_rate_reset_interval: Duration::from_secs(1),
            connect_rate_limit: 300,
            connect_rate_reset_interval: Duration::from_secs(1),
            cookie_refresh_duration: Duration::from_secs(120),
            // Retain fresh cookies through the normal retry window. Eviction needs
            // this many other distinct peer keys touched by lookups or inserts.
            // Over 11s (timeout + jitter), new admissions have up to 12 rate-limit
            // batches of 500 unverified + 1,000 verified handshakes + 300 connects:
            // 21,600 opportunities. Existing sessions can also touch cached keys.
            // Retries/rekeys reuse peer keys but bypass the initiated-session cap,
            // so these limits do not guarantee a retention time for every state.
            // Duplicate cookie replies cannot refresh recency. Revisit sizing when
            // changing admission limits; see the API cookie-flood tests.
            cookie_cache_capacity: 256_000,
            ip_rate_limit_window: Duration::from_secs(10),
            ip_history_capacity: 1_000_000,
            total_transport_sessions: 40_000,
            max_pending_accepted_sessions: 20_000,
            max_pending_initiated_sessions: 1_000,
            max_established_peers_per_ip: 8,
            psk: Zeroizing::new([0u8; 32]),
            max_buffered_bytes_per_session: 128 * 1024,
            gc_idle_timeout: Duration::from_secs(120),
            max_expired_timers_per_tick: 10_000,
        }
    }
}
