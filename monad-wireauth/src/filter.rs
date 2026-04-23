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
    net::{IpAddr, SocketAddr},
    num::NonZeroUsize,
    time::Duration,
};

use lru::LruCache;
use monad_executor::ExecutorMetrics;
use tracing::{debug, trace, warn};

use crate::{metrics::MetricNames, state::State};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterAction {
    Pass,
    SendCookie,
    Drop,
}
// Filter ...
// NOTE that rate limiting for ipv6 is not properly supported
pub struct Filter {
    cookie_unverified_counter: u64,
    cookie_verified_counter: u64,
    last_reset: Duration,
    handshake_cookie_unverified_rate_limit: u64,
    handshake_cookie_verified_rate_limit: u64,
    handshake_rate_reset_interval: Duration,
    ip_request_history: LruCache<IpAddr, Duration>,
    ip_rate_limit_window: Duration,
    total_transport_sessions: usize,
    total_pending_sessions: usize,
    metrics: ExecutorMetrics,
    metric_names: &'static MetricNames,
}

impl Filter {
    // This is essentially a "configuration constructor" for `Filter`.
    // Keeping it as a single constructor avoids proliferating ad-hoc config structs
    // across call sites. Clippy's default threshold is 7 args; we intentionally exceed it.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        metric_names: &'static MetricNames,
        handshake_cookie_unverified_rate_limit: u64,
        handshake_cookie_verified_rate_limit: u64,
        handshake_rate_reset_interval: Duration,
        ip_rate_limit_window: Duration,
        ip_history_capacity: usize,
        total_transport_sessions: usize,
        total_pending_sessions: usize,
    ) -> Self {
        Self {
            cookie_unverified_counter: 0,
            cookie_verified_counter: 0,
            last_reset: Duration::ZERO,
            handshake_cookie_unverified_rate_limit,
            handshake_cookie_verified_rate_limit,
            handshake_rate_reset_interval,
            ip_request_history: LruCache::new(NonZeroUsize::new(ip_history_capacity).unwrap()),
            ip_rate_limit_window,
            total_transport_sessions,
            total_pending_sessions,
            metrics: ExecutorMetrics::default(),
            metric_names,
        }
    }

    pub fn metrics(&self) -> &ExecutorMetrics {
        &self.metrics
    }

    pub fn tick(&mut self, duration_since_start: Duration) {
        let expected_reset_time = self.last_reset + self.handshake_rate_reset_interval;
        if duration_since_start.saturating_sub(self.last_reset)
            >= self.handshake_rate_reset_interval
        {
            // tick on filter is expected to be atleast as often as the reset interval
            if let Some(elapsed) = duration_since_start.checked_sub(expected_reset_time) {
                let elapsed_ms = elapsed.as_millis();
                if elapsed_ms > 100 {
                    warn!(
                        elapsed_ms=elapsed_ms,
                        last_reset=?self.last_reset,
                        expected_reset_time=?expected_reset_time,
                        "filter reset deadline is too old"
                    );
                }
            }
            self.cookie_unverified_counter = 0;
            self.cookie_verified_counter = 0;
            self.last_reset = duration_since_start;
        }
    }

    pub fn next_reset_time(&self) -> Duration {
        self.last_reset + self.handshake_rate_reset_interval
    }

    pub fn apply(
        &mut self,
        state: &State,
        remote_addr: SocketAddr,
        duration_since_start: Duration,
        cookie_valid: bool,
    ) -> FilterAction {
        trace!(remote_addr = %remote_addr, cookie_valid = cookie_valid, "applying filter");
        let transport_sessions = state.transport_sessions_count();
        let pending_sessions = state.pending_sessions_count();

        let action = self
            .check_total_transport_sessions(transport_sessions, remote_addr)
            .or_else(|| self.check_pending_session_limit(pending_sessions, remote_addr))
            .unwrap_or_else(|| {
                if cookie_valid {
                    self.check_verified_request(remote_addr, duration_since_start)
                } else {
                    self.check_unverified_request(remote_addr)
                }
            });

        if action == FilterAction::Pass {
            if cookie_valid {
                self.cookie_verified_counter += 1;
            } else {
                self.cookie_unverified_counter += 1;
            }
        }

        self.record_metric(action);
        action
    }

    fn check_total_transport_sessions(
        &self,
        transport_sessions: usize,
        remote_addr: SocketAddr,
    ) -> Option<FilterAction> {
        (transport_sessions >= self.total_transport_sessions).then(|| {
            debug!(
                remote_addr = %remote_addr,
                transport_sessions,
                total_transport_sessions = self.total_transport_sessions,
                "too many established transport sessions - rejecting new handshake"
            );
            FilterAction::Drop
        })
    }

    fn check_pending_session_limit(
        &self,
        pending_sessions: usize,
        remote_addr: SocketAddr,
    ) -> Option<FilterAction> {
        (pending_sessions >= self.total_pending_sessions).then(|| {
            debug!(
                remote_addr = %remote_addr,
                pending_sessions,
                total_pending_sessions = self.total_pending_sessions,
                "too many pending sessions - rejecting new handshake"
            );
            FilterAction::Drop
        })
    }

    fn check_unverified_request(&self, remote_addr: SocketAddr) -> FilterAction {
        if self.cookie_unverified_counter >= self.handshake_cookie_unverified_rate_limit {
            debug!(
                remote_addr = %remote_addr,
                unverified_counter = self.cookie_unverified_counter,
                unverified_rate_limit = self.handshake_cookie_unverified_rate_limit,
                "cookie-unverified rate limit exceeded - sending cookie reply"
            );
            FilterAction::SendCookie
        } else {
            FilterAction::Pass
        }
    }

    fn check_verified_request(
        &mut self,
        remote_addr: SocketAddr,
        duration_since_start: Duration,
    ) -> FilterAction {
        if self.cookie_verified_counter >= self.handshake_cookie_verified_rate_limit {
            debug!(
                remote_addr = %remote_addr,
                counter = self.cookie_verified_counter,
                verified_rate_limit = self.handshake_cookie_verified_rate_limit,
                "cookie-verified rate limit exceeded - dropping handshake"
            );
            return FilterAction::Drop;
        }

        if self.check_ip_rate_limit(remote_addr.ip(), duration_since_start) {
            debug!(remote_addr = %remote_addr, "ip rate limit exceeded");
            return FilterAction::Drop;
        }

        self.ip_request_history
            .put(remote_addr.ip(), duration_since_start);
        self.metrics[self.metric_names.filter_ip_request_history_size] =
            self.ip_request_history.len() as u64;
        FilterAction::Pass
    }

    fn check_ip_rate_limit(&mut self, ip: IpAddr, duration_since_start: Duration) -> bool {
        let window_start = duration_since_start.saturating_sub(self.ip_rate_limit_window);
        matches!(
            self.ip_request_history.peek(&ip),
            Some(last_time) if *last_time >= window_start
        )
    }

    fn record_metric(&mut self, action: FilterAction) {
        let metric = match action {
            FilterAction::Pass => self.metric_names.filter_pass,
            FilterAction::SendCookie => self.metric_names.filter_send_cookie,
            FilterAction::Drop => self.metric_names.filter_drop,
        };
        self.metrics[metric] += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        metrics::DEFAULT_METRICS,
        state::{insert_test_initiator_session, insert_test_transport_session},
    };

    fn default_filter() -> Filter {
        Filter::new(
            DEFAULT_METRICS,
            100,
            100,
            Duration::from_secs(60),
            Duration::from_secs(60),
            1_000,
            100,
            100,
        )
    }

    #[test]
    fn test_basic_pass_no_limits() {
        let mut filter = default_filter();
        let state = State::new(DEFAULT_METRICS);
        let addr = "127.0.0.1:8080".parse().unwrap();
        let action = filter.apply(&state, addr, Duration::ZERO, false);
        assert_eq!(action, FilterAction::Pass);
    }

    #[test]
    fn test_total_transport_sessions_drops() {
        let total_transport_sessions = 10;
        let mut filter = Filter::new(
            DEFAULT_METRICS,
            100,
            100,
            Duration::from_secs(60),
            Duration::from_secs(60),
            1_000,
            total_transport_sessions,
            100,
        );
        let mut state = State::new(DEFAULT_METRICS);
        for i in 0..total_transport_sessions {
            let addr: SocketAddr = format!("10.0.0.{}:51820", i).parse().unwrap();
            insert_test_transport_session(&mut state, addr);
        }
        let addr = "127.0.0.1:8080".parse().unwrap();
        let action = filter.apply(&state, addr, Duration::ZERO, false);
        assert_eq!(action, FilterAction::Drop);
    }

    #[test]
    fn test_pending_session_limit_drops() {
        let total_pending_sessions = 10;
        let mut filter = Filter::new(
            DEFAULT_METRICS,
            100,
            100,
            Duration::from_secs(60),
            Duration::from_secs(60),
            1_000,
            100,
            total_pending_sessions,
        );
        let mut state = State::new(DEFAULT_METRICS);
        for i in 0..total_pending_sessions {
            let addr: SocketAddr = format!("10.0.0.{}:51820", i).parse().unwrap();
            insert_test_initiator_session(&mut state, addr);
        }
        let addr = "127.0.0.1:8080".parse().unwrap();
        let action = filter.apply(&state, addr, Duration::ZERO, false);
        assert_eq!(action, FilterAction::Drop);
    }

    #[test]
    fn test_unverified_rate_limit_sends_cookie_after_limit() {
        let unverified_rate_limit = 5;
        let mut filter = Filter::new(
            DEFAULT_METRICS,
            unverified_rate_limit,
            100,
            Duration::from_secs(60),
            Duration::from_secs(60),
            1_000,
            100,
            100,
        );
        let state = State::new(DEFAULT_METRICS);
        let addr = "127.0.0.1:8080".parse().unwrap();
        for _ in 0..unverified_rate_limit {
            let action = filter.apply(&state, addr, Duration::ZERO, false);
            assert_eq!(action, FilterAction::Pass);
        }
        let action = filter.apply(&state, addr, Duration::ZERO, false);
        assert_eq!(action, FilterAction::SendCookie);
        assert_eq!(filter.cookie_unverified_counter, unverified_rate_limit);
    }

    #[test]
    fn test_handshake_rate_limit_drops() {
        let handshake_rate_limit = 5;
        let verified_rate_limit = 2;
        let mut filter = Filter::new(
            DEFAULT_METRICS,
            handshake_rate_limit,
            verified_rate_limit,
            Duration::from_secs(60),
            Duration::from_secs(60),
            1_000,
            100,
            100,
        );
        let state = State::new(DEFAULT_METRICS);
        let addr = "127.0.0.1:8080".parse().unwrap();
        for _ in 0..handshake_rate_limit {
            let action = filter.apply(&state, addr, Duration::ZERO, false);
            assert_eq!(action, FilterAction::Pass);
        }

        let action = filter.apply(&state, addr, Duration::ZERO, false);
        assert_eq!(action, FilterAction::SendCookie);

        for i in 0..verified_rate_limit {
            let verified_addr: SocketAddr = format!("127.0.0.{}:8080", i + 2).parse().unwrap();
            let action = filter.apply(&state, verified_addr, Duration::from_secs(61), true);
            assert_eq!(action, FilterAction::Pass);
        }

        let action = filter.apply(&state, addr, Duration::ZERO, false);
        assert_eq!(action, FilterAction::SendCookie);
    }

    #[test]
    fn test_handshake_cookie_verified_rate_limit_drops() {
        let verified_rate_limit = 5;
        let mut filter = Filter::new(
            DEFAULT_METRICS,
            100,
            verified_rate_limit,
            Duration::from_secs(60),
            Duration::from_secs(60),
            1_000,
            100,
            100,
        );
        let state = State::new(DEFAULT_METRICS);
        for i in 0..verified_rate_limit {
            let addr: SocketAddr = format!("127.0.0.{}:8080", i + 2).parse().unwrap();
            let action = filter.apply(&state, addr, Duration::from_secs(61), true);
            assert_eq!(action, FilterAction::Pass);
        }
        let action = filter.apply(
            &state,
            "127.0.0.2:8080".parse().unwrap(),
            Duration::from_secs(61),
            true,
        );
        assert_eq!(action, FilterAction::Drop);
    }

    #[test]
    fn test_unverified_rate_limit_does_not_depend_on_verified_budget() {
        let mut filter = Filter::new(
            DEFAULT_METRICS,
            2, // unverified
            0, // verified
            Duration::from_secs(60),
            Duration::from_secs(60),
            1_000,
            100,
            100,
        );
        let state = State::new(DEFAULT_METRICS);
        let addr = "127.0.0.1:8080".parse().unwrap();

        assert_eq!(
            filter.apply(&state, addr, Duration::ZERO, false),
            FilterAction::Pass
        );
        assert_eq!(
            filter.apply(&state, addr, Duration::ZERO, false),
            FilterAction::Pass
        );
        assert_eq!(
            filter.apply(&state, addr, Duration::ZERO, false),
            FilterAction::SendCookie
        );
    }

    #[test]
    fn test_tick_resets_counter() {
        let handshake_rate_limit = 5;
        let mut filter = Filter::new(
            DEFAULT_METRICS,
            handshake_rate_limit,
            handshake_rate_limit,
            Duration::from_secs(1),
            Duration::from_secs(60),
            1_000,
            100,
            100,
        );
        let state = State::new(DEFAULT_METRICS);
        let addr = "127.0.0.1:8080".parse().unwrap();
        for _ in 0..handshake_rate_limit {
            filter.apply(&state, addr, Duration::ZERO, false);
        }
        filter.tick(Duration::from_secs(1));
        let action = filter.apply(&state, addr, Duration::ZERO, false);
        assert_eq!(action, FilterAction::Pass);
    }

    #[test]
    fn test_tick_does_not_reset_before_interval() {
        let handshake_rate_limit = 5;
        let mut filter = Filter::new(
            DEFAULT_METRICS,
            handshake_rate_limit,
            handshake_rate_limit,
            Duration::from_secs(10),
            Duration::from_secs(60),
            1_000,
            100,
            100,
        );
        let state = State::new(DEFAULT_METRICS);
        let addr = "127.0.0.1:8080".parse().unwrap();
        for _ in 0..handshake_rate_limit {
            filter.apply(&state, addr, Duration::ZERO, false);
        }
        filter.tick(Duration::from_secs(5));
        let action = filter.apply(&state, addr, Duration::from_secs(5), false);
        assert_eq!(action, FilterAction::SendCookie);
    }

    #[test]
    fn test_verified_rate_limit_remains_available_after_unverified_limit() {
        let handshake_rate_limit = 5;
        let mut filter = Filter::new(
            DEFAULT_METRICS,
            handshake_rate_limit,
            handshake_rate_limit,
            Duration::from_secs(60),
            Duration::from_secs(60),
            1_000,
            100,
            100,
        );
        let state = State::new(DEFAULT_METRICS);
        let addr = "127.0.0.1:8080".parse().unwrap();
        for _ in 0..handshake_rate_limit {
            filter.apply(&state, addr, Duration::ZERO, false);
        }
        assert_eq!(
            filter.apply(&state, addr, Duration::ZERO, false),
            FilterAction::SendCookie
        );
        for i in 0..handshake_rate_limit {
            let verified_addr: SocketAddr = format!("127.0.0.{}:8080", i + 2).parse().unwrap();
            assert_eq!(
                filter.apply(&state, verified_addr, Duration::from_secs(61), true,),
                FilterAction::Pass
            );
        }
        assert_eq!(
            filter.apply(
                &state,
                "127.0.0.3:8080".parse().unwrap(),
                Duration::from_secs(61),
                true,
            ),
            FilterAction::Drop
        );
    }

    #[test]
    fn test_ip_rate_limit_only_applies_to_verified_requests() {
        let mut filter = Filter::new(
            DEFAULT_METRICS,
            100,
            100,
            Duration::from_secs(60),
            Duration::from_secs(5),
            1_000,
            100,
            100,
        );
        let state = State::new(DEFAULT_METRICS);
        let addr = "127.0.0.1:8080".parse().unwrap();

        assert_eq!(
            filter.apply(&state, addr, Duration::ZERO, false),
            FilterAction::Pass
        );
        assert_eq!(
            filter.apply(&state, addr, Duration::from_secs(1), false),
            FilterAction::Pass
        );
        assert_eq!(
            filter.apply(&state, addr, Duration::from_secs(2), true),
            FilterAction::Pass
        );
        assert_eq!(
            filter.apply(&state, addr, Duration::from_secs(3), true),
            FilterAction::Drop
        );
    }

    #[test]
    fn test_drop_does_not_increment_verified_counter() {
        let mut filter = Filter::new(
            DEFAULT_METRICS,
            100,
            2,
            Duration::from_secs(60),
            Duration::from_secs(10),
            1_000,
            100,
            100,
        );
        let state = State::new(DEFAULT_METRICS);

        assert_eq!(
            filter.apply(
                &state,
                "127.0.0.1:8080".parse().unwrap(),
                Duration::ZERO,
                true,
            ),
            FilterAction::Pass
        );
        assert_eq!(
            filter.apply(
                &state,
                "127.0.0.1:8080".parse().unwrap(),
                Duration::from_secs(1),
                true,
            ),
            FilterAction::Drop
        );
        assert_eq!(filter.cookie_verified_counter, 1);
        assert_eq!(
            filter.apply(
                &state,
                "127.0.0.2:8080".parse().unwrap(),
                Duration::from_secs(2),
                true,
            ),
            FilterAction::Pass
        );
        assert_eq!(
            filter.apply(
                &state,
                "127.0.0.3:8080".parse().unwrap(),
                Duration::from_secs(3),
                true,
            ),
            FilterAction::Drop
        );
        assert_eq!(filter.cookie_verified_counter, 2);
    }

    #[test]
    fn test_send_cookie_does_not_increment_unverified_counter() {
        let mut filter = Filter::new(
            DEFAULT_METRICS,
            1,
            100,
            Duration::from_secs(60),
            Duration::from_secs(10),
            1_000,
            100,
            100,
        );
        let state = State::new(DEFAULT_METRICS);
        let addr = "127.0.0.1:8080".parse().unwrap();

        assert_eq!(
            filter.apply(&state, addr, Duration::ZERO, false),
            FilterAction::Pass
        );
        assert_eq!(filter.cookie_unverified_counter, 1);
        assert_eq!(
            filter.apply(&state, addr, Duration::from_secs(1), false),
            FilterAction::SendCookie
        );
        assert_eq!(filter.cookie_unverified_counter, 1);
        assert_eq!(
            filter.apply(&state, addr, Duration::from_secs(2), false),
            FilterAction::SendCookie
        );
        assert_eq!(filter.cookie_unverified_counter, 1);
    }

    #[test]
    fn test_ip_rate_limit_check_does_not_refresh_lru_recency() {
        let mut filter = Filter::new(
            DEFAULT_METRICS,
            100,
            100,
            Duration::from_secs(60),
            Duration::from_secs(30),
            2,
            100,
            100,
        );
        let state = State::new(DEFAULT_METRICS);

        let addr1: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let addr2: SocketAddr = "127.0.0.2:8080".parse().unwrap();
        let addr3: SocketAddr = "127.0.0.3:8080".parse().unwrap();

        assert_eq!(
            filter.apply(&state, addr1, Duration::ZERO, true),
            FilterAction::Pass
        );
        assert_eq!(
            filter.apply(&state, addr2, Duration::from_secs(20), true),
            FilterAction::Pass
        );
        assert_eq!(
            filter.apply(&state, addr1, Duration::from_secs(25), true),
            FilterAction::Drop
        );
        assert_eq!(
            filter.apply(&state, addr3, Duration::from_secs(40), true),
            FilterAction::Pass
        );
        assert_eq!(
            filter.apply(&state, addr2, Duration::from_secs(41), true),
            FilterAction::Drop
        );
    }
}
