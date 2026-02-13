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
use tracing::{debug, info, warn};

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
    counter: u64,
    last_reset: Duration,
    handshake_rate_limit: u64,
    handshake_rate_reset_interval: Duration,
    ip_request_history: LruCache<IpAddr, Duration>,
    ip_rate_limit_window: Duration,
    max_sessions_per_ip: usize,
    low_watermark_sessions: usize,
    high_watermark_sessions: usize,
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
        handshake_rate_limit: u64,
        handshake_rate_reset_interval: Duration,
        ip_rate_limit_window: Duration,
        ip_history_capacity: usize,
        max_sessions_per_ip: usize,
        low_watermark_sessions: usize,
        high_watermark_sessions: usize,
    ) -> Self {
        Self {
            counter: 0,
            last_reset: Duration::ZERO,
            handshake_rate_limit,
            handshake_rate_reset_interval,
            ip_request_history: LruCache::new(NonZeroUsize::new(ip_history_capacity).unwrap()),
            ip_rate_limit_window,
            max_sessions_per_ip,
            low_watermark_sessions,
            high_watermark_sessions,
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
            self.counter = 0;
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
        info!(remote_addr = %remote_addr, cookie_valid = cookie_valid, "applying filter");
        self.counter += 1;
        let total_sessions = state.total_sessions();
        let ip = remote_addr.ip();

        let action = self
            .check_high_watermark(total_sessions, remote_addr)
            .or_else(|| self.check_rate_limit(remote_addr))
            .or_else(|| self.check_low_watermark(total_sessions))
            .or_else(|| self.check_cookie_validity(cookie_valid))
            .or_else(|| self.check_ip_rate_limit(ip, duration_since_start))
            .or_else(|| self.check_max_sessions_per_ip(state, ip))
            .unwrap_or(FilterAction::Pass);

        self.record_metric(action);
        action
    }

    fn check_high_watermark(
        &self,
        total_sessions: usize,
        remote_addr: SocketAddr,
    ) -> Option<FilterAction> {
        (total_sessions >= self.high_watermark_sessions).then(|| {
            debug!(
                remote_addr = %remote_addr,
                sessions = total_sessions,
                high_watermark = self.high_watermark_sessions,
                "high load - rejecting new handshake"
            );
            FilterAction::Drop
        })
    }

    fn check_rate_limit(&self, remote_addr: SocketAddr) -> Option<FilterAction> {
        (self.counter >= self.handshake_rate_limit).then(|| {
            debug!(
                remote_addr = %remote_addr,
                counter = self.counter,
                rate_limit = self.handshake_rate_limit,
                "rate limit exceeded - dropping handshake"
            );
            FilterAction::Drop
        })
    }

    fn check_low_watermark(&self, total_sessions: usize) -> Option<FilterAction> {
        (total_sessions < self.low_watermark_sessions).then_some(FilterAction::Pass)
    }

    fn check_cookie_validity(&self, cookie_valid: bool) -> Option<FilterAction> {
        (!cookie_valid).then_some(FilterAction::SendCookie)
    }

    fn check_ip_rate_limit(
        &mut self,
        ip: IpAddr,
        duration_since_start: Duration,
    ) -> Option<FilterAction> {
        let window_start = duration_since_start.saturating_sub(self.ip_rate_limit_window);

        match self.ip_request_history.get_mut(&ip) {
            Some(last_time) if *last_time >= window_start => {
                debug!(ip = %ip, "ip rate limit exceeded");
                Some(FilterAction::Drop)
            }
            Some(last_time) => {
                *last_time = duration_since_start;
                None
            }
            None => {
                self.ip_request_history.put(ip, duration_since_start);
                self.metrics[self.metric_names.filter_ip_request_history_size] =
                    self.ip_request_history.len() as u64;
                None
            }
        }
    }

    fn check_max_sessions_per_ip(&self, state: &State, ip: IpAddr) -> Option<FilterAction> {
        (state.ip_session_count(&ip) >= self.max_sessions_per_ip).then(|| {
            debug!(
                ip = %ip,
                max = self.max_sessions_per_ip,
                "too many sessions for ip"
            );
            FilterAction::Drop
        })
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
    use crate::{metrics::DEFAULT_METRICS, state::insert_test_initiator_session};

    fn default_filter() -> Filter {
        Filter::new(
            DEFAULT_METRICS,
            100,
            Duration::from_secs(60),
            Duration::from_secs(60),
            1000,
            10,
            50,
            100,
        )
    }

    #[test]
    fn test_basic_pass_no_limits() {
        let mut filter = default_filter();
        let state = State::new(DEFAULT_METRICS);
        let addr = "127.0.0.1:8080".parse().unwrap();
        let action = filter.apply(&state, addr, Duration::from_secs(1), false);
        assert_eq!(action, FilterAction::Pass);
    }

    #[test]
    fn test_high_watermark_drops() {
        let high_watermark = 10;
        let mut filter = Filter::new(
            DEFAULT_METRICS,
            100,
            Duration::from_secs(60),
            Duration::from_secs(60),
            1000,
            10,
            5,
            high_watermark,
        );
        let mut state = State::new(DEFAULT_METRICS);
        for i in 0..high_watermark {
            let ip: IpAddr = format!("10.0.0.{}", i).parse().unwrap();
            insert_test_initiator_session(&mut state, SocketAddr::new(ip, 51820));
        }
        let addr = "127.0.0.1:8080".parse().unwrap();
        let action = filter.apply(&state, addr, Duration::from_secs(1), false);
        assert_eq!(action, FilterAction::Drop);
    }

    #[test]
    fn test_between_watermarks_requires_cookie() {
        let low_watermark = 5;
        let mut filter = Filter::new(
            DEFAULT_METRICS,
            100,
            Duration::from_secs(60),
            Duration::from_secs(60),
            1000,
            10,
            low_watermark,
            10,
        );
        let mut state = State::new(DEFAULT_METRICS);
        for i in 0..low_watermark {
            let ip: IpAddr = format!("10.0.0.{}", i).parse().unwrap();
            insert_test_initiator_session(&mut state, SocketAddr::new(ip, 51820));
        }
        let addr = "127.0.0.1:8080".parse().unwrap();
        let action = filter.apply(&state, addr, Duration::from_secs(1), false);
        assert_eq!(action, FilterAction::SendCookie);
    }

    #[test]
    fn test_between_watermarks_passes_with_cookie() {
        let low_watermark = 5;
        let mut filter = Filter::new(
            DEFAULT_METRICS,
            100,
            Duration::from_secs(60),
            Duration::from_secs(60),
            1000,
            10,
            low_watermark,
            10,
        );
        let mut state = State::new(DEFAULT_METRICS);
        for i in 0..low_watermark {
            let ip: IpAddr = format!("10.0.0.{}", i).parse().unwrap();
            insert_test_initiator_session(&mut state, SocketAddr::new(ip, 51820));
        }
        let addr = "127.0.0.1:8080".parse().unwrap();
        let action = filter.apply(&state, addr, Duration::from_secs(1), true);
        assert_eq!(action, FilterAction::Pass);
    }

    #[test]
    fn test_handshake_rate_limit_drops() {
        let handshake_rate_limit = 5;
        let mut filter = Filter::new(
            DEFAULT_METRICS,
            handshake_rate_limit,
            Duration::from_secs(60),
            Duration::from_secs(60),
            1000,
            10,
            50,
            100,
        );
        let state = State::new(DEFAULT_METRICS);
        let addr = "127.0.0.1:8080".parse().unwrap();
        for _ in 0..handshake_rate_limit {
            filter.apply(&state, addr, Duration::from_secs(1), false);
        }
        let action = filter.apply(&state, addr, Duration::from_secs(1), false);
        assert_eq!(action, FilterAction::Drop);
    }

    #[test]
    fn test_handshake_rate_limit_drops_with_cookie() {
        let handshake_rate_limit = 5;
        let mut filter = Filter::new(
            DEFAULT_METRICS,
            handshake_rate_limit,
            Duration::from_secs(60),
            Duration::from_secs(60),
            1000,
            10,
            50,
            100,
        );
        let state = State::new(DEFAULT_METRICS);
        let addr = "127.0.0.1:8080".parse().unwrap();
        for _ in 0..handshake_rate_limit {
            filter.apply(&state, addr, Duration::from_secs(1), false);
        }
        let action = filter.apply(&state, addr, Duration::from_secs(1), true);
        assert_eq!(action, FilterAction::Drop);
    }

    #[test]
    fn test_tick_resets_counter() {
        let handshake_rate_limit = 5;
        let mut filter = Filter::new(
            DEFAULT_METRICS,
            handshake_rate_limit,
            Duration::from_secs(1),
            Duration::from_secs(60),
            1000,
            10,
            50,
            100,
        );
        let state = State::new(DEFAULT_METRICS);
        let addr = "127.0.0.1:8080".parse().unwrap();
        for _ in 0..handshake_rate_limit {
            filter.apply(&state, addr, Duration::from_secs(0), false);
        }
        filter.tick(Duration::from_secs(1));
        let action = filter.apply(&state, addr, Duration::from_secs(1), false);
        assert_eq!(action, FilterAction::Pass);
    }

    #[test]
    fn test_tick_does_not_reset_before_interval() {
        let handshake_rate_limit = 5;
        let mut filter = Filter::new(
            DEFAULT_METRICS,
            handshake_rate_limit,
            Duration::from_secs(10),
            Duration::from_secs(60),
            1000,
            10,
            50,
            100,
        );
        let state = State::new(DEFAULT_METRICS);
        let addr = "127.0.0.1:8080".parse().unwrap();
        for _ in 0..handshake_rate_limit {
            filter.apply(&state, addr, Duration::from_secs(0), false);
        }
        filter.tick(Duration::from_secs(5));
        let action = filter.apply(&state, addr, Duration::from_secs(5), false);
        assert_eq!(action, FilterAction::Drop);
    }

    #[test]
    fn test_ip_rate_limit_within_window() {
        let low_watermark = 5;
        let mut filter = Filter::new(
            DEFAULT_METRICS,
            100,
            Duration::from_secs(60),
            Duration::from_secs(5),
            1000,
            10,
            low_watermark,
            10,
        );
        let mut state = State::new(DEFAULT_METRICS);
        for i in 0..low_watermark {
            let ip: IpAddr = format!("10.0.0.{}", i).parse().unwrap();
            insert_test_initiator_session(&mut state, SocketAddr::new(ip, 51820));
        }
        let addr = "127.0.0.1:8080".parse().unwrap();
        filter.apply(&state, addr, Duration::from_secs(0), true);
        let action = filter.apply(&state, addr, Duration::from_secs(3), true);
        assert_eq!(action, FilterAction::Drop);
    }

    #[test]
    fn test_ip_rate_limit_after_window() {
        let low_watermark = 5;
        let mut filter = Filter::new(
            DEFAULT_METRICS,
            100,
            Duration::from_secs(60),
            Duration::from_secs(5),
            1000,
            10,
            low_watermark,
            10,
        );
        let mut state = State::new(DEFAULT_METRICS);
        for i in 0..low_watermark {
            let ip: IpAddr = format!("10.0.0.{}", i).parse().unwrap();
            insert_test_initiator_session(&mut state, SocketAddr::new(ip, 51820));
        }
        let addr = "127.0.0.1:8080".parse().unwrap();
        filter.apply(&state, addr, Duration::from_secs(0), true);
        let action = filter.apply(&state, addr, Duration::from_secs(6), true);
        assert_eq!(action, FilterAction::Pass);
    }

    #[test]
    fn test_max_sessions_per_ip_drops() {
        let low_watermark = 5;
        let max_sessions_per_ip = 2;
        let mut filter = Filter::new(
            DEFAULT_METRICS,
            100,
            Duration::from_secs(60),
            Duration::from_secs(60),
            1000,
            max_sessions_per_ip,
            low_watermark,
            10,
        );
        let mut state = State::new(DEFAULT_METRICS);
        for i in 0..low_watermark {
            let ip: IpAddr = format!("10.0.0.{}", i).parse().unwrap();
            insert_test_initiator_session(&mut state, SocketAddr::new(ip, 51820));
        }
        let ip: IpAddr = "192.168.1.1".parse().unwrap();
        for _ in 0..max_sessions_per_ip {
            insert_test_initiator_session(&mut state, SocketAddr::new(ip, 51820));
        }
        let addr = "192.168.1.1:8080".parse().unwrap();
        let action = filter.apply(&state, addr, Duration::from_secs(1), true);
        assert_eq!(action, FilterAction::Drop);
    }

    #[test]
    fn test_max_sessions_per_ip_passes_under_limit() {
        let low_watermark = 5;
        let max_sessions_per_ip = 2;
        let mut filter = Filter::new(
            DEFAULT_METRICS,
            100,
            Duration::from_secs(60),
            Duration::from_secs(60),
            1000,
            max_sessions_per_ip,
            low_watermark,
            10,
        );
        let mut state = State::new(DEFAULT_METRICS);
        for i in 0..low_watermark {
            let ip: IpAddr = format!("10.0.0.{}", i).parse().unwrap();
            insert_test_initiator_session(&mut state, SocketAddr::new(ip, 51820));
        }
        let ip: IpAddr = "192.168.1.1".parse().unwrap();
        insert_test_initiator_session(&mut state, SocketAddr::new(ip, 51820));
        let addr = "192.168.1.1:8080".parse().unwrap();
        let action = filter.apply(&state, addr, Duration::from_secs(1), true);
        assert_eq!(action, FilterAction::Pass);
    }

    #[test]
    fn test_combined_rate_limit_and_watermark() {
        let handshake_rate_limit = 5;
        let low_watermark = 5;
        let mut filter = Filter::new(
            DEFAULT_METRICS,
            handshake_rate_limit,
            Duration::from_secs(60),
            Duration::from_secs(60),
            1000,
            10,
            low_watermark,
            10,
        );
        let mut state = State::new(DEFAULT_METRICS);
        for i in 0..low_watermark {
            let ip: IpAddr = format!("10.0.0.{}", i).parse().unwrap();
            insert_test_initiator_session(&mut state, SocketAddr::new(ip, 51820));
        }
        let addr = "127.0.0.1:8080".parse().unwrap();
        for _ in 0..handshake_rate_limit {
            filter.apply(&state, addr, Duration::from_secs(1), false);
        }
        let action = filter.apply(&state, addr, Duration::from_secs(1), false);
        assert_eq!(action, FilterAction::Drop);
    }

    #[test]
    fn test_lru_cache_eviction() {
        let low_watermark = 5;
        let mut filter = Filter::new(
            DEFAULT_METRICS,
            100,
            Duration::from_secs(60),
            Duration::from_secs(5),
            2,
            10,
            low_watermark,
            10,
        );
        let mut state = State::new(DEFAULT_METRICS);
        for i in 0..low_watermark {
            let ip: IpAddr = format!("10.0.0.{}", i).parse().unwrap();
            insert_test_initiator_session(&mut state, SocketAddr::new(ip, 51820));
        }
        let addr1 = "192.168.1.1:8080".parse().unwrap();
        let addr2 = "192.168.1.2:8080".parse().unwrap();
        let addr3 = "192.168.1.3:8080".parse().unwrap();
        filter.apply(&state, addr1, Duration::from_secs(0), true);
        filter.apply(&state, addr2, Duration::from_secs(1), true);
        filter.apply(&state, addr3, Duration::from_secs(2), true);
        let action = filter.apply(&state, addr1, Duration::from_secs(3), true);
        assert_eq!(action, FilterAction::Pass);
    }
}
