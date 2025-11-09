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
use tracing::debug;

use crate::{metrics::*, state::State};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterAction {
    Pass,
    SendCookie,
    Drop,
}

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
}

impl Filter {
    pub fn new(
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
        }
    }

    pub fn metrics(&self) -> &ExecutorMetrics {
        &self.metrics
    }

    pub fn tick(&mut self, duration_since_start: Duration) {
        if duration_since_start.saturating_sub(self.last_reset)
            >= self.handshake_rate_reset_interval
        {
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
        self.counter += 1;

        let total_sessions = state.total_sessions();
        if total_sessions >= self.high_watermark_sessions {
            debug!(
                remote_addr = %remote_addr,
                sessions = total_sessions,
                high_watermark = self.high_watermark_sessions,
                "high load - rejecting new handshake"
            );
            self.metrics[GAUGE_WIREAUTH_FILTER_DROP] += 1;
            return FilterAction::Drop;
        }

        let under_load = self.counter >= self.handshake_rate_limit;

        if under_load {
            debug!(
                remote_addr = %remote_addr,
                counter = self.counter,
                rate_limit = self.handshake_rate_limit,
                "rate limit exceeded - dropping handshake"
            );
            self.metrics[GAUGE_WIREAUTH_FILTER_DROP] += 1;
            return FilterAction::Drop;
        }

        if total_sessions < self.low_watermark_sessions {
            self.metrics[GAUGE_WIREAUTH_FILTER_PASS] += 1;
            return FilterAction::Pass;
        }

        if !cookie_valid {
            self.metrics[GAUGE_WIREAUTH_FILTER_SEND_COOKIE] += 1;
            return FilterAction::SendCookie;
        }

        let ip = remote_addr.ip();
        let window_start = duration_since_start.saturating_sub(self.ip_rate_limit_window);

        if let Some(last_time) = self.ip_request_history.get_mut(&ip) {
            if *last_time >= window_start {
                debug!(ip = %ip, "ip rate limit exceeded");
                return FilterAction::Drop;
            }
            *last_time = duration_since_start;
        } else {
            self.ip_request_history.put(ip, duration_since_start);
        }

        let session_count = state.ip_session_count(&ip);
        if session_count >= self.max_sessions_per_ip {
            debug!(
                ip = %ip,
                max = self.max_sessions_per_ip,
                "too many sessions for ip"
            );
            self.metrics[GAUGE_WIREAUTH_FILTER_DROP] += 1;
            return FilterAction::Drop;
        }

        self.metrics[GAUGE_WIREAUTH_FILTER_PASS] += 1;
        FilterAction::Pass
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::insert_test_initiator_session;

    fn default_filter() -> Filter {
        Filter::new(
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
        let state = State::new();
        let addr = "127.0.0.1:8080".parse().unwrap();
        let action = filter.apply(&state, addr, Duration::from_secs(1), false);
        assert_eq!(action, FilterAction::Pass);
    }

    #[test]
    fn test_high_watermark_drops() {
        let high_watermark = 10;
        let mut filter = Filter::new(
            100,
            Duration::from_secs(60),
            Duration::from_secs(60),
            1000,
            10,
            5,
            high_watermark,
        );
        let mut state = State::new();
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
            100,
            Duration::from_secs(60),
            Duration::from_secs(60),
            1000,
            10,
            low_watermark,
            10,
        );
        let mut state = State::new();
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
            100,
            Duration::from_secs(60),
            Duration::from_secs(60),
            1000,
            10,
            low_watermark,
            10,
        );
        let mut state = State::new();
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
            handshake_rate_limit,
            Duration::from_secs(60),
            Duration::from_secs(60),
            1000,
            10,
            50,
            100,
        );
        let state = State::new();
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
            handshake_rate_limit,
            Duration::from_secs(60),
            Duration::from_secs(60),
            1000,
            10,
            50,
            100,
        );
        let state = State::new();
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
            handshake_rate_limit,
            Duration::from_secs(1),
            Duration::from_secs(60),
            1000,
            10,
            50,
            100,
        );
        let state = State::new();
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
            handshake_rate_limit,
            Duration::from_secs(10),
            Duration::from_secs(60),
            1000,
            10,
            50,
            100,
        );
        let state = State::new();
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
            100,
            Duration::from_secs(60),
            Duration::from_secs(5),
            1000,
            10,
            low_watermark,
            10,
        );
        let mut state = State::new();
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
            100,
            Duration::from_secs(60),
            Duration::from_secs(5),
            1000,
            10,
            low_watermark,
            10,
        );
        let mut state = State::new();
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
            100,
            Duration::from_secs(60),
            Duration::from_secs(60),
            1000,
            max_sessions_per_ip,
            low_watermark,
            10,
        );
        let mut state = State::new();
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
            100,
            Duration::from_secs(60),
            Duration::from_secs(60),
            1000,
            max_sessions_per_ip,
            low_watermark,
            10,
        );
        let mut state = State::new();
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
            handshake_rate_limit,
            Duration::from_secs(60),
            Duration::from_secs(60),
            1000,
            10,
            low_watermark,
            10,
        );
        let mut state = State::new();
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
            100,
            Duration::from_secs(60),
            Duration::from_secs(5),
            2,
            10,
            low_watermark,
            10,
        );
        let mut state = State::new();
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
