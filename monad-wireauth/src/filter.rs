use std::{
    collections::HashMap,
    net::{IpAddr, SocketAddr},
    num::NonZeroUsize,
    time::Duration,
};

use lru::LruCache;
use tracing::debug;

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
    ip_session_counts: HashMap<IpAddr, usize>,
    max_sessions_per_ip: usize,
    low_watermark_sessions: usize,
    high_watermark_sessions: usize,
    total_sessions: usize,
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
            ip_session_counts: HashMap::new(),
            max_sessions_per_ip,
            low_watermark_sessions,
            high_watermark_sessions,
            total_sessions: 0,
        }
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
        remote_addr: SocketAddr,
        duration_since_start: Duration,
        cookie_valid: bool,
    ) -> FilterAction {
        self.counter += 1;

        if self.total_sessions >= self.high_watermark_sessions {
            debug!(
                remote_addr = %remote_addr,
                sessions = self.total_sessions,
                high_watermark = self.high_watermark_sessions,
                "high load - rejecting new handshake"
            );
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
            return FilterAction::Drop;
        }

        if self.total_sessions < self.low_watermark_sessions {
            return FilterAction::Pass;
        }

        if !cookie_valid {
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

        let session_count = self.ip_session_counts.get(&ip).copied().unwrap_or(0);
        if session_count >= self.max_sessions_per_ip {
            debug!(
                ip = %ip,
                max = self.max_sessions_per_ip,
                "too many sessions for ip"
            );
            return FilterAction::Drop;
        }

        FilterAction::Pass
    }

    pub fn on_session_added(&mut self, ip: IpAddr) {
        *self.ip_session_counts.entry(ip).or_insert(0) += 1;
        self.total_sessions += 1;
    }

    pub fn on_session_removed(&mut self, ip: IpAddr) {
        if let Some(count) = self.ip_session_counts.get_mut(&ip) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                self.ip_session_counts.remove(&ip);
            }
        }
        self.total_sessions = self.total_sessions.saturating_sub(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let addr = "127.0.0.1:8080".parse().unwrap();
        let action = filter.apply(addr, Duration::from_secs(1), false);
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
        for i in 0..high_watermark {
            filter.on_session_added(format!("10.0.0.{}", i).parse().unwrap());
        }
        let addr = "127.0.0.1:8080".parse().unwrap();
        let action = filter.apply(addr, Duration::from_secs(1), false);
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
        for i in 0..low_watermark {
            filter.on_session_added(format!("10.0.0.{}", i).parse().unwrap());
        }
        let addr = "127.0.0.1:8080".parse().unwrap();
        let action = filter.apply(addr, Duration::from_secs(1), false);
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
        for i in 0..low_watermark {
            filter.on_session_added(format!("10.0.0.{}", i).parse().unwrap());
        }
        let addr = "127.0.0.1:8080".parse().unwrap();
        let action = filter.apply(addr, Duration::from_secs(1), true);
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
        let addr = "127.0.0.1:8080".parse().unwrap();
        for _ in 0..handshake_rate_limit {
            filter.apply(addr, Duration::from_secs(1), false);
        }
        let action = filter.apply(addr, Duration::from_secs(1), false);
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
        let addr = "127.0.0.1:8080".parse().unwrap();
        for _ in 0..handshake_rate_limit {
            filter.apply(addr, Duration::from_secs(1), false);
        }
        let action = filter.apply(addr, Duration::from_secs(1), true);
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
        let addr = "127.0.0.1:8080".parse().unwrap();
        for _ in 0..handshake_rate_limit {
            filter.apply(addr, Duration::from_secs(0), false);
        }
        filter.tick(Duration::from_secs(1));
        let action = filter.apply(addr, Duration::from_secs(1), false);
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
        let addr = "127.0.0.1:8080".parse().unwrap();
        for _ in 0..handshake_rate_limit {
            filter.apply(addr, Duration::from_secs(0), false);
        }
        filter.tick(Duration::from_secs(5));
        let action = filter.apply(addr, Duration::from_secs(5), false);
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
        for i in 0..low_watermark {
            filter.on_session_added(format!("10.0.0.{}", i).parse().unwrap());
        }
        let addr = "127.0.0.1:8080".parse().unwrap();
        filter.apply(addr, Duration::from_secs(0), true);
        let action = filter.apply(addr, Duration::from_secs(3), true);
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
        for i in 0..low_watermark {
            filter.on_session_added(format!("10.0.0.{}", i).parse().unwrap());
        }
        let addr = "127.0.0.1:8080".parse().unwrap();
        filter.apply(addr, Duration::from_secs(0), true);
        let action = filter.apply(addr, Duration::from_secs(6), true);
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
        for i in 0..low_watermark {
            filter.on_session_added(format!("10.0.0.{}", i).parse().unwrap());
        }
        let ip: IpAddr = "192.168.1.1".parse().unwrap();
        for _ in 0..max_sessions_per_ip {
            filter.on_session_added(ip);
        }
        let addr = "192.168.1.1:8080".parse().unwrap();
        let action = filter.apply(addr, Duration::from_secs(1), true);
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
        for i in 0..low_watermark {
            filter.on_session_added(format!("10.0.0.{}", i).parse().unwrap());
        }
        let ip: IpAddr = "192.168.1.1".parse().unwrap();
        filter.on_session_added(ip);
        let addr = "192.168.1.1:8080".parse().unwrap();
        let action = filter.apply(addr, Duration::from_secs(1), true);
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
        for i in 0..low_watermark {
            filter.on_session_added(format!("10.0.0.{}", i).parse().unwrap());
        }
        let addr = "127.0.0.1:8080".parse().unwrap();
        for _ in 0..handshake_rate_limit {
            filter.apply(addr, Duration::from_secs(1), false);
        }
        let action = filter.apply(addr, Duration::from_secs(1), false);
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
        for i in 0..low_watermark {
            filter.on_session_added(format!("10.0.0.{}", i).parse().unwrap());
        }
        let addr1 = "192.168.1.1:8080".parse().unwrap();
        let addr2 = "192.168.1.2:8080".parse().unwrap();
        let addr3 = "192.168.1.3:8080".parse().unwrap();
        filter.apply(addr1, Duration::from_secs(0), true);
        filter.apply(addr2, Duration::from_secs(1), true);
        filter.apply(addr3, Duration::from_secs(2), true);
        let action = filter.apply(addr1, Duration::from_secs(3), true);
        assert_eq!(action, FilterAction::Pass);
    }
}
