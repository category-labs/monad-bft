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
    sync::{
        atomic::{AtomicU32, AtomicU64, AtomicU8, Ordering},
        Mutex,
    },
    time::{Duration, Instant},
};

use eyre::Result;
use tracing::debug;

/// A circuit breaker with atomics on the *fast path*
/// and a small mutex for *rare transitions*.
///
/// Design:
/// - `state_code` holds the current state (Closed, Open, HalfOpen).
/// - `failure_count` increments on failure, resets on success.
/// - `reopen_at_ticks` is a timestamp (relative to `epoch`) after which we
///   can attempt to transition from Open -> HalfOpen.
/// - The `Mutex` (`slow_guard`) is only taken on rare transitions
///   (Closed->Open threshold trip, HalfOpen->Open after failed probe).
///
/// Goal:
/// - Hot operations (`should_use_fallback`, `record_success`) stay lock-free.
/// - Complex transitions are serialized with the mutex to avoid subtle bugs.
pub struct CircuitBreaker {
    // --- Fast path atomics ---
    state_code: AtomicU8, // 0 = Closed, 1 = Open, 2 = HalfOpen
    failure_count: AtomicU32,
    reopen_at_ticks: AtomicU64, // deadline in nanoseconds since `epoch`

    // --- Rare transition guard & config ---
    slow_guard: Mutex<()>,      // only taken when transitioning to OPEN
    failure_threshold: u32,     // number of consecutive failures before opening
    recovery_timeout: Duration, // how long to stay open before half-open probe
    epoch: Instant,             // reference point for monotonic timestamps
}

#[derive(Clone, Copy)]
#[repr(u8)]
enum State {
    Closed = 0,
    Open = 1,
    HalfOpen = 2,
}

/// Exposed breaker metrics for monitoring & debugging
pub struct CircuitBreakerMetrics {
    pub state: &'static str,
    pub failure_count: u32,
    pub time_until_recovery: Option<Duration>,
}

impl CircuitBreaker {
    /// Construct a new breaker with given failure threshold and timeout
    pub fn new(failure_threshold: u32, recovery_timeout: Duration) -> Self {
        Self {
            state_code: AtomicU8::new(State::Closed as u8),
            failure_count: AtomicU32::new(0),
            reopen_at_ticks: AtomicU64::new(0),
            slow_guard: Mutex::new(()),
            failure_threshold,
            recovery_timeout,
            epoch: Instant::now(),
        }
    }

    /// Decide whether to route requests to fallback.
    ///
    /// - Closed: always try primary
    /// - Open: stay with fallback until `reopen_at_ticks` passes
    /// - Open & expired: flip to HalfOpen (best-effort CAS) and allow primary
    /// - HalfOpen: allow primary (probing)
    pub fn should_use_fallback(&self) -> bool {
        let code = self.state_code.load(Ordering::Acquire);

        // Closed or HalfOpen -> always try primary
        if code == State::Closed as u8 || code == State::HalfOpen as u8 {
            return false;
        }

        // State == Open
        let now_ticks = self.epoch.elapsed().as_nanos() as u64;
        let reopen_at = self.reopen_at_ticks.load(Ordering::Acquire);

        if now_ticks < reopen_at {
            return true; // still cooling down, fallback only
        }

        // Deadline passed: attempt to flip Open -> HalfOpen
        let _ = self.state_code.compare_exchange(
            State::Open as u8,
            State::HalfOpen as u8,
            Ordering::AcqRel,
            Ordering::Acquire,
        );

        false // allow a probe (even if CAS lost, still safe)
    }

    /// Record a successful request.
    ///
    /// - HalfOpen + success: close breaker, reset counters
    /// - Closed + success: reset counters
    /// - Open + success: ignore state, just reset counters
    pub fn record_success(&self) {
        let code = self.state_code.load(Ordering::Acquire);
        if code == State::HalfOpen as u8 {
            debug!("Breaker closing after success in half-open state");
            self.failure_count.store(0, Ordering::Relaxed);
            self.reopen_at_ticks.store(0, Ordering::Relaxed);
            self.state_code
                .store(State::Closed as u8, Ordering::Release);
        } else if code == State::Closed as u8 {
            self.failure_count.store(0, Ordering::Relaxed);
        } else {
            // Still open: reset counter but stay open
            self.failure_count.store(0, Ordering::Relaxed);
        }
    }

    /// Record a failed request.
    ///
    /// - Closed: increment failure_count; if threshold reached,
    ///   transition to Open and set recovery deadline.
    /// - HalfOpen + failure: immediately reopen with new deadline.
    /// - Open: stay open, keep deadline as-is.
    pub fn record_failure(&self) {
        let n = self.failure_count.fetch_add(1, Ordering::Relaxed) + 1;
        let code = self.state_code.load(Ordering::Acquire);

        if code == State::Closed as u8 {
            if n >= self.failure_threshold {
                // Serialize Closed->Open promotion
                let _g = self.slow_guard.lock().ok();
                if self.state_code.load(Ordering::Acquire) == State::Closed as u8 {
                    debug!("Breaker opening after {n} consecutive failures");
                    let now_ticks = self.epoch.elapsed().as_nanos() as u64;
                    let deadline =
                        now_ticks.saturating_add(self.recovery_timeout.as_nanos() as u64);
                    self.reopen_at_ticks.store(deadline, Ordering::SeqCst);
                    self.state_code.store(State::Open as u8, Ordering::SeqCst);
                }
            }
        } else if code == State::HalfOpen as u8 {
            debug!("Breaker reopening after failure in half-open state");
            let _g = self.slow_guard.lock().ok();
            let now_ticks = self.epoch.elapsed().as_nanos() as u64;
            let deadline = now_ticks.saturating_add(self.recovery_timeout.as_nanos() as u64);
            self.reopen_at_ticks.store(deadline, Ordering::SeqCst);
            self.state_code.store(State::Open as u8, Ordering::SeqCst);
        } else {
            // Already open -> nothing to change
        }
    }

    /// Snapshot breaker state for monitoring/metrics.
    ///
    /// Reads atomics directly; values may be slightly inconsistent,
    /// but good enough for observability.
    pub fn metrics(&self) -> CircuitBreakerMetrics {
        let code = self.state_code.load(Ordering::Acquire);
        let failure_count = self.failure_count.load(Ordering::Relaxed);
        let state_str = if code == State::Closed as u8 {
            "closed"
        } else if code == State::Open as u8 {
            "open"
        } else if code == State::HalfOpen as u8 {
            "half_open"
        } else {
            "unknown"
        };

        let time_until_recovery = if state_str == "open" {
            let now_ticks = self.epoch.elapsed().as_nanos() as u64;
            let reopen_at = self.reopen_at_ticks.load(Ordering::Acquire);
            if now_ticks >= reopen_at {
                Some(Duration::ZERO)
            } else {
                Some(Duration::from_nanos(reopen_at - now_ticks))
            }
        } else {
            None
        };

        CircuitBreakerMetrics {
            state: state_str,
            failure_count,
            time_until_recovery,
        }
    }
}

/// A generic fallback executor that uses circuit breaker pattern
pub struct FallbackExecutor<P> {
    pub primary: P,
    pub fallback: Option<P>,
    pub circuit_breaker: CircuitBreaker,
}

impl<P> FallbackExecutor<P> {
    pub fn new(primary: P, fallback: Option<P>, circuit_breaker: CircuitBreaker) -> Self {
        Self {
            primary,
            fallback,
            circuit_breaker,
        }
    }

    /// Execute a function with automatic fallback and circuit breaker logic
    pub async fn execute<'a, Ret, Func, Fut>(&'a self, f: Func) -> Result<Ret>
    where
        Func: Fn(&'a P) -> Fut,
        Fut: std::future::Future<Output = Result<Ret>>,
    {
        // If no fallback, just execute on primary
        let Some(fallback) = self.fallback.as_ref() else {
            return f(&self.primary).await;
        };

        // Check circuit breaker state
        if self.circuit_breaker.should_use_fallback() {
            debug!("Circuit breaker is open, using fallback");
            return f(fallback).await;
        }

        // Try primary
        match f(&self.primary).await {
            Ok(result) => {
                self.circuit_breaker.record_success();
                Ok(result)
            }
            Err(e) => {
                debug!(?e, "Primary source failed, recording failure");
                self.circuit_breaker.record_failure();

                // Try fallback
                f(fallback).await
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            atomic::{AtomicUsize, Ordering as AOrd},
            Arc, Barrier,
        },
        thread,
        time::{Duration, Instant},
    };

    use super::*;

    /// Utility: poll a condition until it becomes true or timeout elapses.
    /// Returns true if the condition became true.
    fn wait_until(timeout: Duration, check_every: Duration, cond: impl Fn() -> bool) -> bool {
        let start = Instant::now();
        loop {
            if cond() {
                return true;
            }
            if start.elapsed() >= timeout {
                return false;
            }
            thread::sleep(check_every);
        }
    }

    /// The breaker starts closed and does not use fallback.
    #[test]
    fn initial_state_is_closed() {
        let cb = CircuitBreaker::new(3, Duration::from_millis(200));

        assert!(!cb.should_use_fallback(), "closed should route to primary");
        let m = cb.metrics();
        assert_eq!(m.state, "closed");
        assert_eq!(m.failure_count, 0);
        assert!(m.time_until_recovery.is_none());
    }

    /// Crossing the failure threshold opens the breaker and enforces cooldown.
    #[test]
    fn opens_after_threshold_and_respects_cooldown() {
        let cb = CircuitBreaker::new(3, Duration::from_millis(200));

        cb.record_failure();
        cb.record_failure();
        assert!(!cb.should_use_fallback(), "below threshold remains closed");

        // Third failure trips to Open
        cb.record_failure();
        assert!(cb.should_use_fallback(), "open should route to fallback");

        let m = cb.metrics();
        assert_eq!(m.state, "open");
        assert_eq!(m.failure_count, 3);
        assert!(m.time_until_recovery.is_some());

        // Still within cooldown - should continue to use fallback
        assert!(cb.should_use_fallback());
    }

    /// After cooldown elapses, the breaker allows a half-open probe and a success closes it.
    #[test]
    fn half_open_success_closes() {
        let cb = CircuitBreaker::new(1, Duration::from_millis(120));

        // Trip to open
        cb.record_failure();
        assert!(cb.should_use_fallback());

        // Wait past cooldown window
        thread::sleep(Duration::from_millis(140));

        // First call after deadline flips to half-open and allows primary
        assert!(
            !cb.should_use_fallback(),
            "after deadline we should probe primary"
        );
        // Success in half-open closes and resets counters
        cb.record_success();

        let m = cb.metrics();
        assert_eq!(m.state, "closed");
        assert_eq!(m.failure_count, 0);
        assert!(m.time_until_recovery.is_none());
        assert!(!cb.should_use_fallback());
    }

    /// If the half-open probe fails, we reopen and reset cooldown.
    #[test]
    fn half_open_failure_reopens() {
        let cb = CircuitBreaker::new(1, Duration::from_millis(120));

        // Trip to open
        cb.record_failure();
        assert!(cb.should_use_fallback());

        // Wait past cooldown then attempt a probe
        thread::sleep(Duration::from_millis(140));
        assert!(!cb.should_use_fallback(), "probe allowed");

        // Probe fails -> back to open with fresh deadline
        cb.record_failure();

        let m = cb.metrics();
        assert_eq!(m.state, "open");
        assert!(m.time_until_recovery.is_some(), "cooldown should reset");

        // Immediately after failure we should be in fallback again
        assert!(cb.should_use_fallback());
    }

    /// Success while open should not close the breaker - only a successful
    /// half-open probe may close it.
    #[test]
    fn success_while_open_does_not_close() {
        let cb = CircuitBreaker::new(2, Duration::from_millis(200));

        cb.record_failure();
        cb.record_failure(); // open
        assert!(cb.should_use_fallback());

        // Success while still open
        cb.record_success();

        let m = cb.metrics();
        assert_eq!(m.state, "open", "still open after success while open");
        assert!(m.time_until_recovery.is_some());
        assert!(cb.should_use_fallback());
    }

    /// Many threads trying just after the deadline:
    /// we allow multiple callers through to probe in this simplified design.
    /// We assert that at least one got through and that the state reflects half-open shortly.
    #[test]
    fn concurrent_probe_after_deadline() {
        let cb = Arc::new(CircuitBreaker::new(1, Duration::from_millis(60)));

        // Trip to open
        cb.record_failure();
        assert!(cb.should_use_fallback());

        // Wait just past deadline so a burst of threads arrive together
        thread::sleep(Duration::from_millis(80));

        let n = 32;
        let barrier = Arc::new(Barrier::new(n));
        let passed = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::with_capacity(n);
        for _ in 0..n {
            let b = barrier.clone();
            let cb_cloned = cb.clone();
            let passed_cloned = passed.clone();
            handles.push(thread::spawn(move || {
                b.wait(); // line up calls
                if !cb_cloned.should_use_fallback() {
                    passed_cloned.fetch_add(1, AOrd::Relaxed);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        let probes = passed.load(AOrd::Relaxed);
        assert!(
            probes >= 1,
            "at least one thread should have been allowed to probe, got {probes}"
        );

        // The state should quickly reflect half-open
        let ok = wait_until(Duration::from_millis(50), Duration::from_millis(2), || {
            cb.metrics().state == "half_open" || cb.metrics().state == "closed"
        });
        assert!(
            ok,
            "breaker should be half_open or closed shortly after probes"
        );
    }

    /// Multiple threads racing to push the counter across the threshold should
    /// open the breaker at most once. We cannot directly count promotions here,
    /// but we can assert that we end up open with a deadline set.
    #[test]
    fn concurrent_threshold_trip_opens() {
        let cb = Arc::new(CircuitBreaker::new(8, Duration::from_millis(200)));

        let n_threads = 16;
        let barrier = Arc::new(Barrier::new(n_threads));
        let mut handles = Vec::with_capacity(n_threads);

        for _ in 0..n_threads {
            let b = barrier.clone();
            let cb_c = cb.clone();
            handles.push(thread::spawn(move || {
                b.wait();
                cb_c.record_failure();
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        // We should be open and have a remaining recovery time
        let m = cb.metrics();
        assert_eq!(m.state, "open");
        assert!(m.time_until_recovery.is_some());
        assert!(cb.should_use_fallback());
    }

    /// The reported time_until_recovery should decrease over time while open.
    #[test]
    fn metrics_time_until_recovery_counts_down() {
        let cb = CircuitBreaker::new(1, Duration::from_millis(200));

        // Trip to open
        cb.record_failure();
        let m1 = cb.metrics();
        assert_eq!(m1.state, "open");
        let t1 = m1.time_until_recovery.expect("expected a cooldown");

        // Wait a bit and ensure the remaining time decreased but is not negative
        thread::sleep(Duration::from_millis(80));
        let m2 = cb.metrics();
        let t2 = m2.time_until_recovery.expect("still within cooldown");
        assert!(t2 <= t1, "cooldown should count down");
        assert!(t2 > Duration::ZERO, "still not yet expired");
    }

    /// A full cycle smoke test: closed -> open -> half-open -> closed.
    /// This asserts the common operational loop holds under simple use.
    #[test]
    fn full_cycle_works() {
        let cb = CircuitBreaker::new(2, Duration::from_millis(100));

        // Closed -> Open
        cb.record_failure();
        cb.record_failure();
        assert!(cb.should_use_fallback());
        assert_eq!(cb.metrics().state, "open");

        // Wait for half-open eligibility
        thread::sleep(Duration::from_millis(130));
        assert!(!cb.should_use_fallback()); // probe

        // Success closes
        cb.record_success();
        assert_eq!(cb.metrics().state, "closed");
        assert!(!cb.should_use_fallback());
    }
}
