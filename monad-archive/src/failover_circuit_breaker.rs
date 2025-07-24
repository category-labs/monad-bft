use std::{
    sync::RwLock,
    time::{Duration, Instant},
};

use eyre::Result;
use tracing::{debug, warn};

/// A circuit breaker that tracks failures and manages failover behavior
///
/// The circuit breaker has three states:
/// - Closed: Normal operation, all requests go to primary
/// - Open: Too many failures, all requests go to fallback
/// - Half-Open: After timeout, allow one request to test if primary is back
#[derive(Clone)]
pub struct CircuitBreaker {
    state: Arc<RwLock<CircuitBreakerState>>,
}

#[derive(Clone)]
struct CircuitBreakerState {
    /// Current state of the circuit
    state: State,
    /// Number of consecutive failures
    failure_count: u32,
    /// Threshold for opening the circuit
    failure_threshold: u32,
    /// How long to wait before trying primary again when circuit is open
    recovery_timeout: Duration,
    /// When the circuit was opened (for timeout calculation)
    opened_at: Option<Instant>,
}

#[derive(Clone, Copy, PartialEq)]
enum State {
    Closed,
    Open,
    HalfOpen,
}

impl CircuitBreaker {
    pub fn new(failure_threshold: u32, recovery_timeout: Duration) -> Self {
        Self {
            state: Arc::new(RwLock::new(CircuitBreakerState {
                state: State::Closed,
                failure_count: 0,
                failure_threshold,
                recovery_timeout,
                opened_at: None,
            })),
        }
    }

    /// Check if we should use the fallback
    pub fn should_use_fallback(&self) -> bool {
        match self.state.read() {
            Ok(state) => match state.state {
                State::Closed => false,
                State::Open => {
                    // Check if we should transition to half-open
                    if let Some(opened_at) = state.opened_at {
                        if opened_at.elapsed() >= state.recovery_timeout {
                            // Try to transition to half-open
                            drop(state);
                            if let Ok(mut state) = self.state.write() {
                                if state.state == State::Open {
                                    debug!("Circuit breaker transitioning to half-open");
                                    state.state = State::HalfOpen;
                                }
                            }
                            return false; // Try primary once
                        }
                    }
                    true
                }
                State::HalfOpen => false, // Try primary
            },
            Err(e) => {
                warn!("Failed to acquire read lock on circuit breaker: {}", e);
                false // Default to trying primary on lock failure
            }
        }
    }

    /// Record a successful operation
    pub fn record_success(&self) {
        if let Ok(mut state) = self.state.write() {
            match state.state {
                State::HalfOpen => {
                    debug!("Circuit breaker closing after successful request in half-open state");
                    state.state = State::Closed;
                    state.failure_count = 0;
                    state.opened_at = None;
                }
                State::Closed => {
                    state.failure_count = 0;
                }
                State::Open => {
                    // Shouldn't happen but reset anyway
                    state.failure_count = 0;
                }
            }
        }
    }

    /// Record a failed operation
    pub fn record_failure(&self) {
        if let Ok(mut state) = self.state.write() {
            state.failure_count += 1;

            match state.state {
                State::Closed => {
                    if state.failure_count >= state.failure_threshold {
                        debug!(
                            "Circuit breaker opening after {} consecutive failures",
                            state.failure_count
                        );
                        state.state = State::Open;
                        state.opened_at = Some(Instant::now());
                    }
                }
                State::HalfOpen => {
                    debug!("Circuit breaker reopening after failure in half-open state");
                    state.state = State::Open;
                    state.opened_at = Some(Instant::now());
                }
                State::Open => {
                    // Already open, just track the failure
                }
            }
        }
    }

    /// Get current circuit breaker metrics for monitoring
    pub fn metrics(&self) -> CircuitBreakerMetrics {
        match self.state.read() {
            Ok(state) => CircuitBreakerMetrics {
                state: match state.state {
                    State::Closed => "closed",
                    State::Open => "open",
                    State::HalfOpen => "half_open",
                },
                failure_count: state.failure_count,
                time_until_recovery: match (state.state, state.opened_at) {
                    (State::Open, Some(opened_at)) => {
                        let elapsed = opened_at.elapsed();
                        if elapsed < state.recovery_timeout {
                            Some(state.recovery_timeout - elapsed)
                        } else {
                            Some(Duration::ZERO)
                        }
                    }
                    _ => None,
                },
            },
            Err(_) => CircuitBreakerMetrics {
                state: "unknown",
                failure_count: 0,
                time_until_recovery: None,
            },
        }
    }
}

pub struct CircuitBreakerMetrics {
    pub state: &'static str,
    pub failure_count: u32,
    pub time_until_recovery: Option<Duration>,
}

use std::sync::Arc;

/// A generic fallback executor that uses circuit breaker pattern
pub struct FallbackExecutor<P, F> {
    pub primary: P,
    pub fallback: Option<F>,
    pub circuit_breaker: CircuitBreaker,
}

impl<P, F> FallbackExecutor<P, F> {
    pub fn new(primary: P, fallback: Option<F>, circuit_breaker: CircuitBreaker) -> Self {
        Self {
            primary,
            fallback,
            circuit_breaker,
        }
    }

    /// Execute a function with automatic fallback and circuit breaker logic
    pub async fn execute<'a, Ret, Func, Fut>(&'a self, f: Func) -> Result<Ret>
    where
        Func: Fn(&'a P) -> Fut + Clone,
        Fut: std::future::Future<Output = Result<Ret>>,
        F: 'a,
        Func: Fn(&'a F) -> Fut,
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
    use super::*;

    #[test]
    fn test_circuit_breaker_states() {
        let cb = CircuitBreaker::new(3, Duration::from_secs(1));

        // Initially closed
        assert!(!cb.should_use_fallback());

        // Record failures
        cb.record_failure();
        cb.record_failure();
        assert!(!cb.should_use_fallback()); // Still closed

        cb.record_failure(); // Third failure opens circuit
        assert!(cb.should_use_fallback());

        // Success doesn't close when open
        cb.record_success();
        assert!(cb.should_use_fallback());

        // Wait for timeout
        std::thread::sleep(Duration::from_secs(1));
        assert!(!cb.should_use_fallback()); // Half-open, try primary

        // Success in half-open closes circuit
        cb.record_success();
        assert!(!cb.should_use_fallback());
    }

    #[test]
    fn test_circuit_breaker_metrics() {
        let cb = CircuitBreaker::new(2, Duration::from_secs(5));

        let metrics = cb.metrics();
        assert_eq!(metrics.state, "closed");
        assert_eq!(metrics.failure_count, 0);
        assert!(metrics.time_until_recovery.is_none());

        cb.record_failure();
        cb.record_failure();

        let metrics = cb.metrics();
        assert_eq!(metrics.state, "open");
        assert_eq!(metrics.failure_count, 2);
        assert!(metrics.time_until_recovery.is_some());
    }
}
