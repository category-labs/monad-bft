use std::time::Instant;

pub mod ema;
pub mod metrics;

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct Score(f64);

impl Score {
    pub const ONE: Self = Self(1.0);

    pub fn reciprocal(self) -> f64 {
        debug_assert!(self.0.is_finite() && self.0 > 0.0);
        1.0 / self.0
    }
}

impl TryFrom<f64> for Score {
    type Error = ();

    fn try_from(value: f64) -> Result<Self, Self::Error> {
        if value.is_finite() && value > 0.0 {
            Ok(Self(value))
        } else {
            Err(())
        }
    }
}

impl TryFrom<u64> for Score {
    type Error = ();

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        let value = value as f64;
        if value.is_finite() && value > 0.0 {
            Ok(Self(value))
        } else {
            Err(())
        }
    }
}

pub trait Clock: Clone + Send + Sync {
    /// Returns the current instant.
    ///
    /// Contract: implementations must be monotonic (never move backwards)
    /// for the same clock instance. `monad-peer-score` computes elapsed time
    /// using `duration_since`, which assumes monotonic instants.
    fn now(&self) -> Instant;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct StdClock;

impl Clock for StdClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
}

#[cfg(any(test, feature = "test-utils"))]
pub mod mock {
    use std::{
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc,
        },
        time::{Duration, Instant},
    };

    use super::Clock;

    #[derive(Clone)]
    pub struct MockClock {
        base: Instant,
        offset_nanos: Arc<AtomicU64>,
    }

    impl MockClock {
        pub fn new() -> Self {
            Self {
                base: Instant::now(),
                offset_nanos: Arc::new(AtomicU64::new(0)),
            }
        }

        pub fn advance(&self, duration: Duration) {
            let increment = u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX);
            let _ = self
                .offset_nanos
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                    Some(current.saturating_add(increment))
                });
        }
    }

    impl Default for MockClock {
        fn default() -> Self {
            Self::new()
        }
    }

    impl Clock for MockClock {
        fn now(&self) -> Instant {
            let offset = Duration::from_nanos(self.offset_nanos.load(Ordering::SeqCst));
            self.base + offset
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PeerStatus {
    Promoted(Score),
    Newcomer(Score),
    Unknown,
}

impl PeerStatus {
    /// Returns a score suitable for arithmetic operations like
    /// reciprocal-based scheduling.
    ///
    /// `PeerStatus::Unknown` maps to a constant non-zero score.
    pub fn score(&self) -> Score {
        match self {
            PeerStatus::Promoted(s) | PeerStatus::Newcomer(s) => *s,
            PeerStatus::Unknown => Score::ONE,
        }
    }

    pub fn is_promoted(&self) -> bool {
        matches!(self, PeerStatus::Promoted(_))
    }
}

pub trait IdentityScore {
    type Identity;

    /// Contract:
    /// - `PeerStatus::Promoted(score)` and `PeerStatus::Newcomer(score)` must carry a strictly
    ///   positive finite `Score`.
    /// - `PeerStatus::Unknown` represents lack of scoring information and maps to a
    ///   constant non-zero score (`Score::ONE`) via `PeerStatus::score()`.
    fn score(&self, identity: &Self::Identity) -> PeerStatus;
}
