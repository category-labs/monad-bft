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
    cell::RefCell,
    fmt::Debug,
    hash::Hash,
    num::NonZeroUsize,
    ops::Deref,
    rc::Rc,
    time::{Duration, Instant},
};

use lru::LruCache;
use monad_executor::ExecutorMetrics;
use tracing::debug;

use crate::{metrics::*, Clock, IdentityScore, PeerStatus, Score};

const MAX_EMA_GAS: f64 = u64::MAX as f64;
const MAX_TIME_WEIGHT_BOUND: f64 = f64::MAX / MAX_EMA_GAS;

#[derive(Debug, Clone)]
pub struct ScoreConfig {
    pub promoted_capacity: usize,
    pub newcomer_capacity: usize,
    pub max_time_weight: f64,
    pub time_weight_unit: Duration,
    pub ema_half_life: Duration,
    pub block_time: Duration,
    pub promotion_threshold: f64,
}

impl Default for ScoreConfig {
    fn default() -> Self {
        Self {
            promoted_capacity: 90_000,
            newcomer_capacity: 10_000,
            max_time_weight: 1.0,
            time_weight_unit: Duration::from_secs(3600),
            ema_half_life: Duration::from_secs(24 * 3600),
            block_time: Duration::from_millis(400),
            promotion_threshold: 1_000_000.0,
        }
    }
}

struct IdentityState {
    ema_gas: f64,
    first_seen: Instant,
    last_update: Instant,
}

struct SharedState<I> {
    promoted: LruCache<I, IdentityState>,
    newcomers: LruCache<I, IdentityState>,
    config: ScoreConfig,
    alpha: f64,
}

pub struct ScoreProvider<I, C> {
    state: Rc<RefCell<SharedState<I>>>,
    clock: C,
    metrics: ExecutorMetrics,
}

pub struct ScoreReader<I, C> {
    state: Rc<RefCell<SharedState<I>>>,
    clock: C,
}

impl<I, C: Clone> Clone for ScoreReader<I, C> {
    fn clone(&self) -> Self {
        Self {
            state: Rc::clone(&self.state),
            clock: self.clock.clone(),
        }
    }
}

pub fn create<I: Hash + Eq, C: Clock + Clone>(
    config: ScoreConfig,
    clock: C,
) -> (ScoreProvider<I, C>, ScoreReader<I, C>) {
    // panic is acceptable for cfg validation: this is startup-only operator config,
    // and failing fast on invalid values is preferred over running with bad state.
    assert!(
        config.promoted_capacity > 0,
        "promoted_capacity must be > 0"
    );
    assert!(
        config.newcomer_capacity > 0,
        "newcomer_capacity must be > 0"
    );
    assert!(
        !config.time_weight_unit.is_zero(),
        "time_weight_unit must be > 0"
    );
    assert!(!config.ema_half_life.is_zero(), "ema_half_life must be > 0");
    assert!(!config.block_time.is_zero(), "block_time must be > 0");
    assert!(
        config.max_time_weight.is_finite()
            && config.max_time_weight > 0.0
            && config.max_time_weight <= MAX_TIME_WEIGHT_BOUND,
        "max_time_weight must be finite, > 0, and <= {MAX_TIME_WEIGHT_BOUND}"
    );
    assert!(
        config.promotion_threshold.is_finite() && config.promotion_threshold > 0.0,
        "promotion_threshold must be finite and > 0"
    );
    let promoted_capacity = NonZeroUsize::new(config.promoted_capacity).unwrap();
    let newcomer_capacity = NonZeroUsize::new(config.newcomer_capacity).unwrap();
    let alpha = ema_alpha(config.block_time, &config.ema_half_life);
    let state = Rc::new(RefCell::new(SharedState {
        promoted: LruCache::new(promoted_capacity),
        newcomers: LruCache::new(newcomer_capacity),
        config,
        alpha,
    }));
    let provider = ScoreProvider {
        state: Rc::clone(&state),
        clock: clock.clone(),
        metrics: ExecutorMetrics::default(),
    };
    let reader = ScoreReader { state, clock };
    (provider, reader)
}

pub struct ScoreProviderMetrics<'a>(&'a ExecutorMetrics);

impl AsRef<ExecutorMetrics> for ScoreProviderMetrics<'_> {
    fn as_ref(&self) -> &ExecutorMetrics {
        self.0
    }
}

impl Deref for ScoreProviderMetrics<'_> {
    type Target = ExecutorMetrics;

    fn deref(&self) -> &Self::Target {
        self.0
    }
}

fn apply_ema_decay(ema_gas: f64, elapsed: Duration, half_life: &Duration) -> f64 {
    if elapsed.is_zero() {
        return clamp_ema(ema_gas);
    }
    let decay = 0.5_f64.powf(elapsed.as_secs_f64() / half_life.as_secs_f64());
    let decayed = clamp_ema(ema_gas) * decay;
    let decayed = clamp_ema(decayed);
    debug_assert!(decayed.is_finite());
    debug_assert!(decayed >= 0.0);
    decayed
}

fn ema_alpha(block_time: Duration, half_life: &Duration) -> f64 {
    let alpha = 1.0 - 0.5_f64.powf(block_time.as_secs_f64() / half_life.as_secs_f64());
    if alpha.is_finite() {
        alpha.clamp(0.0, 1.0)
    } else {
        1.0
    }
}

fn ema_update(alpha: f64, gas: u64, decayed: f64) -> f64 {
    let alpha = if alpha.is_finite() {
        alpha.clamp(0.0, 1.0)
    } else {
        1.0
    };
    let value = alpha * gas as f64 + (1.0 - alpha) * clamp_ema(decayed);
    let value = clamp_ema(value);
    debug_assert!(value.is_finite());
    debug_assert!(value >= 0.0);
    value
}

fn elapsed_since(now: Instant, then: Instant) -> Duration {
    now.checked_duration_since(then).unwrap_or_default()
}

fn update_identity_state(
    id_state: &mut IdentityState,
    gas: u64,
    now: Instant,
    alpha: f64,
    half_life: Duration,
) {
    let elapsed = elapsed_since(now, id_state.last_update);
    let decayed = apply_ema_decay(id_state.ema_gas, elapsed, &half_life);
    id_state.ema_gas = ema_update(alpha, gas, decayed);
    id_state.last_update = now;
}

fn update_and_score(
    id_state: &mut IdentityState,
    gas: u64,
    now: Instant,
    alpha: f64,
    config: &ScoreConfig,
) -> f64 {
    update_identity_state(id_state, gas, now, alpha, config.ema_half_life);
    compute_score(id_state, config, now)
}

fn clamp_ema(value: f64) -> f64 {
    if !value.is_finite() {
        return MAX_EMA_GAS;
    }
    value.clamp(0.0, MAX_EMA_GAS)
}

fn clamp_score(value: f64) -> f64 {
    if !value.is_finite() {
        return f64::MAX;
    }
    if value <= 0.0 {
        return 0.0;
    }
    value
}

fn min_initial_time_weight(config: &ScoreConfig) -> f64 {
    let ratio = config.block_time.as_secs_f64() / config.time_weight_unit.as_secs_f64();
    (ratio * ratio).min(config.max_time_weight)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NewcomerDecision {
    Inserted,
    Rejected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PromoteDecision {
    Promoted {
        evicted_demotion: Option<NewcomerDecision>,
    },
    Rejected {
        newcomer: NewcomerDecision,
    },
}

impl<I: Hash + Eq + Clone, C: Clock> ScoreProvider<I, C> {
    pub fn metrics(&self) -> ScoreProviderMetrics<'_> {
        ScoreProviderMetrics(&self.metrics)
    }

    pub fn executor_metrics(&self) -> &ExecutorMetrics {
        &self.metrics
    }

    pub fn record_contribution(&mut self, identity: I, gas: u64) {
        let now = self.clock.now();
        self.metrics[COUNTER_PEER_SCORE_RECORD_CONTRIBUTION_TOTAL] += 1;
        let mut state = self.state.borrow_mut();

        if state.promoted.peek(&identity).is_some() {
            record_promoted_contribution(&mut self.metrics, &mut state, identity, gas, now);
        } else if state.newcomers.peek(&identity).is_some() {
            record_newcomer_contribution(&mut self.metrics, &mut state, identity, gas, now);
        } else {
            record_unknown_contribution(&mut self.metrics, &mut state, identity, gas, now);
        }

        self.metrics[GAUGE_PEER_SCORE_PROMOTED_SIZE] = state.promoted.len() as u64;
        self.metrics[GAUGE_PEER_SCORE_NEWCOMER_SIZE] = state.newcomers.len() as u64;
        self.metrics[GAUGE_PEER_SCORE_TOTAL_SIZE] =
            (state.promoted.len() + state.newcomers.len()) as u64;
    }
}

fn record_promoted_contribution<I: Hash + Eq + Clone>(
    metrics: &mut ExecutorMetrics,
    state: &mut SharedState<I>,
    identity: I,
    gas: u64,
    now: Instant,
) {
    let SharedState {
        promoted,
        newcomers,
        config,
        alpha,
    } = state;
    let score = {
        let id_state = promoted
            .get_mut(&identity)
            .expect("promoted identity exists while updating");
        update_and_score(id_state, gas, now, *alpha, config)
    };
    if score < config.promotion_threshold {
        let id_state = promoted
            .pop(&identity)
            .expect("promoted identity exists while demoting");
        let demotion = try_newcomer(newcomers, identity, id_state, config, now);
        metrics[COUNTER_PEER_SCORE_DEMOTION] += 1;
        match demotion {
            NewcomerDecision::Inserted => {
                metrics[COUNTER_PEER_SCORE_NEWCOMER_ADMITTED] += 1;
            }
            NewcomerDecision::Rejected => {
                metrics[COUNTER_PEER_SCORE_NEWCOMER_REJECTED] += 1;
            }
        }
    }
}

fn record_newcomer_contribution<I: Hash + Eq + Clone>(
    metrics: &mut ExecutorMetrics,
    state: &mut SharedState<I>,
    identity: I,
    gas: u64,
    now: Instant,
) {
    let SharedState {
        promoted,
        newcomers,
        config,
        alpha,
    } = state;
    let score = {
        let id_state = newcomers
            .get_mut(&identity)
            .expect("newcomer identity exists while updating");
        update_and_score(id_state, gas, now, *alpha, config)
    };
    if score >= config.promotion_threshold {
        let id_state = newcomers
            .pop(&identity)
            .expect("newcomer identity exists while promoting");
        let decision = try_promote(promoted, newcomers, identity, id_state, config, now);
        match decision {
            PromoteDecision::Promoted {
                evicted_demotion: Some(NewcomerDecision::Inserted),
            } => {
                metrics[COUNTER_PEER_SCORE_PROMOTION_SUCCEEDED] += 1;
                metrics[COUNTER_PEER_SCORE_DEMOTION] += 1;
                metrics[COUNTER_PEER_SCORE_NEWCOMER_ADMITTED] += 1;
            }
            PromoteDecision::Promoted {
                evicted_demotion: Some(NewcomerDecision::Rejected),
            } => {
                metrics[COUNTER_PEER_SCORE_PROMOTION_SUCCEEDED] += 1;
                metrics[COUNTER_PEER_SCORE_DEMOTION] += 1;
                metrics[COUNTER_PEER_SCORE_NEWCOMER_REJECTED] += 1;
            }
            PromoteDecision::Promoted {
                evicted_demotion: None,
            } => {
                metrics[COUNTER_PEER_SCORE_PROMOTION_SUCCEEDED] += 1;
            }
            PromoteDecision::Rejected {
                newcomer: NewcomerDecision::Inserted,
            } => {
                metrics[COUNTER_PEER_SCORE_PROMOTION_REJECTED] += 1;
                metrics[COUNTER_PEER_SCORE_NEWCOMER_ADMITTED] += 1;
            }
            PromoteDecision::Rejected {
                newcomer: NewcomerDecision::Rejected,
            } => {
                metrics[COUNTER_PEER_SCORE_PROMOTION_REJECTED] += 1;
                metrics[COUNTER_PEER_SCORE_NEWCOMER_REJECTED] += 1;
            }
        }
    }
}

fn record_unknown_contribution<I: Hash + Eq + Clone>(
    metrics: &mut ExecutorMetrics,
    state: &mut SharedState<I>,
    identity: I,
    gas: u64,
    now: Instant,
) {
    let SharedState {
        newcomers,
        config,
        alpha,
        ..
    } = state;
    let id_state = IdentityState {
        ema_gas: ema_update(*alpha, gas, 0.0),
        first_seen: now,
        last_update: now,
    };
    let decision = try_newcomer(newcomers, identity, id_state, config, now);
    match decision {
        NewcomerDecision::Inserted => {
            metrics[COUNTER_PEER_SCORE_NEWCOMER_ADMITTED] += 1;
        }
        NewcomerDecision::Rejected => {
            metrics[COUNTER_PEER_SCORE_NEWCOMER_REJECTED] += 1;
        }
    }
}

fn try_newcomer<I: Hash + Eq>(
    newcomers: &mut LruCache<I, IdentityState>,
    identity: I,
    id_state: IdentityState,
    config: &ScoreConfig,
    now: Instant,
) -> NewcomerDecision {
    let new_score = compute_score(&id_state, config, now);

    if newcomers.len() < newcomers.cap().get() {
        newcomers.push(identity, id_state);
        return NewcomerDecision::Inserted;
    }

    let lru_score = newcomers
        .peek_lru()
        .map(|(_, state)| compute_score(state, config, now));

    let Some(lru_score) = lru_score else {
        debug_assert!(
            false,
            "newcomer pool full but missing LRU entry (len={}, cap={})",
            newcomers.len(),
            newcomers.cap().get()
        );
        return NewcomerDecision::Rejected;
    };

    if lru_score < new_score {
        let _ = newcomers.pop_lru();
        newcomers.push(identity, id_state);
        NewcomerDecision::Inserted
    } else {
        debug!(
            candidate_score = new_score,
            lru_score, "rejected newcomer: score below newcomer LRU"
        );
        NewcomerDecision::Rejected
    }
}

fn try_promote<I: Hash + Eq + Clone>(
    promoted: &mut LruCache<I, IdentityState>,
    newcomers: &mut LruCache<I, IdentityState>,
    identity: I,
    id_state: IdentityState,
    config: &ScoreConfig,
    now: Instant,
) -> PromoteDecision {
    let new_score = compute_score(&id_state, config, now);

    if promoted.len() < promoted.cap().get() {
        promoted.push(identity, id_state);
        return PromoteDecision::Promoted {
            evicted_demotion: None,
        };
    }

    let lru_score = promoted
        .peek_lru()
        .map(|(_, s)| compute_score(s, config, now));

    let Some(lru_s) = lru_score else {
        debug_assert!(
            false,
            "promoted pool full but missing LRU entry (len={}, cap={})",
            promoted.len(),
            promoted.cap().get()
        );
        let newcomer = try_newcomer(newcomers, identity, id_state, config, now);
        return PromoteDecision::Rejected { newcomer };
    };

    if lru_s < new_score {
        let Some((evicted_id, evicted_state)) = promoted.pop_lru() else {
            let newcomer = try_newcomer(newcomers, identity, id_state, config, now);
            return PromoteDecision::Rejected { newcomer };
        };
        let evicted_demotion = try_newcomer(newcomers, evicted_id, evicted_state, config, now);
        promoted.push(identity, id_state);
        PromoteDecision::Promoted {
            evicted_demotion: Some(evicted_demotion),
        }
    } else {
        let newcomer = try_newcomer(newcomers, identity, id_state, config, now);
        PromoteDecision::Rejected { newcomer }
    }
}

impl<I: Hash + Eq, C: Clock> ScoreReader<I, C> {
    pub fn score(&self, identity: &I) -> PeerStatus {
        let now = self.clock.now();
        let mut state = self.state.borrow_mut();
        let config = state.config.clone();
        let promotion_threshold = config.promotion_threshold;

        let promoted_score = state
            .promoted
            .peek(identity)
            .map(|id_state| compute_score(id_state, &config, now));

        if let Some(score) = promoted_score {
            debug_assert!(score.is_finite() && score >= 0.0);
            if score < promotion_threshold {
                let Some((demoted_id, demoted_state)) = state.promoted.pop_entry(identity) else {
                    return PeerStatus::Unknown;
                };
                let _ = try_newcomer(
                    &mut state.newcomers,
                    demoted_id,
                    demoted_state,
                    &config,
                    now,
                );
                if let Some(id_state) = state.newcomers.peek(identity) {
                    let score = compute_score(id_state, &config, now);
                    debug_assert!(score.is_finite() && score >= 0.0);
                    return Score::try_from(score)
                        .map(PeerStatus::Newcomer)
                        .unwrap_or(PeerStatus::Unknown);
                }
                return PeerStatus::Unknown;
            }

            return Score::try_from(score)
                .map(PeerStatus::Promoted)
                .unwrap_or(PeerStatus::Unknown);
        }
        if let Some(id_state) = state.newcomers.peek(identity) {
            let score = compute_score(id_state, &config, now);
            debug_assert!(score.is_finite() && score >= 0.0);
            return Score::try_from(score)
                .map(PeerStatus::Newcomer)
                .unwrap_or(PeerStatus::Unknown);
        }
        PeerStatus::Unknown
    }
}

fn compute_score(id_state: &IdentityState, config: &ScoreConfig, now: Instant) -> f64 {
    let time_known = elapsed_since(now, id_state.first_seen);
    let ratio = time_known.as_secs_f64() / config.time_weight_unit.as_secs_f64();
    let base_time_weight = (ratio * ratio).min(config.max_time_weight);
    let time_weight = base_time_weight.max(min_initial_time_weight(config));

    let elapsed = elapsed_since(now, id_state.last_update);
    let current_ema = apply_ema_decay(id_state.ema_gas, elapsed, &config.ema_half_life);

    let score = clamp_score(current_ema * time_weight);
    debug_assert!(score.is_finite());
    debug_assert!(score >= 0.0);
    score
}

impl<I: Hash + Eq, C: Clock> IdentityScore for ScoreReader<I, C> {
    type Identity = I;

    fn score(&self, identity: &Self::Identity) -> PeerStatus {
        self.score(identity)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use proptest::prelude::*;
    use tracing::Level;
    use tracing_subscriber::FmtSubscriber;

    use super::*;
    use crate::mock::MockClock;

    fn score_value(status: PeerStatus) -> f64 {
        1.0 / status.score().reciprocal()
    }

    fn with_debug_tracing<T>(f: impl FnOnce() -> T) -> T {
        let subscriber = FmtSubscriber::builder()
            .with_max_level(Level::DEBUG)
            .with_test_writer()
            .without_time()
            .finish();
        tracing::subscriber::with_default(subscriber, f)
    }

    #[derive(Clone)]
    struct SkewClock(Arc<Mutex<Instant>>);

    impl SkewClock {
        fn new(base: Instant) -> Self {
            Self(Arc::new(Mutex::new(base)))
        }

        fn set(&self, now: Instant) {
            *self.0.lock().expect("clock lock poisoned") = now;
        }
    }

    impl Clock for SkewClock {
        fn now(&self) -> Instant {
            *self.0.lock().expect("clock lock poisoned")
        }
    }

    #[test]
    fn new_identity_has_zero_score() {
        let clock = MockClock::new();
        let (_, reader) = create::<u32, _>(ScoreConfig::default(), clock);
        assert_eq!(reader.score(&1), PeerStatus::Unknown);
    }

    #[test]
    fn score_after_contribution() {
        let clock = MockClock::new();
        let (mut provider, reader) = create::<u32, _>(ScoreConfig::default(), clock.clone());

        provider.record_contribution(1, 1_000_000);
        clock.advance(Duration::from_secs(3600));
        assert!(matches!(reader.score(&1), PeerStatus::Newcomer(_)));
    }

    #[test]
    fn first_contribution_has_nonzero_initial_time_weight() {
        let config = ScoreConfig {
            promotion_threshold: f64::MAX,
            ..Default::default()
        };
        let clock = MockClock::new();
        let (mut provider, reader) = create::<u32, _>(config, clock);

        provider.record_contribution(1, 1_000_000);
        assert!(matches!(reader.score(&1), PeerStatus::Newcomer(_)));
    }

    #[test]
    fn non_monotonic_clock_does_not_panic() {
        let base = Instant::now();
        let clock = SkewClock::new(base);
        let (mut provider, reader) = create::<u32, _>(ScoreConfig::default(), clock.clone());

        clock.set(base + Duration::from_secs(10));
        provider.record_contribution(1, 1000);
        clock.set(base + Duration::from_secs(5));
        provider.record_contribution(1, 1000);
        let _ = reader.score(&1);
    }

    #[test]
    fn counters_increment_normally() {
        let mut metrics = ExecutorMetrics::default();
        metrics[COUNTER_PEER_SCORE_RECORD_CONTRIBUTION_TOTAL] = 41;
        metrics[COUNTER_PEER_SCORE_NEWCOMER_ADMITTED] = 9;

        metrics[COUNTER_PEER_SCORE_RECORD_CONTRIBUTION_TOTAL] += 10;
        metrics[COUNTER_PEER_SCORE_NEWCOMER_ADMITTED] += 1;

        assert_eq!(metrics[COUNTER_PEER_SCORE_RECORD_CONTRIBUTION_TOTAL], 51);
        assert_eq!(metrics[COUNTER_PEER_SCORE_NEWCOMER_ADMITTED], 10);
    }

    #[test]
    #[should_panic(expected = "max_time_weight must be finite")]
    fn rejects_unsafe_time_weight() {
        let config = ScoreConfig {
            max_time_weight: f64::MAX,
            ..Default::default()
        };
        let _ = create::<u32, _>(config, MockClock::new());
    }

    #[test]
    fn ema_converges_to_rate() {
        let config = ScoreConfig {
            ema_half_life: Duration::from_secs(10),
            time_weight_unit: Duration::from_secs(1),
            max_time_weight: 1.0,
            block_time: Duration::from_secs(1),
            ..Default::default()
        };
        let clock = MockClock::new();
        let (mut provider, reader) = create::<u32, _>(config, clock.clone());

        clock.advance(Duration::from_secs(10));
        for _ in 0..100 {
            provider.record_contribution(1, 1000);
            clock.advance(Duration::from_secs(1));
        }

        let score = score_value(reader.score(&1));
        assert!(
            score > 0.0,
            "score should be positive after steady contributions"
        );

        let score_before = score;
        for _ in 0..100 {
            provider.record_contribution(1, 500);
            clock.advance(Duration::from_secs(1));
        }

        let score_after = score_value(reader.score(&1));
        assert!(
            score_after < score_before,
            "score should decrease when contribution rate halves: {score_before} -> {score_after}"
        );
    }

    #[test]
    fn no_gas_cap() {
        let config = ScoreConfig {
            time_weight_unit: Duration::from_secs(1),
            max_time_weight: 1.0,
            ema_half_life: Duration::from_secs(3600),
            block_time: Duration::from_secs(1),
            ..Default::default()
        };
        let clock = MockClock::new();
        let (mut provider, reader) = create::<u32, _>(config, clock.clone());

        clock.advance(Duration::from_secs(10));

        provider.record_contribution(1, 1_000_000_000);
        clock.advance(Duration::from_secs(1));
        let s1 = score_value(reader.score(&1));

        provider.record_contribution(2, 5_000_000_000);
        clock.advance(Duration::from_secs(1));
        let s2 = score_value(reader.score(&2));

        assert!(
            s2 / s1 > 4.0,
            "peer with 5x gas should have ~5x score, ratio = {}",
            s2 / s1
        );
    }

    #[test]
    fn time_weight_caps() {
        let config = ScoreConfig {
            max_time_weight: 5.0,
            ema_half_life: Duration::from_secs(365 * 24 * 3600),
            ..Default::default()
        };
        let clock = MockClock::new();
        let (mut provider, reader) = create::<u32, _>(config, clock.clone());

        provider.record_contribution(1, 1_000_000);

        clock.advance(Duration::from_secs(10 * 3600));
        let score_10h = score_value(reader.score(&1));

        provider.record_contribution(1, 1_000_000);

        clock.advance(Duration::from_secs(20 * 3600));
        let score_30h = score_value(reader.score(&1));

        assert!(score_10h > 0.0 && score_30h > 0.0);
    }

    #[test]
    fn clone_shares_state() {
        let clock = MockClock::new();
        let (mut provider, reader) = create::<u32, _>(ScoreConfig::default(), clock.clone());
        let reader_clone = reader.clone();

        provider.record_contribution(1, 1_000_000);
        clock.advance(Duration::from_secs(3600));

        assert!(score_value(reader.score(&1)) > 0.0);
        assert!(score_value(reader_clone.score(&1)) > 0.0);
    }

    #[test]
    fn score_decays_with_inactivity() {
        let config = ScoreConfig {
            max_time_weight: 1.0,
            time_weight_unit: Duration::from_secs(1),
            ema_half_life: Duration::from_secs(1800),
            block_time: Duration::from_secs(1),
            ..Default::default()
        };
        let clock = MockClock::new();
        let (mut provider, reader) = create::<u32, _>(config, clock.clone());

        clock.advance(Duration::from_secs(10));
        for _ in 0..100 {
            provider.record_contribution(1, 1_000_000);
            clock.advance(Duration::from_secs(1));
        }
        let score_active = score_value(reader.score(&1));

        clock.advance(Duration::from_secs(1800));
        let score_after_half_life = score_value(reader.score(&1));

        let ratio = score_after_half_life / score_active;
        assert!(
            (ratio - 0.5).abs() < 0.05,
            "ratio should be ~0.5: {}",
            ratio
        );
    }

    #[test]
    fn lru_eviction_on_newcomer_capacity() {
        let config = ScoreConfig {
            newcomer_capacity: 3,
            promoted_capacity: 3,
            time_weight_unit: Duration::from_secs(1),
            ema_half_life: Duration::from_secs(365 * 24 * 3600),
            promotion_threshold: f64::MAX,
            ..Default::default()
        };
        let clock = MockClock::new();
        let (mut provider, reader) = create::<u32, _>(config, clock.clone());

        provider.record_contribution(1, 1_000_000);
        provider.record_contribution(2, 1_000_000);
        provider.record_contribution(3, 1_000_000);
        clock.advance(Duration::from_secs(100));

        assert!(score_value(reader.score(&1)) > 0.0);
        assert!(score_value(reader.score(&2)) > 0.0);
        assert!(score_value(reader.score(&3)) > 0.0);

        provider.record_contribution(4, 1_000_000);
        clock.advance(Duration::from_secs(100));

        assert!(score_value(reader.score(&1)) > 0.0);
        assert!(score_value(reader.score(&2)) > 0.0);
        assert!(score_value(reader.score(&3)) > 0.0);
        assert_eq!(reader.score(&4), PeerStatus::Unknown);
    }

    #[test]
    fn lru_promotes_on_contribution() {
        let config = ScoreConfig {
            newcomer_capacity: 3,
            promoted_capacity: 3,
            time_weight_unit: Duration::from_secs(1),
            ema_half_life: Duration::from_secs(365 * 24 * 3600),
            promotion_threshold: f64::MAX,
            ..Default::default()
        };
        let clock = MockClock::new();
        let (mut provider, reader) = create::<u32, _>(config, clock.clone());

        provider.record_contribution(1, 1_000_000);
        provider.record_contribution(2, 1_000_000);
        provider.record_contribution(3, 1_000_000);

        provider.record_contribution(1, 1_000_000);

        provider.record_contribution(4, 1_000_000);
        clock.advance(Duration::from_secs(100));

        assert!(
            score_value(reader.score(&1)) > 0.0,
            "identity 1 should still exist (was promoted in LRU)"
        );
        assert!(score_value(reader.score(&2)) > 0.0);
        assert!(score_value(reader.score(&3)) > 0.0);
        assert_eq!(reader.score(&4), PeerStatus::Unknown);
    }

    #[test]
    fn two_tier_promotion() {
        let config = ScoreConfig {
            newcomer_capacity: 3,
            promoted_capacity: 3,
            time_weight_unit: Duration::from_secs(1),
            ema_half_life: Duration::from_secs(3600),
            block_time: Duration::from_secs(1),
            promotion_threshold: 100.0,
            ..Default::default()
        };
        let clock = MockClock::new();
        let (mut provider, reader) = create::<u32, _>(config, clock.clone());

        provider.record_contribution(1, 10);
        provider.record_contribution(2, 10);
        provider.record_contribution(3, 10);
        clock.advance(Duration::from_secs(100));

        assert!(matches!(reader.score(&1), PeerStatus::Newcomer(_)));

        provider.record_contribution(1, 10_000_000);

        assert!(
            reader.score(&1).is_promoted(),
            "identity 1 should now be promoted"
        );

        provider.record_contribution(4, 10);
        provider.record_contribution(5, 10);
        provider.record_contribution(6, 10);
        clock.advance(Duration::from_secs(100));

        assert!(
            reader.score(&1).is_promoted(),
            "identity 1 should still exist (protected in promoted pool)"
        );
        let newcomer_count = [2, 3, 4, 5, 6]
            .into_iter()
            .filter(|id| matches!(reader.score(id), PeerStatus::Newcomer(_)))
            .count();
        assert_eq!(
            newcomer_count, 3,
            "newcomer pool should remain capped at configured capacity"
        );
    }

    #[test]
    fn demoted_peer_replaces_newcomer_lru_when_score_is_higher() {
        let config = ScoreConfig {
            newcomer_capacity: 1,
            promoted_capacity: 1,
            time_weight_unit: Duration::from_secs(1),
            max_time_weight: 1.0,
            ema_half_life: Duration::from_secs(3600),
            block_time: Duration::from_secs(1),
            promotion_threshold: 10.0,
        };
        let clock = MockClock::new();
        let (mut provider, reader) = create::<u32, _>(config, clock.clone());

        // Promote id=1.
        clock.advance(Duration::from_secs(10));
        provider.record_contribution(1, 1_000_000);
        clock.advance(Duration::from_secs(10));
        provider.record_contribution(1, 1_000_000);
        assert!(reader.score(&1).is_promoted());

        // Fill newcomer pool with id=2 at effectively zero score.
        provider.record_contribution(2, 0);
        assert_eq!(reader.score(&2), PeerStatus::Unknown);

        // Let promoted id=1 decay below threshold, triggering demotion into newcomer pool.
        // id=1 still has a positive score and should replace newcomer id=2.
        clock.advance(Duration::from_secs(9 * 3600));
        provider.record_contribution(1, 0);

        assert!(!reader.score(&1).is_promoted());
        assert!(score_value(reader.score(&1)) > 0.0);
        assert_eq!(reader.score(&2), PeerStatus::Unknown);
    }

    #[test]
    fn newcomer_rejected_when_lru_has_higher_score() {
        let config = ScoreConfig {
            newcomer_capacity: 1,
            promoted_capacity: 1,
            time_weight_unit: Duration::from_secs(1),
            max_time_weight: 1.0,
            ema_half_life: Duration::from_secs(3600),
            block_time: Duration::from_secs(1),
            promotion_threshold: f64::MAX,
        };
        let clock = MockClock::new();
        let (mut provider, reader) = create::<u32, _>(config, clock.clone());

        clock.advance(Duration::from_secs(10));
        provider.record_contribution(1, 1_000_000);
        clock.advance(Duration::from_secs(10));

        with_debug_tracing(|| {
            provider.record_contribution(2, 1);
        });

        assert!(matches!(reader.score(&1), PeerStatus::Newcomer(_)));
        assert_eq!(reader.score(&2), PeerStatus::Unknown);
    }

    #[test]
    fn promotion_checks_score_when_full() {
        let config = ScoreConfig {
            newcomer_capacity: 10,
            promoted_capacity: 2,
            time_weight_unit: Duration::from_secs(1),
            ema_half_life: Duration::from_secs(3600),
            block_time: Duration::from_secs(1),
            promotion_threshold: 0.1,
            ..Default::default()
        };
        let clock = MockClock::new();
        let (mut provider, reader) = create::<u32, _>(config, clock.clone());

        provider.record_contribution(1, 1_000);
        provider.record_contribution(2, 1_000);
        clock.advance(Duration::from_secs(10));

        provider.record_contribution(1, 1_000);
        assert!(
            reader.score(&1).is_promoted(),
            "identity 1 should be promoted"
        );

        provider.record_contribution(2, 1_000);
        assert!(
            reader.score(&2).is_promoted(),
            "identity 2 should be promoted"
        );

        provider.record_contribution(3, 10_000_000);
        clock.advance(Duration::from_secs(10));
        provider.record_contribution(3, 10_000_000);

        let s1 = reader.score(&1);
        let s3 = reader.score(&3);
        assert!(
            s3.is_promoted(),
            "identity 3 with higher score should be promoted, score={}",
            score_value(s3)
        );
        assert!(
            !s1.is_promoted(),
            "identity 1 (LRU with lower score) should be demoted to newcomers"
        );
        assert!(
            reader.score(&2).is_promoted(),
            "identity 2 should remain promoted"
        );
    }

    #[test]
    fn promotion_rejected_when_lru_has_higher_score() {
        let config = ScoreConfig {
            newcomer_capacity: 10,
            promoted_capacity: 2,
            time_weight_unit: Duration::from_secs(1),
            ema_half_life: Duration::from_secs(3600),
            block_time: Duration::from_secs(1),
            promotion_threshold: 0.1,
            ..Default::default()
        };
        let clock = MockClock::new();
        let (mut provider, reader) = create::<u32, _>(config, clock.clone());

        provider.record_contribution(1, 100_000);
        provider.record_contribution(2, 100_000);
        clock.advance(Duration::from_secs(10));
        provider.record_contribution(1, 100_000);
        provider.record_contribution(2, 100_000);

        assert!(reader.score(&1).is_promoted());
        assert!(reader.score(&2).is_promoted());

        provider.record_contribution(3, 1);
        clock.advance(Duration::from_secs(10));
        provider.record_contribution(3, 1);

        assert!(
            !reader.score(&3).is_promoted(),
            "identity 3 with lower score should stay in newcomers"
        );
        assert!(reader.score(&1).is_promoted());
        assert!(reader.score(&2).is_promoted());
    }

    #[test]
    fn promotion_rejection_reinserts_identity_to_newcomers() {
        let config = ScoreConfig {
            newcomer_capacity: 4,
            promoted_capacity: 1,
            time_weight_unit: Duration::from_secs(1),
            max_time_weight: 1.0,
            ema_half_life: Duration::from_secs(3600),
            block_time: Duration::from_secs(1),
            promotion_threshold: 1.0,
        };
        let clock = MockClock::new();
        let (mut provider, reader) = create::<u32, _>(config, clock.clone());

        clock.advance(Duration::from_secs(10));
        provider.record_contribution(1, 1_000_000);
        clock.advance(Duration::from_secs(10));
        provider.record_contribution(1, 1_000_000);
        assert!(reader.score(&1).is_promoted());

        clock.advance(Duration::from_secs(10));
        provider.record_contribution(2, 100_000);
        clock.advance(Duration::from_secs(10));
        provider.record_contribution(2, 100_000);

        assert!(!reader.score(&2).is_promoted());
        assert!(matches!(reader.score(&2), PeerStatus::Newcomer(_)));
        assert!(reader.score(&1).is_promoted());
    }

    #[test]
    fn demotion_on_score_decay_without_new_contribution() {
        let config = ScoreConfig {
            newcomer_capacity: 10,
            promoted_capacity: 10,
            time_weight_unit: Duration::from_secs(1),
            ema_half_life: Duration::from_secs(100),
            block_time: Duration::from_secs(1),
            promotion_threshold: 0.1,
            ..Default::default()
        };
        let clock = MockClock::new();
        let (mut provider, reader) = create::<u32, _>(config, clock.clone());

        provider.record_contribution(1, 10_000);
        clock.advance(Duration::from_secs(10));
        provider.record_contribution(1, 10_000);
        assert!(reader.score(&1).is_promoted());

        clock.advance(Duration::from_secs(2000));
        assert!(
            !reader.score(&1).is_promoted(),
            "identity should be demoted after score decays below threshold"
        );
    }

    #[test]
    fn promotion_replacement_uses_lru_score() {
        let config = ScoreConfig {
            newcomer_capacity: 10,
            promoted_capacity: 2,
            time_weight_unit: Duration::from_secs(1),
            ema_half_life: Duration::from_secs(3600),
            block_time: Duration::from_secs(1),
            promotion_threshold: 0.1,
            ..Default::default()
        };
        let clock = MockClock::new();
        let (mut provider, reader) = create::<u32, _>(config, clock.clone());

        clock.advance(Duration::from_secs(10));
        provider.record_contribution(1, 10_000);
        clock.advance(Duration::from_secs(1));
        provider.record_contribution(1, 10_000);
        assert!(reader.score(&1).is_promoted());

        clock.advance(Duration::from_secs(1));
        provider.record_contribution(2, 1_000);
        clock.advance(Duration::from_secs(1));
        provider.record_contribution(2, 1_000);
        assert!(reader.score(&2).is_promoted());

        // Make id=1 MRU and keep id=2 as LRU.
        clock.advance(Duration::from_secs(1));
        provider.record_contribution(1, 10_000);

        // Candidate is better than LRU (id=2), but worse than MRU (id=1).
        clock.advance(Duration::from_secs(1));
        provider.record_contribution(3, 3_000);
        clock.advance(Duration::from_secs(1));
        provider.record_contribution(3, 3_000);

        assert!(reader.score(&1).is_promoted());
        assert!(
            reader.score(&3).is_promoted(),
            "id=3 should replace id=2 by beating the LRU promoted score",
        );
        assert!(!reader.score(&2).is_promoted());
    }

    #[test]
    fn score_types_reject_invalid_inputs() {
        assert!(Score::try_from(0.0).is_err());
        assert!(Score::try_from(-1.0).is_err());
        assert!(Score::try_from(f64::NAN).is_err());
        assert!(Score::try_from(f64::INFINITY).is_err());
        assert!(Score::try_from(1.0).is_ok());
    }

    proptest! {
        #[test]
        fn score_domain_invariant(
            ops in prop::collection::vec((0u8..16, 0u64..1_000_000, 0u16..2_000), 1..200)
        ) {
            let config = ScoreConfig {
                newcomer_capacity: 128,
                promoted_capacity: 128,
                time_weight_unit: Duration::from_secs(1),
                ema_half_life: Duration::from_secs(3600),
                block_time: Duration::from_secs(1),
                promotion_threshold: 0.1,
                ..Default::default()
            };
            let clock = MockClock::new();
            let (mut provider, reader) = create::<u8, _>(config, clock.clone());

            for (identity, gas, advance_ms) in ops {
                clock.advance(Duration::from_millis(advance_ms as u64));
                provider.record_contribution(identity, gas);
            }

            for identity in 0u8..16 {
                match reader.score(&identity) {
                    PeerStatus::Unknown => {}
                    PeerStatus::Newcomer(score) => {
                        let reciprocal = score.reciprocal();
                        prop_assert!(reciprocal.is_finite());
                        prop_assert!(reciprocal > 0.0);
                    }
                    PeerStatus::Promoted(score) => {
                        let reciprocal = score.reciprocal();
                        prop_assert!(reciprocal.is_finite());
                        prop_assert!(reciprocal > 0.0);
                    }
                }
            }
        }

    }
}
