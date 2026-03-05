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
    fmt::{Debug, Display},
    hash::Hash,
};

use monad_dynamic_cap::DynamicCapConfig;
use monad_executor::ExecutorMetrics;

use crate::{metrics::*, pool::Pool, IdentityScore, PeerStatus, PushError, Score};

const DEFAULT_REGULAR_SCORE: Score = Score::ONE;
const POP_COUNTER_WINDOW: u8 = 100;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PoolKind {
    Priority,
    Regular,
}

impl PoolKind {
    fn pop_from_counter(self) -> &'static monad_executor::MetricDef {
        match self {
            Self::Priority => COUNTER_FAIR_QUEUE_POP_FROM_PRIORITY,
            Self::Regular => COUNTER_FAIR_QUEUE_POP_FROM_REGULAR,
        }
    }
}

fn pool_and_score(status: PeerStatus) -> (PoolKind, Score) {
    match status {
        PeerStatus::Promoted(score) => (PoolKind::Priority, score),
        // all newcomers and unknown intentionally use the same score
        PeerStatus::Newcomer(_) | PeerStatus::Unknown => (PoolKind::Regular, DEFAULT_REGULAR_SCORE),
    }
}

#[derive(Debug, Clone)]
pub struct FairQueueBuilder {
    per_id_limit: usize,
    max_size: usize,
    regular_per_id_limit: usize,
    regular_max_size: usize,
    regular_bandwidth_pct: u8,
}

impl Default for FairQueueBuilder {
    fn default() -> Self {
        Self {
            per_id_limit: 4_000,
            max_size: 40_000,
            regular_per_id_limit: 4_000,
            regular_max_size: 40_000,
            regular_bandwidth_pct: 10,
        }
    }
}

impl FairQueueBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn per_id_limit(mut self, limit: usize) -> Self {
        self.per_id_limit = limit;
        self
    }

    pub fn max_size(mut self, size: usize) -> Self {
        self.max_size = size;
        self
    }

    pub fn regular_per_id_limit(mut self, limit: usize) -> Self {
        self.regular_per_id_limit = limit;
        self
    }

    pub fn regular_max_size(mut self, size: usize) -> Self {
        self.regular_max_size = size;
        self
    }

    pub fn regular_bandwidth_pct(mut self, pct: u8) -> Self {
        self.regular_bandwidth_pct = pct.min(100);
        self
    }

    pub fn build<S: IdentityScore, T>(self, scorer: S) -> FairQueue<S, T>
    where
        S::Identity: Hash + Eq + Clone + Debug + Display,
    {
        assert!(self.per_id_limit > 0, "per_id_limit must be > 0");
        assert!(self.max_size > 0, "max_size must be > 0");
        assert!(
            self.regular_per_id_limit > 0,
            "regular_per_id_limit must be > 0"
        );
        assert!(self.regular_max_size > 0, "regular_max_size must be > 0");
        let priority_dynamic_cap_cfg = DynamicCapConfig::default();
        priority_dynamic_cap_cfg.validate();
        let regular_dynamic_cap_cfg = DynamicCapConfig::default();
        regular_dynamic_cap_cfg.validate();

        FairQueue {
            priority_pool: Pool::new(self.per_id_limit, self.max_size, priority_dynamic_cap_cfg),
            regular_pool: Pool::new(
                self.regular_per_id_limit,
                self.regular_max_size,
                regular_dynamic_cap_cfg,
            ),
            scorer,
            regular_bandwidth_pct: self.regular_bandwidth_pct,
            pop_counter: 0,
            service_sequence: 0,
            metrics: ExecutorMetrics::default(),
        }
    }
}

pub struct FairQueue<S: IdentityScore, T> {
    priority_pool: Pool<S::Identity, T>,
    regular_pool: Pool<S::Identity, T>,
    scorer: S,
    regular_bandwidth_pct: u8,
    pop_counter: u8,
    service_sequence: u64,
    metrics: ExecutorMetrics,
}

impl<S: IdentityScore, T> FairQueue<S, T>
where
    S::Identity: Hash + Eq + Clone + Debug + Display,
{
    pub fn push(&mut self, id: S::Identity, item: T) -> Result<(), PushError<S::Identity>> {
        let result = self.push_inner(id, item);
        match &result {
            Ok(pool_kind) => {
                self.metrics[COUNTER_FAIR_QUEUE_PUSH_TOTAL] += 1;
                match pool_kind {
                    PoolKind::Priority => self.metrics[COUNTER_FAIR_QUEUE_PUSH_PRIORITY] += 1,
                    PoolKind::Regular => self.metrics[COUNTER_FAIR_QUEUE_PUSH_REGULAR] += 1,
                }
            }
            Err(PushError::PerIdLimitExceeded { .. }) => {
                self.metrics[COUNTER_FAIR_QUEUE_PUSH_ERROR_PER_ID_LIMIT] += 1;
            }
            Err(PushError::Full { .. }) => {
                self.metrics[COUNTER_FAIR_QUEUE_PUSH_ERROR_FULL] += 1;
            }
        }
        self.update_len_metrics();
        result.map(|_| ())
    }

    pub fn pop(&mut self) -> Option<(S::Identity, T)> {
        let (first_pool, second_pool) = if self.pop_counter < self.regular_bandwidth_pct {
            (PoolKind::Regular, PoolKind::Priority)
        } else {
            (PoolKind::Priority, PoolKind::Regular)
        };

        let result = if let Some(item) = self.pop_from_pool(first_pool) {
            self.record_successful_pop(first_pool, true);
            Some(item)
        } else if let Some(item) = self.pop_from_pool(second_pool) {
            self.record_successful_pop(second_pool, false);
            Some(item)
        } else {
            self.metrics[COUNTER_FAIR_QUEUE_POP_EMPTY] += 1;
            None
        };

        self.update_len_metrics();
        result
    }

    pub fn metrics(&self) -> &ExecutorMetrics {
        &self.metrics
    }

    pub fn len(&self) -> usize {
        self.priority_pool
            .len()
            .saturating_add(self.regular_pool.len())
    }

    pub fn is_empty(&self) -> bool {
        self.priority_pool.is_empty() && self.regular_pool.is_empty()
    }

    fn push_inner(&mut self, id: S::Identity, item: T) -> Result<PoolKind, PushError<S::Identity>> {
        if let Some(pool_kind) = self.pool_containing_identity(&id) {
            let service_sequence = self.service_sequence;
            self.pool_mut(pool_kind)
                .push_existing(&id, item, service_sequence)?;
            return Ok(pool_kind);
        }

        self.push_new_identity(id, item)
    }

    fn push_new_identity(
        &mut self,
        id: S::Identity,
        item: T,
    ) -> Result<PoolKind, PushError<S::Identity>> {
        let (desired_pool, score) = pool_and_score(self.scorer.score(&id));
        let service_sequence = self.service_sequence;

        match self
            .pool_mut(desired_pool)
            .push_new(id.clone(), item, score, service_sequence)
        {
            Ok(()) => Ok(desired_pool),
            Err((PushError::Full { .. }, item)) if desired_pool == PoolKind::Priority => self
                .pool_mut(PoolKind::Regular)
                .push_new(id, item, DEFAULT_REGULAR_SCORE, service_sequence)
                .map(|_| PoolKind::Regular)
                .map_err(|(err, _)| err),
            Err((err, _)) => Err(err),
        }
    }

    fn update_len_metrics(&mut self) {
        self.metrics[GAUGE_FAIR_QUEUE_PRIORITY_ITEMS] = self.priority_pool.total_items() as u64;
        self.metrics[GAUGE_FAIR_QUEUE_REGULAR_ITEMS] = self.regular_pool.total_items() as u64;
    }

    fn pop_from_pool(&mut self, pool_kind: PoolKind) -> Option<(S::Identity, T)> {
        let popped = self.pool_mut(pool_kind).pop_next()?;
        self.service_sequence += 1;
        let service_sequence = self.service_sequence;
        self.pool_mut(pool_kind)
            .observe_service(&popped.id, service_sequence);
        Some((popped.id, popped.item))
    }

    fn pool_containing_identity(&self, id: &S::Identity) -> Option<PoolKind> {
        if self.priority_pool.contains_identity(id) {
            Some(PoolKind::Priority)
        } else if self.regular_pool.contains_identity(id) {
            Some(PoolKind::Regular)
        } else {
            None
        }
    }

    fn record_successful_pop(&mut self, pool_kind: PoolKind, first_attempt: bool) {
        self.metrics[COUNTER_FAIR_QUEUE_POP_TOTAL] += 1;
        self.metrics[pool_kind.pop_from_counter()] += 1;
        if first_attempt {
            self.pop_counter = (self.pop_counter + 1) % POP_COUNTER_WINDOW;
        }
    }

    fn pool_mut(&mut self, pool_kind: PoolKind) -> &mut Pool<S::Identity, T> {
        match pool_kind {
            PoolKind::Priority => &mut self.priority_pool,
            PoolKind::Regular => &mut self.regular_pool,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use proptest::prelude::*;

    use super::*;
    use crate::Score;

    #[derive(Clone)]
    struct TestScorer {
        scores: HashMap<u32, f64>,
        threshold: f64,
    }

    impl IdentityScore for TestScorer {
        type Identity = u32;

        fn score(&self, identity: &Self::Identity) -> PeerStatus {
            let s = self.scores.get(identity).copied().unwrap_or(0.0);
            status_from_raw_score(s, self.threshold)
        }
    }

    type TestQueue = FairQueue<TestScorer, u32>;

    const TEST_THRESHOLD: f64 = 0.5;

    fn status_from_raw_score(score: f64, threshold: f64) -> PeerStatus {
        if score >= threshold {
            PeerStatus::Promoted(Score::try_from(score).unwrap())
        } else if score > 0.0 {
            PeerStatus::Newcomer(Score::try_from(score).unwrap())
        } else {
            PeerStatus::Unknown
        }
    }

    fn make_test_scorer(entries: impl IntoIterator<Item = (u32, f64)>) -> TestScorer {
        TestScorer {
            scores: entries.into_iter().collect(),
            threshold: TEST_THRESHOLD,
        }
    }

    fn make_queue_with_limits<S: IdentityScore<Identity = u32>>(
        scorer: S,
        per_id_limit: usize,
        max_size: usize,
        regular_per_id_limit: usize,
        regular_max_size: usize,
        regular_bandwidth_pct: u8,
    ) -> FairQueue<S, u32> {
        FairQueueBuilder::new()
            .per_id_limit(per_id_limit)
            .max_size(max_size)
            .regular_per_id_limit(regular_per_id_limit)
            .regular_max_size(regular_max_size)
            .regular_bandwidth_pct(regular_bandwidth_pct)
            .build(scorer)
    }

    fn make_queue(scorer: TestScorer) -> TestQueue {
        make_queue_with_limits(scorer, 100, 10000, 100, 10000, 10)
    }

    fn pop_all_counts<S: IdentityScore<Identity = u32>>(
        queue: &mut FairQueue<S, u32>,
    ) -> HashMap<u32, usize> {
        let mut counts: HashMap<u32, usize> = HashMap::new();
        while let Some((id, _)) = queue.pop() {
            *counts.entry(id).or_default() += 1;
        }
        counts
    }

    fn pop_n_counts<S: IdentityScore<Identity = u32>>(
        queue: &mut FairQueue<S, u32>,
        n: usize,
    ) -> HashMap<u32, usize> {
        let mut counts: HashMap<u32, usize> = HashMap::new();
        for _ in 0..n {
            let Some((id, _)) = queue.pop() else {
                break;
            };
            *counts.entry(id).or_default() += 1;
        }
        counts
    }

    #[test]
    fn newcomer_and_unknown_route_to_regular() {
        // id=0: newcomer (score 0.3, below threshold 0.5), id=1: unknown
        // Both should go to regular pool per spec
        let scorer = make_test_scorer([(0, 0.3)]);
        let mut queue: TestQueue = make_queue_with_limits(scorer, 100, 10000, 100, 10000, 100);

        for i in 0..30u32 {
            queue.push(0, i).unwrap();
            queue.push(1, i + 1000).unwrap();
        }

        let counts = pop_all_counts(&mut queue);
        assert_eq!(counts[&0], 30);
        assert_eq!(counts[&1], 30);
    }

    #[test]
    fn unknown_routes_to_regular_only() {
        // All three ids are unknown (not in scores map).
        // With regular_bandwidth_pct=100, regular is always tried first.
        let scorer = make_test_scorer([]);
        let mut queue: TestQueue = make_queue_with_limits(scorer, 100, 10000, 100, 10000, 100);

        for i in 0..30u32 {
            queue.push(i % 3, i).unwrap();
        }

        let counts = pop_all_counts(&mut queue);
        // All three unknown peers get equal bandwidth (score=1.0 each)
        for id in 0..3 {
            assert_eq!(counts[&id], 10);
        }
    }

    #[test]
    fn promoted_overflow_to_regular() {
        // priority pool max_size=1, so second promoted overflows to regular.
        // id=0: promoted (score 2.0), id=1: promoted (score 1.0), id=2: newcomer (score 0.3)
        let scorer = make_test_scorer([(0, 2.0), (1, 1.0), (2, 0.3)]);
        let mut queue: TestQueue = make_queue_with_limits(scorer, 100, 1, 100, 100, 0);

        queue.push(0, 10).unwrap(); // promoted → priority (fills it)
        queue.push(1, 20).unwrap(); // promoted → overflow to regular
        queue.push(2, 30).unwrap(); // newcomer → regular

        // With regular_bandwidth_pct=0, priority is tried first.
        let (first_id, _) = queue.pop().unwrap();
        assert_eq!(first_id, 0, "first pop should be from priority pool (id=0)");

        // Remaining two are in regular pool
        let (second_id, _) = queue.pop().unwrap();
        let (third_id, _) = queue.pop().unwrap();
        assert!(
            [1, 2].contains(&second_id) && [1, 2].contains(&third_id),
            "remaining pops should be from regular pool, got {second_id}, {third_id}"
        );
        assert!(queue.is_empty());
    }

    #[test]
    fn first_item_prefers_higher_score() {
        let scores: HashMap<u32, f64> = [(0, 10.0), (1, 1.0)].into_iter().collect();
        let scorer = make_test_scorer(scores);
        let mut queue: TestQueue = make_queue_with_limits(scorer, 100, 1000, 2000, 40000, 0);

        queue.push(1, 10).unwrap();
        queue.push(0, 20).unwrap();

        let (first_id, _) = queue.pop().unwrap();
        assert_eq!(first_id, 0);
    }

    #[test]
    fn invalid_promoted_score_not_constructible() {
        assert!(Score::try_from(f64::NAN).is_err());
        assert!(Score::try_from(0.0).is_err());
    }

    #[test]
    fn second_attempt_does_not_consume_regular_share() {
        let scorer = make_test_scorer([(0, 1.0)]);
        let mut queue: TestQueue = make_queue_with_limits(scorer, 1000, 1000, 1000, 1000, 10);

        for i in 0..30u32 {
            queue.push(0, i).unwrap();
        }
        for _ in 0..20 {
            queue.pop().unwrap();
        }

        queue.push(1, 999).unwrap();
        let (first_id, _) = queue.pop().unwrap();
        assert_eq!(first_id, 1);
    }

    #[derive(Clone)]
    struct MutableScorer {
        scores: std::sync::Arc<std::sync::Mutex<HashMap<u32, f64>>>,
        threshold: f64,
    }

    impl IdentityScore for MutableScorer {
        type Identity = u32;

        fn score(&self, identity: &Self::Identity) -> PeerStatus {
            let scores = self.scores.lock().unwrap();
            let s = scores.get(identity).copied().unwrap_or(0.0);
            status_from_raw_score(s, self.threshold)
        }
    }

    type SharedScores = std::sync::Arc<std::sync::Mutex<HashMap<u32, f64>>>;

    fn make_mutable_scorer(
        entries: impl IntoIterator<Item = (u32, f64)>,
    ) -> (SharedScores, MutableScorer) {
        let scores = std::sync::Arc::new(std::sync::Mutex::new(entries.into_iter().collect()));
        let scorer = MutableScorer {
            scores: scores.clone(),
            threshold: TEST_THRESHOLD,
        };
        (scores, scorer)
    }

    #[test]
    fn existing_identity_remains_in_assigned_pool_after_score_change() {
        let (scores, scorer) = make_mutable_scorer([(0, 0.1), (1, 10.0)]);
        let mut queue: FairQueue<MutableScorer, u32> =
            make_queue_with_limits(scorer, 100, 1000, 100, 1000, 100);

        queue.push(0, 10).unwrap();
        queue.push(0, 11).unwrap();
        queue.push(0, 12).unwrap();
        queue.push(1, 20).unwrap();

        let (first_id, _) = queue.pop().unwrap();
        assert_eq!(first_id, 0);

        {
            let mut scores = scores.lock().unwrap();
            scores.insert(0, 1.0);
        }

        let (second_id, _) = queue.pop().unwrap();
        assert_eq!(second_id, 0);

        assert!(queue.regular_pool.contains_identity(&0));
        assert!(!queue.priority_pool.contains_identity(&0));
    }

    #[test]
    fn existing_identity_push_is_routed_by_pool_membership() {
        let (scores, scorer) = make_mutable_scorer([(0, 0.1)]);
        let mut queue: FairQueue<MutableScorer, u32> =
            make_queue_with_limits(scorer, 100, 1000, 100, 1000, 100);

        queue.push(0, 10).unwrap();
        {
            let mut scores = scores.lock().unwrap();
            scores.insert(0, 10.0);
        }
        queue.push(0, 11).unwrap();

        assert!(queue.regular_pool.contains_identity(&0));
        assert!(!queue.priority_pool.contains_identity(&0));
        assert_eq!(queue.priority_pool.total_items(), 0);
        assert_eq!(queue.regular_pool.total_items(), 2);
    }

    #[test]
    fn score_change_does_not_drop_items_when_regular_limit_is_small() {
        let (scores, scorer) = make_mutable_scorer([(0, 10.0)]);
        let mut queue: FairQueue<MutableScorer, u32> =
            make_queue_with_limits(scorer, 100, 1000, 2, 1000, 0);

        for i in 0..5 {
            queue.push(0, i).unwrap();
        }
        {
            let mut scores = scores.lock().unwrap();
            scores.insert(0, 0.1);
        }

        let counts = pop_all_counts(&mut queue);
        assert_eq!(counts.get(&0), Some(&5));
        assert!(queue.is_empty());
    }

    #[test]
    fn score_change_does_not_drop_items_when_regular_pool_is_full() {
        let (scores, scorer) = make_mutable_scorer([(0, 10.0), (1, 0.1)]);
        let mut queue: FairQueue<MutableScorer, u32> =
            make_queue_with_limits(scorer, 100, 1000, 10, 3, 0);

        queue.push(0, 10).unwrap();
        queue.push(0, 11).unwrap();
        queue.push(0, 12).unwrap();
        queue.push(1, 20).unwrap();
        queue.push(1, 21).unwrap();
        queue.push(1, 22).unwrap();

        {
            let mut scores = scores.lock().unwrap();
            scores.insert(0, 0.1);
        }

        let counts = pop_all_counts(&mut queue);
        assert_eq!(counts.get(&0), Some(&3));
        assert_eq!(counts.get(&1), Some(&3));
        assert!(queue.is_empty());
    }

    #[test]
    fn three_peer_proportional_bandwidth() {
        // Scores 6:3:1 — expect ~60%, ~30%, ~10% of bandwidth
        let scores: HashMap<u32, f64> = [(0, 6.0), (1, 3.0), (2, 1.0)].into_iter().collect();
        let scorer = make_test_scorer(scores);
        let mut queue: TestQueue = make_queue_with_limits(scorer, 100000, 1000000, 2000, 40000, 0);

        let items_per_id = 100_000;
        for i in 0..items_per_id as u32 {
            queue.push(0, i).unwrap();
            queue.push(1, i + 100000).unwrap();
            queue.push(2, i + 200000).unwrap();
        }

        let total_pops = 10_000;
        let counts = pop_n_counts(&mut queue, total_pops);

        let total_score = 6.0 + 3.0 + 1.0;
        for (&id, &count) in &counts {
            let score = [6.0, 3.0, 1.0][id as usize];
            let expected = score / total_score;
            let actual = count as f64 / total_pops as f64;
            assert!(
                (actual - expected).abs() < 0.03,
                "id={id}: expected {expected:.2}, got {actual:.2}"
            );
        }
    }

    #[test]
    fn metrics_are_updated_on_push_and_pop() {
        let scorer = make_test_scorer([(0, 1.0)]);
        let mut queue: TestQueue = make_queue_with_limits(scorer, 100, 100, 100, 100, 50);

        assert_eq!(
            queue.metrics()[crate::metrics::GAUGE_FAIR_QUEUE_PRIORITY_ITEMS],
            0
        );
        assert_eq!(
            queue.metrics()[crate::metrics::GAUGE_FAIR_QUEUE_REGULAR_ITEMS],
            0
        );

        assert_eq!(queue.pop(), None);
        assert_eq!(
            queue.metrics()[crate::metrics::COUNTER_FAIR_QUEUE_POP_EMPTY],
            1
        );

        queue.push(0, 10).unwrap();
        queue.push(1, 11).unwrap();
        assert_eq!(
            queue.metrics()[crate::metrics::COUNTER_FAIR_QUEUE_PUSH_TOTAL],
            2
        );
        assert_eq!(
            queue.metrics()[crate::metrics::COUNTER_FAIR_QUEUE_PUSH_PRIORITY],
            1
        );
        assert_eq!(
            queue.metrics()[crate::metrics::COUNTER_FAIR_QUEUE_PUSH_REGULAR],
            1
        );
        assert_eq!(
            queue.metrics()[crate::metrics::GAUGE_FAIR_QUEUE_PRIORITY_ITEMS],
            1
        );
        assert_eq!(
            queue.metrics()[crate::metrics::GAUGE_FAIR_QUEUE_REGULAR_ITEMS],
            1
        );

        let _ = queue.pop();
        assert_eq!(
            queue.metrics()[crate::metrics::COUNTER_FAIR_QUEUE_POP_TOTAL],
            1
        );
        assert_eq!(
            queue.metrics()[crate::metrics::COUNTER_FAIR_QUEUE_POP_FROM_REGULAR],
            1
        );
        assert_eq!(
            queue.metrics()[crate::metrics::COUNTER_FAIR_QUEUE_POP_FROM_PRIORITY],
            0
        );
        assert_eq!(
            queue.metrics()[crate::metrics::GAUGE_FAIR_QUEUE_PRIORITY_ITEMS],
            1
        );
        assert_eq!(
            queue.metrics()[crate::metrics::GAUGE_FAIR_QUEUE_REGULAR_ITEMS],
            0
        );
    }

    #[test]
    fn pop_second_attempt_is_counted_as_from_pool() {
        let scorer = make_test_scorer([(0, 1.0)]);
        let mut queue: TestQueue = make_queue_with_limits(scorer, 100, 100, 100, 100, 100);

        // id=0 is promoted -> priority pool, leaving regular empty.
        queue.push(0, 10).unwrap();
        let _ = queue.pop();

        assert_eq!(
            queue.metrics()[crate::metrics::COUNTER_FAIR_QUEUE_POP_FROM_PRIORITY],
            1
        );
        assert_eq!(
            queue.metrics()[crate::metrics::COUNTER_FAIR_QUEUE_POP_FROM_REGULAR],
            0
        );
    }

    #[test]
    fn dynamic_cap_is_inactive_when_pool_undersaturated() {
        let scorer = make_test_scorer([]);
        let mut queue: TestQueue = make_queue_with_limits(scorer, 20, 100, 20, 100, 100);

        for i in 0..20u32 {
            queue.push(0, i).unwrap();
        }
        let err = queue.push(0, 999).unwrap_err();
        assert!(
            matches!(err, PushError::PerIdLimitExceeded { limit: 20, .. }),
            "expected static per-id limit while undersaturated, got {err:?}"
        );
    }

    #[test]
    fn pressure_mode_uses_bootstrap_for_new_identity_and_clamps_growth() {
        let scorer = make_test_scorer([]);
        let per_id_limit = 200;
        let mut queue: TestQueue =
            make_queue_with_limits(scorer, per_id_limit, 1000, per_id_limit, 1000, 100);

        for i in 0..800u32 {
            queue.push(i + 1, i).unwrap();
        }

        for i in 0..DynamicCapConfig::DEFAULT_BOOTSTRAP_LIMIT as u32 {
            queue.push(999, i).unwrap();
        }
        let err = queue.push(999, 9999).unwrap_err();
        assert!(
            matches!(
                err,
                PushError::PerIdLimitExceeded {
                    limit,
                    ..
                } if limit == DynamicCapConfig::DEFAULT_BOOTSTRAP_LIMIT
            ),
            "expected bootstrap dynamic cap for new identity under pressure, got {err:?}"
        );
    }

    #[test]
    fn recently_served_identity_is_still_capped_by_configured_per_id_limit() {
        let scorer = make_test_scorer([]);
        let per_id_limit = 200;
        let mut queue: TestQueue =
            make_queue_with_limits(scorer, per_id_limit, 1000, per_id_limit, 1000, 100);

        for i in 0..per_id_limit as u32 {
            queue.push(0, i).unwrap();
        }
        for _ in 0..140 {
            assert_eq!(queue.pop().unwrap().0, 0);
        }
        for i in 1..=740u32 {
            queue.push(i, 10_000 + i).unwrap();
        }

        let existing_items_for_id0 = 60u32;
        let remaining_budget = per_id_limit as u32 - existing_items_for_id0;
        for i in 0..remaining_budget {
            queue.push(0, 20_000 + i).unwrap();
        }

        let err = queue.push(0, 39_999).unwrap_err();
        assert!(
            matches!(
                err,
                PushError::PerIdLimitExceeded {
                    limit,
                    ..
                } if limit == per_id_limit
            ),
            "expected configured per-id limit for recently served identity under pressure, got {err:?}"
        );
    }

    proptest! {
        #[test]
        fn conservation(ops in prop::collection::vec(
            (0u32..10, 0u32..1000),
            0..100
        )) {
            let scorer = make_test_scorer((0..100).map(|id| (id, 1.0)));
            let mut queue = make_queue(scorer);
            let mut count = 0usize;

            for (id, val) in ops {
                if queue.push(id, val).is_ok() {
                    count += 1;
                }
            }

            let mut popped = 0;
            while queue.pop().is_some() {
                popped += 1;
            }
            prop_assert_eq!(popped, count);
        }

        #[test]
        fn proportional_bandwidth(score_ratio in 2u32..10, cycles in 10usize..50) {
            let scores: HashMap<u32, f64> = [(0, score_ratio as f64), (1, 1.0)].into_iter().collect();
            let scorer = make_test_scorer(scores);
            let mut queue: TestQueue =
                make_queue_with_limits(scorer, 10000, 100000, 2000, 40000, 0);

            let items = cycles * (score_ratio as usize + 1) * 10;
            for i in 0..items {
                queue.push(0, i as u32).unwrap();
                queue.push(1, (i + 100000) as u32).unwrap();
            }

            let total_pops = cycles * (score_ratio as usize + 1);
            let mut counts: HashMap<u32, usize> = HashMap::new();
            for _ in 0..total_pops {
                if let Some((id, _)) = queue.pop() {
                    *counts.entry(id).or_default() += 1;
                }
            }

            let id0 = counts.get(&0).copied().unwrap_or(0);
            let id1 = counts.get(&1).copied().unwrap_or(0);
            let expected = score_ratio as f64 / (score_ratio as f64 + 1.0);
            let actual = id0 as f64 / (id0 + id1) as f64;

            prop_assert!((actual - expected).abs() < 0.05);
        }

        #[test]
        fn regular_bandwidth_split(regular_pct in 5u8..50) {
            // id=0 is promoted (score 1.0 >= threshold 0.5), id=1 is unknown (not in scores)
            let scorer = make_test_scorer([(0, 1.0)]);
            let mut queue: TestQueue =
                make_queue_with_limits(scorer, 10000, 100000, 10000, 100000, regular_pct);

            for i in 0..10000u32 {
                queue.push(0, i).unwrap();
                queue.push(1, i + 100000).unwrap();
            }

            let total_pops = 1000;
            let mut priority_count = 0;
            let mut regular_count = 0;
            for _ in 0..total_pops {
                match queue.pop() {
                    Some((0, _)) => priority_count += 1,
                    Some((1, _)) => regular_count += 1,
                    Some(_) => {}
                    None => break,
                }
            }

            let expected_regular = regular_pct as f64 / 100.0;
            let actual_regular = regular_count as f64 / (priority_count + regular_count) as f64;

            prop_assert!((actual_regular - expected_regular).abs() < 0.05);
        }
    }
}
