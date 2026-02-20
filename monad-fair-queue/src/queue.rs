use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashMap, VecDeque},
    fmt::{Debug, Display},
    hash::Hash,
    ops::Deref,
};

use monad_executor::ExecutorMetrics;

use crate::{metrics::*, IdentityScore, PeerStatus, PushError, Score};

const DEFAULT_REGULAR_SCORE: Score = Score::ONE;
const POP_COUNTER_WINDOW: u8 = 100;
const DEFAULT_DYNAMIC_CAP_ENTER_PRESSURE: f64 = 0.80;
const DEFAULT_DYNAMIC_CAP_EXIT_PRESSURE: f64 = 0.60;
const DEFAULT_DYNAMIC_CAP_BOOTSTRAP_LIMIT: usize = 64;
const DEFAULT_DYNAMIC_CAP_EMA_ALPHA: f64 = 0.10;
const DEFAULT_DYNAMIC_CAP_SHARE_MULTIPLIER: f64 = 1.0;
const DEFAULT_DYNAMIC_CAP_DECAY_BASE: f64 = 1.0 - DEFAULT_DYNAMIC_CAP_EMA_ALPHA;

macro_rules! ensure {
    ($condition:expr, $error:expr $(,)?) => {
        if !$condition {
            return Err($error);
        }
    };
    ($condition:expr, $on_fail:expr; $error:expr $(,)?) => {
        if !$condition {
            ($on_fail)();
            return Err($error);
        }
    };
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PoolKind {
    Priority,
    Regular,
}

#[derive(Clone, Copy, Debug)]
enum QueueStatus {
    Priority(Score),
    Regular,
}

impl QueueStatus {
    fn into_pool_and_score(self) -> (PoolKind, Score) {
        match self {
            Self::Priority(score) => (PoolKind::Priority, score),
            Self::Regular => (PoolKind::Regular, DEFAULT_REGULAR_SCORE),
        }
    }
}

impl From<PeerStatus> for QueueStatus {
    fn from(status: PeerStatus) -> Self {
        match status {
            PeerStatus::Promoted(score) => Self::Priority(score),
            PeerStatus::Newcomer(_score) => Self::Regular,
            PeerStatus::Unknown => Self::Regular,
        }
    }
}

struct IdentityState<T> {
    queue: VecDeque<T>,
    score: Score,
    finish_time: f64,
    pool: PoolKind,
    service_ema: f64,
    service_seq: u64,
}

struct HeapEntry<Id> {
    finish_time: f64,
    id: Id,
}

struct PopCandidate<Id, T> {
    entry: HeapEntry<Id>,
    status: PeerStatus,
    item: T,
    remaining_len: usize,
    current_pool: PoolKind,
    current_score: Score,
}

enum PostPopAction {
    Requeue {
        target_pool: PoolKind,
        target_score: Score,
    },
    DropRemaining,
}

impl<Id: Eq> PartialEq for HeapEntry<Id> {
    fn eq(&self, other: &Self) -> bool {
        self.finish_time == other.finish_time && self.id == other.id
    }
}

impl<Id: Eq> Eq for HeapEntry<Id> {}

impl<Id: Eq> PartialOrd for HeapEntry<Id> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<Id: Eq> Ord for HeapEntry<Id> {
    fn cmp(&self, other: &Self) -> Ordering {
        other.finish_time.total_cmp(&self.finish_time)
    }
}

struct Pool<Id> {
    heap: BinaryHeap<HeapEntry<Id>>,
    virtual_time: f64,
    total_items: usize,
    max_size: usize,
    per_id_limit: usize,
    dynamic_cap_enforced: bool,
}

impl<Id: Eq> Pool<Id> {
    fn new(per_id_limit: usize, max_size: usize) -> Self {
        Self {
            heap: BinaryHeap::new(),
            virtual_time: 0.0,
            total_items: 0,
            max_size,
            per_id_limit,
            dynamic_cap_enforced: false,
        }
    }

    fn len(&self) -> usize {
        self.total_items
    }

    fn is_empty(&self) -> bool {
        self.total_items == 0
    }
}

pub struct FairQueueMetrics<'a>(&'a ExecutorMetrics);

impl AsRef<ExecutorMetrics> for FairQueueMetrics<'_> {
    fn as_ref(&self) -> &ExecutorMetrics {
        self.0
    }
}

impl Deref for FairQueueMetrics<'_> {
    type Target = ExecutorMetrics;

    fn deref(&self) -> &Self::Target {
        self.0
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

        FairQueue {
            identities: HashMap::new(),
            priority_pool: Pool::new(self.per_id_limit, self.max_size),
            regular_pool: Pool::new(self.regular_per_id_limit, self.regular_max_size),
            scorer,
            regular_bandwidth_pct: self.regular_bandwidth_pct,
            pop_counter: 0,
            service_sequence: 0,
            metrics: ExecutorMetrics::default(),
        }
    }
}

pub struct FairQueue<S: IdentityScore, T> {
    identities: HashMap<S::Identity, IdentityState<T>>,
    priority_pool: Pool<S::Identity>,
    regular_pool: Pool<S::Identity>,
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
    pub fn executor_metrics(&self) -> &ExecutorMetrics {
        &self.metrics
    }

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
        let (preferred_pool, fallback_pool) = if self.pop_counter < self.regular_bandwidth_pct {
            (PoolKind::Regular, PoolKind::Priority)
        } else {
            (PoolKind::Priority, PoolKind::Regular)
        };

        let result = if let Some(item) = self.pop_from_pool(preferred_pool) {
            self.metrics[COUNTER_FAIR_QUEUE_POP_PREFERRED] += 1;
            match preferred_pool {
                PoolKind::Priority => self.metrics[COUNTER_FAIR_QUEUE_POP_FROM_PRIORITY] += 1,
                PoolKind::Regular => self.metrics[COUNTER_FAIR_QUEUE_POP_FROM_REGULAR] += 1,
            }
            self.pop_counter = (self.pop_counter + 1) % POP_COUNTER_WINDOW;
            Some(item)
        } else if let Some(item) = self.pop_from_pool(fallback_pool) {
            self.metrics[COUNTER_FAIR_QUEUE_POP_FALLBACK] += 1;
            match fallback_pool {
                PoolKind::Priority => self.metrics[COUNTER_FAIR_QUEUE_POP_FROM_PRIORITY] += 1,
                PoolKind::Regular => self.metrics[COUNTER_FAIR_QUEUE_POP_FROM_REGULAR] += 1,
            }
            Some(item)
        } else {
            self.metrics[COUNTER_FAIR_QUEUE_POP_EMPTY] += 1;
            None
        };

        if result.is_some() {
            self.metrics[COUNTER_FAIR_QUEUE_POP_TOTAL] += 1;
        }
        self.update_len_metrics();
        result
    }

    pub fn metrics(&self) -> FairQueueMetrics<'_> {
        FairQueueMetrics(&self.metrics)
    }

    pub fn len(&self) -> usize {
        self.priority_pool.len() + self.regular_pool.len()
    }

    pub fn is_empty(&self) -> bool {
        self.priority_pool.is_empty() && self.regular_pool.is_empty()
    }

    fn push_inner(&mut self, id: S::Identity, item: T) -> Result<PoolKind, PushError<S::Identity>> {
        if self.identities.contains_key(&id) {
            let (pool_kind, new_len) = {
                let state = self.identities.get(&id).expect("state exists");
                (state.pool, state.queue.len() + 1)
            };
            let effective_limit = self.effective_per_id_limit(&id, pool_kind, 1);

            {
                let pool = self.pool(pool_kind);
                ensure!(
                    new_len <= effective_limit,
                    PushError::PerIdLimitExceeded {
                        id: id.clone(),
                        limit: effective_limit,
                    }
                );
                ensure!(
                    pool.total_items + 1 <= pool.max_size,
                    PushError::Full {
                        size: pool.total_items,
                        max_size: pool.max_size,
                    }
                );
            }
            let state = self.identities.get_mut(&id).expect("state exists");
            state.queue.push_back(item);
            self.pool_mut(pool_kind).total_items += 1;
            return Ok(pool_kind);
        }

        self.push_new_identity(id, item)
    }

    fn push_new_identity(
        &mut self,
        id: S::Identity,
        item: T,
    ) -> Result<PoolKind, PushError<S::Identity>> {
        let (desired_pool, desired_score) =
            QueueStatus::from(self.scorer.score(&id)).into_pool_and_score();
        let _ = self.refresh_dynamic_cap_mode(desired_pool, 1);

        let accept = {
            let pool = self.pool(desired_pool);
            if pool.total_items + 1 > pool.max_size {
                Err(PushError::Full {
                    size: pool.total_items,
                    max_size: pool.max_size,
                })
            } else {
                Ok(())
            }
        };

        let (pool, score) = match accept {
            Ok(()) => (desired_pool, desired_score),
            Err(PushError::Full { .. }) if desired_pool == PoolKind::Priority => {
                let _ = self.refresh_dynamic_cap_mode(PoolKind::Regular, 1);
                {
                    let pool = self.pool(PoolKind::Regular);
                    ensure!(
                        pool.total_items + 1 <= pool.max_size,
                        PushError::Full {
                            size: pool.total_items,
                            max_size: pool.max_size,
                        }
                    );
                }
                (PoolKind::Regular, DEFAULT_REGULAR_SCORE)
            }
            Err(err) => return Err(err),
        };

        let finish_time = self.pool(pool).virtual_time + score.reciprocal();
        let service_seq = self.service_sequence;
        self.identities.insert(
            id.clone(),
            IdentityState {
                queue: VecDeque::from([item]),
                score,
                finish_time,
                pool,
                service_ema: 0.0,
                service_seq,
            },
        );

        let assigned_pool = pool;
        let pool_state = self.pool_mut(assigned_pool);
        pool_state.total_items += 1;
        pool_state.heap.push(HeapEntry { finish_time, id });
        Ok(assigned_pool)
    }

    fn update_len_metrics(&mut self) {
        self.metrics[GAUGE_FAIR_QUEUE_PRIORITY_ITEMS] = self.priority_pool.total_items as u64;
        self.metrics[GAUGE_FAIR_QUEUE_REGULAR_ITEMS] = self.regular_pool.total_items as u64;
        self.metrics[GAUGE_FAIR_QUEUE_TOTAL_ITEMS] =
            (self.priority_pool.total_items + self.regular_pool.total_items) as u64;
    }

    fn refresh_dynamic_cap_mode(&mut self, pool_kind: PoolKind, incoming_items: usize) -> bool {
        let pool = self.pool_mut(pool_kind);
        let occupancy =
            pool.total_items.saturating_add(incoming_items) as f64 / pool.max_size as f64;
        if pool.dynamic_cap_enforced {
            if occupancy <= DEFAULT_DYNAMIC_CAP_EXIT_PRESSURE {
                pool.dynamic_cap_enforced = false;
            }
        } else if occupancy >= DEFAULT_DYNAMIC_CAP_ENTER_PRESSURE {
            pool.dynamic_cap_enforced = true;
        }
        pool.dynamic_cap_enforced
    }

    fn decayed_service_share(&mut self, id: &S::Identity) -> f64 {
        let service_seq = self.service_sequence;
        let Some(state) = self.identities.get_mut(id) else {
            return 0.0;
        };
        let delta = service_seq.saturating_sub(state.service_seq);
        if delta > 0 {
            state.service_ema *= DEFAULT_DYNAMIC_CAP_DECAY_BASE.powf(delta as f64);
            state.service_seq = service_seq;
        }
        state.service_ema = state.service_ema.clamp(0.0, 1.0);
        state.service_ema
    }

    fn effective_per_id_limit(
        &mut self,
        id: &S::Identity,
        pool_kind: PoolKind,
        incoming_items: usize,
    ) -> usize {
        if !self.refresh_dynamic_cap_mode(pool_kind, incoming_items) {
            return self.pool(pool_kind).per_id_limit;
        }

        let (pool_limit, pool_max_size) = {
            let pool = self.pool(pool_kind);
            (pool.per_id_limit, pool.max_size)
        };
        let share = self.decayed_service_share(id);
        let bonus =
            (share * pool_max_size as f64 * DEFAULT_DYNAMIC_CAP_SHARE_MULTIPLIER).ceil() as usize;
        let dynamic_limit = DEFAULT_DYNAMIC_CAP_BOOTSTRAP_LIMIT.saturating_add(bonus);
        dynamic_limit.clamp(1, pool_limit)
    }

    fn record_service_on_pop(&mut self, id: &S::Identity) {
        self.service_sequence = self.service_sequence.saturating_add(1);
        let service_seq = self.service_sequence;
        let Some(state) = self.identities.get_mut(id) else {
            return;
        };
        let delta = service_seq.saturating_sub(state.service_seq);
        if delta > 0 {
            state.service_ema *= DEFAULT_DYNAMIC_CAP_DECAY_BASE.powf(delta as f64);
        }
        state.service_ema += 1.0 - DEFAULT_DYNAMIC_CAP_DECAY_BASE;
        state.service_ema = state.service_ema.clamp(0.0, 1.0);
        state.service_seq = service_seq;
    }

    fn pop_from_pool(&mut self, pool_kind: PoolKind) -> Option<(S::Identity, T)> {
        let candidate = self.pop_candidate(pool_kind)?;
        let PopCandidate {
            entry,
            status,
            item,
            remaining_len,
            current_pool,
            current_score,
        } = candidate;

        self.apply_popped_entry(pool_kind, entry.finish_time);
        self.record_service_on_pop(&entry.id);

        if remaining_len == 0 {
            self.identities.remove(&entry.id);
            return Some((entry.id, item));
        }

        let action = self.select_target_after_pop(
            status,
            current_pool,
            current_score,
            remaining_len,
            &entry.id,
        );
        match action {
            PostPopAction::DropRemaining => {
                self.remove_identity_items(&entry.id, current_pool, remaining_len);
            }
            PostPopAction::Requeue {
                target_pool,
                target_score,
            } => {
                self.migrate_remaining_items(current_pool, target_pool, remaining_len);

                let next_finish =
                    self.reschedule_after_pop(&entry.id, current_pool, target_pool, target_score);

                self.pool_mut(target_pool).heap.push(HeapEntry {
                    finish_time: next_finish,
                    id: entry.id.clone(),
                });
            }
        }

        Some((entry.id, item))
    }

    fn pop_candidate(&mut self, pool_kind: PoolKind) -> Option<PopCandidate<S::Identity, T>> {
        loop {
            let entry = {
                let pool = self.pool_mut(pool_kind);
                pool.heap.pop()
            }?;

            let id = entry.id.clone();
            let (item, remaining_len, current_pool, current_score) = {
                let Some(state) = self.identities.get_mut(&id) else {
                    continue;
                };
                if state.pool != pool_kind || entry.finish_time != state.finish_time {
                    continue;
                }

                let Some(item) = state.queue.pop_front() else {
                    self.identities.remove(&id);
                    continue;
                };
                (item, state.queue.len(), state.pool, state.score)
            };
            let status = self.scorer.score(&id);

            return Some(PopCandidate {
                entry,
                status,
                item,
                remaining_len,
                current_pool,
                current_score,
            });
        }
    }

    fn apply_popped_entry(&mut self, pool_kind: PoolKind, finish_time: f64) {
        {
            let pool = self.pool_mut(pool_kind);
            pool.total_items -= 1;
            pool.virtual_time = finish_time;
        }
        let _ = self.refresh_dynamic_cap_mode(pool_kind, 0);
    }

    fn select_target_after_pop(
        &self,
        status: PeerStatus,
        current_pool: PoolKind,
        current_score: Score,
        remaining_len: usize,
        id: &S::Identity,
    ) -> PostPopAction {
        let (desired_pool, desired_score) = QueueStatus::from(status).into_pool_and_score();
        if desired_pool == current_pool {
            return PostPopAction::Requeue {
                target_pool: current_pool,
                target_score: desired_score,
            };
        }

        let can_move = {
            let pool = self.pool(desired_pool);
            if remaining_len > pool.per_id_limit {
                Err(PushError::PerIdLimitExceeded {
                    id: id.clone(),
                    limit: pool.per_id_limit,
                })
            } else if pool.total_items + remaining_len > pool.max_size {
                Err(PushError::Full {
                    size: pool.total_items,
                    max_size: pool.max_size,
                })
            } else {
                Ok(())
            }
        };

        match (desired_pool, can_move) {
            (PoolKind::Priority, Ok(())) => PostPopAction::Requeue {
                target_pool: PoolKind::Priority,
                target_score: desired_score,
            },
            (PoolKind::Priority, Err(PushError::Full { .. })) => PostPopAction::Requeue {
                target_pool: PoolKind::Regular,
                target_score: DEFAULT_REGULAR_SCORE,
            },
            (PoolKind::Priority, Err(_)) => PostPopAction::Requeue {
                target_pool: current_pool,
                target_score: current_score,
            },
            (PoolKind::Regular, Ok(())) => PostPopAction::Requeue {
                target_pool: PoolKind::Regular,
                target_score: DEFAULT_REGULAR_SCORE,
            },
            (PoolKind::Regular, Err(_)) => PostPopAction::DropRemaining,
        }
    }

    fn migrate_remaining_items(
        &mut self,
        current_pool: PoolKind,
        target_pool: PoolKind,
        remaining_len: usize,
    ) {
        if target_pool == current_pool || remaining_len == 0 {
            return;
        }

        {
            let pool = self.pool_mut(current_pool);
            assert!(
                pool.total_items >= remaining_len,
                "pool total_items underflow while migrating between pools"
            );
            pool.total_items -= remaining_len;
        }
        self.pool_mut(target_pool).total_items += remaining_len;
    }

    fn reschedule_after_pop(
        &mut self,
        id: &S::Identity,
        current_pool: PoolKind,
        target_pool: PoolKind,
        target_score: Score,
    ) -> f64 {
        let target_virtual_time = self.pool(target_pool).virtual_time;
        let state = self.identities.get_mut(id).expect("state exists");
        if target_pool != current_pool {
            state.pool = target_pool;
            state.finish_time = target_virtual_time;
        }
        state.score = target_score;
        let base = target_virtual_time.max(state.finish_time);
        state.finish_time = base + state.score.reciprocal();
        state.finish_time
    }

    fn pool(&self, pool_kind: PoolKind) -> &Pool<S::Identity> {
        match pool_kind {
            PoolKind::Priority => &self.priority_pool,
            PoolKind::Regular => &self.regular_pool,
        }
    }

    fn pool_mut(&mut self, pool_kind: PoolKind) -> &mut Pool<S::Identity> {
        match pool_kind {
            PoolKind::Priority => &mut self.priority_pool,
            PoolKind::Regular => &mut self.regular_pool,
        }
    }

    fn remove_identity_items(&mut self, id: &S::Identity, pool_kind: PoolKind, item_count: usize) {
        let Some(state) = self.identities.remove(id) else {
            return;
        };
        debug_assert_eq!(state.pool, pool_kind);
        debug_assert_eq!(state.queue.len(), item_count);

        let pool = self.pool_mut(pool_kind);
        assert!(
            pool.total_items >= item_count,
            "pool total_items underflow while removing identity"
        );
        pool.total_items -= item_count;
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
            if s >= self.threshold {
                PeerStatus::Promoted(Score::try_from(s).unwrap())
            } else if s > 0.0 {
                PeerStatus::Newcomer(Score::try_from(s).unwrap())
            } else {
                PeerStatus::Unknown
            }
        }
    }

    type TestQueue = FairQueue<TestScorer, u32>;

    const TEST_THRESHOLD: f64 = 0.5;

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

        let mut counts: HashMap<u32, usize> = HashMap::new();
        while let Some((id, _)) = queue.pop() {
            *counts.entry(id).or_default() += 1;
        }
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

        let mut counts: HashMap<u32, usize> = HashMap::new();
        while let Some((id, _)) = queue.pop() {
            *counts.entry(id).or_default() += 1;
        }
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
    fn fallback_does_not_consume_regular_share() {
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
            if s >= self.threshold {
                PeerStatus::Promoted(Score::try_from(s).unwrap())
            } else if s > 0.0 {
                PeerStatus::Newcomer(Score::try_from(s).unwrap())
            } else {
                PeerStatus::Unknown
            }
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
    fn move_between_pools_on_pop() {
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

        let state = queue.identities.get(&0).unwrap();
        assert_eq!(state.pool, PoolKind::Priority);
    }

    #[test]
    fn demotion_over_regular_per_id_limit_drops_remaining_items() {
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

        let (id, _) = queue.pop().unwrap();
        assert_eq!(id, 0);
        assert!(queue.pop().is_none());
        assert!(queue.is_empty());
    }

    #[test]
    fn demotion_when_regular_full_drops_remaining_items() {
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

        let (first, _) = queue.pop().unwrap();
        assert_eq!(first, 0);
        assert_eq!(queue.pop().unwrap().0, 1);
        assert_eq!(queue.pop().unwrap().0, 1);
        assert_eq!(queue.pop().unwrap().0, 1);
        assert!(queue.pop().is_none());
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
        let mut counts: HashMap<u32, usize> = HashMap::new();
        for _ in 0..total_pops {
            if let Some((id, _)) = queue.pop() {
                *counts.entry(id).or_default() += 1;
            }
        }

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
            queue.metrics()[crate::metrics::GAUGE_FAIR_QUEUE_TOTAL_ITEMS],
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
            queue.metrics()[crate::metrics::GAUGE_FAIR_QUEUE_TOTAL_ITEMS],
            2
        );

        let _ = queue.pop();
        assert_eq!(
            queue.metrics()[crate::metrics::COUNTER_FAIR_QUEUE_POP_TOTAL],
            1
        );
        assert_eq!(
            queue.metrics()[crate::metrics::COUNTER_FAIR_QUEUE_POP_PREFERRED],
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
            queue.metrics()[crate::metrics::GAUGE_FAIR_QUEUE_TOTAL_ITEMS],
            1
        );
    }

    #[test]
    fn pop_fallback_is_counted_as_from_pool() {
        let scorer = make_test_scorer([(0, 1.0)]);
        let mut queue: TestQueue = make_queue_with_limits(scorer, 100, 100, 100, 100, 100);

        // id=0 is promoted -> priority pool, leaving regular empty.
        queue.push(0, 10).unwrap();
        let _ = queue.pop();

        assert_eq!(
            queue.metrics()[crate::metrics::COUNTER_FAIR_QUEUE_POP_FALLBACK],
            1
        );
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
    fn pressure_mode_gives_new_identity_bootstrap_budget() {
        let scorer = make_test_scorer([]);
        let mut queue: TestQueue = make_queue_with_limits(scorer, 200, 320, 200, 320, 100);

        for i in 0..256u32 {
            queue.push(i + 1, i).unwrap();
        }

        for i in 0..DEFAULT_DYNAMIC_CAP_BOOTSTRAP_LIMIT as u32 {
            queue.push(999, i).unwrap();
        }
        let err = queue.push(999, 9999).unwrap_err();
        assert!(
            matches!(
                err,
                PushError::PerIdLimitExceeded {
                    limit: DEFAULT_DYNAMIC_CAP_BOOTSTRAP_LIMIT,
                    ..
                }
            ),
            "expected bootstrap dynamic cap for new identity, got {err:?}"
        );
    }

    #[test]
    fn recently_served_identity_gets_larger_dynamic_cap_under_pressure() {
        let scorer = make_test_scorer([]);
        let mut queue: TestQueue = make_queue_with_limits(scorer, 200, 400, 200, 400, 100);

        for i in 0..130u32 {
            queue.push(0, i).unwrap();
        }
        for _ in 0..80 {
            assert_eq!(queue.pop().unwrap().0, 0);
        }
        for i in 1..=270u32 {
            queue.push(i, 10_000 + i).unwrap();
        }

        for i in 0..15u32 {
            queue.push(0, 20_000 + i).unwrap();
        }

        for i in 0..DEFAULT_DYNAMIC_CAP_BOOTSTRAP_LIMIT as u32 {
            queue.push(777, 30_000 + i).unwrap();
        }
        let err = queue.push(777, 39_999).unwrap_err();
        assert!(
            matches!(
                err,
                PushError::PerIdLimitExceeded {
                    limit: DEFAULT_DYNAMIC_CAP_BOOTSTRAP_LIMIT,
                    ..
                }
            ),
            "expected bootstrap cap for non-served identity under pressure, got {err:?}"
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
