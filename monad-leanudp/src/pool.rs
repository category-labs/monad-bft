use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    hash::Hash,
    time::{Duration, Instant},
};

use bytes::{Bytes, BytesMut};
use indexmap::IndexMap;
use monad_peer_score::dynamic_cap::{
    effective_limit as dynamic_cap_effective_limit, update_pressure_mode, DynamicCapConfig,
    DynamicCapIdentity,
};
use rand::{CryptoRng, Rng as _, RngCore};

use crate::{
    decoder::{DecodeError, DecodeOutcome},
    encoder::MAX_FRAGMENTS,
    metrics::*,
    Config, FragmentType,
};

const PRIORITY_DYNAMIC_CAP_BOOTSTRAP_LIMIT: usize = 10;

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

pub(crate) struct FragmentInput<I> {
    pub(crate) identity: I,
    pub(crate) msg_id: u32,
    pub(crate) seq_num: u16,
    pub(crate) fragment_type: FragmentType,
    pub(crate) payload: Bytes,
}

struct MessageState {
    fragments: BTreeMap<u16, Bytes>,
    total_size: usize,
    total_frags: Option<u16>,
    eviction_deadline: Instant,
}

impl MessageState {
    fn new(eviction_deadline: Instant) -> Self {
        Self {
            fragments: BTreeMap::new(),
            total_size: 0,
            total_frags: None,
            eviction_deadline,
        }
    }

    fn extract(self) -> Bytes {
        let mut buf = BytesMut::with_capacity(self.total_size);
        for frag in self.fragments.into_values() {
            buf.extend_from_slice(&frag);
        }
        buf.freeze()
    }
}

pub(crate) struct PoolConfig {
    max_messages: usize,
    max_message_size: usize,
    message_timeout: Duration,
}

impl PoolConfig {
    pub(crate) fn from_config(config: &Config, max_messages: usize) -> Self {
        Self {
            max_messages,
            max_message_size: config.max_message_size,
            message_timeout: config.message_timeout,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EvictionKind {
    Timeout,
    Random,
}

impl EvictionKind {
    pub(crate) fn metric_name(self) -> &'static str {
        match self {
            EvictionKind::Timeout => COUNTER_LEANUDP_DECODE_EVICTED_TIMEOUT,
            EvictionKind::Random => COUNTER_LEANUDP_DECODE_EVICTED_RANDOM,
        }
    }
}

pub(crate) trait IdentityLimitPolicy<I>
where
    I: Eq + Hash + Clone + Ord,
{
    fn limit_for_new_message(
        &mut self,
        identity: &I,
        priority_pool_size: usize,
        priority_pool_max_size: usize,
    ) -> usize;

    fn record_priority_service(&mut self, identity: &I);

    fn on_identity_removed(&mut self, identity: &I);
}

pub(crate) struct DynamicCapIdentityLimitPolicy<I>
where
    I: Eq + Hash + Clone + Ord,
{
    max_messages_per_identity: usize,
    priority_dynamic_cap_enforced: bool,
    priority_dynamic_cap_cfg: DynamicCapConfig,
    priority_dynamic_cap: HashMap<I, DynamicCapIdentity>,
    priority_service_sequence: u64,
}

impl<I> DynamicCapIdentityLimitPolicy<I>
where
    I: Eq + Hash + Clone + Ord,
{
    fn from_config(config: &Config) -> Self {
        let priority_dynamic_cap_cfg = DynamicCapConfig {
            bootstrap_limit: PRIORITY_DYNAMIC_CAP_BOOTSTRAP_LIMIT,
            ..DynamicCapConfig::default()
        };
        priority_dynamic_cap_cfg.validate();
        Self {
            max_messages_per_identity: config.max_messages_per_identity,
            priority_dynamic_cap_enforced: false,
            priority_dynamic_cap_cfg,
            priority_dynamic_cap: HashMap::new(),
            priority_service_sequence: 0,
        }
    }
}

impl<I> IdentityLimitPolicy<I> for DynamicCapIdentityLimitPolicy<I>
where
    I: Eq + Hash + Clone + Ord,
{
    fn limit_for_new_message(
        &mut self,
        identity: &I,
        priority_pool_size: usize,
        priority_pool_max_size: usize,
    ) -> usize {
        self.priority_dynamic_cap_enforced = update_pressure_mode(
            self.priority_dynamic_cap_enforced,
            priority_pool_size,
            priority_pool_max_size,
            1,
            &self.priority_dynamic_cap_cfg,
        );

        let share = self
            .priority_dynamic_cap
            .get_mut(identity)
            .map_or(0.0, |state| {
                state.decayed_share(
                    self.priority_service_sequence,
                    &self.priority_dynamic_cap_cfg,
                )
            });
        dynamic_cap_effective_limit(
            self.max_messages_per_identity,
            priority_pool_max_size,
            self.priority_dynamic_cap_enforced,
            share,
            &self.priority_dynamic_cap_cfg,
        )
    }

    fn record_priority_service(&mut self, identity: &I) {
        self.priority_service_sequence = self.priority_service_sequence.saturating_add(1);
        let service_seq = self.priority_service_sequence;
        self.priority_dynamic_cap
            .entry(identity.clone())
            .or_insert_with(|| DynamicCapIdentity::new(service_seq))
            .observe_service(service_seq, &self.priority_dynamic_cap_cfg);
    }

    fn on_identity_removed(&mut self, identity: &I) {
        self.priority_dynamic_cap.remove(identity);
    }
}

pub(crate) struct IdentityUsage<I, L = DynamicCapIdentityLimitPolicy<I>>
where
    I: Eq + Hash + Clone + Ord,
    L: IdentityLimitPolicy<I>,
{
    message_counts: HashMap<I, usize>,
    max_messages_per_identity: usize,
    limit_policy: L,
}

impl<I> IdentityUsage<I>
where
    I: Eq + Hash + Clone + Ord,
{
    pub(crate) fn from_config(config: &Config) -> Self {
        Self::with_limit_policy(
            config.max_messages_per_identity,
            DynamicCapIdentityLimitPolicy::from_config(config),
        )
    }
}

impl<I, L> IdentityUsage<I, L>
where
    I: Eq + Hash + Clone + Ord,
    L: IdentityLimitPolicy<I>,
{
    pub(crate) fn with_limit_policy(max_messages_per_identity: usize, limit_policy: L) -> Self {
        Self {
            message_counts: HashMap::new(),
            max_messages_per_identity,
            limit_policy,
        }
    }

    pub(crate) fn identity_count(&self, identity: &I) -> usize {
        self.message_counts
            .get(identity)
            .copied()
            .unwrap_or_default()
    }

    pub(crate) fn record_priority_service(&mut self, identity: &I) {
        self.limit_policy.record_priority_service(identity);
    }

    pub(crate) fn priority_limit_for_new_message(
        &mut self,
        identity: &I,
        priority_pool_size: usize,
        priority_pool_max_size: usize,
    ) -> usize {
        self.limit_policy.limit_for_new_message(
            identity,
            priority_pool_size,
            priority_pool_max_size,
        )
    }

    pub(crate) fn regular_limit_for_new_message(&self) -> usize {
        self.max_messages_per_identity
    }

    pub(crate) fn is_at_priority_limit(
        &mut self,
        identity: &I,
        priority_pool_size: usize,
        priority_pool_max_size: usize,
    ) -> bool {
        self.identity_count(identity)
            >= self.priority_limit_for_new_message(
                identity,
                priority_pool_size,
                priority_pool_max_size,
            )
    }

    pub(crate) fn is_at_regular_limit(&self, identity: &I) -> bool {
        self.identity_count(identity) >= self.regular_limit_for_new_message()
    }

    pub(crate) fn increment_identity_count(&mut self, identity: &I) {
        *self.message_counts.entry(identity.clone()).or_insert(0) += 1;
    }

    pub(crate) fn decrement_identity_count(&mut self, identity: &I) {
        let count = self
            .message_counts
            .get_mut(identity)
            .expect("identity count must exist for active message");
        *count -= 1;
        if *count == 0 {
            self.message_counts.remove(identity);
            self.limit_policy.on_identity_removed(identity);
        }
    }

    pub(crate) fn ensure_priority_identity_capacity(
        &mut self,
        identity: &I,
        priority_pool_size: usize,
        priority_pool_max_size: usize,
    ) -> Result<(), DecodeError> {
        let limit = self.priority_limit_for_new_message(
            identity,
            priority_pool_size,
            priority_pool_max_size,
        );
        ensure!(
            self.identity_count(identity) < limit,
            DecodeError::IdentityLimitExceeded { max: limit }
        );
        Ok(())
    }

    pub(crate) fn ensure_regular_identity_capacity(&self, identity: &I) -> Result<(), DecodeError> {
        let limit = self.regular_limit_for_new_message();
        ensure!(
            self.identity_count(identity) < limit,
            DecodeError::IdentityLimitExceeded { max: limit }
        );
        Ok(())
    }
}

pub(crate) struct MessagePool<I, R>
where
    I: Eq + Hash + Clone + Ord,
    R: CryptoRng + RngCore,
{
    messages: IndexMap<(I, u32), MessageState>,
    // Min-by-deadline eviction index.
    eviction_index: BTreeSet<(Instant, (I, u32))>,
    // Per-identity min-by-deadline eviction index.
    eviction_index_by_identity: BTreeMap<I, BTreeSet<(Instant, u32)>>,
    cfg: PoolConfig,
    rng: R,
}

impl<I: Eq + Hash + Clone + Ord, R: CryptoRng + RngCore> MessagePool<I, R> {
    pub(crate) fn new(cfg: PoolConfig, rng: R) -> Self {
        Self {
            messages: IndexMap::new(),
            eviction_index: BTreeSet::new(),
            eviction_index_by_identity: BTreeMap::new(),
            cfg,
            rng,
        }
    }

    pub(crate) fn has_message(&self, key: &(I, u32)) -> bool {
        self.messages.contains_key(key)
    }

    pub(crate) fn evict_before_admission<L>(
        &mut self,
        usage: &mut IdentityUsage<I, L>,
        now: Instant,
    ) -> Option<EvictionKind>
    where
        L: IdentityLimitPolicy<I>,
    {
        if self.messages.is_empty() {
            return None;
        }

        if self.evict_candidate_if_stale(usage, self.eviction_index.first().cloned(), now) {
            return Some(EvictionKind::Timeout);
        }

        let random_index = self.rng.gen_range(0..self.messages.len());
        let random_key = self
            .messages
            .get_index(random_index)
            .map(|(key, _)| key.clone());
        random_key.map(|key| {
            let _ = self.remove_message(&key, usage);
            EvictionKind::Random
        })
    }

    pub(crate) fn admission_blocked(&self) -> bool {
        self.messages.len() >= self.cfg.max_messages
    }

    pub(crate) fn insert_message(&mut self, key: (I, u32), now: Instant) {
        let deadline = now + self.cfg.message_timeout;
        self.messages
            .insert(key.clone(), MessageState::new(deadline));
        self.eviction_index.insert((deadline, key.clone()));
        self.eviction_index_by_identity
            .entry(key.0.clone())
            .or_default()
            .insert((deadline, key.1));
    }

    pub(crate) fn decode_with_admission<L>(
        &mut self,
        usage: &mut IdentityUsage<I, L>,
        now: Instant,
        key: &(I, u32),
        input: FragmentInput<I>,
    ) -> (Result<DecodeOutcome, DecodeError>, Option<EvictionKind>)
    where
        L: IdentityLimitPolicy<I>,
    {
        let mut evicted = None;

        if !self.has_message(key) {
            if self.admission_blocked() {
                evicted = self.evict_before_admission(usage, now);
            }
            if self.admission_blocked() {
                return (Err(DecodeError::PoolFull), evicted);
            }
            self.insert_message(key.clone(), now);
            usage.increment_identity_count(&input.identity);
        }

        let result = self.decode(usage, input);
        (result, evicted)
    }

    pub(crate) fn evict_stale_for_identity<L>(
        &mut self,
        usage: &mut IdentityUsage<I, L>,
        identity: &I,
        now: Instant,
    ) -> bool
    where
        L: IdentityLimitPolicy<I>,
    {
        let stale_candidate = self
            .eviction_index_by_identity
            .get(identity)
            .and_then(|identity_index| identity_index.first().copied())
            .map(|(deadline, msg_id)| (deadline, (identity.clone(), msg_id)));
        self.evict_candidate_if_stale(usage, stale_candidate, now)
    }

    fn evict_candidate_if_stale<L>(
        &mut self,
        usage: &mut IdentityUsage<I, L>,
        stale_candidate: Option<(Instant, (I, u32))>,
        now: Instant,
    ) -> bool
    where
        L: IdentityLimitPolicy<I>,
    {
        let Some((deadline, stale_key)) = stale_candidate else {
            return false;
        };
        if deadline > now {
            return false;
        }
        let _ = self.remove_message(&stale_key, usage);
        true
    }

    /// Decode a fragment into the pool.
    pub(crate) fn decode<L>(
        &mut self,
        usage: &mut IdentityUsage<I, L>,
        input: FragmentInput<I>,
    ) -> Result<DecodeOutcome, DecodeError>
    where
        L: IdentityLimitPolicy<I>,
    {
        let FragmentInput {
            identity,
            msg_id,
            seq_num,
            fragment_type,
            payload: data,
        } = input;
        let key = (identity, msg_id);
        let fragment_size = data.len();
        ensure!(self.messages.contains_key(&key), DecodeError::PoolFull);

        let is_complete = {
            let state = self
                .messages
                .get_mut(&key)
                .expect("message was just inserted or confirmed to exist");

            ensure!(
                (seq_num as usize) < MAX_FRAGMENTS,
                || {
                    self.remove_message(&key, usage);
                };
                DecodeError::TooManyFragments {
                    count: MAX_FRAGMENTS + 1,
                    max: MAX_FRAGMENTS,
                }
            );

            // NOTE(dshulyak): duplicate fragments should only happen when a sender is buggy
            // or malicious and resends the same chunk.
            ensure!(
                !state.fragments.contains_key(&seq_num),
                DecodeError::DuplicateFragment { msg_id, seq_num }
            );

            if matches!(fragment_type, FragmentType::End | FragmentType::Complete) {
                let new_total = seq_num + 1;
                let existing_total_frags = state.total_frags;
                ensure!(
                    existing_total_frags.is_none(),
                    || {
                        self.remove_message(&key, usage);
                    };
                    DecodeError::ConflictingEndMarker {
                        expected: existing_total_frags.unwrap_or_default(),
                        actual: new_total,
                    }
                );
                state.total_frags = Some(new_total);
            }

            let total_size = state.total_size.checked_add(fragment_size);
            ensure!(
                matches!(total_size, Some(size) if size <= self.cfg.max_message_size),
                || {
                    self.remove_message(&key, usage);
                };
                DecodeError::MessageSizeExceeded {
                    size: total_size.unwrap_or(usize::MAX),
                    max: self.cfg.max_message_size,
                }
            );
            state.total_size = total_size.expect("total_size validated above");
            state.fragments.insert(seq_num, data);

            state.total_frags == Some(state.fragments.len() as u16)
        };
        if !is_complete {
            return Ok(DecodeOutcome::Pending);
        }

        let state = self
            .remove_message(&key, usage)
            .expect("message must exist when extracting");
        Ok(DecodeOutcome::Complete(state.extract()))
    }

    fn remove_message<L>(
        &mut self,
        key: &(I, u32),
        usage: &mut IdentityUsage<I, L>,
    ) -> Option<MessageState>
    where
        L: IdentityLimitPolicy<I>,
    {
        let state = self.messages.swap_remove(key)?;
        let deadline = state.eviction_deadline;
        let _ = self.eviction_index.remove(&(deadline, key.clone()));
        let mut should_remove_identity = false;
        if let Some(identity_index) = self.eviction_index_by_identity.get_mut(&key.0) {
            let _ = identity_index.remove(&(deadline, key.1));
            if identity_index.is_empty() {
                should_remove_identity = true;
            }
        }
        if should_remove_identity {
            self.eviction_index_by_identity.remove(&key.0);
        }
        usage.decrement_identity_count(&key.0);
        Some(state)
    }

    pub(crate) fn message_count(&self) -> usize {
        self.messages.len()
    }

    pub(crate) fn max_messages(&self) -> usize {
        self.cfg.max_messages
    }
}
