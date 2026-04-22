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
    collections::{BTreeMap, BTreeSet, HashMap},
    hash::Hash,
    time::{Duration, Instant},
};

use bytes::{Bytes, BytesMut};
use indexmap::IndexMap;
use monad_executor::MetricDef;
use rand::{CryptoRng, Rng as _, RngCore};

use crate::{
    decoder::{DecodeError, DecodeOutcome},
    encoder::MAX_FRAGMENTS,
    metrics::*,
    Config, FragmentType,
};

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
    pub(crate) msg_id: u16,
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
    pub(crate) fn metric_name(self) -> &'static MetricDef {
        match self {
            EvictionKind::Timeout => COUNTER_LEANUDP_DECODE_EVICTED_TIMEOUT,
            EvictionKind::Random => COUNTER_LEANUDP_DECODE_EVICTED_RANDOM,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MessageStatus {
    Missing,
    Active,
    Stale,
}

pub(crate) struct IdentityUsage<I>
where
    I: Eq + Hash + Clone + Ord,
{
    message_counts: HashMap<I, usize>,
    max_messages_per_identity: usize,
}

impl<I> IdentityUsage<I>
where
    I: Eq + Hash + Clone + Ord,
{
    pub(crate) fn new(config: &Config) -> Self {
        Self {
            message_counts: HashMap::new(),
            max_messages_per_identity: config.max_messages_per_identity,
        }
    }

    pub(crate) fn identity_count(&self, identity: &I) -> usize {
        self.message_counts
            .get(identity)
            .copied()
            .unwrap_or_default()
    }

    pub(crate) fn is_at_limit(&self, identity: &I) -> bool {
        self.identity_count(identity) >= self.max_messages_per_identity
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
        }
    }

    pub(crate) fn ensure_identity_capacity(&self, identity: &I) -> Result<(), DecodeError> {
        ensure!(
            self.identity_count(identity) < self.max_messages_per_identity,
            DecodeError::IdentityLimitExceeded {
                max: self.max_messages_per_identity
            }
        );
        Ok(())
    }
}

pub(crate) struct MessagePool<I, R>
where
    I: Eq + Hash + Clone + Ord,
    R: CryptoRng + RngCore,
{
    messages: IndexMap<(I, u16), MessageState>,
    // Min-by-deadline eviction index.
    eviction_index: BTreeSet<(Instant, (I, u16))>,
    // Per-identity min-by-deadline eviction index.
    eviction_index_by_identity: BTreeMap<I, BTreeSet<(Instant, u16)>>,
    cfg: PoolConfig,
    rng: R,
    identity_usage: IdentityUsage<I>,
}

impl<I: Eq + Hash + Clone + Ord, R: CryptoRng + RngCore> MessagePool<I, R> {
    pub(crate) fn new(cfg: PoolConfig, rng: R, identity_usage: IdentityUsage<I>) -> Self {
        Self {
            messages: IndexMap::new(),
            eviction_index: BTreeSet::new(),
            eviction_index_by_identity: BTreeMap::new(),
            cfg,
            rng,
            identity_usage,
        }
    }

    pub(crate) fn has_message(&self, key: &(I, u16)) -> bool {
        self.messages.contains_key(key)
    }

    pub(crate) fn message_status(&mut self, key: &(I, u16), now: Instant) -> MessageStatus {
        let Some(deadline) = self.messages.get(key).map(|state| state.eviction_deadline) else {
            return MessageStatus::Missing;
        };
        if deadline > now {
            return MessageStatus::Active;
        }
        let _ = self.remove_message(key);
        MessageStatus::Stale
    }

    pub(crate) fn ensure_identity_capacity(&self, identity: &I) -> Result<(), DecodeError> {
        self.identity_usage.ensure_identity_capacity(identity)
    }

    pub(crate) fn is_at_identity_limit(&self, identity: &I) -> bool {
        self.identity_usage.is_at_limit(identity)
    }

    pub(crate) fn evict_one_stale_for_identity_if_limited(
        &mut self,
        identity: &I,
        now: Instant,
    ) -> bool {
        if !self.is_at_identity_limit(identity) {
            return false;
        }
        self.evict_stale_for_identity(identity, now)
    }

    pub(crate) fn evict_before_admission(&mut self, now: Instant) -> Option<EvictionKind> {
        if self.messages.is_empty() {
            return None;
        }

        if self.evict_candidate_if_stale(self.eviction_index.first().cloned(), now) {
            return Some(EvictionKind::Timeout);
        }

        let random_index = self.rng.gen_range(0..self.messages.len());
        let random_key = self
            .messages
            .get_index(random_index)
            .map(|(key, _)| key.clone());
        random_key.map(|key| {
            let _ = self.remove_message(&key);
            EvictionKind::Random
        })
    }

    pub(crate) fn admission_blocked(&self) -> bool {
        self.messages.len() >= self.cfg.max_messages
    }

    pub(crate) fn insert_message(&mut self, key: (I, u16), now: Instant) {
        let deadline = now + self.cfg.message_timeout;
        self.messages
            .insert(key.clone(), MessageState::new(deadline));
        self.eviction_index.insert((deadline, key.clone()));
        self.eviction_index_by_identity
            .entry(key.0.clone())
            .or_default()
            .insert((deadline, key.1));
    }

    pub(crate) fn decode_with_admission(
        &mut self,
        now: Instant,
        key: &(I, u16),
        input: FragmentInput<I>,
    ) -> (Result<DecodeOutcome, DecodeError>, Option<EvictionKind>) {
        let mut evicted = None;

        if !self.has_message(key) {
            if self.admission_blocked() {
                evicted = self.evict_before_admission(now);
            }
            if self.admission_blocked() {
                return (Err(DecodeError::PoolFull), evicted);
            }
            self.insert_message(key.clone(), now);
            self.identity_usage
                .increment_identity_count(&input.identity);
        }

        (self.decode(input), evicted)
    }

    pub(crate) fn evict_stale_for_identity(&mut self, identity: &I, now: Instant) -> bool {
        let stale_candidate = self
            .eviction_index_by_identity
            .get(identity)
            .and_then(|identity_index| identity_index.first().copied())
            .map(|(deadline, msg_id)| (deadline, (identity.clone(), msg_id)));
        self.evict_candidate_if_stale(stale_candidate, now)
    }

    fn evict_candidate_if_stale(
        &mut self,
        stale_candidate: Option<(Instant, (I, u16))>,
        now: Instant,
    ) -> bool {
        let Some((deadline, stale_key)) = stale_candidate else {
            return false;
        };
        if deadline > now {
            return false;
        }
        let _ = self.remove_message(&stale_key);
        true
    }

    /// Decode a fragment into the pool.
    pub(crate) fn decode(&mut self, input: FragmentInput<I>) -> Result<DecodeOutcome, DecodeError> {
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
                    self.remove_message(&key);
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
                        self.remove_message(&key);
                    };
                    DecodeError::ConflictingEndMarker {
                        expected: existing_total_frags.unwrap_or_default(),
                        actual: new_total,
                    }
                );
                state.total_frags = Some(new_total);
            }
            if let Some(total_frags) = state.total_frags {
                ensure!(
                    seq_num < total_frags,
                    || {
                        self.remove_message(&key);
                    };
                    DecodeError::ConflictingEndMarker {
                        expected: total_frags,
                        actual: seq_num + 1,
                    }
                );
            }

            let total_size = state.total_size.checked_add(fragment_size);
            ensure!(
                matches!(total_size, Some(size) if size <= self.cfg.max_message_size),
                || {
                    self.remove_message(&key);
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
            .remove_message(&key)
            .expect("message must exist when extracting");
        Ok(DecodeOutcome::Complete(state.extract()))
    }

    fn remove_message(&mut self, key: &(I, u16)) -> Option<MessageState> {
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
        self.identity_usage.decrement_identity_count(&key.0);
        Some(state)
    }

    pub(crate) fn message_count(&self) -> usize {
        self.messages.len()
    }
}

#[cfg(test)]
mod tests {
    use rand::{rngs::StdRng, SeedableRng};

    use super::*;

    #[test]
    fn completing_last_message_clears_identity_tracking() {
        let config = Config::default();
        let mut pool = MessagePool::new(
            PoolConfig::from_config(&config, config.max_regular_messages),
            StdRng::seed_from_u64(1),
            IdentityUsage::new(&config),
        );
        let identity = 7u64;
        let msg_id = 11u16;

        let (result, evicted) = pool.decode_with_admission(
            Instant::now(),
            &(identity, msg_id),
            FragmentInput {
                identity,
                msg_id,
                seq_num: 0,
                fragment_type: FragmentType::Complete,
                payload: Bytes::from_static(b"done"),
            },
        );

        assert_eq!(
            result,
            Ok(DecodeOutcome::Complete(Bytes::from_static(b"done")))
        );
        assert_eq!(evicted, None);
        assert_eq!(pool.identity_usage.identity_count(&identity), 0);
        assert!(pool.identity_usage.message_counts.is_empty());
    }
}
