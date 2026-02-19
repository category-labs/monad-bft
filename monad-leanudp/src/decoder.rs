use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    convert::Infallible,
    hash::Hash,
    ops::Deref,
    time::{Duration, Instant},
};

use bytes::{Bytes, BytesMut};
use indexmap::IndexMap;
use monad_executor::ExecutorMetrics;
use rand::{rngs::StdRng, CryptoRng, Rng as _, RngCore, SeedableRng};
use thiserror::Error;
use zerocopy::FromBytes;

use crate::{
    encoder::MAX_FRAGMENTS, metrics::*, Config, FragmentPolicy, FragmentType, IdentityScore,
    PacketHeader, LEANUDP_HEADER_SIZE, LEANUDP_PROTOCOL_VERSION,
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

pub trait Clock: Clone {
    fn now(&self) -> Instant;
}

#[derive(Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodeOutcome {
    Pending,
    Complete(Bytes),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum DecodeError {
    #[error("packet too short: {actual} bytes, need {required}")]
    InvalidHeaderSize { actual: usize, required: usize },

    #[error("invalid header")]
    InvalidHeader,

    #[error("unsupported protocol version {version}, expected {expected}")]
    UnsupportedVersion { version: u8, expected: u8 },

    #[error("identity at message limit ({max})")]
    IdentityLimitExceeded { max: usize },

    #[error("duplicate fragment msg_id={msg_id} seq={seq_num}")]
    DuplicateFragment { msg_id: u32, seq_num: u16 },

    #[error("too many fragments: {count} exceeds max {max}")]
    TooManyFragments { count: usize, max: usize },

    #[error("conflicting END marker: expected {expected} fragments, got {actual}")]
    ConflictingEndMarker { expected: u16, actual: u16 },

    #[error("message size {size} exceeds max {max}")]
    MessageSizeExceeded { size: usize, max: usize },

    #[error("pool full")]
    PoolFull,
}

impl DecodeError {
    fn metric_name(&self) -> &'static str {
        match self {
            DecodeError::InvalidHeaderSize { .. } | DecodeError::InvalidHeader => {
                COUNTER_LEANUDP_ERROR_INVALID_HEADER
            }
            DecodeError::UnsupportedVersion { .. } => COUNTER_LEANUDP_ERROR_UNSUPPORTED_VERSION,
            DecodeError::IdentityLimitExceeded { .. } => COUNTER_LEANUDP_ERROR_IDENTITY_LIMIT,
            DecodeError::DuplicateFragment { .. } => COUNTER_LEANUDP_ERROR_DUPLICATE_FRAGMENT,
            DecodeError::TooManyFragments { .. } => COUNTER_LEANUDP_ERROR_TOO_MANY_FRAGMENTS,
            DecodeError::ConflictingEndMarker { .. } => COUNTER_LEANUDP_ERROR_CONFLICTING_END,
            DecodeError::MessageSizeExceeded { .. } => COUNTER_LEANUDP_ERROR_MESSAGE_TOO_LARGE,
            DecodeError::PoolFull => COUNTER_LEANUDP_ERROR_POOL_FULL,
        }
    }
}

impl From<Infallible> for DecodeError {
    fn from(never: Infallible) -> Self {
        match never {}
    }
}

#[derive(Debug, Clone)]
pub struct Packet {
    pub header: PacketHeader,
    pub payload: Bytes,
}

struct FragmentInput<I> {
    identity: I,
    msg_id: u32,
    seq_num: u16,
    fragment_type: FragmentType,
    payload: Bytes,
}

impl TryFrom<Bytes> for Packet {
    type Error = DecodeError;

    fn try_from(packet: Bytes) -> Result<Self, Self::Error> {
        ensure!(
            packet.len() >= LEANUDP_HEADER_SIZE,
            DecodeError::InvalidHeaderSize {
                actual: packet.len(),
                required: LEANUDP_HEADER_SIZE,
            }
        );

        let header = PacketHeader::read_from_bytes(&packet[..LEANUDP_HEADER_SIZE])
            .map_err(|_| DecodeError::InvalidHeader)?;
        let payload = packet.slice(LEANUDP_HEADER_SIZE..);

        Ok(Self { header, payload })
    }
}

impl From<(PacketHeader, Bytes)> for Packet {
    fn from((header, payload): (PacketHeader, Bytes)) -> Self {
        Self { header, payload }
    }
}

struct MessageState {
    fragments: BTreeMap<u16, Bytes>,
    total_size: usize,
    total_frags: Option<u16>,
    eviction_deadline: Instant,
    eviction_nonce: u64,
}

impl MessageState {
    fn new(eviction_deadline: Instant, eviction_nonce: u64) -> Self {
        Self {
            fragments: BTreeMap::new(),
            total_size: 0,
            total_frags: None,
            eviction_deadline,
            eviction_nonce,
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

struct PoolConfig {
    max_messages: usize,
    max_message_size: usize,
    message_timeout: Duration,
}

impl PoolConfig {
    fn from_config(config: &Config, max_messages: usize) -> Self {
        Self {
            max_messages,
            max_message_size: config.max_message_size,
            message_timeout: config.message_timeout,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EvictionKind {
    Timeout,
    Random,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct EvictionCounts {
    timeout: u64,
    random: u64,
}

impl EvictionCounts {
    fn add(&mut self, kind: EvictionKind) {
        match kind {
            EvictionKind::Timeout => self.timeout += 1,
            EvictionKind::Random => self.random += 1,
        }
    }

    fn merge(&mut self, other: Self) {
        self.timeout += other.timeout;
        self.random += other.random;
    }
}

impl EvictionKind {
    fn metric_name(self) -> &'static str {
        match self {
            EvictionKind::Timeout => COUNTER_LEANUDP_DECODE_EVICTED_TIMEOUT,
            EvictionKind::Random => COUNTER_LEANUDP_DECODE_EVICTED_RANDOM,
        }
    }
}

struct IdentityUsage<I>
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
    fn from_config(config: &Config) -> Self {
        Self {
            message_counts: HashMap::new(),
            max_messages_per_identity: config.max_messages_per_identity,
        }
    }
}

struct MessagePool<I, R>
where
    I: Eq + Hash + Clone + Ord,
    R: CryptoRng + RngCore,
{
    messages: IndexMap<(I, u32), MessageState>,
    // Min-by-deadline eviction index.
    eviction_index: BTreeSet<(Instant, u64, (I, u32))>,
    // Per-identity min-by-deadline eviction index.
    eviction_index_by_identity: BTreeMap<I, BTreeSet<(Instant, u64, u32)>>,
    next_nonce: u64,
    cfg: PoolConfig,
    rng: R,
}

impl<I: Eq + Hash + Clone + Ord, R: CryptoRng + RngCore> MessagePool<I, R> {
    fn new(cfg: PoolConfig, rng: R) -> Self {
        Self {
            messages: IndexMap::new(),
            eviction_index: BTreeSet::new(),
            eviction_index_by_identity: BTreeMap::new(),
            next_nonce: 0,
            cfg,
            rng,
        }
    }

    fn has_message(&self, key: &(I, u32)) -> bool {
        self.messages.contains_key(key)
    }

    fn evict_before_admission(
        &mut self,
        usage: &mut IdentityUsage<I>,
        now: Instant,
    ) -> Option<EvictionKind> {
        if self.messages.is_empty() {
            return None;
        }

        if let Some(stale_key) = self.find_stale_key(now) {
            let _ = self.remove_message(&stale_key, usage);
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

    fn admission_blocked(&self) -> bool {
        self.messages.len() >= self.cfg.max_messages
    }

    fn insert_message(&mut self, key: (I, u32), now: Instant) {
        let deadline = now + self.cfg.message_timeout;
        let nonce = self.next_nonce;
        self.next_nonce = self.next_nonce.wrapping_add(1);
        self.messages
            .insert(key.clone(), MessageState::new(deadline, nonce));
        self.insert_eviction_index_entry(&key, deadline, nonce);
    }

    fn find_stale_key(&self, now: Instant) -> Option<(I, u32)> {
        let (deadline, _nonce, key) = self.eviction_index.first()?.clone();
        if deadline <= now {
            Some(key)
        } else {
            None
        }
    }

    fn evict_stale_for_identity(
        &mut self,
        usage: &mut IdentityUsage<I>,
        identity: &I,
        now: Instant,
    ) -> u64 {
        let Some(identity_index) = self.eviction_index_by_identity.get(identity) else {
            return 0;
        };
        let Some((deadline, _nonce, msg_id)) = identity_index.first().copied() else {
            return 0;
        };
        if deadline > now {
            return 0;
        }
        let stale_key = (identity.clone(), msg_id);
        let _ = self.remove_message(&stale_key, usage);
        1
    }

    fn insert_eviction_index_entry(&mut self, key: &(I, u32), deadline: Instant, nonce: u64) {
        self.eviction_index.insert((deadline, nonce, key.clone()));
        self.eviction_index_by_identity
            .entry(key.0.clone())
            .or_default()
            .insert((deadline, nonce, key.1));
    }

    fn remove_eviction_index_entry(&mut self, key: &(I, u32), deadline: Instant, nonce: u64) {
        let _ = self.eviction_index.remove(&(deadline, nonce, key.clone()));
        let mut should_remove_identity = false;
        if let Some(identity_index) = self.eviction_index_by_identity.get_mut(&key.0) {
            let _ = identity_index.remove(&(deadline, nonce, key.1));
            if identity_index.is_empty() {
                should_remove_identity = true;
            }
        }
        if should_remove_identity {
            self.eviction_index_by_identity.remove(&key.0);
        }
    }

    /// Decode a fragment into the pool.
    fn decode(
        &mut self,
        usage: &mut IdentityUsage<I>,
        now: Instant,
        input: FragmentInput<I>,
    ) -> Result<DecodeOutcome, DecodeError> {
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
            let new_deadline = now + self.cfg.message_timeout;
            let new_nonce = self.next_nonce;
            self.next_nonce = self.next_nonce.wrapping_add(1);
            let (old_deadline, old_nonce) = {
                let state = self
                    .messages
                    .get_mut(&key)
                    .expect("message was just inserted or confirmed to exist");
                let old_deadline = state.eviction_deadline;
                let old_nonce = state.eviction_nonce;
                state.eviction_deadline = new_deadline;
                state.eviction_nonce = new_nonce;
                (old_deadline, old_nonce)
            };
            self.remove_eviction_index_entry(&key, old_deadline, old_nonce);
            self.insert_eviction_index_entry(&key, new_deadline, new_nonce);

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

            let total_size = state.total_size + fragment_size;
            ensure!(
                total_size <= self.cfg.max_message_size,
                || {
                    self.remove_message(&key, usage);
                };
                DecodeError::MessageSizeExceeded {
                    size: total_size,
                    max: self.cfg.max_message_size,
                }
            );
            state.total_size = total_size;
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

    fn remove_message(
        &mut self,
        key: &(I, u32),
        usage: &mut IdentityUsage<I>,
    ) -> Option<MessageState> {
        let state = self.messages.swap_remove(key)?;
        self.remove_eviction_index_entry(key, state.eviction_deadline, state.eviction_nonce);
        let identity = &key.0;
        let count = usage
            .message_counts
            .get_mut(identity)
            .expect("identity count must exist for active message");
        *count -= 1;
        if *count == 0 {
            usage.message_counts.remove(identity);
        }
        Some(state)
    }

    fn message_count(&self) -> usize {
        self.messages.len()
    }
}

pub struct DecoderMetrics<'a>(&'a ExecutorMetrics);

impl AsRef<ExecutorMetrics> for DecoderMetrics<'_> {
    fn as_ref(&self) -> &ExecutorMetrics {
        self.0
    }
}

impl Deref for DecoderMetrics<'_> {
    type Target = ExecutorMetrics;

    fn deref(&self) -> &Self::Target {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PoolSelection {
    Priority,
    Regular,
}

impl From<FragmentPolicy> for PoolSelection {
    fn from(policy: FragmentPolicy) -> Self {
        match policy {
            FragmentPolicy::Prioritized => Self::Priority,
            FragmentPolicy::Regular => Self::Regular,
        }
    }
}

impl PoolSelection {
    fn fragments_counter(self) -> &'static str {
        match self {
            PoolSelection::Priority => COUNTER_LEANUDP_DECODE_FRAGMENTS_PRIORITY,
            PoolSelection::Regular => COUNTER_LEANUDP_DECODE_FRAGMENTS_REGULAR,
        }
    }

    fn bytes_counter(self) -> &'static str {
        match self {
            PoolSelection::Priority => COUNTER_LEANUDP_DECODE_BYTES_PRIORITY,
            PoolSelection::Regular => COUNTER_LEANUDP_DECODE_BYTES_REGULAR,
        }
    }
}

pub struct Decoder<I, P, C, R = StdRng>
where
    I: Eq + Hash + Clone + Ord,
    P: IdentityScore<Identity = I>,
    C: Clock,
    R: CryptoRng + RngCore + Clone,
{
    priority_pool: MessagePool<I, R>,
    regular_pool: MessagePool<I, R>,
    identity_usage: IdentityUsage<I>,
    identity_score: P,
    clock: C,
    metrics: ExecutorMetrics,
}

impl<I, P, C> Decoder<I, P, C, StdRng>
where
    I: Eq + Hash + Clone + Ord,
    P: IdentityScore<Identity = I>,
    C: Clock,
{
    pub(crate) fn with_clock(config: &Config, identity_score: P, clock: C) -> Self {
        Self::with_clock_and_crypto_rng(config, identity_score, clock, StdRng::from_entropy())
    }
}

impl<I, P, C, R> Decoder<I, P, C, R>
where
    I: Eq + Hash + Clone + Ord,
    P: IdentityScore<Identity = I>,
    C: Clock,
    R: CryptoRng + RngCore + Clone,
{
    pub(crate) fn with_clock_and_crypto_rng(
        config: &Config,
        identity_score: P,
        clock: C,
        regular_pool_rng: R,
    ) -> Self {
        Self {
            priority_pool: MessagePool::new(
                PoolConfig::from_config(config, config.max_priority_messages),
                regular_pool_rng.clone(),
            ),
            regular_pool: MessagePool::new(
                PoolConfig::from_config(config, config.max_regular_messages),
                regular_pool_rng,
            ),
            identity_usage: IdentityUsage::from_config(config),
            identity_score,
            clock,
            metrics: ExecutorMetrics::default(),
        }
    }

    fn identity_is_at_limit(&self, identity: &I) -> bool {
        let identity_count = self
            .identity_usage
            .message_counts
            .get(identity)
            .copied()
            .unwrap_or_default();
        identity_count >= self.identity_usage.max_messages_per_identity
    }

    fn decode_in_specific_pool(
        pool: &mut MessagePool<I, R>,
        identity_usage: &mut IdentityUsage<I>,
        now: Instant,
        key: &(I, u32),
        input: FragmentInput<I>,
    ) -> (Result<DecodeOutcome, DecodeError>, EvictionCounts) {
        let mut evicted = EvictionCounts::default();

        if !pool.has_message(key) {
            while pool.admission_blocked() {
                let Some(kind) = pool.evict_before_admission(identity_usage, now) else {
                    break;
                };
                evicted.add(kind);
            }
            if pool.admission_blocked() {
                return (Err(DecodeError::PoolFull), evicted);
            }
            pool.insert_message(key.clone(), now);
            *identity_usage
                .message_counts
                .entry(input.identity.clone())
                .or_insert(0) += 1;
        }

        let result = pool.decode(identity_usage, now, input);
        (result, evicted)
    }

    fn select_pool(&self, identity: &I, key: &(I, u32)) -> PoolSelection {
        if self.priority_pool.has_message(key) {
            PoolSelection::Priority
        } else if self.regular_pool.has_message(key) {
            PoolSelection::Regular
        } else {
            self.identity_score.score(identity).into()
        }
    }

    fn evict_stale_for_identity_until_recovered(&mut self, identity: &I, now: Instant) -> u64 {
        let mut evicted_timeout = 0;
        while self.identity_is_at_limit(identity) {
            let evicted_from_priority = self.priority_pool.evict_stale_for_identity(
                &mut self.identity_usage,
                identity,
                now,
            );
            let evicted_from_regular =
                self.regular_pool
                    .evict_stale_for_identity(&mut self.identity_usage, identity, now);
            let evicted_now = evicted_from_priority + evicted_from_regular;
            if evicted_now == 0 {
                break;
            }
            evicted_timeout += evicted_now;
        }
        evicted_timeout
    }

    fn decode_in_pool(
        &mut self,
        now: Instant,
        selected_pool: PoolSelection,
        key: &(I, u32),
        input: FragmentInput<I>,
    ) -> (Result<DecodeOutcome, DecodeError>, EvictionCounts) {
        let mut evicted = EvictionCounts::default();
        let is_new_message = match selected_pool {
            PoolSelection::Priority => !self.priority_pool.has_message(key),
            PoolSelection::Regular => !self.regular_pool.has_message(key),
        };

        if is_new_message {
            evicted.timeout += self.evict_stale_for_identity_until_recovered(&input.identity, now);
            if self.identity_is_at_limit(&input.identity) {
                return (
                    Err(DecodeError::IdentityLimitExceeded {
                        max: self.identity_usage.max_messages_per_identity,
                    }),
                    evicted,
                );
            }
        }

        let (result, pool_evicted) = match selected_pool {
            PoolSelection::Priority => Self::decode_in_specific_pool(
                &mut self.priority_pool,
                &mut self.identity_usage,
                now,
                key,
                input,
            ),
            PoolSelection::Regular => Self::decode_in_specific_pool(
                &mut self.regular_pool,
                &mut self.identity_usage,
                now,
                key,
                input,
            ),
        };
        evicted.merge(pool_evicted);
        (result, evicted)
    }

    fn record_decode_metrics(
        &mut self,
        selected_pool: PoolSelection,
        payload_bytes: u64,
        evicted: EvictionCounts,
        result: &Result<DecodeOutcome, DecodeError>,
    ) {
        self.metrics[selected_pool.fragments_counter()] += 1;
        if payload_bytes > 0 {
            self.metrics[selected_pool.bytes_counter()] += payload_bytes;
        }
        if evicted.timeout > 0 {
            self.metrics[EvictionKind::Timeout.metric_name()] += evicted.timeout;
        }
        if evicted.random > 0 {
            self.metrics[EvictionKind::Random.metric_name()] += evicted.random;
        }

        match result {
            Ok(DecodeOutcome::Complete(msg)) => {
                self.metrics[COUNTER_LEANUDP_DECODE_MESSAGES_COMPLETED] += 1;
                self.metrics[COUNTER_LEANUDP_DECODE_BYTES_COMPLETED] += msg.len() as u64;
            }
            Ok(DecodeOutcome::Pending) => {}
            Err(e) => {
                self.metrics[e.metric_name()] += 1;
            }
        }
    }

    fn update_pool_gauges(&mut self) {
        self.metrics[GAUGE_LEANUDP_POOL_PRIORITY_MESSAGES] =
            self.priority_pool.message_count() as u64;
        self.metrics[GAUGE_LEANUDP_POOL_REGULAR_MESSAGES] =
            self.regular_pool.message_count() as u64;
    }

    pub fn decode<T>(&mut self, identity: I, packet: T) -> Result<DecodeOutcome, DecodeError>
    where
        T: TryInto<Packet>,
        T::Error: Into<DecodeError>,
    {
        self.metrics[COUNTER_LEANUDP_DECODE_FRAGMENTS_RECEIVED] += 1;

        let Packet { header, payload } = packet.try_into().map_err(Into::into)?;
        let payload_bytes = payload.len() as u64;
        if payload_bytes > 0 {
            self.metrics[COUNTER_LEANUDP_DECODE_BYTES_RECEIVED] += payload_bytes;
        }
        ensure!(
            header.version() == LEANUDP_PROTOCOL_VERSION,
            DecodeError::UnsupportedVersion {
                version: header.version(),
                expected: LEANUDP_PROTOCOL_VERSION,
            }
        );

        let msg_id = header.msg_id();
        let key = (identity.clone(), msg_id);
        let selected_pool = self.select_pool(&identity, &key);
        let now = self.clock.now();
        let input = FragmentInput {
            identity,
            msg_id,
            seq_num: header.seq_num(),
            fragment_type: header.fragment_type(),
            payload,
        };

        let (result, evicted) = self.decode_in_pool(now, selected_pool, &key, input);
        self.record_decode_metrics(selected_pool, payload_bytes, evicted, &result);
        self.update_pool_gauges();

        result
    }

    pub fn executor_metrics(&self) -> &ExecutorMetrics {
        &self.metrics
    }

    pub fn metrics(&self) -> DecoderMetrics<'_> {
        DecoderMetrics(&self.metrics)
    }
}
