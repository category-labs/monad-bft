use std::{
    collections::{BTreeMap, HashMap},
    convert::Infallible,
    hash::Hash,
    ops::Deref,
    time::Instant,
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

    #[error("identity in-flight bytes {size} exceed max {max}")]
    IdentityBytesLimitExceeded { size: usize, max: usize },

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
            DecodeError::IdentityLimitExceeded { .. }
            | DecodeError::IdentityBytesLimitExceeded { .. } => {
                COUNTER_LEANUDP_ERROR_IDENTITY_LIMIT
            }
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
}

impl MessageState {
    fn new() -> Self {
        Self {
            fragments: BTreeMap::new(),
            total_size: 0,
            total_frags: None,
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
}

impl PoolConfig {
    fn from_config(config: &Config, max_messages: usize) -> Self {
        Self {
            max_messages,
            max_message_size: config.max_message_size,
        }
    }
}

struct IdentityUsage<I>
where
    I: Eq + Hash + Clone + Ord,
{
    message_counts: HashMap<I, usize>,
    message_bytes: HashMap<I, usize>,
    max_messages_per_identity: usize,
    max_bytes_per_identity: usize,
}

impl<I> IdentityUsage<I>
where
    I: Eq + Hash + Clone + Ord,
{
    fn from_config(config: &Config) -> Self {
        Self {
            message_counts: HashMap::new(),
            message_bytes: HashMap::new(),
            max_messages_per_identity: config.max_messages_per_identity,
            max_bytes_per_identity: config.max_bytes_per_identity,
        }
    }
}

struct MessagePool<I, R>
where
    I: Eq + Hash + Clone + Ord,
    R: CryptoRng + RngCore,
{
    messages: IndexMap<(I, u32), MessageState>,
    cfg: PoolConfig,
    rng: R,
}

impl<I: Eq + Hash + Clone + Ord, R: CryptoRng + RngCore> MessagePool<I, R> {
    fn new(cfg: PoolConfig, rng: R) -> Self {
        Self {
            messages: IndexMap::new(),
            cfg,
            rng,
        }
    }

    fn has_message(&self, key: &(I, u32)) -> bool {
        self.messages.contains_key(key)
    }

    fn evict_before_admission(&mut self, usage: &mut IdentityUsage<I>) -> Option<(I, u32)> {
        let key = if self.messages.len() >= self.cfg.max_messages && !self.messages.is_empty() {
            let random_index = self.rng.gen_range(0..self.messages.len());
            self.messages
                .get_index(random_index)
                .map(|(key, _)| key.clone())
        } else {
            None
        };
        key.inspect(|k| {
            let _ = self.remove_message(k, usage);
        })
    }

    /// Decode a fragment into the pool.
    fn decode(
        &mut self,
        usage: &mut IdentityUsage<I>,
        identity: I,
        msg_id: u32,
        seq_num: u16,
        fragment_type: FragmentType,
        data: Bytes,
    ) -> Result<DecodeOutcome, DecodeError> {
        let key = (identity.clone(), msg_id);
        let fragment_size = data.len();

        if !self.messages.contains_key(&key) {
            let identity_count = usage
                .message_counts
                .get(&identity)
                .copied()
                .unwrap_or_default();
            ensure!(
                identity_count < usage.max_messages_per_identity,
                DecodeError::IdentityLimitExceeded {
                    max: usage.max_messages_per_identity,
                }
            );
            ensure!(
                self.messages.len() < self.cfg.max_messages,
                DecodeError::PoolFull
            );
            self.messages.insert(key.clone(), MessageState::new());
            *usage.message_counts.entry(identity).or_insert(0) += 1;
        }

        let identity_bytes = usage.message_bytes.get(&key.0).copied().unwrap_or_default();

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
            let identity_total_size = identity_bytes + fragment_size;
            ensure!(
                identity_total_size <= usage.max_bytes_per_identity,
                || {
                    self.remove_message(&key, usage);
                };
                DecodeError::IdentityBytesLimitExceeded {
                    size: identity_total_size,
                    max: usage.max_bytes_per_identity,
                }
            );
            state.total_size = total_size;
            state.fragments.insert(seq_num, data);

            state.total_frags == Some(state.fragments.len() as u16)
        };
        *usage.message_bytes.entry(key.0.clone()).or_insert(0) += fragment_size;

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
        let identity = &key.0;
        let count = usage
            .message_counts
            .get_mut(identity)
            .expect("identity count must exist for active message");
        *count -= 1;
        if *count == 0 {
            usage.message_counts.remove(identity);
        }
        if let Some(bytes) = usage.message_bytes.get_mut(identity) {
            *bytes = bytes.saturating_sub(state.total_size);
            if *bytes == 0 {
                usage.message_bytes.remove(identity);
            }
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

    fn evicted_counter(self) -> &'static str {
        match self {
            PoolSelection::Priority => COUNTER_LEANUDP_DECODE_EVICTED_RANDOM,
            PoolSelection::Regular => COUNTER_LEANUDP_DECODE_EVICTED_RANDOM,
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
    _clock: C,
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
            _clock: clock,
            metrics: ExecutorMetrics::default(),
        }
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

    fn decode_in_pool(
        &mut self,
        selected_pool: PoolSelection,
        key: &(I, u32),
        identity: I,
        msg_id: u32,
        seq_num: u16,
        fragment_type: FragmentType,
        payload: Bytes,
    ) -> (Result<DecodeOutcome, DecodeError>, bool) {
        match selected_pool {
            PoolSelection::Priority => {
                let evicted = if self.priority_pool.has_message(key) {
                    false
                } else {
                    self.priority_pool
                        .evict_before_admission(&mut self.identity_usage)
                        .is_some()
                };
                let result = self.priority_pool.decode(
                    &mut self.identity_usage,
                    identity,
                    msg_id,
                    seq_num,
                    fragment_type,
                    payload,
                );
                (result, evicted)
            }
            PoolSelection::Regular => {
                let evicted = if self.regular_pool.has_message(key) {
                    false
                } else {
                    self.regular_pool
                        .evict_before_admission(&mut self.identity_usage)
                        .is_some()
                };
                let result = self.regular_pool.decode(
                    &mut self.identity_usage,
                    identity,
                    msg_id,
                    seq_num,
                    fragment_type,
                    payload,
                );
                (result, evicted)
            }
        }
    }

    fn record_decode_metrics(
        &mut self,
        selected_pool: PoolSelection,
        payload_bytes: u64,
        evicted: bool,
        result: &Result<DecodeOutcome, DecodeError>,
    ) {
        self.metrics[selected_pool.fragments_counter()] += 1;
        self.metrics[selected_pool.bytes_counter()] += payload_bytes;
        if evicted {
            self.metrics[selected_pool.evicted_counter()] += 1;
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
        self.metrics[COUNTER_LEANUDP_DECODE_BYTES_RECEIVED] += payload_bytes;
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

        let (result, evicted) = self.decode_in_pool(
            selected_pool,
            &key,
            identity,
            msg_id,
            header.seq_num(),
            header.fragment_type(),
            payload,
        );
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

#[cfg(test)]
mod tests {
    use std::{cell::Cell, time::Duration};

    use bytes::BufMut;
    use zerocopy::IntoBytes;

    use super::*;
    use crate::{encoder::MAX_FRAGMENTS, FragmentType, MAX_CONCURRENT_BYTES_PER_IDENTITY};

    #[derive(Clone)]
    struct MockClock(std::rc::Rc<Cell<Instant>>);

    impl MockClock {
        fn new() -> Self {
            Self(std::rc::Rc::new(Cell::new(Instant::now())))
        }
    }

    impl Clock for MockClock {
        fn now(&self) -> Instant {
            self.0.get()
        }
    }

    struct TestIdentityScore(Vec<u64>);

    impl IdentityScore for TestIdentityScore {
        type Identity = u64;
        fn score(&self, id: &u64) -> FragmentPolicy {
            if self.0.contains(id) {
                FragmentPolicy::Prioritized
            } else {
                FragmentPolicy::Regular
            }
        }
    }

    struct TestDecoder {
        inner: Decoder<u64, TestIdentityScore, MockClock>,
        clock: MockClock,
    }

    impl TestDecoder {
        fn new() -> Self {
            Self::with_config(Config {
                max_message_size: 128 * 1024,
                max_priority_messages: 10,
                max_regular_messages: 5,
                max_messages_per_identity: 10,
                max_bytes_per_identity: MAX_CONCURRENT_BYTES_PER_IDENTITY,
                message_timeout: Duration::from_millis(400),
                max_fragment_payload: 1440,
            })
        }

        fn with_config(config: Config) -> Self {
            Self::with_config_and_priority(config, &[])
        }

        fn priority(ids: &[u64]) -> Self {
            Self::with_config_and_priority(
                Config {
                    max_message_size: 128 * 1024,
                    max_priority_messages: 10,
                    max_regular_messages: 5,
                    max_messages_per_identity: 10,
                    max_bytes_per_identity: MAX_CONCURRENT_BYTES_PER_IDENTITY,
                    message_timeout: Duration::from_millis(400),
                    max_fragment_payload: 1440,
                },
                ids,
            )
        }

        fn with_config_and_priority(config: Config, priority_ids: &[u64]) -> Self {
            let clock = MockClock::new();
            let inner = Decoder::with_clock(
                &config,
                TestIdentityScore(priority_ids.to_vec()),
                clock.clone(),
            );
            Self { inner, clock }
        }

        fn decode(&mut self, id: u64, packet: Bytes) -> Result<DecodeOutcome, DecodeError> {
            self.inner.decode(id, packet)
        }

        fn priority_messages(&self) -> u64 {
            self.inner.metrics()[GAUGE_LEANUDP_POOL_PRIORITY_MESSAGES]
        }

        fn regular_messages(&self) -> u64 {
            self.inner.metrics()[GAUGE_LEANUDP_POOL_REGULAR_MESSAGES]
        }

        fn advance_ms(&self, ms: u64) {
            self.clock
                .0
                .set(self.clock.0.get() + Duration::from_millis(ms));
        }
    }

    fn pkt(msg_id: u32, seq: u16, frag_type: FragmentType, data: &[u8]) -> Bytes {
        let header = PacketHeader::new(msg_id, seq, frag_type);
        let mut buf = bytes::BytesMut::with_capacity(LEANUDP_HEADER_SIZE + data.len());
        buf.put_slice(header.as_bytes());
        buf.put_slice(data);
        buf.freeze()
    }

    use FragmentType::{Complete, End, Middle, Start};

    #[test]
    fn test_decode_single_message() {
        let mut d = TestDecoder::priority(&[1000]);
        assert_eq!(
            d.decode(1000, pkt(0, 0, Complete, b"hello")),
            Ok(DecodeOutcome::Complete(Bytes::from_static(b"hello")))
        );
        assert_eq!(d.priority_messages(), 0);
    }

    #[test]
    fn test_decode_priority_vs_regular() {
        let mut d = TestDecoder::priority(&[1000, 1001]);
        let _ = d.decode(1000, pkt(0, 0, Start, b"p1"));
        let _ = d.decode(2000, pkt(0, 0, Start, b"r1"));
        let _ = d.decode(1001, pkt(0, 0, Start, b"p2"));
        let _ = d.decode(2001, pkt(0, 0, Start, b"r2"));
        assert_eq!(d.priority_messages(), 2);
        assert_eq!(d.regular_messages(), 2);
    }

    #[test]
    fn test_decode_records_bytes_by_pool() {
        let mut d = TestDecoder::priority(&[1000]);

        let _ = d.decode(1000, pkt(0, 0, Complete, b"hello"));
        let _ = d.decode(2000, pkt(0, 0, Complete, b"world!"));

        assert_eq!(
            d.inner.metrics()[COUNTER_LEANUDP_DECODE_FRAGMENTS_RECEIVED],
            2
        );
        assert_eq!(d.inner.metrics()[COUNTER_LEANUDP_DECODE_BYTES_RECEIVED], 11);
        assert_eq!(
            d.inner.metrics()[COUNTER_LEANUDP_DECODE_BYTES_COMPLETED],
            11
        );

        assert_eq!(d.inner.metrics()[COUNTER_LEANUDP_DECODE_BYTES_PRIORITY], 5);
        assert_eq!(d.inner.metrics()[COUNTER_LEANUDP_DECODE_BYTES_REGULAR], 6);
    }

    #[test]
    fn test_priority_pool_full_evicts_random() {
        let mut d = TestDecoder::with_config_and_priority(
            Config {
                max_message_size: 128 * 1024,
                max_priority_messages: 5,
                max_regular_messages: 5,
                max_messages_per_identity: 10,
                max_bytes_per_identity: MAX_CONCURRENT_BYTES_PER_IDENTITY,
                message_timeout: Duration::from_millis(400),
                max_fragment_payload: 1440,
            },
            &[1000, 1001, 1002, 1003, 1004, 1005],
        );
        for i in 0..5u64 {
            assert_eq!(
                d.decode(1000 + i, pkt(i as u32, 0, Start, b"x")),
                Ok(DecodeOutcome::Pending)
            );
        }
        assert_eq!(d.priority_messages(), 5);
        assert_eq!(
            d.decode(1005, pkt(100, 0, Start, b"x")),
            Ok(DecodeOutcome::Pending)
        );
        assert_eq!(d.priority_messages(), 5);
    }

    #[test]
    fn test_priority_pool_respects_zero_capacity() {
        let mut d = TestDecoder::with_config_and_priority(
            Config {
                max_message_size: 128 * 1024,
                max_priority_messages: 0,
                max_regular_messages: 5,
                max_messages_per_identity: 10,
                max_bytes_per_identity: MAX_CONCURRENT_BYTES_PER_IDENTITY,
                message_timeout: Duration::from_millis(100),
                max_fragment_payload: 1440,
            },
            &[1000],
        );
        assert_eq!(
            d.decode(1000, pkt(0, 0, Start, b"x")),
            Err(DecodeError::PoolFull)
        );
        assert_eq!(d.priority_messages(), 0);
    }

    #[test]
    fn test_existing_message_fragment_does_not_trigger_eviction() {
        let mut d = TestDecoder::with_config_and_priority(
            Config {
                max_priority_messages: 1,
                max_regular_messages: 5,
                ..Config::default()
            },
            &[1000],
        );
        assert_eq!(
            d.decode(1000, pkt(0, 0, Start, b"A")),
            Ok(DecodeOutcome::Pending)
        );
        assert_eq!(
            d.decode(1000, pkt(0, 1, End, b"B")),
            Ok(DecodeOutcome::Complete(Bytes::from_static(b"AB")))
        );
        assert_eq!(d.priority_messages(), 0);
    }

    #[test]
    fn test_regular_pool_full_evicts_random() {
        let mut d = TestDecoder::new();
        for i in 0..5 {
            assert_eq!(
                d.decode(2000 + i, pkt(i as u32, 0, Start, b"x")),
                Ok(DecodeOutcome::Pending)
            );
        }
        assert_eq!(d.regular_messages(), 5);
        assert_eq!(
            d.decode(3000, pkt(100, 0, Start, b"x")),
            Ok(DecodeOutcome::Pending)
        );
        assert_eq!(d.regular_messages(), 5);
    }

    #[test]
    fn test_regular_pool_full_no_evict_when_empty() {
        let mut d = TestDecoder::with_config(Config {
            max_regular_messages: 0,
            ..Config::default()
        });
        assert_eq!(
            d.decode(2000, pkt(0, 0, Start, b"x")),
            Err(DecodeError::PoolFull)
        );
        assert_eq!(d.regular_messages(), 0);
    }

    #[test]
    fn test_multi_fragment_reassembly() {
        let mut d = TestDecoder::priority(&[1000]);
        assert_eq!(
            d.decode(1000, pkt(0, 0, Start, b"hel")),
            Ok(DecodeOutcome::Pending)
        );
        assert_eq!(
            d.decode(1000, pkt(0, 1, Middle, b"lo ")),
            Ok(DecodeOutcome::Pending)
        );
        assert_eq!(
            d.decode(1000, pkt(0, 2, End, b"world")),
            Ok(DecodeOutcome::Complete(Bytes::from_static(b"hello world")))
        );
    }

    #[test]
    fn test_same_msg_id_different_senders() {
        let mut d = TestDecoder::new();
        let _ = d.decode(1000, pkt(0, 0, Start, b"A"));
        let _ = d.decode(2000, pkt(0, 0, Start, b"B"));
        assert_eq!(d.regular_messages(), 2);
        assert_eq!(
            d.decode(1000, pkt(0, 1, End, b"1")),
            Ok(DecodeOutcome::Complete(Bytes::from_static(b"A1")))
        );
        assert_eq!(
            d.decode(2000, pkt(0, 1, End, b"2")),
            Ok(DecodeOutcome::Complete(Bytes::from_static(b"B2")))
        );
    }

    #[test]
    fn test_out_of_order_fragments() {
        let mut d = TestDecoder::priority(&[1000]);
        let _ = d.decode(1000, pkt(0, 2, End, b"C"));
        let _ = d.decode(1000, pkt(0, 0, Start, b"A"));
        assert_eq!(
            d.decode(1000, pkt(0, 1, Middle, b"B")),
            Ok(DecodeOutcome::Complete(Bytes::from_static(b"ABC")))
        );
    }

    #[test]
    fn test_duplicate_fragment_returns_error() {
        let mut d = TestDecoder::priority(&[1000]);
        let _ = d.decode(1000, pkt(0, 0, Start, b"A"));
        assert_eq!(
            d.decode(1000, pkt(0, 0, Start, b"X")),
            Err(DecodeError::DuplicateFragment {
                msg_id: 0,
                seq_num: 0
            })
        );
        assert_eq!(
            d.decode(1000, pkt(0, 1, End, b"B")),
            Ok(DecodeOutcome::Complete(Bytes::from_static(b"AB")))
        );
    }

    #[test]
    fn test_too_many_fragments() {
        let mut d = TestDecoder::priority(&[1000]);
        for i in 0..MAX_FRAGMENTS as u16 {
            let frag_type = if i == 0 { Start } else { Middle };
            assert_eq!(
                d.decode(1000, pkt(0, i, frag_type, b"X")),
                Ok(DecodeOutcome::Pending)
            );
        }
        assert_eq!(
            d.decode(1000, pkt(0, MAX_FRAGMENTS as u16, End, b"Y")),
            Err(DecodeError::TooManyFragments {
                count: MAX_FRAGMENTS + 1,
                max: MAX_FRAGMENTS
            })
        );
        assert_eq!(d.priority_messages(), 0);
    }

    #[test]
    fn test_message_size_exceeded() {
        let mut d = TestDecoder::with_config(Config {
            max_message_size: 3,
            ..Config::default()
        });
        assert_eq!(
            d.decode(1000, pkt(0, 0, Start, b"ab")),
            Ok(DecodeOutcome::Pending)
        );
        assert_eq!(
            d.decode(1000, pkt(0, 1, End, b"cd")),
            Err(DecodeError::MessageSizeExceeded { size: 4, max: 3 })
        );
        assert_eq!(d.regular_messages(), 0);
    }

    #[test]
    fn test_seq_num_out_of_range() {
        let mut d = TestDecoder::priority(&[1000]);
        assert_eq!(
            d.decode(1000, pkt(0, MAX_FRAGMENTS as u16, Start, b"X")),
            Err(DecodeError::TooManyFragments {
                count: MAX_FRAGMENTS + 1,
                max: MAX_FRAGMENTS
            })
        );
        assert_eq!(d.priority_messages(), 0);
    }

    #[test]
    fn test_seq_num_u16_max_does_not_panic() {
        let mut d = TestDecoder::priority(&[1000]);
        assert_eq!(
            d.decode(1000, pkt(0, u16::MAX, End, b"X")),
            Err(DecodeError::TooManyFragments {
                count: MAX_FRAGMENTS + 1,
                max: MAX_FRAGMENTS
            })
        );
        assert_eq!(d.priority_messages(), 0);
    }

    #[test]
    fn test_score_change_keeps_existing_message_in_original_pool() {
        #[derive(Clone)]
        struct ToggleScore(std::rc::Rc<Cell<bool>>);

        impl IdentityScore for ToggleScore {
            type Identity = u64;

            fn score(&self, _id: &u64) -> FragmentPolicy {
                if self.0.get() {
                    FragmentPolicy::Prioritized
                } else {
                    FragmentPolicy::Regular
                }
            }
        }

        let priority = std::rc::Rc::new(Cell::new(true));
        let score = ToggleScore(priority.clone());
        let clock = MockClock::new();
        let mut d = Decoder::with_clock(&Config::default(), score, clock);
        assert_eq!(
            d.decode(1000, pkt(0, 0, Start, b"A")),
            Ok(DecodeOutcome::Pending)
        );
        priority.set(false);
        assert_eq!(
            d.decode(1000, pkt(0, 1, End, b"B")),
            Ok(DecodeOutcome::Complete(Bytes::from_static(b"AB")))
        );
        assert_eq!(d.metrics()[COUNTER_LEANUDP_DECODE_FRAGMENTS_PRIORITY], 2);
        assert_eq!(d.metrics()[COUNTER_LEANUDP_DECODE_FRAGMENTS_REGULAR], 0);
        assert_eq!(d.metrics()[GAUGE_LEANUDP_POOL_PRIORITY_MESSAGES], 0);
        assert_eq!(d.metrics()[GAUGE_LEANUDP_POOL_REGULAR_MESSAGES], 0);
    }

    #[test]
    fn test_per_identity_message_limit_is_combined_across_pools() {
        #[derive(Clone)]
        struct ToggleScore(std::rc::Rc<Cell<bool>>);

        impl IdentityScore for ToggleScore {
            type Identity = u64;

            fn score(&self, _id: &u64) -> FragmentPolicy {
                if self.0.get() {
                    FragmentPolicy::Prioritized
                } else {
                    FragmentPolicy::Regular
                }
            }
        }

        let priority = std::rc::Rc::new(Cell::new(true));
        let score = ToggleScore(priority.clone());
        let clock = MockClock::new();
        let mut d = Decoder::with_clock(
            &Config {
                max_messages_per_identity: 2,
                ..Config::default()
            },
            score,
            clock,
        );

        assert_eq!(
            d.decode(1000, pkt(0, 0, Start, b"A")),
            Ok(DecodeOutcome::Pending)
        );
        priority.set(false);
        assert_eq!(
            d.decode(1000, pkt(1, 0, Start, b"B")),
            Ok(DecodeOutcome::Pending)
        );
        assert_eq!(d.metrics()[GAUGE_LEANUDP_POOL_PRIORITY_MESSAGES], 1);
        assert_eq!(d.metrics()[GAUGE_LEANUDP_POOL_REGULAR_MESSAGES], 1);

        assert_eq!(
            d.decode(1000, pkt(2, 0, Start, b"C")),
            Err(DecodeError::IdentityLimitExceeded { max: 2 })
        );
    }

    #[test]
    fn test_per_identity_bytes_limit_is_combined_across_pools() {
        #[derive(Clone)]
        struct ToggleScore(std::rc::Rc<Cell<bool>>);

        impl IdentityScore for ToggleScore {
            type Identity = u64;

            fn score(&self, _id: &u64) -> FragmentPolicy {
                if self.0.get() {
                    FragmentPolicy::Prioritized
                } else {
                    FragmentPolicy::Regular
                }
            }
        }

        let priority = std::rc::Rc::new(Cell::new(true));
        let score = ToggleScore(priority.clone());
        let clock = MockClock::new();
        let mut d = Decoder::with_clock(
            &Config {
                max_bytes_per_identity: 10,
                ..Config::default()
            },
            score,
            clock,
        );

        assert_eq!(
            d.decode(1000, pkt(0, 0, Start, b"AAAAAA")),
            Ok(DecodeOutcome::Pending)
        );
        priority.set(false);
        assert_eq!(
            d.decode(1000, pkt(1, 0, Start, b"BBBB")),
            Ok(DecodeOutcome::Pending)
        );
        assert_eq!(
            d.decode(1000, pkt(2, 0, Start, b"X")),
            Err(DecodeError::IdentityBytesLimitExceeded { size: 11, max: 10 })
        );
    }

    #[test]
    fn test_decoder_metrics_accessor() {
        let mut d = TestDecoder::new();
        let _ = d.decode(1000, pkt(0, 0, Complete, b"ok"));
        let _ = d.inner.metrics();
    }

    #[test]
    fn test_system_clock_decoder_path() {
        let (_encoder, mut decoder) = Config::default().build(TestIdentityScore(vec![]));
        assert_eq!(
            decoder.decode(42, pkt(0, 0, Complete, b"ok")),
            Ok(DecodeOutcome::Complete(Bytes::from_static(b"ok")))
        );
    }

    #[test]
    fn test_short_packet_returns_error() {
        let mut d = TestDecoder::new();
        assert_eq!(
            d.decode(1000, Bytes::from_static(b"short")),
            Err(DecodeError::InvalidHeaderSize {
                actual: 5,
                required: 8
            })
        );
    }

    #[test]
    fn test_unsupported_version_returns_error() {
        let mut d = TestDecoder::new();
        let mut packet = vec![0u8; 8 + 5];
        packet[0] = 99;
        packet[8..].copy_from_slice(b"hello");
        assert_eq!(
            d.decode(1000, Bytes::from(packet)),
            Err(DecodeError::UnsupportedVersion {
                version: 99,
                expected: 1
            })
        );
    }

    #[test]
    fn test_conflicting_end_markers_returns_error() {
        let mut d = TestDecoder::priority(&[1000]);
        let _ = d.decode(1000, pkt(0, 0, Start, b"A"));
        let _ = d.decode(1000, pkt(0, 2, End, b"C"));
        assert_eq!(d.priority_messages(), 1);
        assert_eq!(
            d.decode(1000, pkt(0, 5, End, b"X")),
            Err(DecodeError::ConflictingEndMarker {
                expected: 3,
                actual: 6
            })
        );
        assert_eq!(d.priority_messages(), 0);
    }

    #[test]
    fn test_per_identity_message_limit() {
        let mut d = TestDecoder::with_config_and_priority(
            Config {
                max_messages_per_identity: 3,
                ..Config::default()
            },
            &[1000],
        );
        let _ = d.decode(1000, pkt(0, 0, Start, b"A"));
        let _ = d.decode(1000, pkt(1, 0, Start, b"B"));
        let _ = d.decode(1000, pkt(2, 0, Start, b"C"));
        assert_eq!(d.priority_messages(), 3);
        assert_eq!(
            d.decode(1000, pkt(3, 0, Start, b"D")),
            Err(DecodeError::IdentityLimitExceeded { max: 3 })
        );
        assert_eq!(
            d.decode(1000, pkt(0, 1, End, b"1")),
            Ok(DecodeOutcome::Complete(Bytes::from_static(b"A1")))
        );
        assert_eq!(d.priority_messages(), 2);
        assert_eq!(
            d.decode(1000, pkt(3, 0, Start, b"D")),
            Ok(DecodeOutcome::Pending)
        );
    }

    #[test]
    fn test_per_identity_limit_independent_per_identity() {
        let mut d = TestDecoder::with_config(Config {
            max_messages_per_identity: 2,
            ..Config::default()
        });
        let _ = d.decode(1000, pkt(0, 0, Start, b"A"));
        let _ = d.decode(1000, pkt(1, 0, Start, b"B"));
        let _ = d.decode(2000, pkt(0, 0, Start, b"X"));
        let _ = d.decode(2000, pkt(1, 0, Start, b"Y"));
        assert_eq!(d.regular_messages(), 4);
        assert_eq!(
            d.decode(1000, pkt(2, 0, Start, b"C")),
            Err(DecodeError::IdentityLimitExceeded { max: 2 })
        );
        assert_eq!(
            d.decode(2000, pkt(2, 0, Start, b"Z")),
            Err(DecodeError::IdentityLimitExceeded { max: 2 })
        );
        assert_eq!(
            d.decode(3000, pkt(0, 0, Start, b"N")),
            Ok(DecodeOutcome::Pending)
        );
        assert_eq!(d.regular_messages(), 5);
    }

    #[test]
    fn test_per_identity_message_limit_respects_configured_value_above_10() {
        let mut d = TestDecoder::with_config(Config {
            max_messages_per_identity: 17,
            ..Config::default()
        });
        for msg_id in 0..17u32 {
            assert_eq!(
                d.decode(1000, pkt(msg_id, 0, Start, b"A")),
                Ok(DecodeOutcome::Pending)
            );
        }
        assert_eq!(d.regular_messages(), 17);
        assert_eq!(
            d.decode(1000, pkt(17, 0, Start, b"A")),
            Err(DecodeError::IdentityLimitExceeded { max: 17 })
        );
    }

    #[test]
    fn test_per_identity_bytes_limit() {
        let mut d = TestDecoder::priority(&[1000]);
        let chunk = vec![0u8; 64 * 1024];

        for msg_id in 0..4u32 {
            assert_eq!(
                d.decode(1000, pkt(msg_id, 0, Start, &chunk)),
                Ok(DecodeOutcome::Pending)
            );
        }

        assert_eq!(
            d.decode(1000, pkt(4, 0, Start, b"x")),
            Err(DecodeError::IdentityBytesLimitExceeded {
                size: MAX_CONCURRENT_BYTES_PER_IDENTITY + 1,
                max: MAX_CONCURRENT_BYTES_PER_IDENTITY
            })
        );
        assert_eq!(d.priority_messages(), 4);
        assert_eq!(
            d.decode(1000, pkt(0, 1, End, b"")),
            Ok(DecodeOutcome::Complete(Bytes::from(chunk)))
        );
        assert_eq!(
            d.decode(1000, pkt(4, 0, Start, b"x")),
            Ok(DecodeOutcome::Pending)
        );
    }

    #[test]
    fn test_per_identity_limit_requires_slot_recovery() {
        let mut d = TestDecoder::with_config_and_priority(
            Config {
                max_message_size: 128 * 1024,
                max_priority_messages: 10,
                max_regular_messages: 10,
                max_messages_per_identity: 2,
                max_bytes_per_identity: MAX_CONCURRENT_BYTES_PER_IDENTITY,
                message_timeout: Duration::from_millis(100),
                max_fragment_payload: 1440,
            },
            &[1000],
        );
        let _ = d.decode(1000, pkt(0, 0, Start, b"A"));
        let _ = d.decode(1000, pkt(1, 0, Start, b"B"));
        assert_eq!(d.priority_messages(), 2);
        assert_eq!(
            d.decode(1000, pkt(2, 0, Start, b"C")),
            Err(DecodeError::IdentityLimitExceeded { max: 2 })
        );
        assert_eq!(
            d.decode(1000, pkt(0, 1, End, b"1")),
            Ok(DecodeOutcome::Complete(Bytes::from_static(b"A1")))
        );
        assert_eq!(
            d.decode(1000, pkt(2, 0, Start, b"C")),
            Ok(DecodeOutcome::Pending)
        );
        assert_eq!(d.priority_messages(), 2);
    }

    #[test]
    fn test_regular_pool_eviction_does_not_depend_on_time() {
        let mut d = TestDecoder::with_config(Config {
            max_message_size: 128 * 1024,
            max_priority_messages: 10,
            max_regular_messages: 2,
            max_messages_per_identity: 10,
            max_bytes_per_identity: MAX_CONCURRENT_BYTES_PER_IDENTITY,
            message_timeout: Duration::from_millis(100),
            max_fragment_payload: 1440,
        });
        let _ = d.decode(1000, pkt(0, 0, Start, b"A"));
        let _ = d.decode(2000, pkt(1, 0, Start, b"B"));
        assert_eq!(d.regular_messages(), 2);
        d.advance_ms(200);
        assert_eq!(
            d.decode(3000, pkt(2, 0, Start, b"C")),
            Ok(DecodeOutcome::Pending)
        );
        assert_eq!(d.regular_messages(), 2);
    }
}
