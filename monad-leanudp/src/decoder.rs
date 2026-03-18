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

use std::{convert::Infallible, hash::Hash, time::Instant};

use bytes::Bytes;
use monad_executor::{ExecutorMetrics, MetricDef};
use rand::{rngs::StdRng, CryptoRng, RngCore, SeedableRng};
use thiserror::Error;
use zerocopy::FromBytes;

use crate::{
    metrics::*,
    pool::{EvictionKind, FragmentInput, IdentityUsage, MessagePool, PoolConfig},
    Config, FragmentPolicy, IdentityScore, PacketHeader, LEANUDP_HEADER_SIZE,
    LEANUDP_PROTOCOL_VERSION,
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
    DuplicateFragment { msg_id: u16, seq_num: u16 },

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
    fn metric_name(&self) -> &'static MetricDef {
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
    fn fragments_counter(self) -> &'static MetricDef {
        match self {
            PoolSelection::Priority => COUNTER_LEANUDP_DECODE_FRAGMENTS_PRIORITY,
            PoolSelection::Regular => COUNTER_LEANUDP_DECODE_FRAGMENTS_REGULAR,
        }
    }

    fn bytes_counter(self) -> &'static MetricDef {
        match self {
            PoolSelection::Priority => COUNTER_LEANUDP_DECODE_BYTES_PRIORITY,
            PoolSelection::Regular => COUNTER_LEANUDP_DECODE_BYTES_REGULAR,
        }
    }
}

pub struct Decoder<I, P, C>
where
    I: Eq + Hash + Clone + Ord,
    P: IdentityScore<Identity = I>,
    C: Clock,
{
    priority_pool: MessagePool<I, StdRng>,
    regular_pool: MessagePool<I, StdRng>,
    identity_score: P,
    clock: C,
    metrics: ExecutorMetrics,
}

impl<I, P, C> Decoder<I, P, C>
where
    I: Eq + Hash + Clone + Ord,
    P: IdentityScore<Identity = I>,
    C: Clock,
{
    pub(crate) fn with_clock(config: &Config, identity_score: P, clock: C) -> Self {
        Self::with_clock_and_crypto_rng(config, identity_score, clock, StdRng::from_entropy())
    }
}

impl<I, P, C> Decoder<I, P, C>
where
    I: Eq + Hash + Clone + Ord,
    P: IdentityScore<Identity = I>,
    C: Clock,
{
    pub(crate) fn with_clock_and_crypto_rng(
        config: &Config,
        identity_score: P,
        clock: C,
        mut pool_rng: impl CryptoRng + RngCore,
    ) -> Self {
        let priority_pool_rng =
            StdRng::from_rng(&mut pool_rng).expect("failed to seed priority pool rng");
        let regular_pool_rng =
            StdRng::from_rng(&mut pool_rng).expect("failed to seed regular pool rng");
        Self {
            priority_pool: MessagePool::new(
                PoolConfig::from_config(config, config.max_priority_messages),
                priority_pool_rng,
                IdentityUsage::for_priority_pool(config),
            ),
            regular_pool: MessagePool::new(
                PoolConfig::from_config(config, config.max_regular_messages),
                regular_pool_rng,
                IdentityUsage::for_regular_pool(config),
            ),
            identity_score,
            clock,
            metrics: init_executor_metrics(),
        }
    }

    fn select_pool(&self, identity: &I, key: &(I, u16)) -> PoolSelection {
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
        now: Instant,
        selected_pool: PoolSelection,
        key: &(I, u16),
        input: FragmentInput<I>,
    ) -> Result<DecodeOutcome, DecodeError> {
        let is_new_message = match selected_pool {
            PoolSelection::Priority => !self.priority_pool.has_message(key),
            PoolSelection::Regular => !self.regular_pool.has_message(key),
        };

        if is_new_message {
            let evicted = match selected_pool {
                PoolSelection::Priority => self
                    .priority_pool
                    .evict_one_stale_for_identity_if_limited(&input.identity, now),
                PoolSelection::Regular => self
                    .regular_pool
                    .evict_one_stale_for_identity_if_limited(&input.identity, now),
            };
            if evicted {
                self.metrics.inc(EvictionKind::Timeout.metric_name());
            }
            match selected_pool {
                PoolSelection::Priority => self
                    .priority_pool
                    .ensure_identity_capacity(&input.identity)?,
                PoolSelection::Regular => self
                    .regular_pool
                    .ensure_identity_capacity(&input.identity)?,
            }
        }

        let (result, pool_evicted) = match selected_pool {
            PoolSelection::Priority => self.priority_pool.decode_with_admission(now, key, input),
            PoolSelection::Regular => self.regular_pool.decode_with_admission(now, key, input),
        };

        if let Some(kind) = pool_evicted {
            self.metrics.inc(kind.metric_name());
        }

        result
    }

    fn record_decode_metrics(
        &mut self,
        selected_pool: PoolSelection,
        payload_bytes: u64,
        result: &Result<DecodeOutcome, DecodeError>,
    ) {
        self.metrics.inc(selected_pool.fragments_counter());
        if payload_bytes > 0 {
            self.metrics.add(selected_pool.bytes_counter(), payload_bytes);
        }

        match result {
            Ok(DecodeOutcome::Complete(msg)) => {
                self.metrics.inc(COUNTER_LEANUDP_DECODE_MESSAGES_COMPLETED);
                self.metrics
                    .add(COUNTER_LEANUDP_DECODE_BYTES_COMPLETED, msg.len() as u64);
            }
            Ok(DecodeOutcome::Pending) => {}
            Err(e) => {
                self.metrics.inc(e.metric_name());
            }
        }
    }

    fn update_pool_gauges(&mut self) {
        self.metrics.set(
            GAUGE_LEANUDP_POOL_PRIORITY_MESSAGES,
            self.priority_pool.message_count() as u64,
        );
        self.metrics.set(
            GAUGE_LEANUDP_POOL_REGULAR_MESSAGES,
            self.regular_pool.message_count() as u64,
        );
    }

    fn reject_decode(&mut self, error: DecodeError) -> DecodeError {
        self.metrics.inc(error.metric_name());
        self.update_pool_gauges();
        error
    }

    pub fn decode<T>(&mut self, identity: I, packet: T) -> Result<DecodeOutcome, DecodeError>
    where
        T: TryInto<Packet>,
        T::Error: Into<DecodeError>,
    {
        self.metrics.inc(COUNTER_LEANUDP_DECODE_FRAGMENTS_RECEIVED);

        let Packet { header, payload } = match packet.try_into().map_err(Into::into) {
            Ok(packet) => packet,
            Err(error) => return Err(self.reject_decode(error)),
        };
        let payload_bytes = payload.len() as u64;
        if payload_bytes > 0 {
            self.metrics
                .add(COUNTER_LEANUDP_DECODE_BYTES_RECEIVED, payload_bytes);
        }
        if header.version() != LEANUDP_PROTOCOL_VERSION {
            return Err(self.reject_decode(DecodeError::UnsupportedVersion {
                version: header.version(),
                expected: LEANUDP_PROTOCOL_VERSION,
            }));
        }
        if !header.has_known_flags() {
            return Err(self.reject_decode(DecodeError::InvalidHeader));
        }

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

        let result = self.decode_in_pool(now, selected_pool, &key, input);
        self.record_decode_metrics(selected_pool, payload_bytes, &result);
        self.update_pool_gauges();

        result
    }

    pub fn executor_metrics(&self) -> &ExecutorMetrics {
        &self.metrics
    }

    pub fn metrics(&self) -> &ExecutorMetrics {
        &self.metrics
    }
}
