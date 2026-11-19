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

//! Types shared by the mcp crates. The crate root holds
//! environment-independent types. `spec` defines
//! implementation-dependent traits, and `stub` and `prod` are its
//! instantiations.

pub mod containers;
pub mod spec;

// a stub implementation of spec for development & testing
// TODO: gate behind enable_stub once prod no longer reuses the stub types
pub mod stub;

// demonstration only. reuses all stub types.
pub mod prod;

use std::{fmt, time::Duration};

use alloy_rlp::{Decodable, RlpDecodable, RlpDecodableWrapper, RlpEncodable, RlpEncodableWrapper};

pub use self::containers::{
    Lane, Lanes, OrderKey, ValidatorIndex, ValidatorMap, ValidatorSet, ValidatorVec,
};

// Slot number, starting from 0.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, RlpEncodableWrapper)]
pub struct Slot(pub u64);

impl Decodable for Slot {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let slot = u64::decode(buf)?;
        Self::from_u64(slot).ok_or(alloy_rlp::Error::Custom("slot out of range"))
    }
}

impl Slot {
    pub const FIRST: Self = Slot(0);

    // the first and last meaningful slot numbers
    pub const MIN: Self = Self::FIRST;
    pub const MAX: Self = Slot(u64::MAX - 1);

    // the max meaningful slot number used as cap
    pub const MAX_CAP: Self = Slot(u64::MAX);

    pub const fn get(self) -> u64 {
        self.0
    }

    pub const fn from_u64(slot: u64) -> Option<Self> {
        if slot > Self::MAX.0 {
            return None;
        }
        Some(Self(slot))
    }

    pub fn checked_add(self, slots: u64) -> Option<Self> {
        self.0.checked_add(slots).map(Self)
    }

    pub fn checked_sub(self, slots: u64) -> Option<Self> {
        self.0.checked_sub(slots).map(Self)
    }

    pub fn checked_next(self) -> Option<Self> {
        self.checked_add(1)
    }

    pub fn slots_since(self, earlier: Self) -> Option<u64> {
        self.0.checked_sub(earlier.0)
    }
}

// Message scoped to a slot. If it contains one multiple slots they
// must agree.
pub trait SlotScoped {
    fn slot(&self) -> Slot;
}

// A message on its way out, with its recipients and delivery contract.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Outbound<M, R, Q = Qos> {
    pub to: R,
    pub message: M,
    pub qos: Q,
}

impl<M, R, Q> Outbound<M, R, Q> {
    pub fn with_qos(self, qos: Q) -> Self {
        Self { qos, ..self }
    }

    pub fn map<N>(self, wrap: impl FnOnce(M) -> N) -> Outbound<N, R, Q> {
        Outbound {
            to: self.to,
            message: wrap(self.message),
            qos: self.qos,
        }
    }
}

// todo: name the recipients by slot and validator index
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Recipients<N> {
    Everyone,
    Node(N),
    Nodes(Vec<N>),
}

impl<M, N, Q> Outbound<M, Recipients<N>, Q> {
    pub fn broadcast(message: M) -> Self
    where
        Q: Default,
    {
        let to = Recipients::Everyone;
        let qos = Q::default();
        Self { to, message, qos }
    }

    pub fn unicast(to: N, message: M) -> Self
    where
        Q: Default,
    {
        let to = Recipients::Node(to);
        let qos = Q::default();
        Self { to, message, qos }
    }

    pub fn multicast(to: Vec<N>, message: M) -> Self
    where
        Q: Default,
    {
        let to = Recipients::Nodes(to);
        let qos = Q::default();
        Self { to, message, qos }
    }
}

impl<M, N, Q> Outbound<M, Vec<N>, Q> {
    // recipients in send order
    pub fn multicast(to: Vec<N>, message: M) -> Self
    where
        Q: Default,
    {
        let qos = Q::default();
        Self { to, message, qos }
    }
}

// A message with its authenticated sender.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Inbound<M, S> {
    pub sender: S,
    pub message: M,
}

// The delivery contract of a message.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct Qos {
    pub priority: WirePriority,
    // None: the message is sent once
    pub repeat_until: Option<RepeatUntil>,
}

// The priority values of the authenticated udp transport.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub enum WirePriority {
    High = 0,
    #[default]
    Regular = 1,
}

// The event that ends the repeats of a message.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RepeatUntil {
    // the slot commits
    Commit(Slot),
    // the cap is past the slot
    CapAdvanced(Slot),
}

impl<M, R> Outbound<M, R, Qos> {
    pub fn with_priority(mut self, priority: WirePriority) -> Self {
        self.qos.priority = priority;
        self
    }

    // The first rule stays: the maker of a message knows when its repeats end.
    pub fn repeat_until(mut self, event: RepeatUntil) -> Self {
        self.qos.repeat_until.get_or_insert(event);
        self
    }
}

// the max number of lanes we currently permit
pub const MAX_PROPOSER_SET_SIZE: usize = 5;

// the most validators an epoch may have
pub const MAX_VALIDATORS: usize = 300;

/// The slot and lane authenticated by a per-proposal vote.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, RlpEncodable, RlpDecodable)]
pub struct ProposalScope {
    pub slot: Slot,
    pub lane: Lane,
}

impl ProposalScope {
    pub const fn new(slot: Slot, lane: Lane) -> Self {
        Self { slot, lane }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Proposal<B> {
    pub slot: Slot,
    pub lane: Lane,
    pub payload: B,
}

/// An absolute point on the timeline, stored in nanoseconds.
#[derive(
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Debug,
    RlpEncodableWrapper,
    RlpDecodableWrapper,
)]
pub struct Timestamp(u128);

impl Timestamp {
    pub const MIN: Self = Timestamp(0);
    pub const GENESIS: Self = Timestamp(0);

    pub const fn from_millis(millis: u64) -> Self {
        Self((millis as u128) * (TimestampDelta::NANOS_PER_MILLISECOND as u128))
    }

    pub const fn from_micros(micros: u64) -> Self {
        Self((micros as u128) * (TimestampDelta::NANOS_PER_MICROSECOND as u128))
    }

    pub const fn from_nanos(nanos: u128) -> Self {
        Self(nanos)
    }

    pub const fn as_nanos(self) -> u128 {
        self.0
    }

    pub fn duration_since(&self, earlier: Timestamp) -> Option<TimestampDelta> {
        let delta = self.0.checked_sub(earlier.0)?;
        let delta = u64::try_from(delta).ok()?;
        Some(TimestampDelta::from_nanos(delta))
    }

    pub fn checked_add_delta(self, delta: TimestampDelta) -> Option<Self> {
        self.0.checked_add(u128::from(delta.as_nanos())).map(Self)
    }

    pub fn checked_add_deltas(self, delta: TimestampDelta, count: u64) -> Option<Self> {
        let delta = delta.as_nanos().checked_mul(count)?;
        self.0.checked_add(u128::from(delta)).map(Self)
    }

    pub fn saturating_sub_delta(self, delta: TimestampDelta) -> Self {
        Self(self.0.saturating_sub(u128::from(delta.as_nanos())))
    }

    pub fn max(self, other: Self) -> Self {
        Self(self.0.max(other.0))
    }
}

#[derive(
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Debug,
    derive_more::Add,
    derive_more::Sum,
    RlpEncodableWrapper,
    RlpDecodableWrapper,
)]
pub struct TimestampDelta(u64);

impl std::ops::Add<TimestampDelta> for Timestamp {
    type Output = Self;

    fn add(self, rhs: TimestampDelta) -> Self::Output {
        Timestamp(self.0 + u128::from(rhs.0))
    }
}

impl TimestampDelta {
    pub const ZERO: Self = TimestampDelta(0);
    pub const NANOS_PER_MICROSECOND: u64 = 1_000;
    pub const NANOS_PER_MILLISECOND: u64 = 1_000_000;

    pub const fn from_millis(millis: u64) -> Self {
        match millis.checked_mul(Self::NANOS_PER_MILLISECOND) {
            Some(nanos) => Self(nanos),
            None => panic!("timestamp delta overflow"),
        }
    }

    pub const fn from_micros(micros: u64) -> Self {
        match micros.checked_mul(Self::NANOS_PER_MICROSECOND) {
            Some(nanos) => Self(nanos),
            None => panic!("timestamp delta overflow"),
        }
    }

    pub const fn from_nanos(nanos: u64) -> Self {
        Self(nanos)
    }

    pub const fn as_millis(self) -> u64 {
        self.0 / Self::NANOS_PER_MILLISECOND
    }

    pub const fn as_nanos(self) -> u64 {
        self.0
    }

    pub fn as_duration(self) -> Duration {
        Duration::from_nanos(self.as_nanos())
    }

    pub fn checked_mul(self, rhs: u64) -> Option<Self> {
        self.0.checked_mul(rhs).map(Self)
    }
}

impl std::ops::Mul<usize> for TimestampDelta {
    type Output = Self;

    fn mul(self, rhs: usize) -> Self::Output {
        let rhs = u64::try_from(rhs).expect("timestamp delta overflow");
        self.checked_mul(rhs).expect("timestamp delta overflow")
    }
}

pub type SlotDeadline = Timestamp;

// Identifies a window of contiguous slots, starting from 0.
#[derive(
    Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, RlpEncodableWrapper, RlpDecodableWrapper,
)]
pub struct WindowId(pub u64);

impl WindowId {
    pub const FIRST: Self = Self(0);

    pub const fn get(self) -> u64 {
        self.0
    }

    pub fn checked_next(self) -> Option<Self> {
        self.0.checked_add(1).map(Self)
    }

    pub fn checked_prev(self) -> Option<Self> {
        self.0.checked_sub(1).map(Self)
    }

    pub fn to_index(self) -> Option<usize> {
        usize::try_from(self.0).ok()
    }
}

impl fmt::Debug for WindowId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("WindowId").field(&self.get()).finish()
    }
}

impl Default for WindowId {
    fn default() -> Self {
        Self::FIRST
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_slot_decodes_only_up_to_the_max_slot() {
        let decode = |slot: u64| Slot::decode(&mut alloy_rlp::encode(slot).as_slice()).ok();
        assert_eq!(decode(Slot::MAX.0), Some(Slot::MAX));
        assert_eq!(decode(Slot::MAX_CAP.0), None);
    }

    #[test]
    fn timestamp_arithmetic_is_checked() {
        let timestamp = Timestamp::from_nanos(10);
        let delta = TimestampDelta::from_nanos(5);

        assert_eq!(
            timestamp.checked_add_deltas(delta, 3),
            Some(Timestamp::from_nanos(25))
        );
        assert_eq!(
            Timestamp::from_nanos(u128::MAX).checked_add_delta(delta),
            None
        );
        assert_eq!(Timestamp::from_millis(2).as_nanos(), 2_000_000);
        assert_eq!(TimestampDelta::from_millis(3).as_millis(), 3);
    }

    #[test]
    fn identities_are_zero_indexed() {
        assert_eq!(Slot::FIRST, Slot(0));
        assert_eq!(Slot::MIN, Slot(0));

        assert_eq!(WindowId::FIRST, WindowId(0));
        assert_eq!(WindowId::default(), WindowId(0));
        assert_eq!(WindowId(0).to_index(), Some(0));
    }
}
