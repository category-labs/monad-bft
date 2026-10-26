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

pub mod spec;

// a stub implementation of spec for development & testing
// TODO: gate behind enable_stub once prod no longer reuses the stub types
pub mod stub;

// demonstration only. reuses all stub types.
pub mod prod;

use std::{fmt, time::Duration};

use alloy_rlp::{
    Decodable, Encodable, RlpDecodable, RlpDecodableWrapper, RlpEncodable, RlpEncodableWrapper,
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

// the max number of lanes we currently permit
pub const MAX_PROPOSER_SET_SIZE: usize = 5;

// The position of a proposal in a slot. Only meaningful under the
// context of a given slot. Invariant: .0 < MAX_PROPOSER_SET_SIZE.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, RlpEncodableWrapper)]
pub struct Lane(u8);

impl Lane {
    pub const fn new_unchecked(lane: u8) -> Self {
        Self(lane)
    }

    pub const fn from_u8(lane: u8) -> Option<Self> {
        if lane as usize >= MAX_PROPOSER_SET_SIZE {
            return None;
        }
        Some(Self(lane))
    }
}

impl Decodable for Lane {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let lane = u8::decode(buf)?;
        Self::from_u8(lane).ok_or(alloy_rlp::Error::Custom("lane out of range"))
    }
}

impl From<Lane> for usize {
    fn from(lane: Lane) -> Self {
        usize::from(lane.0)
    }
}

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

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Lanes<T> {
    values: Box<[T]>,
}

impl<T> Lanes<T> {
    // the caller must ensure `size` is contextually valid.
    pub fn new(size: usize, init_fn: impl FnMut(Lane) -> T) -> Self {
        assert!(size <= MAX_PROPOSER_SET_SIZE, "too many lanes");
        Self {
            values: lanes_unchecked(size).map(init_fn).collect(),
        }
    }

    // the caller must ensure `size` is contextually valid.
    pub fn new_default(size: usize) -> Self
    where
        T: Default,
    {
        assert!(size <= MAX_PROPOSER_SET_SIZE, "too many lanes");
        Self {
            values: lanes_unchecked(size).map(|_| T::default()).collect(),
        }
    }

    pub const fn size(&self) -> usize {
        self.values.len()
    }

    pub fn lanes(&self) -> impl Iterator<Item = Lane> + use<T> {
        lanes_unchecked(self.size())
    }

    // None if `index` is not a lane of this slot
    pub fn checked_lane(&self, index: usize) -> Option<Lane> {
        let in_range = index < self.size();
        in_range.then(|| Lane::new_unchecked(index as u8))
    }

    pub fn get(&self, index: usize) -> Option<&T> {
        self.values.get(index)
    }

    pub fn get_mut(&mut self, index: usize) -> Option<&mut T> {
        self.values.get_mut(index)
    }

    pub fn as_ref(&self) -> Lanes<&T> {
        let values = self.values.iter().collect();
        Lanes { values }
    }

    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut T> {
        self.values.iter_mut()
    }

    pub fn map<F, U>(self, f: F) -> Lanes<U>
    where
        F: FnMut(T) -> U,
    {
        let values = self.values.into_iter().map(f).collect();
        Lanes { values }
    }

    pub fn map_indexed<F, U>(self, mut f: F) -> Lanes<U>
    where
        F: FnMut(Lane, T) -> U,
    {
        let values = self
            .lanes()
            .zip(self.values)
            .map(|(lane, v)| f(lane, v))
            .collect();
        Lanes { values }
    }

    pub fn into_indexed_iter(self) -> impl Iterator<Item = (Lane, T)> {
        self.lanes().zip(self.values)
    }
}

impl<T> IntoIterator for Lanes<T> {
    type Item = T;
    type IntoIter = std::vec::IntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        self.values.into_iter()
    }
}

impl<T> Lanes<Option<T>> {
    /// Panics if the lane is out of bounds. The caller must ensure the lane is valid.
    pub fn set(&mut self, lane: Lane, value: T) {
        self.values[usize::from(lane)] = Some(value);
    }

    pub fn try_into_total<S>(self) -> Option<Lanes<S>>
    where
        S: From<T>,
    {
        let is_partial = self.values.iter().any(|v| v.is_none());
        if is_partial {
            return None;
        }

        let values = self
            .values
            .into_iter()
            // SAFETY: we just checked that all values are Some
            .map(|opt| S::from(opt.unwrap()))
            .collect();

        Some(Lanes { values })
    }

    pub fn into_total<S>(self) -> Lanes<S>
    where
        S: From<Option<T>>,
    {
        self.map(|opt| S::from(opt))
    }
}

impl<T> Lanes<&T> {
    pub fn into_owned(self) -> Lanes<T>
    where
        T: Clone,
    {
        self.map(|v| v.clone())
    }
}

/// Panics if the lane is out of bounds. The caller must ensure the lane is valid.
impl<T> std::ops::Index<Lane> for Lanes<T> {
    type Output = T;
    fn index(&self, lane: Lane) -> &T {
        &self.values[usize::from(lane)]
    }
}

/// Panics if the lane is out of bounds. The caller must ensure the lane is valid.
impl<T> std::ops::IndexMut<Lane> for Lanes<T> {
    fn index_mut(&mut self, lane: Lane) -> &mut T {
        &mut self.values[usize::from(lane)]
    }
}

// the caller must ensure count is contextually valid
fn lanes_unchecked(count: usize) -> impl Iterator<Item = Lane> {
    assert!(count <= MAX_PROPOSER_SET_SIZE);
    (0..count).map(|j| Lane::new_unchecked(j as u8))
}

impl<T> Encodable for Lanes<T>
where
    T: Encodable,
{
    fn encode(&self, out: &mut dyn bytes::BufMut) {
        alloy_rlp::encode_list(&self.values, out);
    }

    fn length(&self) -> usize {
        alloy_rlp::list_length(&self.values)
    }
}

impl<T> Decodable for Lanes<T>
where
    T: Decodable,
{
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let values = Vec::<T>::decode(buf)?;
        if values.len() > MAX_PROPOSER_SET_SIZE {
            return Err(alloy_rlp::Error::Custom("too many lanes"));
        }
        let result = Self {
            values: values.into_boxed_slice(),
        };
        Ok(result)
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
    fn a_lane_decodes_only_below_the_proposer_set_size() {
        for lane in 0..=u8::MAX {
            let bytes = alloy_rlp::encode(lane);
            let decoded = Lane::decode(&mut bytes.as_slice());
            let in_range = usize::from(lane) < MAX_PROPOSER_SET_SIZE;
            assert_eq!(decoded.is_ok(), in_range, "lane {lane}");
        }
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
