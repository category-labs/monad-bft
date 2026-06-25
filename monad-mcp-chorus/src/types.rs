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
    collections::{HashMap, HashSet},
    fmt,
    hash::Hash,
    time::Duration,
};

use bytes::Bytes;
use itertools::Either;

// the environment this module subtree is instantiated.
pub use super::env::{
    KeyPair, MerkleRoot, NodeId, OpaqueChunkHeader, ProposalSignature, PubKey, Signature,
    SignatureCollection, Stake, ValidatorData, VoteAggregation,
};
use crate::spec::{
    Stake as _,
    validator::ValidatorData as _,
    vote::{KeyPair as _, Signature as _, SignatureCollection as _, VoteAggregation as _},
};

// Slot number, starting from 0.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Slot(pub u64);

impl Slot {
    pub const FIRST: Self = Slot(0);
    // the first meaningful slot number
    pub const MIN: Self = Self::FIRST;

    pub const fn get(self) -> u64 {
        self.0
    }

    pub fn checked_add(self, slots: u64) -> Option<Self> {
        self.0.checked_add(slots).map(Self)
    }

    pub fn checked_next(self) -> Option<Self> {
        self.checked_add(1)
    }

    pub fn checked_sub(self, other: Self) -> Option<u64> {
        self.0.checked_sub(other.0)
    }
}

/// An absolute point on the timeline, stored in nanoseconds.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Timestamp(u128);

impl Timestamp {
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

    pub fn max(self, other: Self) -> Self {
        Self(self.0.max(other.0))
    }
}

#[derive(
    Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, derive_more::Add, derive_more::Sum,
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

pub type SlotDeadline = Timestamp;

// Identifies a window of contiguous slots, starting from 0.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WindowId(pub(crate) u64);

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
pub struct ProposalMeta {
    pub root: MerkleRoot,
    pub sig: ProposalSignature,
    pub opaque_header: OpaqueChunkHeader,
}

// A message with author signature validated.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Validated<T> {
    message: T,
    author: NodeId,
}

impl<T> Validated<T> {
    // for testing only
    pub fn new_unchecked(message: T, author: NodeId) -> Self {
        Self { message, author }
    }

    pub fn destructure(self) -> (T, NodeId) {
        (self.message, self.author)
    }

    pub fn author(&self) -> &NodeId {
        &self.author
    }

    pub fn into(self) -> T {
        self.message
    }

    pub fn project<T2>(self, f: impl FnOnce(T) -> T2) -> Validated<T2> {
        Validated {
            message: f(self.message),
            author: self.author,
        }
    }

    pub fn try_map<T2, E>(self, f: impl FnOnce(T) -> Result<T2, E>) -> Result<Validated<T2>, E> {
        Ok(Validated {
            message: f(self.message)?,
            author: self.author,
        })
    }
}

pub trait IsVote: Clone + Hash + Eq {
    type Scope: Clone + Hash + Eq + std::fmt::Debug;

    // type SigningDomain;
    fn serialize(&self, scope: &Self::Scope) -> Bytes;
}

// placeholder serialization until we decide on real wire format
pub(crate) fn dummy_serialize(vote: &impl std::fmt::Debug, scope: &impl std::fmt::Debug) -> Bytes {
    Bytes::from(format!("{scope:?}/{vote:?}"))
}

#[derive(Clone)]
pub struct VotePool<V>
where
    V: IsVote,
{
    scope: <V as IsVote>::Scope,
    buckets: HashMap<V, HashSet<NodeId>>,
    votes: HashMap<NodeId, Signature>,
}

impl<V> VotePool<V>
where
    V: IsVote,
{
    pub fn new(scope: <V as IsVote>::Scope) -> Self {
        Self {
            scope,
            buckets: HashMap::new(),
            votes: HashMap::new(),
        }
    }

    // the caller should ensure that the node_id is in the validator
    // set.
    pub fn add_vote(&mut self, node_id: NodeId, msg: VoteMsg<V>) {
        assert!(msg.scope == self.scope);

        if !msg.signature.is_well_formed() {
            return;
        }

        if self.votes.contains_key(&node_id) {
            return;
        }

        self.buckets.entry(msg.vote).or_default().insert(node_id);
        self.votes.insert(node_id, msg.signature);
    }

    pub fn scope(&self) -> &<V as IsVote>::Scope {
        &self.scope
    }

    pub fn all_voters(&self) -> impl Iterator<Item = &NodeId> {
        self.votes.keys()
    }

    pub fn try_aggregate(
        &self,
        target_stake: Stake,
        validator_data: &ValidatorData,
    ) -> Vec<(&V, SignatureCollection)> {
        let mut aggs = vec![];

        for (vote, voters) in &self.buckets {
            let stake = validator_data.sum_stake(voters.iter());
            // pre-filter to only keep buckets with enough stake
            if stake <= target_stake {
                continue;
            }

            let data = vote.serialize(&self.scope);
            let votes = voters
                .iter()
                .map(|node_id| (node_id, &self.votes[node_id]))
                .collect();

            let mut vote_agg = VoteAggregation::from_validator_data(validator_data);

            if let Some(sigcol) = vote_agg.try_aggregate(&data, votes, target_stake) {
                aggs.push((vote, sigcol));
            }
        }

        aggs
    }

    pub fn try_form_strong_qc(&self, validator_data: &ValidatorData) -> Option<StrongQc<V>> {
        let target_stake = validator_data.total_stake().supermajority_threshold();
        let aggs = self.try_aggregate(target_stake, validator_data);
        debug_assert!(aggs.len() <= 1, "at most one strong qc can be formed");

        let (vote, sigcol) = aggs.into_iter().next()?;
        let qc = StrongQc {
            scope: self.scope.clone(),
            verdict: vote.clone(),
            sigcol,
        };
        Some(qc)
    }

    // there are at most two weak qcs.
    pub fn try_form_weak_qc(
        &self,
        validator_data: &ValidatorData,
    ) -> Option<Either<WeakQc<V>, (WeakQc<V>, WeakQc<V>)>> {
        let target_stake = validator_data.total_stake().honest_threshold();
        let aggs = self.try_aggregate(target_stake, validator_data);
        debug_assert!(aggs.len() <= 2, "at most two weak qc can be formed");

        let mut qcs = aggs.into_iter().map(|(vote, sigcol)| WeakQc {
            scope: self.scope.clone(),
            verdict: vote.clone(),
            sigcol,
        });

        match (qcs.next(), qcs.next()) {
            (None, _) => None,
            (Some(qc), None) => Some(Either::Left(qc)),
            (Some(qc1), Some(qc2)) => Some(Either::Right((qc1, qc2))),
        }
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct VoteMsg<V>
where
    V: IsVote,
{
    pub scope: <V as IsVote>::Scope,
    pub vote: V,
    pub signature: Signature,
}

impl<V> VoteMsg<V>
where
    V: IsVote,
{
    pub fn new(scope: <V as IsVote>::Scope, vote: V, signature: Signature) -> Self {
        Self {
            scope,
            vote,
            signature,
        }
    }

    pub fn new_signed(scope: <V as IsVote>::Scope, vote: V, key: &KeyPair) -> Self {
        let serialized_vote = vote.serialize(&scope);
        let sig = key.sign(&serialized_vote);
        Self::new(scope, vote, sig)
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct StrongQc<V>
where
    V: IsVote,
{
    pub scope: <V as IsVote>::Scope,
    pub verdict: V,
    // 2f+1 votes
    pub sigcol: SignatureCollection,
}

impl<V> StrongQc<V>
where
    V: IsVote,
{
    pub fn verify(&self, validator_data: &ValidatorData) -> bool {
        let data = self.verdict.serialize(&self.scope);
        let Some(nodes) = self.sigcol.verify(&data, validator_data) else {
            return false;
        };

        let stake = validator_data.sum_stake(nodes);
        stake > validator_data.total_stake().supermajority_threshold()
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct WeakQc<V>
where
    V: IsVote,
{
    pub scope: <V as IsVote>::Scope,
    pub verdict: V,
    // f+1 votes
    pub sigcol: SignatureCollection,
}

impl<V> WeakQc<V>
where
    V: IsVote,
{
    pub fn verify(&self, validator_data: &ValidatorData) -> bool {
        let data = self.verdict.serialize(&self.scope);
        let Some(nodes) = self.sigcol.verify(&data, validator_data) else {
            return false;
        };

        let stake = validator_data.sum_stake(nodes);
        stake > validator_data.total_stake().honest_threshold()
    }
}

pub type ProposalIndex = usize;

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct ProposalMap<T> {
    values: Box<[T]>,
}

// for documentation only. the "total" here means that all entries are
// guaranteed to be present.
pub type TotalProposalMap<T> = ProposalMap<T>;

impl<T> ProposalMap<T> {
    pub fn new(size: usize, init_fn: impl FnMut(ProposalIndex) -> T) -> Self {
        Self {
            values: (0..size).map(init_fn).collect(),
        }
    }

    pub(crate) fn new_default(size: usize) -> Self
    where
        T: Default,
    {
        Self {
            values: (0..size).map(|_| T::default()).collect(),
        }
    }

    pub const fn size(&self) -> usize {
        self.values.len()
    }

    pub(crate) fn as_ref(&self) -> ProposalMap<&T> {
        let values = self.values.iter().collect();
        ProposalMap { values }
    }

    pub(crate) fn map<F, U>(self, f: F) -> ProposalMap<U>
    where
        F: FnMut(T) -> U,
    {
        let values = self.values.into_iter().map(f).collect();
        ProposalMap { values }
    }

    pub(crate) fn map_indexed<F, U>(self, mut f: F) -> ProposalMap<U>
    where
        F: FnMut(ProposalIndex, T) -> U,
    {
        let values = self
            .values
            .into_iter()
            .enumerate()
            .map(|(i, v)| f(i, v))
            .collect();
        ProposalMap { values }
    }

    pub fn into_indexed_iter(self) -> impl Iterator<Item = (ProposalIndex, T)> {
        self.values.into_iter().enumerate()
    }
}

impl<T> IntoIterator for ProposalMap<T> {
    type Item = T;
    type IntoIter = std::vec::IntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        self.values.into_iter()
    }
}

impl<T> ProposalMap<Option<T>> {
    /// Panics if index out of bounds. The caller must ensure the index is valid.
    fn set(&mut self, index: ProposalIndex, value: T) {
        self.values[index] = Some(value);
    }

    pub(crate) fn try_into_total<S>(self) -> Option<TotalProposalMap<S>>
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

        Some(ProposalMap { values })
    }

    fn into_total<S>(self) -> TotalProposalMap<S>
    where
        S: From<Option<T>>,
    {
        self.map(|opt| S::from(opt))
    }
}

impl<T> ProposalMap<&T> {
    pub(crate) fn into_owned(self) -> ProposalMap<T>
    where
        T: Clone,
    {
        self.map(|v| v.clone())
    }
}

/// Panics if index out of bounds. The caller must ensure the index is valid.
impl<T> std::ops::Index<ProposalIndex> for ProposalMap<T> {
    type Output = T;
    fn index(&self, index: ProposalIndex) -> &T {
        &self.values[index]
    }
}

/// Panics if index out of bounds. The caller must ensure the index is valid.
impl<T> std::ops::IndexMut<ProposalIndex> for ProposalMap<T> {
    fn index_mut(&mut self, index: ProposalIndex) -> &mut T {
        &mut self.values[index]
    }
}

// A helper wrapper type for a type-erased implementation of a trait
pub struct Erased<T>(pub T);

// A stub implementation of DAHandle that never returns any
// proposal. used for testing purposes.
pub struct DAHandle;

// invariant: .0.root != .1.root and both properly signed.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct EquivCert(pub ProposalMeta, pub ProposalMeta);

pub enum FetchProposalError {
    Absent,
    Equivocation(EquivCert),
}

impl DAHandle {
    pub fn proposal_decoded(&self, _s: Slot, _j: ProposalIndex, _root: &MerkleRoot) -> bool {
        false
    }

    /// Info DA about proposals we received through consensus messages (e.g. FallbackSignedEntry)
    pub fn observe_proposal(&self, _s: Slot, _j: ProposalIndex, _meta: ProposalMeta) {
        // do nothing
    }

    pub fn fetch_proposal(
        &self,
        _s: Slot,
        _j: ProposalIndex,
    ) -> Result<ProposalMeta, FetchProposalError> {
        // Please do note that there is an potential to have more than
        // one proposal meta for the same root. This can occur if the
        // proposer sign the same root with different chunk header
        // fields (e.g. varying unix_ts_ms).
        //
        // Q: How should we deal with this situation? Should we count
        // it as equivocation? Or should we simply ignore that? Our
        // current implementation follows the paper which doesn't
        // currently consider this case as equivocation.
        Err(FetchProposalError::Absent)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
