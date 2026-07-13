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
    hash::Hash,
};

use itertools::Either;

// slot number. starting from 0.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Slot(pub u64);

impl Slot {
    // the first meaningful slot number
    pub const MIN: Self = Slot(0);

    pub fn next(self) -> Self {
        Slot(self.0 + 1)
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct NodeId(pub u64);

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct MerkleRoot(pub u64);

/// An absolute point on the timeline. Stores some logical unix offset from genesis (e.g. ms)?
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Timestamp(u64);

impl Timestamp {
    pub const GENESIS: Self = Timestamp(0);

    pub const fn new(ts: u64) -> Self {
        Timestamp(ts)
    }

    pub const fn ticks(self) -> u64 {
        self.0
    }
}

#[derive(
    Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, derive_more::Add, derive_more::Sum,
)]
pub struct TimestampDelta(u64);

impl std::ops::Mul<u64> for TimestampDelta {
    type Output = Self;

    fn mul(self, rhs: u64) -> Self::Output {
        TimestampDelta(self.0 * rhs)
    }
}

impl std::ops::Add<TimestampDelta> for Timestamp {
    type Output = Self;

    fn add(self, rhs: TimestampDelta) -> Self::Output {
        Timestamp(self.0 + rhs.0)
    }
}

impl TimestampDelta {
    pub const fn new(ticks: u64) -> Self {
        TimestampDelta(ticks)
    }

    pub const fn ticks(self) -> u64 {
        self.0
    }
}

pub type SlotDeadline = Timestamp;

pub struct KeyPair;
pub struct PubKey;
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Signature;
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct SignatureCollection;

// not the same as vote signature. at least ProposalSignature is not
// supposed to be aggregatable.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct ProposalSignature;

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct OpaqueChunkHeader;

impl OpaqueChunkHeader {
    pub fn validate(&self, _root: &MerkleRoot, _sig: &ProposalSignature) -> bool {
        todo!()
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
}

#[derive(Clone)]
pub struct VotePool<V>
where
    V: IsVote,
{
    scope: <V as IsVote>::Scope,
    buckets: HashMap<V, HashSet<NodeId>>,
    votes: HashMap<NodeId, (V, Signature)>,
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

    pub fn add_vote(&mut self, node_id: NodeId, msg: VoteMsg<V>) {
        assert!(msg.scope == self.scope);
        if self.votes.contains_key(&node_id) {
            return;
        }

        self.buckets
            .entry(msg.vote.clone())
            .or_default()
            .insert(node_id);

        self.votes.insert(node_id, (msg.vote, msg.signature));
    }

    fn scope(&self) -> &<V as IsVote>::Scope {
        &self.scope
    }

    pub fn all_voters(&self) -> impl Iterator<Item = &NodeId> {
        self.votes.keys()
    }

    // todo: make it fallible
    pub fn try_form_strong_qc(&self, _validator_data: &ValidatorData) -> Option<StrongQc<V>> {
        todo!()
    }

    // todo: make it fallible
    // there are at most two weak qcs.
    pub fn try_form_weak_qc(
        &self,
        _validator_data: &ValidatorData,
    ) -> Option<Either<WeakQc<V>, (WeakQc<V>, WeakQc<V>)>> {
        todo!()
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

    pub fn new_signed(_scope: <V as IsVote>::Scope, _vote: V, _key: &KeyPair) -> Self {
        todo!()
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

    const fn size(&self) -> usize {
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

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, derive_more::Add, derive_more::Sum)]
pub struct Stake(u64);

impl Stake {
    // 2f
    fn supermajority_threshold(&self) -> Self {
        // handle overflow
        Self((self.0 * 2) / 3)
    }

    // f
    fn majority_threshold(&self) -> Self {
        Self(self.0 / 3)
    }
}

pub struct ValidatorData {
    valset: HashMap<NodeId, Stake>,
    mapping: HashMap<NodeId, PubKey>,
}

impl ValidatorData {
    pub fn sum_stake<'a>(&self, nodes: impl IntoIterator<Item = &'a NodeId>) -> Stake {
        nodes.into_iter().map(|node_id| self.valset[node_id]).sum()
    }

    fn total_stake(&self) -> Stake {
        self.valset.values().copied().sum()
    }

    pub fn is_supermajority(&self, stake: Stake) -> bool {
        stake > self.total_stake().supermajority_threshold()
    }

    fn is_majority(&self, stake: Stake) -> bool {
        stake > self.total_stake().majority_threshold()
    }

    pub fn verify_strong_qc<V: IsVote>(&self, _qc: &StrongQc<V>) -> bool {
        todo!()
    }

    pub fn verify_weak_qc<V: IsVote>(&self, _qc: &WeakQc<V>) -> bool {
        todo!()
    }
}

// A helper wrapper type for a type-erased implementation of a trait
pub struct Erased<T>(pub T);

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
        todo!()
    }

    /// Info DA about proposals we received through consensus messages (e.g. FallbackSignedEntry)
    pub fn observe_proposal(&self, _s: Slot, _j: ProposalIndex, _meta: ProposalMeta) {
        todo!()
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
        todo!()
    }
}
