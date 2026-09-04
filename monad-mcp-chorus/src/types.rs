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
    HeaderAuth, KeyPair, MerkleRoot, NodeId, ProposalHeader, PubKey, Signature,
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

    // the first and last meaningful slot numbers
    pub const MIN: Self = Self::FIRST;
    pub const MAX: Self = Slot(u64::MAX - 1);

    // the max meaningful slot number used as cap
    pub const MAX_CAP: Self = Slot(u64::MAX);

    pub const fn get(self) -> u64 {
        self.0
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

        // first write wins: a sender's later vote never displaces its first.
        if self.votes.contains_key(&node_id) {
            return;
        }
        self.add_or_replace_vote(node_id, msg)
    }

    /// Like [`VotePool::add_vote`], except a sender's later vote always
    /// replaces its earlier one. The sender still occupies exactly one bucket
    /// afterwards, so the partition the aggregation counts stake over is
    /// preserved; what replacement concedes is that the pool's answer can
    /// change between polls, which is only sound where every certificate over
    /// the scope certifies the same fact.
    pub fn add_or_replace_vote(&mut self, node_id: NodeId, msg: VoteMsg<V>) {
        assert!(msg.scope == self.scope);

        if !msg.signature.is_well_formed() {
            return;
        }

        if self.votes.contains_key(&node_id) {
            let held = self
                .buckets
                .iter()
                .find_map(|(vote, voters)| voters.contains(&node_id).then_some(vote))
                .expect("a recorded sender is in exactly one bucket");

            // move the sender, dropping a bucket left empty: an empty bucket is
            // indistinguishable from a missing one everywhere else.
            let held = held.clone();
            let voters = self.buckets.get_mut(&held).expect("found above");
            voters.remove(&node_id);
            if voters.is_empty() {
                self.buckets.remove(&held);
            }
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

    /// Quorum over the scope rather than over one bucket: the target is met
    /// by the combined stake of *all* the pool's voters, and one signature
    /// collection comes back per surviving bucket.
    ///
    /// This is for votes that agree on the act while legitimately differing in
    /// verdict -- timeouts abandon the same view but each names the lock its
    /// sender holds -- where a per-bucket quorum would never form. Signatures
    /// are verified here and invalid ones dropped, so the stake is counted
    /// over signers that actually signed; a bucket left with no valid signer
    /// disappears.
    pub fn try_form_vote_groups(
        &self,
        target_stake: Stake,
        validator_data: &ValidatorData,
    ) -> Option<Vec<(&V, SignatureCollection)>> {
        // claimed stake bounds verified stake from above -- verification only
        // drops signers -- so a pool short of the target before any signature
        // is checked forms nothing; skip the aggregation entirely.
        if validator_data.sum_stake(self.all_voters()) <= target_stake {
            return None;
        }

        let mut groups = vec![];
        // buckets partition the pool -- every insert leaves a sender in exactly
        // one -- so per-bucket stake adds up without deduplicating signers.
        let mut stake = Stake::ZERO;

        for (vote, voters) in &self.buckets {
            let data = vote.serialize(&self.scope);
            let votes = voters
                .iter()
                .map(|node_id| (node_id, &self.votes[node_id]))
                .collect();

            let mut vote_agg = VoteAggregation::from_validator_data(validator_data);

            // no threshold within the bucket: the quorum is checked once over
            // every bucket's survivors below. A bucket whose signatures are
            // all invalid holds no stake and is dropped here.
            let Some(sigcol) = vote_agg.try_aggregate(&data, votes, Stake::ZERO) else {
                continue;
            };

            let signers = sigcol
                .signers(validator_data)
                .expect("aggregation returns a collection consistent with the validator set");
            stake = stake + validator_data.sum_stake(signers.iter().copied());

            groups.push((vote, sigcol));
        }

        // still needed: invalid signatures may have dropped the verified sum
        // below the target the claimed sum cleared.
        if stake <= target_stake {
            return None;
        }

        Some(groups)
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

    pub fn new_default(size: usize) -> Self
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

    pub fn as_ref(&self) -> ProposalMap<&T> {
        let values = self.values.iter().collect();
        ProposalMap { values }
    }

    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut T> {
        self.values.iter_mut()
    }

    pub fn map<F, U>(self, f: F) -> ProposalMap<U>
    where
        F: FnMut(T) -> U,
    {
        let values = self.values.into_iter().map(f).collect();
        ProposalMap { values }
    }

    pub fn map_indexed<F, U>(self, mut f: F) -> ProposalMap<U>
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
    pub fn set(&mut self, index: ProposalIndex, value: T) {
        self.values[index] = Some(value);
    }

    pub fn try_into_total<S>(self) -> Option<TotalProposalMap<S>>
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

    pub fn into_total<S>(self) -> TotalProposalMap<S>
    where
        S: From<Option<T>>,
    {
        self.map(|opt| S::from(opt))
    }
}

impl<T> ProposalMap<&T> {
    pub fn into_owned(self) -> ProposalMap<T>
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

// invariant: .0.root != .1.root and both properly signed.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct EquivCert(pub ProposalHeader, pub ProposalHeader);

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

    /// A vote whose verdict is a bare number, standing in for the claims
    /// timeouts differ on.
    #[derive(Clone, PartialEq, Eq, Hash, Debug)]
    struct Claim(u64);

    impl IsVote for Claim {
        type Scope = u64;

        fn serialize(&self, scope: &Self::Scope) -> Bytes {
            dummy_serialize(self, scope)
        }
    }

    /// Seven validators of stake one each, as in the aggregation tests.
    fn claim_setup() -> (Vec<NodeId>, ValidatorData) {
        let nodes: Vec<NodeId> = (0..7).map(NodeId::dummy).collect();
        let valset: HashMap<_, _> = nodes.iter().map(|node| (*node, Stake::from(1))).collect();
        let mapping: HashMap<_, _> = nodes
            .iter()
            .map(|node| (*node, node.keypair().pubkey()))
            .collect();

        (nodes, ValidatorData::new(valset, mapping))
    }

    const SCOPE: u64 = 7;

    fn claim_pool(votes: &[(NodeId, Claim)]) -> VotePool<Claim> {
        let mut pool = VotePool::new(SCOPE);
        for (node, claim) in votes {
            pool.add_vote(
                *node,
                VoteMsg::new_signed(SCOPE, claim.clone(), &node.keypair()),
            );
        }

        pool
    }

    /// Neither claim holds a supermajority alone; together they do, and each
    /// comes back as its own collection.
    #[test]
    fn a_quorum_spanning_buckets_forms_one_group_per_bucket() {
        let (nodes, validator_data) = claim_setup();
        let votes: Vec<_> = nodes[..6]
            .iter()
            .enumerate()
            .map(|(i, node)| (*node, Claim(i as u64 % 2)))
            .collect();
        let pool = claim_pool(&votes);

        // no bucket reaches it on its own: three each against a target of four.
        assert!(
            pool.try_aggregate(Stake::from(4), &validator_data)
                .is_empty()
        );

        let mut groups = pool
            .try_form_vote_groups(Stake::from(4), &validator_data)
            .expect("six of seven exceeds the target across both buckets");
        groups.sort_by_key(|(claim, _)| claim.0);

        let claims: Vec<_> = groups.iter().map(|(claim, _)| (*claim).clone()).collect();
        assert_eq!(claims, vec![Claim(0), Claim(1)]);

        for (claim, sigcol) in &groups {
            let signers = sigcol
                .verify(&claim.serialize(&SCOPE), &validator_data)
                .expect("each group verifies over the digest its bucket signed");
            assert_eq!(signers.len(), 3);
        }
    }

    /// The target is over the whole pool, so a pool short of it forms nothing
    /// even though every bucket is well signed.
    #[test]
    fn a_pool_below_the_target_forms_no_groups() {
        let (nodes, validator_data) = claim_setup();
        let votes: Vec<_> = nodes[..4]
            .iter()
            .enumerate()
            .map(|(i, node)| (*node, Claim(i as u64 % 2)))
            .collect();

        assert!(
            claim_pool(&votes)
                .try_form_vote_groups(Stake::from(4), &validator_data)
                .is_none()
        );
    }

    /// The quorum requires strictly more than the target, and the pool-wide
    /// pre-check honors the same boundary: claimed stake equal to the target
    /// forms nothing, one less tips it.
    #[test]
    fn the_target_boundary_is_strict() {
        let (nodes, validator_data) = claim_setup();
        let votes: Vec<_> = nodes[..5].iter().map(|node| (*node, Claim(0))).collect();
        let pool = claim_pool(&votes);

        assert!(
            pool.try_form_vote_groups(Stake::from(5), &validator_data)
                .is_none()
        );
        assert!(
            pool.try_form_vote_groups(Stake::from(4), &validator_data)
                .is_some()
        );
    }

    /// Well-formed signatures that do not verify are dropped, and the stake
    /// they carried with them: five valid signers clear a target of four, four
    /// of them do not.
    #[test]
    fn invalid_signatures_are_dropped_before_the_quorum_is_counted() {
        let (nodes, validator_data) = claim_setup();
        let mut pool = VotePool::new(SCOPE);
        for (i, node) in nodes.iter().enumerate() {
            let mut msg = VoteMsg::new_signed(SCOPE, Claim(i as u64 % 2), &node.keypair());
            // two of seven sign garbage, one in each bucket.
            if i < 2 {
                msg.signature.make_invalid();
            }
            pool.add_vote(*node, msg);
        }

        let groups = pool
            .try_form_vote_groups(Stake::from(4), &validator_data)
            .expect("the five valid signers exceed the target");
        let signers: usize = groups
            .iter()
            .map(|(claim, sigcol)| {
                sigcol
                    .verify(&claim.serialize(&SCOPE), &validator_data)
                    .expect("only verified signatures are aggregated")
                    .len()
            })
            .sum();
        assert_eq!(signers, 5);

        assert!(
            pool.try_form_vote_groups(Stake::from(5), &validator_data)
                .is_none()
        );
    }

    /// A bucket left with no valid signer disappears rather than coming back
    /// empty.
    #[test]
    fn a_bucket_of_only_invalid_signatures_is_dropped() {
        let (nodes, validator_data) = claim_setup();
        let mut pool = VotePool::new(SCOPE);
        for (i, node) in nodes.iter().enumerate() {
            // one lone dissenter, and its signature is garbage.
            let claim = Claim(u64::from(i == 0));
            let mut msg = VoteMsg::new_signed(SCOPE, claim, &node.keypair());
            if i == 0 {
                msg.signature.make_invalid();
            }
            pool.add_vote(*node, msg);
        }

        let groups = pool
            .try_form_vote_groups(Stake::from(4), &validator_data)
            .expect("the six honest signers exceed the target");
        let claims: Vec<_> = groups.iter().map(|(claim, _)| (*claim).clone()).collect();
        assert_eq!(claims, vec![Claim(0)]);
    }

    /// A replacing insert moves the sender rather than adding it, so the
    /// buckets still partition the pool and the one it emptied is gone.
    #[test]
    fn a_replacing_vote_moves_the_sender_between_buckets() {
        let (nodes, validator_data) = claim_setup();
        let mut pool = VotePool::new(SCOPE);
        // five agree, one dissents alone.
        for (i, node) in nodes[..6].iter().enumerate() {
            let claim = Claim(u64::from(i == 5));
            pool.add_vote(*node, VoteMsg::new_signed(SCOPE, claim, &node.keypair()));
        }

        let raiser = nodes[5];
        pool.add_or_replace_vote(
            raiser,
            VoteMsg::new_signed(SCOPE, Claim(2), &raiser.keypair()),
        );

        let mut groups = pool
            .try_form_vote_groups(Stake::from(4), &validator_data)
            .expect("the same six senders still exceed the target");
        groups.sort_by_key(|(claim, _)| claim.0);

        let counted: Vec<_> = groups
            .iter()
            .map(|(claim, sigcol)| {
                let signers = sigcol
                    .verify(&claim.serialize(&SCOPE), &validator_data)
                    .expect("each group verifies over the digest its bucket signed");
                (claim.0, signers.len())
            })
            .collect();

        assert_eq!(
            counted,
            vec![(0, 5), (2, 1)],
            "the sender is counted once, under its new claim"
        );
    }

    /// A repeated [`VotePool::add_vote`] changes nothing: the first is still
    /// the one aggregated, under the signature it arrived with.
    #[test]
    fn a_repeated_add_vote_leaves_the_first_standing() {
        let (nodes, validator_data) = claim_setup();
        let mut pool = VotePool::new(SCOPE);
        for node in &nodes[..5] {
            pool.add_vote(*node, VoteMsg::new_signed(SCOPE, Claim(1), &node.keypair()));
        }

        let sender = nodes[4];
        pool.add_vote(
            sender,
            VoteMsg::new_signed(SCOPE, Claim(0), &sender.keypair()),
        );

        let groups = pool
            .try_form_vote_groups(Stake::from(4), &validator_data)
            .expect("five of seven exceeds the target");
        let claims: Vec<_> = groups.iter().map(|(claim, _)| (*claim).clone()).collect();
        assert_eq!(claims, vec![Claim(1)]);

        let (claim, sigcol) = &groups[0];
        let signers = sigcol
            .verify(&claim.serialize(&SCOPE), &validator_data)
            .expect("the stored signatures are the ones that arrived first");
        assert_eq!(signers.len(), 5);
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
