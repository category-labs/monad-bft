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

use alloy_rlp::{Decodable, Encodable, RlpEncodable};
use arrayvec::ArrayVec;
use bytes::Bytes;
use monad_mcp_common::spec::{KeyPair as _, Stake as _};
pub(crate) use monad_mcp_common::{
    Lane, Lanes, MAX_PROPOSER_SET_SIZE, ProposalScope, Slot, Timestamp, TimestampDelta, WindowId,
};

// the environment this module subtree is instantiated.
pub(crate) use super::env::{KeyPair, NodeId, PubKey, Signature, Stake};
pub use super::{
    da::{DaHandle, DataAvailability, NullDa},
    env::{
        HeaderAuth, MerkleRoot, ProposalHeader, ProposalId, SignatureCollection,
        SignedProposalHeader, ValidatorData, VoteAggregation,
    },
    proposer_schedule::ScheduleError,
    proposers::{
        CreditLotterySchedule, EpochAnchor, FixedProposerSchedule, ProposerConfig,
        ProposerSchedule, ProposerSet, RotatingProposerSchedule, RoundRobinLeaderSchedule,
    },
};
use crate::spec::{
    validator::ValidatorData as _,
    vote::{SignatureCollection as _, SigningDomain, signing_bytes},
};

pub trait IsVote: Clone + Hash + Eq + Encodable {
    type Scope: Clone + Hash + Eq + std::fmt::Debug + Encodable + Decodable;
    type SigningDomain: SigningDomain;

    fn signing_bytes(&self, scope: &Self::Scope) -> Bytes {
        signing_bytes::<Self::SigningDomain>(scope, self)
    }
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
        use monad_mcp_common::spec::Signature as _;

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

    // the distinct votes cast, each with its voters, for logging only
    // for determinism, avoid iteration ordering-dependent behavior variation
    pub(crate) fn buckets(&self) -> impl Iterator<Item = (&V, &HashSet<NodeId>)> {
        self.buckets.iter()
    }

    pub fn tally<'a>(&'a self, validator_data: &'a ValidatorData) -> Tally<'a, V> {
        use crate::spec::vote::VoteAggregation as _;

        let mut buckets = vec![];
        // a tally sorts its buckets below
        #[allow(clippy::iter_over_hash_type)]
        for (vote, voters) in &self.buckets {
            let data = vote.signing_bytes(&self.scope);
            let votes = voters
                .iter()
                .map(|node_id| (node_id, &self.votes[node_id]))
                .collect();

            // a bucket whose signatures are all invalid holds no stake and is
            // dropped here
            let mut vote_agg = VoteAggregation::from_validator_data(validator_data);
            let Some(sigcol) = vote_agg.try_aggregate(&data, votes, Stake::ZERO) else {
                continue;
            };

            let signers = sigcol
                .signers(validator_data)
                .expect("aggregation returns a collection consistent with the validator set");
            let stake = validator_data.sum_stake(signers.iter().copied());
            buckets.push((data, vote, sigcol, stake));
        }

        // canonical order, by the bytes each vote signs: every node lists the
        // same votes the same way whatever order they arrived in, so the qc it
        // picks and the certificates it builds do not depend on arrival
        buckets.sort_unstable_by(|(a, ..), (b, ..)| a.cmp(b));
        let buckets = buckets
            .into_iter()
            .map(|(_, vote, sigcol, stake)| (vote, sigcol, stake))
            .collect();

        Tally {
            scope: &self.scope,
            validator_data,
            buckets,
        }
    }
}

pub trait Gate<V> {
    fn admits(&self, vote: &V) -> bool;
}

// invariant: every signature verified, no bucket empty, buckets ordered by signed bytes
pub struct Tally<'a, V: IsVote> {
    scope: &'a <V as IsVote>::Scope,
    validator_data: &'a ValidatorData,
    buckets: Vec<(&'a V, SignatureCollection, Stake)>,
}

impl<'a, V> Tally<'a, V>
where
    V: IsVote,
{
    pub fn narrow<G>(mut self, gate: G) -> Tally<'a, V>
    where
        G: Gate<V>,
    {
        self.buckets.retain(|(vote, _, _)| gate.admits(vote));
        self
    }

    // buckets partition the pool, so their stakes add up without
    // deduplicating signers
    pub fn stake(&self) -> Stake {
        let mut stake = Stake::ZERO;
        for (_, _, bucket_stake) in &self.buckets {
            stake = stake + *bucket_stake;
        }
        stake
    }

    pub fn strong_qc(&self) -> Option<StrongQc<V>> {
        let target_stake = self.validator_data.total_stake().supermajority_threshold();
        for (vote, sigcol, stake) in &self.buckets {
            if *stake <= target_stake {
                continue;
            }
            return Some(StrongQc {
                scope: self.scope.clone(),
                verdict: (*vote).clone(),
                sigcol: sigcol.clone(),
            });
        }
        None
    }

    // there are at most two weak qcs.
    pub fn weak_qcs(&self) -> ArrayVec<WeakQc<V>, 2> {
        let target_stake = self.validator_data.total_stake().honest_threshold();
        let mut qcs = ArrayVec::new();
        for (vote, sigcol, stake) in &self.buckets {
            if *stake <= target_stake {
                continue;
            }
            qcs.push(WeakQc {
                scope: self.scope.clone(),
                verdict: (*vote).clone(),
                sigcol: sigcol.clone(),
            });
        }
        qcs
    }

    /// One signature collection per bucket, for votes that agree on the act
    /// while legitimately differing in verdict -- timeouts abandon the same
    /// view but each names the lock its sender holds -- where a per-bucket
    /// quorum would never form. The caller checks the quorum over the scope
    /// with [`Tally::stake`].
    pub fn groups(&self) -> impl Iterator<Item = (&'a V, &SignatureCollection)> {
        self.buckets.iter().map(|(vote, sigcol, _)| (*vote, sigcol))
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
        let serialized_vote = vote.signing_bytes(&scope);
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
        let data = self.verdict.signing_bytes(&self.scope);
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
        let data = self.verdict.signing_bytes(&self.scope);
        let Some(nodes) = self.sigcol.verify(&data, validator_data) else {
            return false;
        };

        let stake = validator_data.sum_stake(nodes);
        stake > validator_data.total_stake().honest_threshold()
    }
}

// The scope is `V::Scope`, which Alloy's derives cannot bound, so the vote
// containers encode their three fields as a list by hand.
impl<V> Encodable for VoteMsg<V>
where
    V: IsVote,
{
    fn encode(&self, out: &mut dyn bytes::BufMut) {
        let fields: [&dyn Encodable; 3] = [&self.scope, &self.vote, &self.signature];
        alloy_rlp::encode_list::<_, dyn Encodable>(&fields, out);
    }

    fn length(&self) -> usize {
        let fields: [&dyn Encodable; 3] = [&self.scope, &self.vote, &self.signature];
        alloy_rlp::list_length::<_, dyn Encodable>(&fields)
    }
}

impl<V> Decodable for VoteMsg<V>
where
    V: IsVote + Decodable,
{
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let mut payload = alloy_rlp::Header::decode_bytes(buf, true)?;
        let scope = <V::Scope as Decodable>::decode(&mut payload)?;
        let vote = <V as Decodable>::decode(&mut payload)?;
        let signature = <Signature as Decodable>::decode(&mut payload)?;
        if !payload.is_empty() {
            return Err(alloy_rlp::Error::UnexpectedLength);
        }
        Ok(Self::new(scope, vote, signature))
    }
}

impl<V> Encodable for StrongQc<V>
where
    V: IsVote,
{
    fn encode(&self, out: &mut dyn bytes::BufMut) {
        let fields: [&dyn Encodable; 3] = [&self.scope, &self.verdict, &self.sigcol];
        alloy_rlp::encode_list::<_, dyn Encodable>(&fields, out);
    }

    fn length(&self) -> usize {
        let fields: [&dyn Encodable; 3] = [&self.scope, &self.verdict, &self.sigcol];
        alloy_rlp::list_length::<_, dyn Encodable>(&fields)
    }
}

impl<V> Decodable for StrongQc<V>
where
    V: IsVote + Decodable,
{
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let mut payload = alloy_rlp::Header::decode_bytes(buf, true)?;
        let scope = <V::Scope as Decodable>::decode(&mut payload)?;
        let verdict = <V as Decodable>::decode(&mut payload)?;
        let sigcol = <SignatureCollection as Decodable>::decode(&mut payload)?;
        if !payload.is_empty() {
            return Err(alloy_rlp::Error::UnexpectedLength);
        }
        Ok(Self {
            scope,
            verdict,
            sigcol,
        })
    }
}

impl<V> Encodable for WeakQc<V>
where
    V: IsVote,
{
    fn encode(&self, out: &mut dyn bytes::BufMut) {
        let fields: [&dyn Encodable; 3] = [&self.scope, &self.verdict, &self.sigcol];
        alloy_rlp::encode_list::<_, dyn Encodable>(&fields, out);
    }

    fn length(&self) -> usize {
        let fields: [&dyn Encodable; 3] = [&self.scope, &self.verdict, &self.sigcol];
        alloy_rlp::list_length::<_, dyn Encodable>(&fields)
    }
}

impl<V> Decodable for WeakQc<V>
where
    V: IsVote + Decodable,
{
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let mut payload = alloy_rlp::Header::decode_bytes(buf, true)?;
        let scope = <V::Scope as Decodable>::decode(&mut payload)?;
        let verdict = <V as Decodable>::decode(&mut payload)?;
        let sigcol = <SignatureCollection as Decodable>::decode(&mut payload)?;
        if !payload.is_empty() {
            return Err(alloy_rlp::Error::UnexpectedLength);
        }
        Ok(Self {
            scope,
            verdict,
            sigcol,
        })
    }
}

// invariant: .0.pid() != .1.pid() and both properly signed.
#[derive(Clone, PartialEq, Eq, Hash, Debug, RlpEncodable)]
pub struct EquivCert(pub SignedProposalHeader, pub SignedProposalHeader);

impl Decodable for EquivCert {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        use crate::spec::proposal::ProposalHeader as _;

        let mut payload = alloy_rlp::Header::decode_bytes(buf, true)?;
        let a = SignedProposalHeader::decode(&mut payload)?;
        let b = SignedProposalHeader::decode(&mut payload)?;
        if !payload.is_empty() {
            return Err(alloy_rlp::Error::UnexpectedLength);
        }
        if a.pid() == b.pid() {
            return Err(alloy_rlp::Error::Custom("equivocation over one proposal"));
        }
        Ok(Self(a, b))
    }
}

#[cfg(test)]
mod tests {
    use alloy_rlp::{RlpDecodableWrapper, RlpEncodableWrapper};

    use super::*;
    use crate::spec::vote::assert_signing_prefix;

    /// A vote whose verdict is a bare number, standing in for the claims
    /// timeouts differ on.
    #[derive(Clone, PartialEq, Eq, Hash, Debug, RlpEncodableWrapper, RlpDecodableWrapper)]
    struct Claim(u64);

    struct ClaimDomain;
    const _: () = assert_signing_prefix::<ClaimDomain>();

    impl SigningDomain for ClaimDomain {
        const PREFIX: &'static [u8] = b"\x1Bmonad/cadence/test-claim/1\n";
    }

    impl IsVote for Claim {
        type Scope = u64;
        type SigningDomain = ClaimDomain;
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

        // no bucket reaches a supermajority on its own: three each of seven.
        assert!(pool.tally(&validator_data).strong_qc().is_none());

        let tally = pool.tally(&validator_data);
        assert!(
            tally.stake() > Stake::from(4),
            "six of seven exceeds the target across both buckets"
        );
        let mut groups: Vec<_> = tally.groups().collect();
        groups.sort_by_key(|(claim, _)| claim.0);

        let claims: Vec<_> = groups.iter().map(|(claim, _)| (*claim).clone()).collect();
        assert_eq!(claims, vec![Claim(0), Claim(1)]);

        for (claim, sigcol) in &groups {
            let signers = sigcol
                .verify(&claim.signing_bytes(&SCOPE), &validator_data)
                .expect("each group verifies over the digest its bucket signed");
            assert_eq!(signers.len(), 3);
        }
    }

    /// A tally lists its buckets in the same order whatever order the votes
    /// arrived in, so every node picks the same qc and builds the same
    /// certificate from the same votes.
    #[test]
    fn a_tally_does_not_depend_on_arrival_order() {
        let (nodes, validator_data) = claim_setup();
        let votes: Vec<_> = nodes[..6]
            .iter()
            .enumerate()
            .map(|(i, node)| (*node, Claim(i as u64 % 3)))
            .collect();
        let mut reversed = votes.clone();
        reversed.reverse();

        let groups = |votes: &[(NodeId, Claim)]| {
            let pool = claim_pool(votes);
            let tally = pool.tally(&validator_data);
            tally
                .groups()
                .map(|(claim, sigcol)| (claim.clone(), sigcol.clone()))
                .collect::<Vec<_>>()
        };
        assert_eq!(groups(&votes), groups(&reversed));
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

        assert!(claim_pool(&votes).tally(&validator_data).stake() <= Stake::from(4));
    }

    /// The quorum requires strictly more than the target: stake equal to the
    /// target forms nothing, one less tips it.
    #[test]
    fn the_target_boundary_is_strict() {
        let (nodes, validator_data) = claim_setup();
        let votes: Vec<_> = nodes[..5].iter().map(|node| (*node, Claim(0))).collect();
        let pool = claim_pool(&votes);

        let stake = pool.tally(&validator_data).stake();
        assert!(stake <= Stake::from(5));
        assert!(stake > Stake::from(4));
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

        let tally = pool.tally(&validator_data);
        assert!(
            tally.stake() > Stake::from(4),
            "the five valid signers exceed the target"
        );
        let groups: Vec<_> = tally.groups().collect();
        let signers: usize = groups
            .iter()
            .map(|(claim, sigcol)| {
                sigcol
                    .verify(&claim.signing_bytes(&SCOPE), &validator_data)
                    .expect("only verified signatures are aggregated")
                    .len()
            })
            .sum();
        assert_eq!(signers, 5);

        assert!(tally.stake() <= Stake::from(5));
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

        let tally = pool.tally(&validator_data);
        assert!(
            tally.stake() > Stake::from(4),
            "the six honest signers exceed the target"
        );
        let groups: Vec<_> = tally.groups().collect();
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

        let tally = pool.tally(&validator_data);
        assert!(
            tally.stake() > Stake::from(4),
            "the same six senders still exceed the target"
        );
        let mut groups: Vec<_> = tally.groups().collect();
        groups.sort_by_key(|(claim, _)| claim.0);

        let counted: Vec<_> = groups
            .iter()
            .map(|(claim, sigcol)| {
                let signers = sigcol
                    .verify(&claim.signing_bytes(&SCOPE), &validator_data)
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

        let tally = pool.tally(&validator_data);
        assert!(
            tally.stake() > Stake::from(4),
            "five of seven exceeds the target"
        );
        let groups: Vec<_> = tally.groups().collect();
        let claims: Vec<_> = groups.iter().map(|(claim, _)| (*claim).clone()).collect();
        assert_eq!(claims, vec![Claim(1)]);

        let (claim, sigcol) = &groups[0];
        let signers = sigcol
            .verify(&claim.signing_bytes(&SCOPE), &validator_data)
            .expect("the stored signatures are the ones that arrived first");
        assert_eq!(signers.len(), 5);
    }

    // admits the listed claims only
    struct Listed<'a>(&'a [Claim]);

    impl Gate<Claim> for Listed<'_> {
        fn admits(&self, vote: &Claim) -> bool {
            self.0.contains(vote)
        }
    }

    #[test]
    fn a_narrowed_tally_forms_no_qc_on_a_rejected_vote() {
        let (nodes, validator_data) = claim_setup();
        let votes: Vec<_> = nodes[..5].iter().map(|node| (*node, Claim(1))).collect();
        let pool = claim_pool(&votes);

        let rejecting = pool.tally(&validator_data).narrow(Listed(&[Claim(2)]));
        assert!(rejecting.strong_qc().is_none());
        assert!(rejecting.weak_qcs().is_empty());

        let admitting = pool.tally(&validator_data).narrow(Listed(&[Claim(1)]));
        assert!(admitting.strong_qc().is_some());
        assert_eq!(admitting.weak_qcs().len(), 1);
    }

    #[test]
    fn a_narrowed_tally_leaves_rejected_buckets_out_of_the_stake() {
        let (nodes, validator_data) = claim_setup();
        let mut votes: Vec<_> = nodes[..3].iter().map(|node| (*node, Claim(1))).collect();
        votes.extend(nodes[3..5].iter().map(|node| (*node, Claim(2))));
        let pool = claim_pool(&votes);

        assert!(pool.tally(&validator_data).stake() == Stake::from(5));
        let one_bucket = pool.tally(&validator_data).narrow(Listed(&[Claim(1)]));
        assert!(one_bucket.stake() == Stake::from(3));
    }

    #[test]
    fn a_narrowed_tally_keeps_only_the_admitted_buckets() {
        let (nodes, validator_data) = claim_setup();
        let mut votes: Vec<_> = nodes[..3].iter().map(|node| (*node, Claim(1))).collect();
        votes.extend(nodes[3..5].iter().map(|node| (*node, Claim(2))));
        let pool = claim_pool(&votes);

        let narrowed = pool.tally(&validator_data).narrow(Listed(&[Claim(2)]));
        assert!(narrowed.stake() == Stake::from(2));

        let claims: Vec<_> = narrowed.groups().map(|(claim, _)| claim).collect();
        assert_eq!(claims, vec![&Claim(2)]);
    }
}
