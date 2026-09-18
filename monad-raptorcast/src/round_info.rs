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

use std::collections::{BTreeMap, BTreeSet, HashMap};

use monad_crypto::certificate_signature::PubKey;
use monad_types::{NodeId, Round};

use crate::{
    packet::{
        assigner::{ChunkAssignment, ChunkRouting},
        deterministic,
    },
    udp::ValidatedChunk,
    util::{EncodingScheme, GlobalMerkleRoot, PrimaryBroadcastGroup, SecondaryBroadcastGroup},
};

pub(crate) const CACHE_MAX_FUTURE_ROUNDS: Round = Round(100);
pub(crate) const CACHE_MAX_PAST_ROUNDS: Round = Round(100);

pub(crate) const AUTHOR_QUOTA_DURING_SYNC: usize = 512; // max ~80KB per author
pub(crate) const AUTHOR_QUOTA_DURING_LIVE: usize = 32;

// Stores information related to the current round.
pub struct RoundInfoCache<PT: PubKey> {
    current_round: Option<Round>,

    // number of new slots an author is allowed to open on primary
    // round info cache. Replenishes on every local round advance.
    author_quota: HashMap<NodeId<PT>, usize>,
    primary: BTreeMap<Round, PrimaryRoundInfo<PT>>,
    // Per-publisher secondary round info. Multiple validators can
    // publish secondary broadcasts in the same round to independent
    // full-node groups, so we key by publisher as well.
    secondary: HashMap<NodeId<PT>, BTreeMap<Round, SecondaryGroupRoundInfo<PT>>>,
}

impl<PT: PubKey> RoundInfoCache<PT> {
    pub fn new() -> Self {
        Self {
            current_round: None,
            author_quota: HashMap::new(),
            primary: BTreeMap::new(),
            secondary: HashMap::new(),
        }
    }

    pub fn update_current_round(&mut self, round: Round) {
        if let Some(current) = self.current_round {
            assert!(
                round > current,
                "Cannot enter a past round: current {}, new {}",
                current,
                round
            );
        }

        self.current_round = Some(round);

        // Evict rounds from the cache
        if let Some(cutoff_future) = round.checked_add(CACHE_MAX_FUTURE_ROUNDS) {
            drop(self.primary.split_off(&cutoff_future));
            for by_round in self.secondary.values_mut() {
                drop(by_round.split_off(&cutoff_future));
            }
        };
        if let Some(cutoff_past) = round.checked_sub(CACHE_MAX_PAST_ROUNDS) {
            let mut active = self.primary.split_off(&cutoff_past);
            std::mem::swap(&mut self.primary, &mut active);
            for by_round in self.secondary.values_mut() {
                let mut active = by_round.split_off(&cutoff_past);
                std::mem::swap(by_round, &mut active);
            }
        }
        self.secondary.retain(|_, by_round| !by_round.is_empty());

        // replenish author quota
        self.author_quota.clear();
    }

    // Returns None on out-of-window round or if the author has
    // exhausted their quota of opening new rounds.
    pub fn get_or_insert_primary(
        &mut self,
        round: Round,
        author: &NodeId<PT>,
    ) -> Option<&mut PrimaryRoundInfo<PT>> {
        if !self.primary.contains_key(&round) {
            self.check_round(round)?;
            self.deduct_author_quota(author)?;
            self.primary.insert(round, Default::default());
        }
        self.primary.get_mut(&round)
    }

    // Returns None on out-of-window round
    pub fn get_or_insert_secondary(
        &mut self,
        publisher: NodeId<PT>,
        round: Round,
    ) -> Option<&mut SecondaryGroupRoundInfo<PT>> {
        self.check_round(round)?;
        let slot_exists = self
            .secondary
            .get(&publisher)
            .is_some_and(|by_round| by_round.contains_key(&round));
        if !slot_exists {
            self.deduct_author_quota(&publisher)?;
        }

        let per_validator = self.secondary.entry(publisher).or_default();
        let per_round = per_validator.entry(round).or_default();
        Some(per_round)
    }

    #[cfg(test)]
    fn get_primary(&self, round: Round) -> Option<&PrimaryRoundInfo<PT>> {
        self.primary.get(&round)
    }

    #[cfg(test)]
    fn get_secondary(
        &self,
        publisher: &NodeId<PT>,
        round: Round,
    ) -> Option<&SecondaryGroupRoundInfo<PT>> {
        self.secondary.get(publisher)?.get(&round)
    }

    fn check_round(&self, round: Round) -> Option<()> {
        if let Some(current) = self.current_round {
            let max_round = current
                .checked_add(CACHE_MAX_FUTURE_ROUNDS)
                .unwrap_or(Round::MAX);
            let min_round = current
                .checked_sub(CACHE_MAX_PAST_ROUNDS)
                .unwrap_or(Round::MIN);

            if round > max_round || round < min_round {
                return None;
            }
        }

        Some(())
    }

    fn deduct_author_quota(&mut self, author: &NodeId<PT>) -> Option<()> {
        if let Some(quota) = self.author_quota.get_mut(author) {
            if *quota == 0 {
                return None;
            }
            *quota -= 1;
            return Some(());
        }

        let initial_quota = if self.current_round.is_none() {
            AUTHOR_QUOTA_DURING_SYNC - 1
        } else {
            AUTHOR_QUOTA_DURING_LIVE - 1
        };
        self.author_quota.insert(*author, initial_quota);
        Some(())
    }
}

pub struct PrimaryRoundInfo<PT: PubKey> {
    assignment: Option<ChunkAssignment<PT>>,
    commitment: Option<EncodingCommitment>,
    // more info:
    //
    // - cache chunks for pulling
}

impl<PT: PubKey> Default for PrimaryRoundInfo<PT> {
    fn default() -> Self {
        Self {
            assignment: None,
            commitment: None,
        }
    }
}

impl<PT: PubKey> PrimaryRoundInfo<PT> {
    pub fn chunk_routing(
        &mut self,
        group: &PrimaryBroadcastGroup<'_, PT>,
        chunk: &ValidatedChunk<PT>,
    ) -> Option<ChunkRouting<'_, PT>> {
        // The construction of encoding and assignment should never
        // return None on a validated chunk where the app_message_len
        // is checked to be within valid range. The try operators
        // are defensive.
        if self.assignment.is_none() {
            let encoding = deterministic::PrimaryEncoding::new(
                chunk.encoding_scheme,
                group,
                chunk.app_message_len as usize,
                chunk.unix_ts_ms,
            )
            .ok()?;
            self.assignment = Some(encoding.make_assignment().ok()?);
        }

        self.assignment
            .as_ref()?
            .resolve_chunk_id(chunk.chunk_id as usize)
    }

    // Returns None if there is a conflicting commitment suggesting
    // publisher equivocation.
    #[must_use]
    pub fn try_commit(&mut self, chunk: &ValidatedChunk<PT>) -> Option<()> {
        try_commit_into(&mut self.commitment, chunk)
    }
}

pub struct SecondaryGroupRoundInfo<PT: PubKey> {
    assignment: Option<ChunkAssignment<PT>>,
    commitment: Option<EncodingCommitment>,
}

impl<PT: PubKey> Default for SecondaryGroupRoundInfo<PT> {
    fn default() -> Self {
        Self {
            assignment: None,
            commitment: None,
        }
    }
}

impl<PT: PubKey> SecondaryGroupRoundInfo<PT> {
    pub fn chunk_routing(
        &mut self,
        group: &SecondaryBroadcastGroup<'_, PT>,
        chunk: &ValidatedChunk<PT>,
    ) -> Option<ChunkRouting<'_, PT>> {
        if self.assignment.is_none() {
            let encoding = deterministic::SecondaryEncoding::new(
                chunk.encoding_scheme,
                group,
                chunk.app_message_len as usize,
                chunk.unix_ts_ms,
            )
            .ok()?;
            self.assignment = Some(encoding.make_assignment().ok()?);
        }

        self.assignment
            .as_ref()?
            .resolve_chunk_id(chunk.chunk_id as usize)
    }

    // Returns None if there is a conflicting commitment suggesting
    // publisher equivocation.
    #[must_use]
    pub fn try_commit(&mut self, chunk: &ValidatedChunk<PT>) -> Option<()> {
        try_commit_into(&mut self.commitment, chunk)
    }
}

fn try_commit_into<PT: PubKey>(
    slot: &mut Option<EncodingCommitment>,
    chunk: &ValidatedChunk<PT>,
) -> Option<()> {
    let Ok(claim) = ChunkCommitmentClaim::try_from(chunk) else {
        // not applicable, so we ignore this chunk for commitment.
        return Some(());
    };

    let Some(commitment) = slot else {
        // no commitment for this round yet, so we will commit to the
        // first claim we see.
        *slot = Some(EncodingCommitment::from(claim));
        return Some(());
    };
    if commitment.is_compatible_with(claim) {
        return Some(());
    }

    // log conflicting commitment once
    if !commitment.conflict_logged {
        tracing::error!(
            author = ?chunk.author,
            chunk_claim = ?claim,
            committed_claim = ?commitment.claim,
            "Conflicting commitment"
        );
        commitment.conflict_logged = true;
    }
    None
}

// Max number of published rounds to keep track of
pub(crate) const PUBLISHED_ROUNDS_CACHE_SIZE: usize = 100;

// Receivers commit to a single message per key (round for primary, (publisher,
// round) for secondary), every build mints conflicting commitments, which will
// flag this node as an equivocator.
// This tracks the rounds where a message has been built for.
pub(crate) struct PublishedRounds {
    rounds: BTreeSet<Round>,
}

impl PublishedRounds {
    pub fn new() -> Self {
        Self {
            rounds: BTreeSet::new(),
        }
    }

    // Claims the round, returning false if a message was already built
    #[must_use]
    pub fn try_claim(&mut self, round: Round) -> bool {
        if !self.rounds.insert(round) {
            return false;
        }
        while self.rounds.len() > PUBLISHED_ROUNDS_CACHE_SIZE {
            self.rounds.pop_first();
        }
        true
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ChunkCommitmentClaim {
    // the commitment is the data that faithfully identify a header:
    // root, encoding scheme, residual fields.
    round: Round,
    global_merkle_root: GlobalMerkleRoot,
    encoding_scheme_variant: u8,
    app_message_len: u32,
    unix_ts_ms: u64,
}

impl<PT: PubKey> TryFrom<&ValidatedChunk<PT>> for ChunkCommitmentClaim {
    type Error = ();

    fn try_from(chunk: &ValidatedChunk<PT>) -> Result<Self, ()> {
        let round = match chunk.encoding_scheme {
            EncodingScheme::Deterministic25(round) => round,
            EncodingScheme::Unspecified => return Err(()), // not applicable
        };
        let encoding_scheme_variant = chunk
            .encoding_scheme
            .variant()
            .expect("deterministic rc must have encoding scheme variant");
        let global_merkle_root = chunk
            .global_merkle_root()
            .expect("deterministic rc must have global merkle root");

        Ok(Self {
            round,
            global_merkle_root: *global_merkle_root,
            encoding_scheme_variant,
            app_message_len: chunk.app_message_len,
            unix_ts_ms: chunk.unix_ts_ms,
        })
    }
}

struct EncodingCommitment {
    claim: ChunkCommitmentClaim,

    // Remember whether this commitment has been logged as conflicting
    // with another commitment, set to avoid log spam.
    conflict_logged: bool,
}

impl From<ChunkCommitmentClaim> for EncodingCommitment {
    fn from(claim: ChunkCommitmentClaim) -> Self {
        Self {
            claim,
            conflict_logged: false,
        }
    }
}

impl EncodingCommitment {
    fn is_compatible_with(&self, claim: ChunkCommitmentClaim) -> bool {
        self.claim == claim
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use monad_crypto::{certificate_signature::PubKey as _, NopPubKey};
    use monad_types::NodeId;

    use super::*;
    use crate::{
        udp::{ChunkVersion, GroupId},
        util::{BroadcastMode, HexBytes, MerkleRoot},
    };

    type Cache = RoundInfoCache<NopPubKey>;

    const MERKLE_A: MerkleRoot = HexBytes([1; 20]);
    const MERKLE_B: MerkleRoot = HexBytes([2; 20]);
    const LEN_A: u32 = 100;
    const LEN_B: u32 = 200;

    fn author(seed: u8) -> NodeId<NopPubKey> {
        NodeId::new(NopPubKey::from_bytes(&[seed; 32]).unwrap())
    }

    fn dummy_chunk(
        round: u64,
        merkle: &MerkleRoot,
        app_message_len: u32,
        unix_ts_ms: u64,
    ) -> ValidatedChunk<NopPubKey> {
        ValidatedChunk {
            chunk: Bytes::new(),
            message: Bytes::new(),
            signature: Bytes::new(),
            author: NodeId::new(NopPubKey::from_bytes(&[0; 32]).unwrap()),
            group_id: GroupId::Primary(monad_types::Epoch(0)),
            unix_ts_ms,
            app_message_hash: None,
            app_message_len,
            recipient_hash: None,
            chunk_id: 0,
            version: ChunkVersion::V1,
            num_source_symbols: 0,
            encoded_symbol_capacity: 0,
            encoding_scheme: EncodingScheme::Deterministic25(Round(round)),
            broadcast_mode: BroadcastMode::Primary,
            merkle_root: *merkle,
        }
    }

    // -- RoundInfoCache tests --
    #[test]
    fn get_or_insert() {
        let mut cache = Cache::new();

        let a = author(0);

        // Any round accepted before first update_current_round.
        assert!(cache.get_or_insert_primary(Round(0), &a).is_some());
        assert!(cache.get_or_insert_primary(Round(200), &a).is_some());
        assert!(cache.get_or_insert_primary(Round(500), &a).is_some());

        // update_current_round evicts out-of-window entries.
        cache.update_current_round(Round(200));
        assert!(cache.get_primary(Round(0)).is_none());
        assert!(cache.get_primary(Round(200)).is_some());
        assert!(cache.get_primary(Round(500)).is_none());

        // Repeated insert returns existing entry.
        assert!(cache.get_or_insert_primary(Round(200), &a).is_some());
    }

    #[test]
    fn round_window_bounds() {
        let mut cache = Cache::new();
        cache.update_current_round(Round(200));
        let a = author(0);

        // Exactly at future boundary: 200 + 100 = 300, accepted.
        assert!(cache.get_or_insert_primary(Round(300), &a).is_some());
        // One past: rejected.
        assert!(cache.get_or_insert_primary(Round(301), &a).is_none());

        // Exactly at past boundary: 200 - 100 = 100, accepted.
        assert!(cache.get_or_insert_primary(Round(100), &a).is_some());
        // One past: rejected.
        assert!(cache.get_or_insert_primary(Round(99), &a).is_none());
    }

    #[test]
    fn eviction() {
        let mut cache = Cache::new();
        let a = author(0);
        cache.get_or_insert_primary(Round(10), &a);
        cache.get_or_insert_primary(Round(11), &a);
        cache.get_or_insert_primary(Round(199), &a);
        cache.get_or_insert_primary(Round(200), &a);

        // Future eviction: cutoff = 100 + 100 = 200, entries >= 200 are dropped.
        cache.update_current_round(Round(100));
        assert!(cache.get_primary(Round(199)).is_some());
        assert!(cache.get_primary(Round(200)).is_none());

        // Past eviction: advance to 111, cutoff = 111 - 100 = 11, entries < 11 are dropped.
        cache.update_current_round(Round(111));
        assert!(cache.get_primary(Round(10)).is_none());
        assert!(cache.get_primary(Round(11)).is_some());

        // In-window entries survive across rounds. Use a distinct author
        // per round so the per-author quota does not interfere with the
        // eviction-window behavior under test.
        let mut cache = Cache::new();
        cache.update_current_round(Round(100));
        for r in 50..=150 {
            cache.get_or_insert_primary(Round(r), &author(r as u8));
        }
        cache.update_current_round(Round(110));
        for r in 50..=150 {
            assert!(cache.get_primary(Round(r)).is_some());
        }
    }

    #[test]
    fn only_accept_compatible_claim() {
        let mut info = PrimaryRoundInfo::<NopPubKey>::default();
        assert!(info
            .try_commit(&dummy_chunk(10, &MERKLE_A, LEN_A, 0))
            .is_some());

        // Conflicting merkle root.
        assert!(info
            .try_commit(&dummy_chunk(10, &MERKLE_B, LEN_A, 0))
            .is_none());
        // Conflicting app message length.
        assert!(info
            .try_commit(&dummy_chunk(10, &MERKLE_A, LEN_B, 0))
            .is_none());
        // Conflicting timestamp (same timestamp bucket).
        assert!(info
            .try_commit(&dummy_chunk(10, &MERKLE_A, LEN_A, 2047))
            .is_none());
        // Compatible.
        assert!(info
            .try_commit(&dummy_chunk(10, &MERKLE_A, LEN_A, 0))
            .is_some());
    }

    #[test]
    fn independent_rounds_have_independent_commitments() {
        let mut info_10 = PrimaryRoundInfo::<NopPubKey>::default();
        let mut info_11 = PrimaryRoundInfo::<NopPubKey>::default();
        assert!(info_10
            .try_commit(&dummy_chunk(10, &MERKLE_A, LEN_A, 0))
            .is_some());
        assert!(info_11
            .try_commit(&dummy_chunk(11, &MERKLE_B, LEN_B, 0))
            .is_some());

        // Each round has its own commitment.
        assert!(info_10
            .try_commit(&dummy_chunk(10, &MERKLE_B, LEN_B, 0))
            .is_none());
        assert!(info_11
            .try_commit(&dummy_chunk(11, &MERKLE_A, LEN_A, 0))
            .is_none());
    }

    #[test]
    fn author_quota_during_sync() {
        let mut cache = Cache::new();
        let attacker = author(1);
        let honest = author(2);

        // An author can open exactly AUTHOR_QUOTA_DURING_SYNC distinct
        // rounds
        for r in 0..AUTHOR_QUOTA_DURING_SYNC as u64 {
            assert!(cache.get_or_insert_primary(Round(r), &attacker).is_some());
        }

        // Any additional rounds is blocked
        let blocked = Round(AUTHOR_QUOTA_DURING_SYNC as u64);
        assert!(cache.get_or_insert_primary(blocked, &attacker).is_none());
        assert!(cache.get_primary(blocked).is_none());

        // Only misses are charged
        assert!(cache.get_or_insert_primary(Round(0), &attacker).is_some());

        // The budget is per-author
        assert!(cache.get_or_insert_primary(blocked, &honest).is_some());
        assert!(cache.get_primary(blocked).is_some());

        // Entering a new round replenishes the budget
        cache.update_current_round(blocked);
        let next = Round(AUTHOR_QUOTA_DURING_SYNC as u64 + 1);
        assert!(cache.get_or_insert_primary(next, &attacker).is_some());
    }

    #[test]
    fn published_rounds_claim_once() {
        let mut published = PublishedRounds::new();
        assert!(published.try_claim(Round(10)));
        assert!(!published.try_claim(Round(10)));
        assert!(published.try_claim(Round(11)));
        assert!(!published.try_claim(Round(10)));
        assert!(!published.try_claim(Round(11)));
    }

    #[test]
    fn published_rounds_eviction() {
        let mut published = PublishedRounds::new();
        for r in 1..=PUBLISHED_ROUNDS_CACHE_SIZE as u64 + 1 {
            assert!(published.try_claim(Round(r)));
        }

        assert!(published.try_claim(Round(1)));

        // in-window rounds are still tracked
        assert!(!published.try_claim(Round(PUBLISHED_ROUNDS_CACHE_SIZE as u64)));
        assert!(!published.try_claim(Round(PUBLISHED_ROUNDS_CACHE_SIZE as u64 + 1)));
    }

    #[test]
    fn author_quota_during_live() {
        let mut cache = Cache::new();
        let a = author(1);

        cache.update_current_round(Round(1000));

        // Out-of-window rounds are rejected and do not charge the budget.
        assert!(cache.get_or_insert_primary(Round(2000), &a).is_none()); // > 1000 + 100
        assert!(cache.get_or_insert_primary(Round(800), &a).is_none()); // < 1000 - 100

        // The full live budget is still available for in-window rounds,
        // confirming the rejected out-of-window rounds were not charged.
        for i in 0..AUTHOR_QUOTA_DURING_LIVE as u64 {
            assert!(cache.get_or_insert_primary(Round(1000 + i), &a).is_some());
        }

        // One past the budget is rejected even though it is in-window.
        let over = Round(1000 + AUTHOR_QUOTA_DURING_LIVE as u64);
        assert!(cache.get_or_insert_primary(over, &a).is_none());

        // Entering a new round replenishes the budget, so the same author
        // can open the previously-blocked round.
        cache.update_current_round(Round(1001));
        assert!(cache.get_or_insert_primary(over, &a).is_some());
    }
}
