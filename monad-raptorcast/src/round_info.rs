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

use std::collections::BTreeMap;

use monad_crypto::certificate_signature::PubKey;
use monad_types::Round;

use crate::{
    packet::{
        assigner::{ChunkAssignment, ChunkRouting, StakePartition},
        deterministic,
    },
    udp::ValidatedChunk,
    util::{EncodingScheme, GlobalMerkleRoot, PrimaryBroadcastGroup},
    SIGNATURE_SIZE,
};

const CACHE_MAX_FUTURE_ROUNDS: Round = Round(100);
const CACHE_MAX_PAST_ROUNDS: Round = Round(100);

// Stores information related to the current round.
pub struct RoundInfoCache<PT: PubKey> {
    current_round: Option<Round>,
    primary: BTreeMap<Round, PrimaryRoundInfo<PT>>,
}

impl<PT: PubKey> RoundInfoCache<PT> {
    pub fn new() -> Self {
        Self {
            current_round: None,
            primary: BTreeMap::new(),
        }
    }

    pub fn update_current_round(&mut self, round: Round) {
        if let Some(current) = self.current_round {
            debug_assert!(
                round >= current,
                "Cannot enter a past round: current {}, new {}",
                current,
                round
            );
        }

        self.current_round = Some(round);

        // Evict rounds from the cache
        if let Some(cutoff_future) = round.checked_add(CACHE_MAX_FUTURE_ROUNDS) {
            drop(self.primary.split_off(&cutoff_future));
        };
        if let Some(cutoff_past) = round.checked_sub(CACHE_MAX_PAST_ROUNDS) {
            let mut active = self.primary.split_off(&cutoff_past);
            std::mem::swap(&mut self.primary, &mut active);
        }
    }

    // Returns None on out-of-window round
    pub fn get_or_insert_primary(&mut self, round: Round) -> Option<&mut PrimaryRoundInfo<PT>> {
        if !self.primary.contains_key(&round) {
            self.check_round(round)?;
            self.primary.insert(round, Default::default());
        }
        self.primary.get_mut(&round)
    }

    #[cfg(test)]
    fn get_primary(&self, round: Round) -> Option<&PrimaryRoundInfo<PT>> {
        self.primary.get(&round)
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
}

pub struct PrimaryRoundInfo<PT: PubKey> {
    encoding: Option<(deterministic::PrimaryEncoding<PT>, ChunkAssignment)>,
    commitment: Option<EncodingCommitment>,
    // more info:
    //
    // - cache chunks for pulling
}

impl<PT: PubKey> Default for PrimaryRoundInfo<PT> {
    fn default() -> Self {
        Self {
            encoding: None,
            commitment: None,
        }
    }
}

impl<PT: PubKey> PrimaryRoundInfo<PT> {
    pub fn chunk_routing(
        &mut self,
        group: &PrimaryBroadcastGroup<'_, PT>,
        chunk: &ValidatedChunk<PT>,
    ) -> Option<ChunkRouting<'_, PT, StakePartition<PT>>> {
        // The construction of encoding and assignment should never
        // return None on a validated chunk where the app_message_len
        // is checked to be within valid range. The try operators
        // are defensive.
        if self.encoding.is_none() {
            let encoding = deterministic::PrimaryEncoding::new(
                chunk.encoding_scheme,
                group,
                chunk.app_message_len as usize,
                chunk.unix_ts_ms,
            )
            .ok()?;
            let assignment = encoding.make_assignment().ok()?;
            self.encoding = Some((encoding, assignment));
        }

        if let Some((encoding, assignment)) = &self.encoding {
            return assignment.resolve_chunk_id(chunk.chunk_id as usize, encoding.partition());
        }

        None
    }

    // Returns None if there is a conflicting commitment suggesting
    // publisher equivocation.
    #[must_use]
    pub fn try_commit(&mut self, chunk: &ValidatedChunk<PT>) -> Option<()> {
        let Ok(claim) = ChunkCommitmentClaim::try_from(chunk) else {
            // not applicable, so we ignore this chunk for commitment.
            return Some(());
        };

        let Some(commitment) = &mut self.commitment else {
            // no commitment for this round yet, so we will commit to
            // the first claim we see.
            self.commitment = Some(EncodingCommitment::from(claim));
            return Some(());
        };
        if commitment.is_compatible_with(claim) {
            return Some(());
        }

        // log conflicting commitment once
        if !commitment.conflict_logged {
            tracing::error!(
                author = ?chunk.author,
                round = ?claim.round,
                chunk_merkle_root = ?claim.global_merkle_root,
                commit_merkle_root = ?commitment.global_merkle_root,
                chunk_signature = ?claim.signature,
                commit_signature = ?commitment.signature,
                "Conflicting commitment"
            );
            commitment.conflict_logged = true;
        }
        None
    }
}

type Signature = [u8; SIGNATURE_SIZE];

#[derive(Clone, Copy)]
struct ChunkCommitmentClaim<'a> {
    round: Round,
    signature: &'a Signature,
    global_merkle_root: &'a GlobalMerkleRoot,
}

impl<'a, PT> TryFrom<&'a ValidatedChunk<PT>> for ChunkCommitmentClaim<'a>
where
    PT: PubKey,
{
    type Error = ();

    fn try_from(chunk: &'a ValidatedChunk<PT>) -> Result<Self, ()> {
        let round = match chunk.encoding_scheme {
            EncodingScheme::Deterministic25(round) => round,
            EncodingScheme::Unspecified => return Err(()), // not applicable
        };
        let global_merkle_root = chunk
            .global_merkle_root()
            .expect("deterministic rc must have global merkle root");
        let signature = <&[u8; SIGNATURE_SIZE]>::try_from(chunk.signature.as_ref())
            .expect("signature of validated chunk must have correct length");

        Ok(Self {
            signature,
            global_merkle_root,
            round,
        })
    }
}

struct EncodingCommitment {
    signature: Signature,
    global_merkle_root: GlobalMerkleRoot,

    // Remember whether this commitment has been logged as conflicting
    // with another commitment, set to avoid log spam.
    conflict_logged: bool,
}

impl From<ChunkCommitmentClaim<'_>> for EncodingCommitment {
    fn from(claim: ChunkCommitmentClaim<'_>) -> Self {
        Self {
            signature: *claim.signature,
            global_merkle_root: *claim.global_merkle_root,
            conflict_logged: false,
        }
    }
}

impl EncodingCommitment {
    fn is_compatible_with(&self, claim: ChunkCommitmentClaim<'_>) -> bool {
        if self.global_merkle_root != *claim.global_merkle_root
            || self.signature != *claim.signature
        {
            return false;
        }

        true
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use monad_crypto::{certificate_signature::PubKey as _, NopPubKey};
    use monad_types::NodeId;

    use super::*;
    use crate::{
        udp::GroupId,
        util::{BroadcastMode, HexBytes, MerkleRoot},
    };

    type Cache = RoundInfoCache<NopPubKey>;

    const SIG_A: [u8; SIGNATURE_SIZE] = [0xAA; SIGNATURE_SIZE];
    const SIG_B: [u8; SIGNATURE_SIZE] = [0xBB; SIGNATURE_SIZE];
    const MERKLE_A: MerkleRoot = HexBytes([1; 20]);
    const MERKLE_B: MerkleRoot = HexBytes([2; 20]);

    fn dummy_chunk(
        round: u64,
        sig: &[u8; SIGNATURE_SIZE],
        merkle: &MerkleRoot,
    ) -> ValidatedChunk<NopPubKey> {
        ValidatedChunk {
            chunk: Bytes::new(),
            message: Bytes::new(),
            signature: Bytes::copy_from_slice(sig),
            author: NodeId::new(NopPubKey::from_bytes(&[0; 32]).unwrap()),
            group_id: GroupId::Primary(monad_types::Epoch(0)),
            unix_ts_ms: 0,
            app_message_hash: None,
            app_message_len: 0,
            recipient_hash: None,
            chunk_id: 0,
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

        // Any round accepted before first update_current_round.
        assert!(cache.get_or_insert_primary(Round(0)).is_some());
        assert!(cache.get_or_insert_primary(Round(200)).is_some());
        assert!(cache.get_or_insert_primary(Round(500)).is_some());

        // update_current_round evicts out-of-window entries.
        cache.update_current_round(Round(200));
        assert!(cache.get_primary(Round(0)).is_none());
        assert!(cache.get_primary(Round(200)).is_some());
        assert!(cache.get_primary(Round(500)).is_none());

        // Repeated insert returns existing entry.
        assert!(cache.get_or_insert_primary(Round(200)).is_some());
    }

    #[test]
    fn round_window_bounds() {
        let mut cache = Cache::new();
        cache.update_current_round(Round(200));

        // Exactly at future boundary: 200 + 100 = 300, accepted.
        assert!(cache.get_or_insert_primary(Round(300)).is_some());
        // One past: rejected.
        assert!(cache.get_or_insert_primary(Round(301)).is_none());

        // Exactly at past boundary: 200 - 100 = 100, accepted.
        assert!(cache.get_or_insert_primary(Round(100)).is_some());
        // One past: rejected.
        assert!(cache.get_or_insert_primary(Round(99)).is_none());
    }

    #[test]
    fn eviction() {
        let mut cache = Cache::new();
        cache.update_current_round(Round(100));
        cache.get_or_insert_primary(Round(10));
        cache.get_or_insert_primary(Round(11));
        cache.get_or_insert_primary(Round(199));
        cache.get_or_insert_primary(Round(200));

        // Future eviction: cutoff = 100 + 100 = 200, entries >= 200 are dropped.
        cache.update_current_round(Round(100));
        assert!(cache.get_primary(Round(199)).is_some());
        assert!(cache.get_primary(Round(200)).is_none());

        // Past eviction: advance to 111, cutoff = 111 - 100 = 11, entries < 11 are dropped.
        cache.update_current_round(Round(111));
        assert!(cache.get_primary(Round(10)).is_none());
        assert!(cache.get_primary(Round(11)).is_some());

        // In-window entries survive across rounds.
        let mut cache = Cache::new();
        cache.update_current_round(Round(100));
        for r in 50..=150 {
            cache.get_or_insert_primary(Round(r));
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
            .try_commit(&dummy_chunk(10, &SIG_A, &MERKLE_A))
            .is_some());

        // Conflicting signature.
        assert!(info
            .try_commit(&dummy_chunk(10, &SIG_B, &MERKLE_A))
            .is_none());
        // Conflicting merkle root.
        assert!(info
            .try_commit(&dummy_chunk(10, &SIG_A, &MERKLE_B))
            .is_none());
        // Compatible.
        assert!(info
            .try_commit(&dummy_chunk(10, &SIG_A, &MERKLE_A))
            .is_some());
    }

    #[test]
    fn independent_rounds_have_independent_commitments() {
        let mut info_10 = PrimaryRoundInfo::<NopPubKey>::default();
        let mut info_11 = PrimaryRoundInfo::<NopPubKey>::default();
        assert!(info_10
            .try_commit(&dummy_chunk(10, &SIG_A, &MERKLE_A))
            .is_some());
        assert!(info_11
            .try_commit(&dummy_chunk(11, &SIG_B, &MERKLE_B))
            .is_some());

        // Each round has its own commitment.
        assert!(info_10
            .try_commit(&dummy_chunk(10, &SIG_B, &MERKLE_B))
            .is_none());
        assert!(info_11
            .try_commit(&dummy_chunk(11, &SIG_A, &MERKLE_A))
            .is_none());
    }
}
