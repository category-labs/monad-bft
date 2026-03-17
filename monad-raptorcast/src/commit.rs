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
use tracing::error;

use crate::{
    udp::ValidatedChunk,
    util::{EncodingScheme, GlobalMerkleRoot},
    SIGNATURE_SIZE,
};

// We keep commitments for a sliding window of rounds:
//   [curr_round - PAST_ROUND_RETENTION, curr_round + FUTURE_ROUND_ALLOWANCE]
//
// Chunks with rounds outside of this window are rejected.

// the number of past rounds to keep, so we can validate against
// former commitments and rebroadcast to other validators.
const PAST_ROUND_RETENTION: Round = Round(5);

// how many future rounds to accept, so a lagging node can receive
// near-future proposals, while also preventing unbounded buffering.
const FUTURE_ROUND_ALLOWANCE: Round = Round(5);

// Deterministic Raptorcast requires the validator to commit to a
// singular proposal per round, which is identified by
// (global_merkle_root, signature) tuple.
pub(crate) struct RoundCommitments {
    // max_round and min_round are only effective only after the first
    // UpdateCurrentRound command to allow for receiving proposals
    // immediately on node startup where the current round is not yet
    // known.
    min_round: Round,
    max_round: Option<Round>,
    map: BTreeMap<Round, EncodingCommitment>,
}

impl Default for RoundCommitments {
    fn default() -> Self {
        Self {
            min_round: Round(0),
            max_round: None,
            map: BTreeMap::new(),
        }
    }
}

#[derive(Debug)]
pub(crate) enum TryCommitError {
    #[allow(unused)] // fields currently unused
    RoundOutOfBounds {
        round: Round,
        min: Round,
        max: Round,
    },
    ConflictingCommitment,
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

impl RoundCommitments {
    pub fn update_current_round(&mut self, current_round: Round) {
        let min_round = Round(current_round.0.saturating_sub(PAST_ROUND_RETENTION.0));
        let max_round = Round(current_round.0.saturating_add(FUTURE_ROUND_ALLOWANCE.0));

        self.min_round = min_round;
        self.max_round = Some(max_round);

        // Remove commitments for rounds that are now out of bounds.
        self.map
            .retain(|&round, _| round >= self.min_round && round <= max_round);
    }

    pub fn try_commit<PT: PubKey>(
        &mut self,
        chunk: &ValidatedChunk<PT>,
    ) -> Result<(), TryCommitError> {
        let Ok(claim) = ChunkCommitmentClaim::try_from(chunk) else {
            return Ok(()); // protocol not applicable, ignore
        };

        self.verify_round(claim.round)?;
        self.verify_commitment(claim, chunk)?;

        Ok(())
    }

    fn verify_round(&self, chunk_round: Round) -> Result<(), TryCommitError> {
        let Some(max_round) = self.max_round else {
            // we do not know the current round yet, so we simply
            // accept any rounds until the first update_current_round
            // call.
            return Ok(());
        };

        if self.min_round > chunk_round || chunk_round > max_round {
            return Err(TryCommitError::RoundOutOfBounds {
                round: chunk_round,
                min: self.min_round,
                max: max_round,
            });
        }

        Ok(())
    }

    fn verify_commitment<PT: PubKey>(
        &mut self,
        claim: ChunkCommitmentClaim<'_>,
        chunk: &ValidatedChunk<PT>, // for logging only
    ) -> Result<(), TryCommitError> {
        use std::collections::btree_map::Entry;

        match self.map.entry(claim.round) {
            Entry::Vacant(entry) => {
                // for the round, we haven't committed to any claims
                // yet, so we will commit to this first claim.
                entry.insert(EncodingCommitment::from(claim));
                Ok(())
            }

            Entry::Occupied(mut entry) => {
                let commitment = entry.get_mut();
                if commitment.is_compatible_with(claim) {
                    return Ok(());
                }

                // log conflicting commitment once
                if !commitment.conflict_logged {
                    error!(
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

                Err(TryCommitError::ConflictingCommitment)
            }
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.map.len()
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
        util::{BroadcastMode, EncodingScheme, HexBytes, MerkleRoot},
    };

    const SIG_A: [u8; SIGNATURE_SIZE] = [0xAA; SIGNATURE_SIZE];
    const SIG_B: [u8; SIGNATURE_SIZE] = [0xBB; SIGNATURE_SIZE];
    const MERKLE_A: MerkleRoot = HexBytes([1; 20]);
    const MERKLE_B: MerkleRoot = HexBytes([2; 20]);

    fn dummy_chunk() -> ValidatedChunk<NopPubKey> {
        ValidatedChunk {
            chunk: Bytes::new(),
            message: Bytes::new(),
            author: NodeId::new(NopPubKey::from_bytes(&[0; 32]).unwrap()),
            group_id: GroupId::Primary(monad_types::Epoch(0)),
            unix_ts_ms: 0,
            app_message_hash: None,
            app_message_len: 0,
            recipient_hash: HexBytes([0; 20]),
            chunk_id: 0,
            num_source_symbols: 0,
            encoded_symbol_capacity: 0,
            version: 0,
            encoding_scheme: EncodingScheme::Unspecified,
            broadcast_mode: BroadcastMode::Primary,
            signature: Bytes::copy_from_slice(&SIG_A),
            merkle_root: MERKLE_A,
        }
    }

    fn chunk(
        round: u64,
        sig: &[u8; SIGNATURE_SIZE],
        merkle: &MerkleRoot,
    ) -> ValidatedChunk<NopPubKey> {
        ValidatedChunk {
            signature: Bytes::copy_from_slice(sig),
            version: 1,
            broadcast_mode: BroadcastMode::Primary,
            encoding_scheme: EncodingScheme::Deterministic25(Round(round)),
            merkle_root: *merkle,
            ..dummy_chunk()
        }
    }

    fn non_deterministic_chunk() -> ValidatedChunk<NopPubKey> {
        ValidatedChunk {
            version: 0,
            broadcast_mode: BroadcastMode::Primary,
            encoding_scheme: EncodingScheme::Unspecified,
            ..dummy_chunk()
        }
    }

    #[test]
    fn non_deterministic_chunk_is_ignored() {
        let mut rc = RoundCommitments::default();
        rc.update_current_round(Round(10));
        assert!(rc.try_commit(&non_deterministic_chunk()).is_ok());
        assert!(rc.len() == 0);
    }

    #[test]
    fn only_accept_compatible_claim() {
        let mut rc = RoundCommitments::default();
        rc.update_current_round(Round(10));
        assert!(rc.try_commit(&chunk(10, &SIG_A, &MERKLE_A)).is_ok());

        // conflicting signature
        assert!(rc.try_commit(&chunk(10, &SIG_B, &MERKLE_A)).is_err());

        // conflicting merkle root
        assert!(rc.try_commit(&chunk(10, &SIG_A, &MERKLE_B)).is_err());

        // compatible
        assert!(rc.try_commit(&chunk(10, &SIG_A, &MERKLE_A)).is_ok());
    }

    #[test]
    fn round_window_boundaries() {
        let mut rc = RoundCommitments::default();

        // accept any rounds before the first update_current_round call
        assert!(rc.try_commit(&chunk(0, &SIG_A, &MERKLE_A)).is_ok());
        assert!(rc.try_commit(&chunk(9999, &SIG_A, &MERKLE_A)).is_ok());

        rc.update_current_round(Round(10));
        // window is [10 - 5, 10 + 5] = [5, 15]
        assert!(rc.len() == 0); // OOB commitments evicted

        assert!(rc.try_commit(&chunk(4, &SIG_A, &MERKLE_A)).is_err());
        assert!(rc.try_commit(&chunk(5, &SIG_A, &MERKLE_A)).is_ok());
        assert!(rc.try_commit(&chunk(15, &SIG_A, &MERKLE_A)).is_ok());
        assert!(rc.try_commit(&chunk(16, &SIG_A, &MERKLE_A)).is_err());

        rc.update_current_round(Round(1));
        // window is [0 (saturated), 6]
        assert!(rc.try_commit(&chunk(0, &SIG_A, &MERKLE_A)).is_ok());
        assert!(rc.try_commit(&chunk(6, &SIG_A, &MERKLE_A)).is_ok());
        assert!(rc.try_commit(&chunk(7, &SIG_A, &MERKLE_A)).is_err());
    }

    #[test]
    fn independent_rounds_have_independent_commitments() {
        let mut rc = RoundCommitments::default();
        rc.update_current_round(Round(10));
        assert!(rc.try_commit(&chunk(10, &SIG_A, &MERKLE_A)).is_ok());
        assert!(rc.try_commit(&chunk(11, &SIG_B, &MERKLE_B)).is_ok());

        // each round has its own commitment
        assert!(rc.try_commit(&chunk(10, &SIG_B, &MERKLE_B)).is_err());
        assert!(rc.try_commit(&chunk(11, &SIG_A, &MERKLE_A)).is_err());
    }
}
