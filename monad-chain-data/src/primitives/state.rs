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

use alloy_rlp::{RlpDecodable, RlpEncodable};

use crate::{
    error::{MonadChainDataError, Result},
    family::Hash32,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, RlpEncodable, RlpDecodable)]
pub struct PrimaryId(u64);

impl PrimaryId {
    pub const ZERO: Self = Self(0);
    /// Number of low bits used for the in-shard local index. The remaining
    /// high bits encode the shard. Shared with `engine::bitmap` so the on-disk
    /// bit layout cannot drift between encode and decode sites.
    pub const LOCAL_ID_BITS: u32 = 24;
    const LOCAL_ID_MASK: u64 = (1u64 << Self::LOCAL_ID_BITS) - 1;

    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn as_u64(self) -> u64 {
        self.0
    }

    pub fn checked_add(self, rhs: u64) -> Result<Self> {
        self.0
            .checked_add(rhs)
            .map(Self)
            .ok_or(MonadChainDataError::Decode("primary id overflow"))
    }

    pub const fn shard(self) -> u64 {
        self.0 >> Self::LOCAL_ID_BITS
    }

    pub const fn local(self) -> u32 {
        (self.0 & Self::LOCAL_ID_MASK) as u32
    }

    pub fn from_parts(shard: u64, local: u32) -> Result<Self> {
        if u64::from(local) > Self::LOCAL_ID_MASK {
            return Err(MonadChainDataError::Decode(
                "primary id local part overflow",
            ));
        }
        if shard > (u64::MAX >> Self::LOCAL_ID_BITS) {
            return Err(MonadChainDataError::Decode("primary id shard overflow"));
        }

        let shifted_shard = shard
            .checked_shl(Self::LOCAL_ID_BITS)
            .ok_or(MonadChainDataError::Decode("primary id shard overflow"))?;
        Ok(Self(shifted_shard | u64::from(local)))
    }

    pub fn idx_in_block(self, first: PrimaryId) -> Result<usize> {
        let delta = self
            .0
            .checked_sub(first.0)
            .ok_or(MonadChainDataError::Decode("primary id below block start"))?;
        usize::try_from(delta)
            .map_err(|_| MonadChainDataError::Decode("primary block index overflow"))
    }
}

/// Defines a `PrimaryId` newtype scoped to one record family, so a family's
/// signatures can't accidentally accept ids minted for another family. Each
/// variant shares `PrimaryId`'s representation and forwards to the inner id.
macro_rules! family_id {
    ($name:ident, $family:literal) => {
        #[doc = concat!("`PrimaryId` scoped to the ", $family, " family. Kept as a distinct")]
        #[doc = "type so that family's signatures don't accept primary ids from other"]
        #[doc = "families by mistake."]
        #[derive(
            Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, RlpEncodable, RlpDecodable,
        )]
        pub struct $name(PrimaryId);

        impl $name {
            pub const ZERO: Self = Self(PrimaryId::ZERO);

            pub fn checked_add(self, rhs: u64) -> Result<Self> {
                self.0.checked_add(rhs).map(Self)
            }

            pub const fn shard(self) -> u64 {
                self.0.shard()
            }

            pub const fn local(self) -> u32 {
                self.0.local()
            }
        }

        impl From<PrimaryId> for $name {
            fn from(id: PrimaryId) -> Self {
                Self(id)
            }
        }

        impl From<$name> for PrimaryId {
            fn from(id: $name) -> Self {
                id.0
            }
        }
    };
}

family_id!(LogId, "log");
family_id!(TxId, "tx");
family_id!(TraceId, "trace");

/// Byte offsets into one family's per-block blob: one entry per stored row plus
/// a trailing sentinel equal to the total blob length, so the length is
/// `row_count + 1`. The log, tx and trace families all use this identical
/// on-disk layout.
#[derive(Debug, Clone, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct BlockBlobHeader {
    pub offsets: Vec<u32>,
    /// Row-codec dictionary version every frame in this block's blob was
    /// compressed under. `0` = plain zstd frames (no dictionary).
    pub dict_version: u32,
    /// Start of this family's region within the shared per-block blob.
    pub base_offset: u32,
    /// Physical blob object key. Empty means the legacy deterministic
    /// `block_number` key.
    pub physical_key: Vec<u8>,
    /// Byte offset of this block's shared blob inside `physical_key`.
    pub physical_base_offset: u64,
}

impl BlockBlobHeader {
    /// Number of stored rows in this block (offsets length minus the sentinel).
    pub fn row_count(&self) -> usize {
        self.offsets.len().saturating_sub(1)
    }

    pub fn abs_range(&self, idx: usize) -> (usize, usize) {
        let base = self.physical_base_offset as usize + self.base_offset as usize;
        (
            base + self.offsets[idx] as usize,
            base + self.offsets[idx + 1] as usize,
        )
    }

    pub fn region_range(&self) -> (usize, usize) {
        let base = self.physical_base_offset as usize + self.base_offset as usize;
        (base, base + *self.offsets.last().unwrap_or(&0) as usize)
    }

    pub fn physical_key<'a>(&'a self, default: &'a [u8]) -> &'a [u8] {
        if self.physical_key.is_empty() {
            default
        } else {
            &self.physical_key
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        alloy_rlp::encode(self)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        alloy_rlp::decode_exact(bytes)
            .map_err(|_| MonadChainDataError::Decode("invalid block blob header rlp"))
    }
}

#[cfg(test)]
mod tests {
    use super::PrimaryId;

    #[test]
    fn primary_id_from_parts_rejects_local_overflow() {
        assert!(PrimaryId::from_parts(0, 1 << PrimaryId::LOCAL_ID_BITS).is_err());
    }

    #[test]
    fn primary_id_from_parts_rejects_shard_overflow() {
        let overflowing_shard = (u64::MAX >> PrimaryId::LOCAL_ID_BITS) + 1;

        assert!(PrimaryId::from_parts(overflowing_shard, 0).is_err());
    }

    #[test]
    fn primary_id_from_parts_round_trips_shard_and_local() {
        let id = PrimaryId::from_parts(7, 42).expect("valid primary id parts");

        assert_eq!(id.shard(), 7);
        assert_eq!(id.local(), 42);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct FamilyWindowRecord {
    pub first_primary_id: PrimaryId,
    pub count: u32,
}

impl FamilyWindowRecord {
    pub fn next_primary_id_exclusive(self) -> Result<PrimaryId> {
        self.first_primary_id.checked_add(u64::from(self.count))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct BlockRecord {
    pub block_number: u64,
    pub block_hash: Hash32,
    pub parent_hash: Hash32,
    pub logs: FamilyWindowRecord,
    pub txs: FamilyWindowRecord,
    /// Per-block trace family window. Adding this field is a hard break
    /// of the on-disk RLP layout; data dirs from before the trace family
    /// existed must be wiped and re-ingested.
    pub traces: FamilyWindowRecord,
    /// Running artifact checksum chained through this block: the head value of
    /// the per-block content-digest chain (see [`crate::engine::digest`]). A
    /// standby that re-derives the chain from the same finalized blocks must
    /// arrive at this exact value, proving it would have written the same
    /// artifacts. Adding this field is a hard break of the on-disk RLP layout.
    pub artifact_checksum: Hash32,
}

impl BlockRecord {
    pub fn encode(&self) -> Vec<u8> {
        alloy_rlp::encode(self)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        alloy_rlp::decode_exact(bytes)
            .map_err(|_| MonadChainDataError::Decode("invalid block record rlp"))
    }
}

/// Per-process session identity for the write-authority lease. Distinct from
/// the stable per-node `owner_id`: a restarted process reuses its `owner_id`
/// but generates a fresh `session_id`, so it always takes the takeover /
/// reacquire path (and thus runs recovery). See `engine::authority`.
pub type SessionId = [u8; 16];

/// The single CAS-guarded coordination row. `indexed_finalized_head` is the
/// reader-visible publication watermark and `head_artifact_checksum` is the
/// chained artifact checksum at that head (see [`crate::engine::digest`]); the
/// lease fields (`owner_id` / `session_id` / `lease_valid_through_block`) are
/// the outer-ring write-authority state and are never consulted by the query
/// path.
///
/// Field order is the on-disk RLP list order — do not reorder without a
/// migration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct PublicationState {
    pub indexed_finalized_head: u64,
    pub owner_id: u64,
    pub session_id: SessionId,
    pub lease_valid_through_block: u64,
    /// Chained artifact checksum at `indexed_finalized_head`, mirroring the
    /// published head block's [`BlockRecord::artifact_checksum`]. Preserved
    /// across lease renew/takeover; only a head advance updates it.
    pub head_artifact_checksum: Hash32,
}

impl PublicationState {
    pub fn encode(self) -> Vec<u8> {
        alloy_rlp::encode(self)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        alloy_rlp::decode_exact::<Self>(bytes)
            .map_err(|_| MonadChainDataError::Decode("invalid publication state rlp"))
    }
}

#[cfg(test)]
mod publication_state_tests {
    use super::{Hash32, PublicationState};

    #[test]
    fn publication_state_round_trips() {
        let state = PublicationState {
            indexed_finalized_head: 91,
            owner_id: 17,
            session_id: *b"session-id-00001",
            lease_valid_through_block: 123,
            head_artifact_checksum: Hash32::repeat_byte(0xab),
        };
        let encoded = state.encode();
        let decoded = PublicationState::decode(&encoded).expect("decode state");
        assert_eq!(decoded, state);
    }
}
