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
    ingest_types::Hash32,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, RlpEncodable, RlpDecodable)]
pub struct PrimaryId(u64);

impl PrimaryId {
    pub const ZERO: Self = Self(0);

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
/// variant shares `PrimaryId`'s representation.
macro_rules! family_id {
    ($name:ident, $family:literal) => {
        #[doc = concat!("`PrimaryId` scoped to the ", $family, " family. Kept as a distinct")]
        #[doc = "type so that family's signatures don't accept primary ids from other"]
        #[doc = "families by mistake."]
        #[derive(
            Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, RlpEncodable, RlpDecodable,
        )]
        pub struct $name(PrimaryId);

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
    /// Physical blob object key. Empty means this block owns its object under
    /// the deterministic `block_number` key (set explicitly once the coalescer
    /// repoints the block into a shared object).
    pub physical_key: Vec<u8>,
    /// Byte offset of this block's shared blob inside `physical_key`.
    pub physical_base_offset: u64,
}

impl BlockBlobHeader {
    /// Number of stored rows in this block (offsets length minus the sentinel).
    pub fn row_count(&self) -> usize {
        self.offsets.len().saturating_sub(1)
    }

    /// Absolute byte offset of this family's region within `physical_key`.
    fn base(&self) -> usize {
        self.physical_base_offset as usize + self.base_offset as usize
    }

    pub fn abs_range(&self, idx: usize) -> (usize, usize) {
        let base = self.base();
        (
            base + self.offsets[idx] as usize,
            base + self.offsets[idx + 1] as usize,
        )
    }

    pub fn region_range(&self) -> (usize, usize) {
        let base = self.base();
        (base, base + *self.offsets.last().unwrap_or(&0) as usize)
    }

    pub fn physical_key_or<'a>(&'a self, default: &'a [u8]) -> &'a [u8] {
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
        decode_rlp(bytes, "invalid block blob header rlp")
    }
}

/// Shared RLP decode body: `decode_exact` mapping any failure to a
/// [`MonadChainDataError::Decode`] carrying the call site's message.
fn decode_rlp<T: alloy_rlp::Decodable>(bytes: &[u8], err: &'static str) -> Result<T> {
    alloy_rlp::decode_exact(bytes).map_err(|_| MonadChainDataError::Decode(err))
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
    /// Per-block trace family window. Its addition was a hard break of the
    /// on-disk RLP layout; pre-trace data dirs must be wiped and re-ingested.
    pub traces: FamilyWindowRecord,
    /// Head of the per-block content-digest chain (see [`crate::engine::digest`]):
    /// a standby re-deriving the chain from the same finalized blocks must reach
    /// this exact value. Its addition was a hard break of the on-disk RLP layout.
    pub row_chain: Hash32,
}

impl BlockRecord {
    pub fn encode(&self) -> Vec<u8> {
        alloy_rlp::encode(self)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        decode_rlp(bytes, "invalid block record rlp")
    }
}

/// The single publication row: the reader-visible head watermark plus the
/// row chain at that head (see [`crate::engine::digest`]).
/// Field order is the on-disk RLP list order; reordering changes the wire format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct PublicationState {
    pub indexed_finalized_head: u64,
    /// Row chain at `indexed_finalized_head`, mirroring the
    /// published head block's [`BlockRecord::row_chain`].
    pub head_row_chain: Hash32,
}

impl PublicationState {
    pub fn encode(self) -> Vec<u8> {
        alloy_rlp::encode(self)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        decode_rlp(bytes, "invalid publication state rlp")
    }
}

#[cfg(test)]
mod tests {
    use super::{Hash32, PrimaryId, PublicationState};

    #[test]
    fn primary_id_checked_add_rejects_overflow() {
        assert!(PrimaryId::new(u64::MAX).checked_add(1).is_err());
        assert_eq!(
            PrimaryId::new(7).checked_add(35).expect("no overflow"),
            PrimaryId::new(42)
        );
    }

    #[test]
    fn publication_state_round_trips() {
        let state = PublicationState {
            indexed_finalized_head: 91,
            head_row_chain: Hash32::repeat_byte(0xab),
        };
        let encoded = state.encode();
        let decoded = PublicationState::decode(&encoded).expect("decode state");
        assert_eq!(decoded, state);
    }
}
