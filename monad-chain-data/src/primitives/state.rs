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

/// `PrimaryId` scoped to the log family. Kept as a distinct type so log-domain
/// signatures don't accept primary ids from other families by mistake.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, RlpEncodable, RlpDecodable)]
pub struct LogId(PrimaryId);

impl LogId {
    pub const ZERO: Self = Self(PrimaryId::ZERO);

    pub const fn new(value: u64) -> Self {
        Self(PrimaryId::new(value))
    }

    pub const fn as_u64(self) -> u64 {
        self.0.as_u64()
    }

    pub fn checked_add(self, rhs: u64) -> Result<Self> {
        self.0.checked_add(rhs).map(Self)
    }

    pub const fn shard(self) -> u64 {
        self.0.shard()
    }

    pub const fn local(self) -> u32 {
        self.0.local()
    }

    pub fn from_parts(shard: u64, local: u32) -> Result<Self> {
        PrimaryId::from_parts(shard, local).map(Self)
    }
}

impl From<PrimaryId> for LogId {
    fn from(id: PrimaryId) -> Self {
        Self(id)
    }
}

impl From<LogId> for PrimaryId {
    fn from(id: LogId) -> Self {
        id.0
    }
}

/// `PrimaryId` scoped to the tx family. Kept as a distinct type so tx-domain
/// signatures don't accept primary ids from other families by mistake.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, RlpEncodable, RlpDecodable)]
pub struct TxId(PrimaryId);

impl TxId {
    pub const ZERO: Self = Self(PrimaryId::ZERO);

    pub const fn new(value: u64) -> Self {
        Self(PrimaryId::new(value))
    }

    pub const fn as_u64(self) -> u64 {
        self.0.as_u64()
    }

    pub fn checked_add(self, rhs: u64) -> Result<Self> {
        self.0.checked_add(rhs).map(Self)
    }

    pub const fn shard(self) -> u64 {
        self.0.shard()
    }

    pub const fn local(self) -> u32 {
        self.0.local()
    }

    pub fn from_parts(shard: u64, local: u32) -> Result<Self> {
        PrimaryId::from_parts(shard, local).map(Self)
    }
}

impl From<PrimaryId> for TxId {
    fn from(id: PrimaryId) -> Self {
        Self(id)
    }
}

impl From<TxId> for PrimaryId {
    fn from(id: TxId) -> Self {
        id.0
    }
}

/// `PrimaryId` scoped to the trace family. Kept as a distinct type so
/// trace-domain signatures don't accept primary ids from other families
/// by mistake.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, RlpEncodable, RlpDecodable)]
pub struct TraceId(PrimaryId);

impl TraceId {
    pub const ZERO: Self = Self(PrimaryId::ZERO);

    pub const fn new(value: u64) -> Self {
        Self(PrimaryId::new(value))
    }

    pub const fn as_u64(self) -> u64 {
        self.0.as_u64()
    }

    pub fn checked_add(self, rhs: u64) -> Result<Self> {
        self.0.checked_add(rhs).map(Self)
    }

    pub const fn shard(self) -> u64 {
        self.0.shard()
    }

    pub const fn local(self) -> u32 {
        self.0.local()
    }

    pub fn from_parts(shard: u64, local: u32) -> Result<Self> {
        PrimaryId::from_parts(shard, local).map(Self)
    }
}

impl From<PrimaryId> for TraceId {
    fn from(id: PrimaryId) -> Self {
        Self(id)
    }
}

impl From<TraceId> for PrimaryId {
    fn from(id: TraceId) -> Self {
        id.0
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct PublicationState {
    pub indexed_finalized_head: u64,
}

impl PublicationState {
    pub fn encode(self) -> Vec<u8> {
        alloy_rlp::encode(self)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        alloy_rlp::decode_exact(bytes)
            .map_err(|_| MonadChainDataError::Decode("invalid publication state rlp"))
    }
}
