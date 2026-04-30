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

    pub const fn from_parts(shard: u64, local: u32) -> Self {
        Self((shard << Self::LOCAL_ID_BITS) | (local as u64))
    }

    pub fn idx_in_block(self, first: LogId) -> Result<usize> {
        let delta = self
            .0
            .checked_sub(first.as_u64())
            .ok_or(MonadChainDataError::Decode("log id below block start"))?;
        usize::try_from(delta).map_err(|_| MonadChainDataError::Decode("log block index overflow"))
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

    pub const fn from_parts(shard: u64, local: u32) -> Self {
        Self(PrimaryId::from_parts(shard, local))
    }

    pub fn idx_in_block(self, first: LogId) -> Result<usize> {
        self.0.idx_in_block(first)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct FamilyWindowRecord {
    pub first_log_id: LogId,
    pub count: u32,
}

impl FamilyWindowRecord {
    pub fn next_log_id(self) -> Result<LogId> {
        self.first_log_id.checked_add(u64::from(self.count))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct BlockRecord {
    pub block_number: u64,
    pub block_hash: Hash32,
    pub parent_hash: Hash32,
    pub logs: FamilyWindowRecord,
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
