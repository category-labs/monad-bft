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

//! Environment-independent types shared by every instantiation of the
//! protocol modules, so `env` and `spec` can name them without picking
//! a variant. `types` re-exports them.

use alloy_rlp::{RlpDecodable, RlpDecodableWrapper, RlpEncodable, RlpEncodableWrapper};

// Slot number, starting from 0.
#[derive(
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Debug,
    RlpEncodableWrapper,
    RlpDecodableWrapper,
)]
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

    pub const fn from_u64(slot: u64) -> Option<Self> {
        if slot > Self::MAX.0 {
            return None;
        }
        Some(Self(slot))
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

pub type ProposalIndex = usize;

/// The slot and proposal index authenticated by a per-proposal vote.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, RlpEncodable, RlpDecodable)]
pub struct ProposalScope {
    pub slot: Slot,
    pub index: ProposalIndex,
}

impl ProposalScope {
    pub const fn new(slot: Slot, index: ProposalIndex) -> Self {
        Self { slot, index }
    }
}
