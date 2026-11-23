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

pub mod fixed_lead;

use monad_mcp_common::{Lane, Slot, Timestamp, ValidatorIndex};

pub use self::fixed_lead::{FixedLeadConfig, FixedLeadPlanner};
use super::env::cadence::slot::chorus::Entry;

/// Fact: the first vote of `voter` for `lane` of `slot` arrived at `at`, with
/// its `entry`. Its signature is not checked.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct VoteReceived {
    pub slot: Slot,
    pub voter: ValidatorIndex,
    pub lane: Lane,
    pub entry: Entry,
    pub at: Timestamp,
}
