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

use std::fmt;

use serde::{Deserialize, Serialize};

// Gradual rollout from v0 (regular) to v1 (deterministic) raptorcast.
//
// The rollout proceeds through four stages, bumped on release:
//
//                    AcceptBoth        AcceptBoth
//   AlwaysV0         PublishV0         PublishV1          AlwaysV1
//  -----------------+------------------+------------------+----------

pub const CURRENT_STAGE: DeterministicProtocolRolloutStage =
    DeterministicProtocolRolloutStage::AlwaysV1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DeterministicProtocolRolloutStage {
    AlwaysV0,
    AcceptBothPublishV0,
    AcceptBothPublishV1,
    AlwaysV1,
}

impl Default for DeterministicProtocolRolloutStage {
    fn default() -> Self {
        CURRENT_STAGE
    }
}

impl fmt::Display for DeterministicProtocolRolloutStage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AlwaysV0 => write!(f, "always_v0"),
            Self::AcceptBothPublishV0 => write!(f, "accept_both_publish_v0"),
            Self::AcceptBothPublishV1 => write!(f, "accept_both_publish_v1"),
            Self::AlwaysV1 => write!(f, "always_v1"),
        }
    }
}
