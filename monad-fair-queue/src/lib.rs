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

pub mod metrics;
mod pool;
mod queue;

use std::fmt::{Debug, Display};

macro_rules! ensure {
    ($condition:expr, $error:expr $(,)?) => {
        if !$condition {
            return Err($error);
        }
    };
}
pub(crate) use ensure;
pub use queue::{FairQueue, FairQueueBuilder};

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct Score(f64);

impl Score {
    pub const ONE: Self = Self(1.0);

    pub fn reciprocal(self) -> f64 {
        debug_assert!(self.0.is_finite() && self.0 > 0.0);
        1.0 / self.0
    }
}

impl TryFrom<f64> for Score {
    type Error = ();

    fn try_from(value: f64) -> Result<Self, Self::Error> {
        if value.is_finite() && value > 0.0 {
            Ok(Self(value))
        } else {
            Err(())
        }
    }
}

impl TryFrom<u64> for Score {
    type Error = ();

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        let value = value as f64;
        if value.is_finite() && value > 0.0 {
            Ok(Self(value))
        } else {
            Err(())
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PeerStatus {
    Promoted(Score),
    Newcomer(Score),
    Unknown,
}

impl PeerStatus {
    pub fn score(&self) -> Score {
        match self {
            PeerStatus::Promoted(s) | PeerStatus::Newcomer(s) => *s,
            PeerStatus::Unknown => Score::ONE,
        }
    }

    pub fn is_promoted(&self) -> bool {
        matches!(self, PeerStatus::Promoted(_))
    }
}

pub trait IdentityScore {
    type Identity;

    fn score(&self, identity: &Self::Identity) -> PeerStatus;
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum PushError<Id: Debug + Display> {
    #[error("per-id limit exceeded for {id}: {limit}")]
    PerIdLimitExceeded { id: Id, limit: usize },
    #[error("queue full: {size}/{max_size}")]
    Full { size: usize, max_size: usize },
}
