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

use monad_mcp_chorus::{
    spec::{Stake as _, validator::ValidatorData as _},
    stub::types::ValidatorData,
};

/// Message-count thresholds over `n` equally staked validators (D8):
/// `weak = f+1` and `strong = ⌈(n+f+1)/2⌉`, which is `2f+1` for `n = 3f+1`.
/// Any two strong sets intersect in at least `weak` validators.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Quorum {
    pub n: usize,
    pub f: usize,
    pub weak: usize,
    pub strong: usize,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum QuorumError {
    Empty,
    UnequalStake,
}

impl std::fmt::Display for QuorumError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Empty => write!(f, "empty validator set"),
            Self::UnequalStake => write!(f, "quantum chorus requires equal stake"),
        }
    }
}

impl std::error::Error for QuorumError {}

impl Quorum {
    /// Panics on `n = 0`.
    pub const fn new(n: usize) -> Self {
        assert!(n > 0, "empty validator set");
        let f = (n - 1) / 3;
        Self {
            n,
            f,
            weak: f + 1,
            strong: (n + f + 1).div_ceil(2),
        }
    }

    pub fn from_validators(validators: &ValidatorData) -> Result<Self, QuorumError> {
        let mut stakes = validators
            .nodes()
            .map(|id| validators.get_stake(id).amount());
        let first = stakes.next().ok_or(QuorumError::Empty)?;
        if stakes.any(|stake| stake != first) {
            return Err(QuorumError::UnequalStake);
        }
        Ok(Self::new(validators.len()))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use monad_mcp_chorus::{
        spec::vote::KeyPair as _,
        stub::types::{NodeId, Stake},
    };

    use super::*;

    fn validators(stakes: &[u64]) -> ValidatorData {
        let ids = (0..stakes.len() as u64).map(NodeId::dummy);
        let valset: HashMap<_, _> = ids
            .clone()
            .zip(stakes.iter().map(|s| Stake::from(*s)))
            .collect();
        let mapping = ids.map(|id| (id, id.keypair().pubkey())).collect();
        ValidatorData::new(valset, mapping)
    }

    #[test]
    fn quorum_table() {
        let table = [(1, 0, 1, 1), (4, 1, 2, 3), (7, 2, 3, 5), (8, 2, 3, 6)];
        for (n, f, weak, strong) in table {
            assert_eq!(Quorum::new(n), Quorum { n, f, weak, strong }, "n = {n}");
        }
    }

    #[test]
    fn strong_sets_intersect_in_weak() {
        for n in 1..=64 {
            let q = Quorum::new(n);
            assert!(2 * q.strong >= n + q.weak, "n = {n}");
            assert!(q.strong <= n - q.f, "n = {n}: liveness with f silent");
        }
    }

    #[test]
    fn equal_stake_enforced() {
        assert_eq!(
            Quorum::from_validators(&validators(&[2, 2, 2, 2])),
            Ok(Quorum::new(4))
        );
        assert_eq!(
            Quorum::from_validators(&validators(&[1, 1, 2, 1])),
            Err(QuorumError::UnequalStake)
        );
    }
}
