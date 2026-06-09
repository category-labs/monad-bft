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

use super::page::{QueryOrder, DEFAULT_QUERY_LIMIT};

/// Common request envelope shared by query families. `from_block`/`to_block`
/// are interpreted in queryX spec semantics, with lower/upper roles depending
/// on `order`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueryEnvelope {
    pub from_block: Option<u64>,
    pub to_block: Option<u64>,
    pub order: QueryOrder,
    /// Target result count. The server completes the current block before
    /// stopping, so the actual count may exceed this. Defaults to
    /// [`DEFAULT_QUERY_LIMIT`].
    pub limit: usize,
}

impl Default for QueryEnvelope {
    fn default() -> Self {
        Self {
            from_block: None,
            to_block: None,
            order: QueryOrder::default(),
            limit: DEFAULT_QUERY_LIMIT,
        }
    }
}

/// Per-deployment caps on accepted query shape. `max_limit` bounds the
/// `request.limit` value the user may pass; `max_block_range` bounds the
/// resolved range span. Breaches surface as
/// [`crate::MonadChainDataError::LimitExceeded`], which the RPC layer maps
/// to the queryX spec's `-32005 Limit exceeded` response. Neither cap
/// constrains how many results a single block may return: the spec
/// requires the server to complete the current block before stopping, so
/// returned counts can exceed `max_limit` for a hot block.
///
/// `max_block_range` doubles as the worst-case scan budget for filters
/// without an indexed clause: that path loads every block in the
/// resolved window, so the bound on the window directly bounds the
/// scan. A separate execution-time budget (e.g. wall-time for hot
/// shards) is left for a future `ExecutionBudget` type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueryLimits {
    pub max_limit: usize,
    pub max_block_range: u64,
}

impl QueryLimits {
    /// Permissive limits for tests and trusted internal callers.
    pub const UNLIMITED: Self = Self {
        max_limit: usize::MAX,
        max_block_range: u64::MAX,
    };

    pub const fn new(max_limit: usize, max_block_range: u64) -> Self {
        Self {
            max_limit,
            max_block_range,
        }
    }

    pub fn check_limit(&self, limit: usize) -> crate::error::Result<()> {
        if limit == 0 {
            return Err(crate::error::MonadChainDataError::InvalidRequest(
                "limit must be at least 1",
            ));
        }
        if limit > self.max_limit {
            return Err(crate::error::MonadChainDataError::LimitExceeded {
                kind: LimitExceededKind::Limit,
                max_limit: self.max_limit,
                max_block_range: self.max_block_range,
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LimitExceededKind {
    Limit,
    BlockRange,
}

impl std::fmt::Display for LimitExceededKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Limit => f.write_str("limit"),
            Self::BlockRange => f.write_str("block range"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::QueryLimits;
    use crate::{LimitExceededKind, MonadChainDataError};

    #[test]
    fn check_limit_accepts_and_rejects_boundary_values() {
        let limits = QueryLimits::new(5, 1_000);

        assert!(matches!(
            limits.check_limit(0),
            Err(MonadChainDataError::InvalidRequest(
                "limit must be at least 1",
            ))
        ));
        assert!(limits.check_limit(1).is_ok());
        assert!(limits.check_limit(5).is_ok());
        assert!(matches!(
            limits.check_limit(6),
            Err(MonadChainDataError::LimitExceeded {
                kind: LimitExceededKind::Limit,
                max_limit: 5,
                max_block_range: 1_000,
            })
        ));
    }
}
