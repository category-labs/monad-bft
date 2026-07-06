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

use monad_query_errors::{LimitExceededKind, QueryError, Result};

use crate::order::QueryOrder;

/// Default for [`QueryEnvelope::limit`].
pub const DEFAULT_QUERY_LIMIT: usize = 100;

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

/// Per-deployment caps on query shape: `max_limit` bounds `request.limit`,
/// `max_block_range` bounds the resolved span (and thus the worst-case
/// non-indexed scan). Breaches surface as
/// [`monad_query_errors::QueryError::LimitExceeded`] (queryX `-32005`).
/// Neither caps per-block results: the spec requires completing the current
/// block, so counts can exceed `max_limit` for a hot block.
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

    pub fn check_limit(&self, limit: usize) -> Result<()> {
        if limit == 0 {
            return Err(QueryError::InvalidRequest("limit must be at least 1"));
        }
        if limit > self.max_limit {
            return Err(QueryError::LimitExceeded {
                kind: LimitExceededKind::Limit,
                max_limit: self.max_limit,
                max_block_range: self.max_block_range,
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use monad_query_errors::{LimitExceededKind, QueryError};

    use super::QueryLimits;

    #[test]
    fn check_limit_accepts_and_rejects_boundary_values() {
        let limits = QueryLimits::new(5, 1_000);

        assert!(matches!(
            limits.check_limit(0),
            Err(QueryError::InvalidRequest("limit must be at least 1",))
        ));
        assert!(limits.check_limit(1).is_ok());
        assert!(limits.check_limit(5).is_ok());
        assert!(matches!(
            limits.check_limit(6),
            Err(QueryError::LimitExceeded {
                kind: LimitExceededKind::Limit,
                max_limit: 5,
                max_block_range: 1_000,
            })
        ));
    }
}
