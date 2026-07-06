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
use monad_query_primitives::{
    limits::{QueryEnvelope, QueryLimits},
    order::QueryOrder,
    refs::BlockRef,
};
use monad_query_store::MetaStore;

use crate::tables::BlockTables;

/// Floor for the lower numeric bound. The current ingest path requires the
/// chain to start at block 1, so block 0 has no record to load.
const EARLIEST_QUERYABLE_BLOCK: u64 = 1;

/// Inclusive block range in `(low, high)` form with `low.number <= high.number`.
/// Iteration direction is the caller's choice via `QueryOrder`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResolvedBlockWindow {
    pub low: BlockRef,
    pub high: BlockRef,
}

impl ResolvedBlockWindow {
    /// Resolves a query's inclusive block range against the published head,
    /// short-circuits if the resolved span exceeds `limits.max_block_range`,
    /// then loads the per-bound block records.
    pub async fn resolve<M: MetaStore>(
        envelope: &QueryEnvelope,
        published_head: u64,
        limits: &QueryLimits,
        blocks: &BlockTables<M>,
    ) -> Result<Self> {
        let (low_number, high_number) = Self::resolve_block_numbers(
            envelope.from_block,
            envelope.to_block,
            envelope.order,
            published_head,
        )?;

        let span = high_number - low_number + 1;
        if span > limits.max_block_range {
            return Err(QueryError::LimitExceeded {
                kind: LimitExceededKind::BlockRange,
                max_limit: limits.max_limit,
                max_block_range: limits.max_block_range,
            });
        }

        let load = |number, msg| async move {
            blocks
                .load_record(number)
                .await?
                .ok_or(QueryError::MissingData(msg))
        };
        let low_record = load(low_number, "missing block record at range low bound").await?;
        let high_record = load(high_number, "missing block record at range high bound").await?;

        Ok(Self {
            low: BlockRef::from(&low_record),
            high: BlockRef::from(&high_record),
        })
    }

    /// Maps internal `(low, high)` back to spec `(from, to)` for the given order.
    pub fn request_endpoints(&self, order: QueryOrder) -> (BlockRef, BlockRef) {
        match order {
            QueryOrder::Ascending => (self.low, self.high),
            QueryOrder::Descending => (self.high, self.low),
        }
    }

    /// Iterates `low..=high` in the requested traversal direction.
    pub fn iter(&self, order: QueryOrder) -> impl Iterator<Item = u64> {
        order.iterate(self.low.number..=self.high.number)
    }

    /// Maps the spec's order-dependent `(from_block, to_block)` to internal
    /// `(low, high)` with `low <= high`. Errors if the inputs do not form a
    /// valid range for `order`, or if the lower bound exceeds the published head.
    fn resolve_block_numbers(
        from_block: Option<u64>,
        to_block: Option<u64>,
        order: QueryOrder,
        published_head: u64,
    ) -> Result<(u64, u64)> {
        // Check user-supplied inversion before defaulting so the error reports
        // the genuine cause rather than a defaulted-inversion artifact.
        if let (Some(from), Some(to)) = (from_block, to_block) {
            let inverted = match order {
                QueryOrder::Ascending => from > to,
                QueryOrder::Descending => from < to,
            };
            if inverted {
                return Err(QueryError::InvalidRequest(
                    "from_block and to_block do not form a valid range for the requested order",
                ));
            }
        }

        let (low, high) = match order {
            QueryOrder::Ascending => (
                from_block.unwrap_or(EARLIEST_QUERYABLE_BLOCK),
                to_block.unwrap_or(published_head),
            ),
            QueryOrder::Descending => (
                to_block.unwrap_or(EARLIEST_QUERYABLE_BLOCK),
                from_block.unwrap_or(published_head),
            ),
        };

        // A lower bound above head is a caller bug and errors; the upper bound
        // means "up to head" and is clipped.
        if low > published_head {
            return Err(QueryError::InvalidRequest(
                "block range starts above the published head",
            ));
        }
        let high = high.min(published_head);
        // Block 0 has no record (ingest starts at 1), so `[0, N]` silently
        // clamps to `[1, N]` like eth_getLogs' genesis clamp — 0 means "from
        // the start". `[0, 0]` alone still fails the low > high check below.
        let low = low.max(EARLIEST_QUERYABLE_BLOCK);

        if low > high {
            return Err(QueryError::InvalidRequest(
                "from_block and to_block do not form a valid range for the requested order",
            ));
        }
        Ok((low, high))
    }
}

#[cfg(test)]
mod tests {
    use monad_query_errors::QueryError;
    use monad_query_primitives::order::QueryOrder;

    use super::{ResolvedBlockWindow, EARLIEST_QUERYABLE_BLOCK};

    const HEAD: u64 = 100;

    fn resolve(
        from: Option<u64>,
        to: Option<u64>,
        order: QueryOrder,
    ) -> Result<(u64, u64), QueryError> {
        ResolvedBlockWindow::resolve_block_numbers(from, to, order, HEAD)
    }

    #[test]
    fn ascending_defaults_span_earliest_to_head() {
        assert_eq!(
            resolve(None, None, QueryOrder::Ascending).unwrap(),
            (EARLIEST_QUERYABLE_BLOCK, HEAD)
        );
    }

    #[test]
    fn descending_defaults_span_earliest_to_head() {
        assert_eq!(
            resolve(None, None, QueryOrder::Descending).unwrap(),
            (EARLIEST_QUERYABLE_BLOCK, HEAD)
        );
    }

    #[test]
    fn explicit_block_zero_lower_bound_clamps_to_earliest() {
        assert_eq!(
            resolve(Some(0), Some(5), QueryOrder::Ascending).unwrap(),
            (1, 5)
        );
    }

    #[test]
    fn block_zero_alone_is_rejected_not_clamped() {
        // The clamp must not silently serve block 1's data for a block-0 request.
        assert!(matches!(
            resolve(Some(0), Some(0), QueryOrder::Ascending),
            Err(QueryError::InvalidRequest(_))
        ));
    }

    #[test]
    fn high_bound_is_clamped_to_published_head() {
        assert_eq!(
            resolve(Some(10), Some(HEAD + 50), QueryOrder::Ascending).unwrap(),
            (10, HEAD)
        );
    }

    #[test]
    fn lower_bound_above_head_is_rejected() {
        assert!(matches!(
            resolve(Some(HEAD + 1), Some(HEAD + 5), QueryOrder::Ascending),
            Err(QueryError::InvalidRequest(_))
        ));
    }

    #[test]
    fn ascending_inverted_range_is_rejected() {
        assert!(matches!(
            resolve(Some(5), Some(3), QueryOrder::Ascending),
            Err(QueryError::InvalidRequest(_))
        ));
    }

    #[test]
    fn descending_valid_range_resolves_low_to_high() {
        assert_eq!(
            resolve(Some(5), Some(2), QueryOrder::Descending).unwrap(),
            (2, 5)
        );
    }

    #[test]
    fn descending_inverted_range_is_rejected() {
        assert!(matches!(
            resolve(Some(2), Some(5), QueryOrder::Descending),
            Err(QueryError::InvalidRequest(_))
        ));
    }

    #[test]
    fn iter_walks_inclusive_range_in_requested_direction() {
        use monad_query_primitives::refs::BlockRef;
        let block_ref = |number| BlockRef {
            number,
            hash: Default::default(),
            parent_hash: Default::default(),
        };
        let window = ResolvedBlockWindow {
            low: block_ref(2),
            high: block_ref(4),
        };
        assert_eq!(
            window.iter(QueryOrder::Ascending).collect::<Vec<_>>(),
            vec![2, 3, 4]
        );
        assert_eq!(
            window.iter(QueryOrder::Descending).collect::<Vec<_>>(),
            vec![4, 3, 2]
        );
    }
}
