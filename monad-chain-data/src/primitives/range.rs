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

use crate::{
    error::{MonadChainDataError, Result},
    kernel::tables::BlockTables,
    primitives::{
        limits::{LimitExceededKind, QueryEnvelope, QueryLimits},
        page::QueryOrder,
        refs::BlockRef,
    },
    store::MetaStore,
};

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
            return Err(MonadChainDataError::LimitExceeded {
                kind: LimitExceededKind::BlockRange,
                max_limit: limits.max_limit,
                max_block_range: limits.max_block_range,
            });
        }

        let low_record =
            blocks
                .load_record(low_number)
                .await?
                .ok_or(MonadChainDataError::MissingData(
                    "missing block record at range low bound",
                ))?;
        let high_record =
            blocks
                .load_record(high_number)
                .await?
                .ok_or(MonadChainDataError::MissingData(
                    "missing block record at range high bound",
                ))?;

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
        let range = self.low.number..=self.high.number;
        let (fwd, rev) = match order {
            QueryOrder::Ascending => (Some(range), None),
            QueryOrder::Descending => (None, Some(range.rev())),
        };
        fwd.into_iter().flatten().chain(rev.into_iter().flatten())
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
        // User-space inversion is checked before defaults so an unspecified
        // bound (e.g. `from=None, to=N` with `N` above head) reports the
        // genuine cause (above-head) rather than a defaulted-inversion artifact.
        if let (Some(from), Some(to)) = (from_block, to_block) {
            let inverted = match order {
                QueryOrder::Ascending => from > to,
                QueryOrder::Descending => from < to,
            };
            if inverted {
                return Err(MonadChainDataError::InvalidRequest(
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

        // The lower bound must be an ingested block — silently collapsing
        // `low > head` to `[head, head]` would mask caller bugs and return
        // empty pages. The upper bound is treated as "up to head" and clipped.
        if low > published_head {
            return Err(MonadChainDataError::InvalidRequest(
                "block range starts above the published head",
            ));
        }
        let high = high.min(published_head);
        // Floors an explicit `from_block = 0` to avoid loading a nonexistent record.
        let low = low.max(EARLIEST_QUERYABLE_BLOCK);

        if low > high {
            return Err(MonadChainDataError::InvalidRequest(
                "from_block and to_block do not form a valid range for the requested order",
            ));
        }
        Ok((low, high))
    }
}
