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
    logs::QueryLogsRequest,
    primitives::refs::BlockRef,
    store::MetaStore,
};

/// Inclusive block range clipped to the published head.
///
/// `from_block.number <= to_block.number` always holds, regardless of query
/// order. Callers choose iteration direction based on `QueryOrder`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResolvedBlockWindow {
    pub from_block: BlockRef,
    pub to_block: BlockRef,
}

impl ResolvedBlockWindow {
    /// Resolves a query's inclusive block range against the published head.
    pub async fn resolve<M: MetaStore>(
        request: &QueryLogsRequest,
        published_head: u64,
        blocks: &BlockTables<M>,
    ) -> Result<Self> {
        let (from_number, to_number) = Self::resolve_query_block_numbers(request, published_head)?;

        let from_record =
            blocks
                .load_record(from_number)
                .await?
                .ok_or(MonadChainDataError::MissingData(
                    "missing from_block record",
                ))?;
        let to_record = blocks
            .load_record(to_number)
            .await?
            .ok_or(MonadChainDataError::MissingData("missing to_block record"))?;

        Ok(Self {
            from_block: BlockRef::from(&from_record),
            to_block: BlockRef::from(&to_record),
        })
    }

    /// Resolves and validates the block range. Always returns (low, high) with
    /// `low <= high`, independent of query order.
    fn resolve_query_block_numbers(
        request: &QueryLogsRequest,
        published_head: u64,
    ) -> Result<(u64, u64)> {
        let from = request.from_block.unwrap_or(1).max(1).min(published_head);
        let to = request
            .to_block
            .unwrap_or(published_head)
            .min(published_head);

        if from > to {
            return Err(MonadChainDataError::InvalidRequest(
                "from_block must be <= to_block",
            ));
        }
        Ok((from, to))
    }
}
