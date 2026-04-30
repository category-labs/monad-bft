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

use super::bitmap::max_local_id;
use crate::{
    engine::tables::Tables,
    error::Result,
    primitives::{
        page::QueryOrder,
        range::ResolvedBlockWindow,
        state::{FamilyWindowRecord, LogId},
    },
    store::{BlobStore, MetaStore},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ResolvedLogWindow {
    pub start: LogId,
    pub end_inclusive: LogId,
}

impl ResolvedLogWindow {
    /// Returns the local-ID range within the given shard, clipped to this
    /// window's boundaries.
    pub fn local_range_for_shard(&self, shard: u64) -> (u32, u32) {
        let local_from = if shard == self.start.shard() {
            self.start.local()
        } else {
            0
        };
        let local_to = if shard == self.end_inclusive.shard() {
            self.end_inclusive.local()
        } else {
            max_local_id()
        };
        (local_from, local_to)
    }

    /// Returns the shards covered by this window in the given query order.
    pub fn shard_iter(&self, order: QueryOrder) -> Vec<u64> {
        let mut shards: Vec<u64> = (self.start.shard()..=self.end_inclusive.shard()).collect();
        if order == QueryOrder::Descending {
            shards.reverse();
        }
        shards
    }
}

pub(crate) async fn resolve_log_window<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    block_window: &ResolvedBlockWindow,
) -> Result<Option<ResolvedLogWindow>> {
    let start_block = block_window.low.number;
    let end_block = block_window.high.number;

    let Some(start) = first_log_id_in_range(tables, start_block, end_block).await? else {
        return Ok(None);
    };
    let Some(end_exclusive) = end_log_id_exclusive_in_range(tables, start_block, end_block).await?
    else {
        return Ok(None);
    };

    Ok(Some(ResolvedLogWindow {
        start,
        end_inclusive: LogId::new(end_exclusive.as_u64().saturating_sub(1)),
    }))
}

async fn first_log_id_in_range<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    start_block: u64,
    end_block: u64,
) -> Result<Option<LogId>> {
    let mut block_number = start_block;
    while block_number <= end_block {
        let Some(record) = tables.blocks().load_record(block_number).await? else {
            return Ok(None);
        };
        if let Some(first_log_id) = non_empty_window_start(record.logs) {
            return Ok(Some(first_log_id));
        }
        block_number = block_number.saturating_add(1);
    }

    Ok(None)
}

async fn end_log_id_exclusive_in_range<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    start_block: u64,
    end_block: u64,
) -> Result<Option<LogId>> {
    let mut block_number = end_block;
    loop {
        let Some(record) = tables.blocks().load_record(block_number).await? else {
            return Ok(None);
        };
        if let Some(end_log_id_exclusive) = non_empty_window_end(record.logs)? {
            return Ok(Some(end_log_id_exclusive));
        }
        if block_number == start_block {
            break;
        }
        block_number = block_number.saturating_sub(1);
    }

    Ok(None)
}

fn non_empty_window_start(window: FamilyWindowRecord) -> Option<LogId> {
    (window.count > 0).then_some(window.first_log_id)
}

fn non_empty_window_end(window: FamilyWindowRecord) -> Result<Option<LogId>> {
    if window.count == 0 {
        return Ok(None);
    }

    window.next_log_id().map(Some)
}
